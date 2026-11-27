package orchestration

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// OwnerObservation is the body-free record retained with an attempt. A zero
// Due means the owner supplied no lower bound for the next observation.
type OwnerObservation struct {
	At     time.Time
	Due    time.Time
	Reason string
	Code   string
	Status int
}

// DiscoveryObservationSelector uses the same key for status and candidates.
func DiscoveryObservationSelector(key []byte, requestID string) string {
	selector, _ := DiscoveryObservationSelectors(key, requestID)
	return selector
}

// DiscoveryObservationSelectors returns the canonical write selector and the
// historical lookup selector for this spelling, when different. During the
// owner's wait window, a plan without the original spelling can miss a legacy
// clock and a rollback can miss a canonical clock on an uppercase public read.
func DiscoveryObservationSelectors(key []byte, requestID string) (selector, legacySelector string) {
	if requestID == "" {
		return "", ""
	}
	selector, legacySelector, _ = discoveryObservationReference(key, requestID)
	return selector, legacySelector
}

func discoveryObservationReference(key []byte, requestID string) (selector, legacySelector, canonical string) {
	const capability = "ouf.semantic.discovery.status"
	if id, err := uuid.Parse(requestID); err == nil {
		canonical = id.String()
		if canonical != requestID {
			legacySelector = observationHMAC(key, capability, map[string]any{"requestId": requestID})
			requestID = canonical
		}
	}
	return observationHMAC(key, capability, map[string]any{"requestId": requestID}), legacySelector, canonical
}

// The lookup key excludes the MCP analysis envelope. Status and candidates
// observe the same discovery request. An adoption is reconciled by commandKey.
func observationSelector(key []byte, capability, class string, args []byte) string {
	selector, _, _ := observationReference(key, capability, class, args)
	return selector
}

func observationReference(key []byte, capability, class string, args []byte) (selector, legacySelector, request string) {
	var fields map[string]any
	decoder := json.NewDecoder(bytes.NewReader(args))
	decoder.UseNumber()
	if decoder.Decode(&fields) != nil || fields == nil {
		return "", "", ""
	}
	delete(fields, "analysisId")
	switch capability {
	case "ouf.semantic.discovery.status", "ouf.semantic.discovery.candidates":
		if raw, ok := fields["requestId"].(string); ok {
			return discoveryObservationReference(key, raw)
		}
		capability = "ouf.semantic.discovery.status"
		fields = map[string]any{"requestId": fields["requestId"]}
	case "ouf.semantic.discovery.adopt", "ouf.semantic.proposal.read":
		if command, ok := fields["commandKey"].(string); ok && command != "" {
			capability = "ouf.semantic.proposal.read"
			fields = map[string]any{"commandKey": command}
		}
	default:
		if class != "READ" && class != "SEARCH" {
			return "", "", ""
		}
	}
	return observationHMAC(key, capability, fields), "", ""
}

func observationHMAC(key []byte, capability string, fields map[string]any) string {
	raw, _ := json.Marshal([]any{"owner-observation-v1", capability, fields})
	mac := hmac.New(sha256.New, key)
	mac.Write(raw)
	return hex.EncodeToString(mac.Sum(nil))
}

type observationReader interface {
	ReadOwnerObservation(context.Context, Identity, string) (OwnerObservation, error)
}

type discoveryObservationReader interface {
	ReadDiscoveryObservation(context.Context, Identity, string, string, string) (OwnerObservation, error)
}

func (s Service) SavedObservation(ctx context.Context, id Identity, capability, class string, args []byte) (OwnerObservation, error) {
	selector, legacySelector, request := observationReference(s.FingerprintKey, capability, class, args)
	if reader, ok := s.Admission.(discoveryObservationReader); ok && request != "" {
		return reader.ReadDiscoveryObservation(ctx, id, selector, legacySelector, request)
	}
	reader, ok := s.Admission.(observationReader)
	if !ok || selector == "" {
		return OwnerObservation{}, nil
	}
	return reader.ReadOwnerObservation(ctx, id, selector)
}

var observationCode = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,79}$`)

func ObserveOwner(response GatewayResponse, callErr error, now time.Time) OwnerObservation {
	o := OwnerObservation{At: now, Status: response.Status, Code: "SUCCEEDED", Reason: "OWNER_OBSERVATION"}
	header := response.RetryAfter
	if p := response.Problem; p != nil {
		o.Code = "OWNER_REQUEST_FAILED"
		if observationCode.MatchString(p.Code) {
			o.Code = p.Code
		}
		if header == "" {
			header = p.RetryAfter
		}
		if o.Status == 0 {
			o.Status = p.Status
		}
	}
	if o.Status >= 400 && o.Code == "SUCCEEDED" {
		o.Code = "OWNER_REQUEST_FAILED"
	}
	var floor time.Time
	var timeout net.Error
	switch {
	case errors.As(callErr, &timeout) && timeout.Timeout():
		o.Code, o.Reason = "OWNER_NETWORK_INTERRUPTED", "OWNER_TIMEOUT"
		floor = now.Add(time.Duration(PollIntervalSeconds) * time.Second)
	case o.Status == 429 || o.Status == 503 || o.Status == 408:
		o.Reason = "TRANSIENT_OWNER_WAIT"
		floor = now.Add(time.Duration(PollIntervalSeconds) * time.Second)
	case callErr != nil:
		o.Code = "OWNER_OUTCOME_UNCERTAIN"
	}
	var body map[string]any
	decoder := json.NewDecoder(bytes.NewReader(response.Body))
	decoder.UseNumber()
	_ = decoder.Decode(&body)
	due, valid := OwnerDue(body, now, floor, header)
	if !valid {
		o.Reason = "RETRY_WINDOW_UNAVAILABLE"
		return o
	}
	// Round up once at observation time to PostgreSQL timestamp precision.
	if !due.IsZero() {
		o.Due = due.UTC().Truncate(time.Microsecond)
		if o.Due.Before(due) {
			o.Due = o.Due.Add(time.Microsecond)
		}
	}
	return o
}

// OwnerDue returns the latest lower bound for the next observation. A stored
// result keeps the clock of its receipt even when read again after a restart.
// floor is a local backoff or a previously persisted lower bound.
func OwnerDue(body map[string]any, now, floor time.Time, retryAfter string) (time.Time, bool) {
	due := floor
	later := func(at time.Time) {
		if at.After(due) {
			due = at
		}
	}
	relative := func(value any, origin time.Time, header bool) bool {
		raw := fmt.Sprint(value)
		seconds, err := strconv.ParseInt(raw, 10, 64)
		if err == nil && seconds >= 0 && seconds <= int64(time.Duration(1<<63-1)/time.Second) {
			later(origin.Add(time.Duration(seconds) * time.Second))
			return true
		}
		if header {
			if at, err := http.ParseTime(raw); err == nil {
				later(at)
				return true
			}
		}
		return false
	}
	if retryAfter != "" && !relative(retryAfter, now, true) {
		return time.Time{}, false
	}
	receipt, _ := body["result"].(map[string]any)
	for i, obs := range []map[string]any{body, receipt} {
		origin := now
		if i == 1 {
			if at, err := time.Parse(time.RFC3339Nano, fmt.Sprint(body["completedAt"])); err == nil && !at.IsZero() {
				origin = at
			}
		}
		explicit := false
		for _, key := range []string{"nextAttemptAt", "notBefore", "next_retry_at", "retryAt", "transportNotBefore"} {
			if v := obs[key]; v != nil {
				at, err := time.Parse(time.RFC3339Nano, fmt.Sprint(v))
				if err != nil {
					return time.Time{}, false
				}
				if !at.IsZero() {
					explicit = true
					later(at)
				}
			}
		}
		for _, key := range []string{"pollAfterSeconds", "retryAfterSeconds", "Retry-After", "RetryAfter"} {
			if v := obs[key]; v != nil && v != "" {
				if !relative(v, origin, key == "RetryAfter" || key == "Retry-After") {
					return time.Time{}, false
				}
				explicit = true
			}
		}
		if obs["recoveryState"] == "RETRY_WAIT" && !explicit {
			later(origin.Add(time.Duration(PollIntervalSeconds) * time.Second))
		}
	}
	return due, true
}
