package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const ReservationContract = "ouf.analysis.reservation-check.v1"

type RunWork struct {
	Worker, Operation, Intent string
	Arguments                 json.RawMessage
	Ordinals                  []int
	Maximum                   RunCost
}

type ReservationCheck struct {
	Contract      string `json:"contract"`
	Nonce         string `json:"nonce"`
	Workload      string `json:"workload"`
	MandateID     string `json:"mandateId"`
	AnalysisID    string `json:"analysisId"`
	Generation    string `json:"generation"`
	IntentKey     string `json:"intentKey"`
	AttemptID     string `json:"attemptId"`
	Fence         string `json:"fence"`
	Operation     string `json:"operation"`
	ArgumentsHash string `json:"argumentsHash"`
	RequestID     string `json:"requestId,omitempty"`
}

type ReservationProof struct {
	RequestHash      string  `json:"requestHash"`
	Current          bool    `json:"current"`
	Issuer           string  `json:"issuer"`
	Tenant           string  `json:"tenant"`
	Subject          string  `json:"subject"`
	ReservationRef   string  `json:"reservationRef"`
	OrdinalsRef      string  `json:"ordinalsRef"`
	ChangeSeq        string  `json:"changeSeq"`
	ManifestChecksum string  `json:"manifestChecksum"`
	ProfileHash      string  `json:"profileHash"`
	ValidUntil       int64   `json:"validUntil"`
	RequestID        *string `json:"requestId"`
}

func canonicalSequence(value string) bool {
	n, err := strconv.ParseInt(value, 10, 64)
	return err == nil && n >= 0 && strconv.FormatInt(n, 10) == value
}

type ReservationVerifier interface {
	CheckReservation(context.Context, ReservationCheck) (ReservationProof, error)
}

// Serve only on a listener requiring verified client certificates. The
// accepted URI SAN identifies the Gateway within the installation CA.
func ReservationHandler(store ReservationVerifier, gatewayURI string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if !gatewayPeer(r, gatewayURI) {
			w.WriteHeader(403)
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			w.WriteHeader(405)
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 8192))
		var check ReservationCheck
		if err != nil || !ValidEvidenceJSON(raw) || json.Unmarshal(raw, &check) != nil {
			w.WriteHeader(400)
			return
		}
		keys := []string{"contract", "nonce", "workload", "mandateId", "analysisId", "generation", "intentKey", "attemptId", "fence", "operation", "argumentsHash"}
		if check.Operation == "semantic.discovery.status" || check.Operation == "semantic.discovery.candidates" {
			keys = append(keys, "requestId")
			if !CanonicalUUID(check.RequestID) {
				w.WriteHeader(400)
				return
			}
		}
		if _, ok := closedFields(raw, keys...); !ok || check.Contract != ReservationContract || check.Nonce == "" || check.Workload == "" ||
			!canonicalSequence(check.Generation) || !canonicalSequence(check.Fence) ||
			check.AnalysisID != strings.ToLower(check.AnalysisID) || check.IntentKey != strings.ToLower(check.IntentKey) || check.ArgumentsHash != strings.ToLower(check.ArgumentsHash) ||
			!CanonicalUUID(check.MandateID) || !CanonicalUUID(check.AttemptID) || ValidateID(check.AnalysisID) != nil ||
			ValidateID(check.IntentKey) != nil || ValidateID(check.ArgumentsHash) != nil || !AllowedOperation(check.Operation) {
			w.WriteHeader(400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		proof, err := store.CheckReservation(ctx, check)
		if err != nil {
			status := http.StatusServiceUnavailable
			switch {
			case errors.Is(err, ErrScope):
				status = http.StatusNotFound
			case errors.Is(err, ErrFenced):
				status = http.StatusConflict
			case errors.Is(err, ErrRunPressure):
				status = http.StatusTooManyRequests
			}
			if status == http.StatusTooManyRequests {
				w.Header().Set("Retry-After", "5")
			}
			w.WriteHeader(status)
			return
		}
		proof.RequestHash = BytesDigest(raw)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(proof)
	})
}

func gatewayPeer(r *http.Request, gatewayURI string) bool {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return false
	}
	for _, uri := range r.TLS.VerifiedChains[0][0].URIs {
		if gatewayURI != "" && uri.String() == gatewayURI {
			return true
		}
	}
	return false
}
