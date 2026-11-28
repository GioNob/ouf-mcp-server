package analysis

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

const BatchMandateContract = "ouf.analysis.batch.mandate.v1"
const BatchStartHeader = "X-OUF-Analysis-Batch-Start"
const BatchDiscoveryBindingHeader = "X-OUF-Analysis-Discovery-Binding"

type BatchMandateView struct {
	Ref            string `json:"mandateRef"`
	Generation     int    `json:"generation"`
	Version        int    `json:"version"`
	State          string `json:"state"`
	AnalysisID     string `json:"analysisId"`
	ExpiresAtMs    int64  `json:"expiresAtMs"`
	StartBinding   string `json:"startBinding"`
	MandateBinding string `json:"mandateBinding"`
}

// BatchMandate holds the Gateway bindings and MCP's checkpoint for their use.
// Start proofs and interactive credentials are never part of this record.
type BatchMandate struct {
	StartRef, StartBinding, Ref, Binding, CommandKey string
	State, Reason, ControlFence                      string
	Generation, Version                              int
}

type BatchMandateKey struct {
	Ref        string `json:"mandateRef"`
	Generation int    `json:"generation"`
	Binding    string `json:"mandateBinding"`
}

func (m BatchMandate) Key() BatchMandateKey {
	return BatchMandateKey{m.Ref, m.Generation, m.Binding}
}

type BatchMandateIssue struct {
	Ref          string `json:"mandateRef"`
	Generation   int    `json:"generation"`
	CommandKey   string `json:"commandKey"`
	AnalysisID   string `json:"analysisId"`
	PolicyEpoch  string `json:"policyEpoch"`
	StartBinding string `json:"startBinding"`
	StartProof   string `json:"startProof,omitempty"`
}

type BatchMandateDispatch struct {
	BatchMandateKey
	AnalysisID    string          `json:"analysisId"`
	Operation     string          `json:"operation"`
	Arguments     json.RawMessage `json:"arguments"`
	AttemptID     string          `json:"attemptId"`
	CorrelationID string          `json:"correlationId"`
}

type BatchStartBinding struct {
	Installation, Issuer, Audience, Workload, Tenant, Subject, Client string
	StartRef, Mode, AssetID, ProfileID, SchemaHash, CommandKey        string
	Scope                                                             []string
}

// The signature is verified by the Gateway. MCP compares the authenticated
// prepare response and the stored payload to the work it has verified locally.
func DecodeBatchStartBinding(binding string) (BatchStartBinding, bool) {
	var start BatchStartBinding
	ok := decodeBatchBinding(binding, &start) &&
		start.Installation != "" && start.Workload != "" && start.Issuer != "" && len(start.Scope) != 0
	return start, ok
}

func decodeBatchBinding(binding string, target any) bool {
	payload, signature, found := strings.Cut(binding, ".")
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	mac, macErr := base64.RawURLEncoding.DecodeString(signature)
	return found && err == nil && macErr == nil && len(mac) == 32 && json.Unmarshal(raw, target) == nil
}

func (m BatchMandate) BoundView() BatchMandateView {
	var payload struct {
		BatchMandateView
		StartRef string
	}
	if !decodeBatchBinding(m.Binding, &payload) || payload.StartRef != m.StartRef ||
		payload.Ref != m.Ref || payload.Generation != m.Generation || payload.Version < 1 || payload.State != "ACTIVE" {
		return BatchMandateView{}
	}
	return payload.BatchMandateView
}

func (b BatchStartBinding) matches(i orchestration.Identity, ref, asset, profile, hash, key string) bool {
	scopes := slices.Clone(i.Scopes)
	slices.Sort(scopes)
	return b.StartRef == ref && b.Mode == BatchMode && b.AssetID == asset && b.ProfileID == profile &&
		b.SchemaHash == hash && b.CommandKey == key && b.Subject == i.PrincipalID && b.Tenant == i.TenantID &&
		b.Client == i.ServicePrincipalID && b.Issuer == i.Issuer && slices.Contains(strings.Fields(i.Audience), b.Audience) &&
		slices.Equal(b.Scope, slices.Compact(scopes))
}

type BatchMandateGateway interface {
	Prepare(context.Context, string) (BatchMandateView, error)
	Issue(context.Context, BatchMandateIssue) (BatchMandateView, error)
	Read(context.Context, BatchMandateKey) (BatchMandateView, error)
	Expire(context.Context, BatchMandateKey, int) (BatchMandateView, error)
	Dispatch(context.Context, BatchMandateDispatch, int64) (orchestration.GatewayResponse, string, error)
}

type BatchMandateError struct {
	Observation orchestration.OwnerObservation
}

func (e *BatchMandateError) Error() string { return e.Observation.Code }
func (e *BatchMandateError) Unwrap() error {
	if e.Observation.Status == 401 || e.Observation.Status == 403 {
		return orchestration.ErrUnauthorized
	}
	return ErrUnavailable
}

type batchMandateContextKey struct{}

func WithBatchMandateGateway(ctx context.Context, gateway BatchMandateGateway) context.Context {
	return context.WithValue(ctx, batchMandateContextKey{}, gateway)
}

type BatchMandateStartStore interface {
	SaveBatchMandateStart(context.Context, Scope, string, string, string, BatchStart, BatchMandate) (BatchStart, error)
	CommitBatchMandate(context.Context, string, BatchMandate, BatchMandateView, time.Time) error
	HoldBatchMandateStart(context.Context, string, BatchMandate, string) error
}

type BatchMandateControl struct {
	AnalysisID, Epoch, Fence string
	Mandate                  BatchMandate
	Started                  time.Time
	Attempts                 int
}

type BatchEvidenceRequest struct {
	Contract      string   `json:"contract"`
	Action        string   `json:"action"`
	Installation  string   `json:"installation"`
	Issuer        string   `json:"issuer"`
	Tenant        string   `json:"tenant"`
	Subject       string   `json:"subject"`
	Client        string   `json:"client"`
	Workload      string   `json:"workload"`
	AnalysisID    string   `json:"analysisId"`
	StartRef      string   `json:"startRef"`
	Mode          string   `json:"mode"`
	AssetID       string   `json:"assetId"`
	ProfileID     string   `json:"profileId"`
	SchemaHash    string   `json:"schemaHash"`
	CommandKey    string   `json:"commandKey"`
	Scope         []string `json:"scope"`
	Generation    int      `json:"generation"`
	Version       int      `json:"version"`
	Nonce         string   `json:"nonce"`
	Operation     string   `json:"operation,omitempty"`
	ArgumentsHash string   `json:"argumentsHash,omitempty"`
	AttemptID     string   `json:"attemptId,omitempty"`
	RequestID     string   `json:"requestId,omitempty"`
}

type BatchEvidence struct {
	Bound                  bool      `json:"bound"`
	AnalysisID             string    `json:"analysisId,omitempty"`
	PolicyEpoch            string    `json:"policyEpoch,omitempty"`
	Phase                  string    `json:"phase,omitempty"`
	WorkRef                string    `json:"workRef,omitempty"`
	DiscoveryArgumentsHash string    `json:"discoveryArgumentsHash,omitempty"`
	DiscoveryBinding       string    `json:"discoveryBinding,omitempty"`
	LeaseUntil             time.Time `json:"-"`
}

func (q BatchEvidenceRequest) Matches(start BatchStart, scope Scope) bool {
	b, ok := DecodeBatchStartBinding(start.Mandate.StartBinding)
	return ok && q.StartRef == b.StartRef && q.Generation == start.Mandate.Generation &&
		q.CommandKey == start.Mandate.CommandKey && q.CommandKey == b.CommandKey && q.Mode == b.Mode && q.Mode == BatchMode &&
		q.Installation == b.Installation && q.Workload == b.Workload && q.Issuer == b.Issuer &&
		q.Client == b.Client && q.Client == scope.Service && q.Tenant == b.Tenant && q.Tenant == scope.Tenant &&
		q.Subject == b.Subject && q.Subject == scope.Principal && slices.Equal(q.Scope, b.Scope) &&
		q.AssetID == b.AssetID && q.AssetID == start.Profile.AssetID &&
		q.ProfileID == b.ProfileID && q.ProfileID == start.Profile.ProfileID &&
		q.SchemaHash == b.SchemaHash && "sha256:"+q.SchemaHash == start.Profile.SchemaHash
}

func BatchStageForOperation(operation string) string {
	switch operation {
	case BatchTool:
		return BatchPublished
	case "semantic.discovery.request":
		return BatchRequest
	case "semantic.discovery.status":
		return BatchStatus
	case "semantic.discovery.candidates":
		return BatchCandidates
	default:
		return ""
	}
}

func BatchDiscoveryWorkRef(id string, ordinal int) string {
	return Digest([]any{BatchMandateContract, id, ordinal})
}

func ValidBatchAttempt(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value && id.Version() == 4 && id.Variant() == uuid.RFC4122
}

func ValidBatchHash(value string) bool {
	_, err := hex.DecodeString(value)
	return err == nil && len(value) == 64 && strings.ToLower(value) == value
}

func BatchEvidenceHash(raw []byte) string { return fmt.Sprintf("%x", sha256.Sum256(raw)) }

// Gateway 67a uses Python's compact sorted-key ASCII JSON. The four operation
// schemas have only integer numbers and fixed ASCII object keys.
func BatchArgumentsHash(raw []byte) (string, error) {
	var value map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if !json.Valid(raw) || d.Decode(&value) != nil || value == nil {
		return "", ErrInvalid
	}
	var encoded bytes.Buffer
	e := json.NewEncoder(&encoded)
	e.SetEscapeHTML(false)
	if err := e.Encode(value); err != nil {
		return "", err
	}
	var wire bytes.Buffer
	for _, r := range strings.TrimSuffix(encoded.String(), "\n") {
		switch {
		case r < 127:
			wire.WriteByte(byte(r))
		case r <= 0xffff:
			fmt.Fprintf(&wire, "\\u%04x", r)
		default:
			high, low := utf16.EncodeRune(r)
			fmt.Fprintf(&wire, "\\u%04x\\u%04x", high, low)
		}
	}
	return BatchEvidenceHash(wire.Bytes()), nil
}

func (c Coordinator) issueBatchMandate(ctx context.Context, store BatchMandateStartStore, gateway BatchMandateGateway, start BatchStart, proof string) (BatchStart, error) {
	m := start.Mandate
	if m.Ref != "" {
		if m.State == "ACTIVE" {
			return start, nil
		}
		return start, batchMandateStateError(m.State)
	}
	in := BatchMandateIssue{m.StartRef, m.Generation, m.CommandKey, start.ID, start.Epoch, m.StartBinding, proof}
	view, err := gateway.Issue(ctx, in)
	if err != nil && proof != "" {
		// A previous issue may have committed before the original proof expired.
		// The proofless form can only replay an owner record.
		var refusal *BatchMandateError
		if errors.As(err, &refusal) && (refusal.Observation.Code == "ANALYSIS_START_PROOF_DENIED" || refusal.Observation.Status == 503) {
			in.StartProof = ""
			if replay, replayErr := gateway.Issue(ctx, in); replayErr == nil {
				view, err = replay, nil
			}
		}
	}
	if err != nil {
		var refusal *BatchMandateError
		if errors.As(err, &refusal) && (refusal.Observation.Status == 401 || refusal.Observation.Status == 403) {
			checkpoint, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer stop()
			if holdErr := store.HoldBatchMandateStart(checkpoint, start.ID, m, refusal.Observation.Code); holdErr != nil {
				return start, holdErr
			}
		}
		return start, err
	}
	if err = store.CommitBatchMandate(ctx, start.ID, m, view, time.Now().UTC()); err != nil {
		return start, err
	}
	if view.State != "ACTIVE" {
		return start, batchMandateStateError(view.State)
	}
	start.Mandate.Ref, start.Mandate.Binding, start.Mandate.Version, start.Mandate.State =
		view.Ref, view.MandateBinding, view.Version, view.State
	return start, nil
}

func batchMandateStateError(state string) error {
	switch state {
	case "REVOKED", "EXPIRED":
		return &BatchMandateError{orchestration.OwnerObservation{Status: 403, Code: "MANDATE_" + state}}
	default:
		return ErrUnavailable
	}
}
