package orchestration

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

var (
	ErrUnauthorized       = errors.New("authorization denied")
	ErrResultLimit        = errors.New("gateway result exceeds governed byte limit")
	ErrToolSelectionStall = errors.New("tool selection stalled")
)

type IdentityClaims struct {
 ExternalRoleRefs []string `json:"externalRoleRefs"`
 Acr string `json:"acr"`
 Amr []string `json:"amr"`
 AuthenticatedAt time.Time `json:"authenticatedAt"`
}

type Identity struct {
 Claims *IdentityClaims `json:"-"`
	ServicePrincipalID, PrincipalID, TenantID, ActorType, AuthenticationContextRef string
	Issuer, Audience                                                               string   `json:"-"`
	Scopes                                                                         []string `json:"-"`
}

type ResourceContext struct {
	ResourceType   string            `json:"-"`
	ResourceID     string            `json:"-"`
	TenantID       string            `json:"-"`
	OrganizationID string            `json:"-"`
	Attributes     map[string]string `json:"-"`
}

type AuthorizationRequest struct {
	Identity                            Identity
	Resource                            ResourceContext
	CapabilityID, Owner, OperationClass string
}
type AuthorizationDecision struct {
 PermittedDetailLevel string
 ResourceScope map[string]string
	Allowed       bool
	DecisionRef   string
	DecisionCode  string
	BundleID      string
	BundleVersion int64
}
type AuthorizationPort interface {
	Authorize(context.Context, AuthorizationRequest) (AuthorizationDecision, error)
}

type Cost struct{ ToolCalls, DistinctObjects, ResultBytes int64 }
type AdmissionRequest struct {
	AttemptID                                                                       uuid.UUID
	Identity                                                                        Identity
	CapabilityID, Owner, OperationClass, ManifestChecksum, AuthorizationDecisionRef string
	IdempotencyKey, RequestHash, SemanticFingerprint, FingerprintVersion            string
	CorrelationID                                                                   string
	Window                                                                          time.Duration
	RetryThreshold                                                                  int
	Maximum                                                                         Cost
}
type AdmissionDecision struct {
	AttemptID   uuid.UUID
	LockVersion int64
	Replay      bool
}
type AttemptOutcome struct {
	Code             string
	Success          bool
	BackendRequestID string
}
type AdmissionPort interface {
	Reserve(context.Context, AdmissionRequest) (AdmissionDecision, error)
	Dispatch(context.Context, uuid.UUID, string, time.Duration, int64) error
	Reconcile(context.Context, uuid.UUID, Cost, AttemptOutcome) error
}

type GatewayRequest struct {
	GatewayBindingRef, CapabilityID, Owner, OperationClass                          string
	Arguments                                                                       json.RawMessage
	Identity                                                                        Identity
	AuthorizationDecisionRef, CorrelationID, IdempotencyKey, AttemptID, RequestHash string
	MaxResultBytes                                                                  int64
}
type Problem struct {
	Type, Title, Code, Detail string
	Status                    int
	RetryAfter                string
}
type GatewayResponse struct {
	Status           int
	Body             []byte
	BackendRequestID string
	Problem          *Problem
}
type GatewayPort interface {
	Execute(context.Context, GatewayRequest, time.Duration) (GatewayResponse, error)
}
type AuditEvent struct {
	EventType, ManifestChecksum, OutcomeCode string
	Identity                                 Identity
	AttemptID                                uuid.UUID
}
type AuditPort interface {
	Audit(context.Context, AuditEvent) error
}

type Invocation struct {
	Identity                                                                 Identity
	Resource                                                                 ResourceContext
	CapabilityID, Owner, OperationClass, GatewayBindingRef, ManifestChecksum string
	Arguments                                                                json.RawMessage
	IdempotencyKey, CorrelationID                                            string
	Window, Timeout                                                          time.Duration
	RetryThreshold                                                           int
	Maximum                                                                  Cost
}
type Result struct {
	Body      []byte
	Problem   *Problem
	AttemptID uuid.UUID
	Replay    bool
}

type Service struct {
	Auth           AuthorizationPort
	Admission      AdmissionPort
	Gateway        GatewayPort
	Audit          AuditPort
	FingerprintKey []byte
}

func (s Service) Call(ctx context.Context, in Invocation) (Result, error) {
	if in.Identity.ServicePrincipalID == "" || in.Identity.PrincipalID == "" || in.Identity.TenantID == "" || in.CapabilityID == "" || in.Owner == "" || in.GatewayBindingRef == "" {
		return Result{}, fmt.Errorf("invalid governed invocation")
	}
	resource := in.Resource
	if resource.TenantID == "" {
		resource.TenantID = in.Identity.TenantID
	}
	if resource.ResourceType == "" {
		resource.ResourceType = "capability"
	}
	decision, err := s.Auth.Authorize(ctx, AuthorizationRequest{Identity: in.Identity, Resource: resource, CapabilityID: in.CapabilityID, Owner: in.Owner, OperationClass: in.OperationClass})
	if err != nil {
		return Result{}, err
	}
	if !decision.Allowed || decision.DecisionRef == "" {
		return Result{}, ErrUnauthorized
	}
	requestHash := sha256.Sum256(in.Arguments)
	fingerprint, err := SemanticFingerprint(s.FingerprintKey, in.CapabilityID, in.Arguments)
	if err != nil {
		return Result{}, err
	}
	admitted, err := s.Admission.Reserve(ctx, AdmissionRequest{
		AttemptID: uuid.New(), Identity: in.Identity, CapabilityID: in.CapabilityID, Owner: in.Owner,
		OperationClass: in.OperationClass, ManifestChecksum: in.ManifestChecksum, AuthorizationDecisionRef: decision.DecisionRef,
		IdempotencyKey: in.IdempotencyKey, RequestHash: hex.EncodeToString(requestHash[:]), SemanticFingerprint: fingerprint,
		FingerprintVersion: "v1", CorrelationID: in.CorrelationID, Window: in.Window, RetryThreshold: in.RetryThreshold, Maximum: in.Maximum,
	})
	if err != nil {
		return Result{}, err
	}
	if admitted.Replay {
		return Result{AttemptID: admitted.AttemptID, Replay: true}, nil
	}
	backendRequestID := uuid.NewString()
	err = s.Admission.Dispatch(ctx, admitted.AttemptID, backendRequestID, in.Timeout+time.Second, admitted.LockVersion)
	if err != nil {
		return Result{}, err
	}
	response, callErr := s.Gateway.Execute(ctx, GatewayRequest{
		GatewayBindingRef: in.GatewayBindingRef, CapabilityID: in.CapabilityID, Owner: in.Owner, OperationClass: in.OperationClass,
		Arguments: in.Arguments, Identity: in.Identity, AuthorizationDecisionRef: decision.DecisionRef,
		CorrelationID: in.CorrelationID, IdempotencyKey: in.IdempotencyKey, AttemptID: admitted.AttemptID.String(), RequestHash: hex.EncodeToString(requestHash[:]),
		MaxResultBytes: in.Maximum.ResultBytes,
	}, in.Timeout)
	outcome := AttemptOutcome{Code: "UPSTREAM_ERROR", BackendRequestID: response.BackendRequestID}
	if response.BackendRequestID == "" {
		outcome.BackendRequestID = backendRequestID
	}
	if response.Problem != nil && response.Problem.Code != "" {
		outcome.Code = response.Problem.Code
	}
	actual := Cost{ToolCalls: 1, ResultBytes: int64(len(response.Body))}
	if callErr == nil && response.Problem == nil && response.Status >= 200 && response.Status < 300 {
		outcome.Success, outcome.Code = true, "SUCCEEDED"
	}
	if int64(len(response.Body)) > in.Maximum.ResultBytes {
		callErr, outcome.Code = ErrResultLimit, "RESULT_LIMIT_EXCEEDED"
	}
	if err := s.Admission.Reconcile(ctx, admitted.AttemptID, actual, outcome); err != nil {
		return Result{}, err
	}
	if s.Audit != nil {
		if err := s.Audit.Audit(ctx, AuditEvent{EventType: "TOOL_ATTEMPT_RECONCILED", Identity: in.Identity, AttemptID: admitted.AttemptID, ManifestChecksum: in.ManifestChecksum, OutcomeCode: outcome.Code}); err != nil {
			return Result{}, err
		}
	}
	if callErr != nil {
		return Result{}, callErr
	}
	return Result{Body: response.Body, Problem: response.Problem, AttemptID: admitted.AttemptID}, nil
}

func SemanticFingerprint(key []byte, capability string, raw json.RawMessage) (string, error) {
	if len(key) < 32 {
		return "", errors.New("fingerprint key must contain at least 32 bytes")
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	value = normalize(value, "")
	canonical, err := json.Marshal(map[string]any{"capabilityId": capability, "arguments": value})
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(canonical)
	return "v1:hmac-sha256:" + hex.EncodeToString(mac.Sum(nil)), nil
}

func normalize(v any, field string) any {
	switch x := v.(type) {
	case map[string]any:
		for k, child := range x {
			x[k] = normalize(child, k)
		}
		return x
	case []any:
		for i := range x {
			x[i] = normalize(x[i], field)
		}
		if field == "targetTypeCodes" {
			sort.Slice(x, func(i, j int) bool { return fmt.Sprint(x[i]) < fmt.Sprint(x[j]) })
		}
		return x
	default:
		return v
	}
}
