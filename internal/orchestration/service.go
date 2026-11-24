package orchestration

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"sort"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/reprocessing"
	"github.com/GioNob/ouf-mcp-server/internal/statusview"
	"github.com/google/uuid"
)

var (
	ErrIdempotencyConflict       = errors.New("idempotency key reused with different request")
	ErrIdempotencyOutcomeUnknown = errors.New("idempotency outcome unknown")
)

var (
	ErrUnauthorized       = errors.New("authorization denied")
	ErrResultLimit        = errors.New("gateway result exceeds governed byte limit")
	ErrToolSelectionStall = errors.New("tool selection stalled")
)

// AuthorizationDeniedError preserves the local decision without making it an
// authority, a retry instruction, or a container for principal/credential data.
// Error and Unwrap preserve existing errors.Is and generic-tool contracts.
type AuthorizationDeniedError struct {
	ReasonCode    string
	PolicyVersion int64
	CapabilityID  string
}

func (e *AuthorizationDeniedError) Error() string { return ErrUnauthorized.Error() }
func (e *AuthorizationDeniedError) Unwrap() error { return ErrUnauthorized }

func SafeDenialReason(code, fallback string) string {
	switch code {
	case "DELEGATION_REQUIRED", "TENANT_MISMATCH", "CAPABILITY_NOT_DECLARED",
		"ACTOR_NOT_ALLOWED", "SCOPE_MISSING", "DATA_LABEL_REQUIRED", "HUMAN_REQUIRED",
		"EXPLICIT_DENY", "NO_APPLICABLE_GRANT":
		return code
	}
	return fallback
}

type IdentityClaims struct {
	ExternalRoleRefs []string  `json:"externalRoleRefs"`
	Acr              string    `json:"acr"`
	Amr              []string  `json:"amr"`
	AuthenticatedAt  time.Time `json:"authenticatedAt"`
}

type Identity struct {
	// Opaque Gateway-signed delegation, request memory only: never serialized/persisted.
	Delegation                                                                     string          `json:"-"`
	Claims                                                                         *IdentityClaims `json:"-"`
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
	ResourceScope        map[string]string
	Allowed              bool
	DecisionRef          string
	DecisionCode         string
	BundleID             string
	BundleVersion        int64
}
type AuthorizationPort interface {
	Authorize(context.Context, AuthorizationRequest) (AuthorizationDecision, error)
}

type Cost struct{ ToolCalls, DistinctObjects, ResultBytes int64 }
type AdmissionRequest struct {
	CallsPerMinute                                                                  int
	StatusRead                                                                      bool
	InternalWaitRead                                                                bool
	Reprocessing                                                                    *reprocessing.VerifiedPermit
	AttemptTimeout                                                                  time.Duration
	BudgetProfile                                                                   string
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
	AttemptState string
	ResultID     *uuid.UUID
	AttemptID    uuid.UUID
	LockVersion  int64
	Replay       bool
}
type AttemptOutcome struct {
	ResultID         *uuid.UUID
	Code             string
	Success          bool
	BackendRequestID string
}
type AdmissionPort interface {
	Reserve(context.Context, AdmissionRequest) (AdmissionDecision, error)
	Dispatch(context.Context, uuid.UUID, string, time.Duration, int64) error
	Reconcile(context.Context, uuid.UUID, Cost, AttemptOutcome) error
}

// IsValueMapProposal identifies the proposal variant with its own transport profile.
func IsValueMapProposal(capabilityID string, args json.RawMessage) bool {
	if capabilityID != "ouf.semantic.propose" {
		return false
	}
	var proposal struct {
		ArtifactType string `json:"artifactType"`
	}
	return json.Unmarshal(args, &proposal) == nil && proposal.ArtifactType == "VALUE_MAP"
}

type GatewayRequest struct {
	MaxRequestBytes                                                                 int `json:"-"`
	GatewayBindingRef, CapabilityID, Owner, OperationClass                          string
	Arguments                                                                       json.RawMessage
	Identity                                                                        Identity
	AuthorizationDecisionRef, CorrelationID, IdempotencyKey, AttemptID, RequestHash string
	MaxResultBytes                                                                  int64
	Upload                                                                          *UploadStream `json:"-"`
}

// UploadStream is consumed only after authorization and admission. Its bytes
// never enter the JSON envelope, fingerprint, database or audit event.
type UploadStream struct {
	Reader         io.Reader
	Size           int64
	SHA256, FileID string
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
	AuthorizationDecisionRef, PermittedDetailLevel string
	Redacted                                       bool
	EventType, ManifestChecksum, OutcomeCode       string
	Identity                                       Identity
	AttemptID                                      uuid.UUID
}
type AuditPort interface {
	Audit(context.Context, AuditEvent) error
}

const HandoffIssueCapability = "ouf.managed-source.file.handoff-issue"

// The issue route shares the upload grant. Its only dispatch argument is the
// MCP-generated idempotency key for this invocation.
func ValidHandoffIssue(identity Identity, operation, key string, raw json.RawMessage) bool {
	var args map[string]string
	return identity.ActorType == "HUMAN" && identity.Delegation != "" && operation == "COMMAND" &&
		len(key) == 36 && uuid.Validate(key) == nil && json.Unmarshal(raw, &args) == nil && len(args) == 1 && args["commandKey"] == key
}

type Invocation struct {
	MaxRequestBytes  int
	CheckDecision    func(AuthorizationDecision) error
	InternalWaitRead bool
	// Trusted manifest binding, never taken from tool arguments. Transport,
	// fingerprints and budget identity continue to use CapabilityID.
	AuthorizationCapabilityID                                                string
	BudgetProfile                                                            string
	ReadPositionRef                                                          string
	Identity                                                                 Identity
	Resource                                                                 ResourceContext
	CapabilityID, Owner, OperationClass, GatewayBindingRef, ManifestChecksum string
	Arguments                                                                json.RawMessage
	IdempotencyKey, CorrelationID                                            string
	Window, Timeout                                                          time.Duration
	RetryThreshold                                                           int
	Maximum                                                                  Cost
	Upload                                                                   *UploadStream
}
type Result struct {
	AttemptState string
	ResultID     *uuid.UUID
	Body         []byte
	Problem      *Problem
	AttemptID    uuid.UUID
	Replay       bool
}

type Service struct {
	AnalysisMandates        AnalysisMandatePort
	AdmissionCallsPerMinute int
	ReprocessingGuard       func() error
	Reprocessing            *reprocessing.VerifiedPermit
	RequireDelegation       bool
	Auth                    AuthorizationPort
	Admission               AdmissionPort
	Gateway                 GatewayPort
	Audit                   AuditPort
	FingerprintKey          []byte
}

func (s Service) Call(ctx context.Context, in Invocation) (Result, error) {
	if in.BudgetProfile == "" {
		if IsSemanticRead(in.CapabilityID, in.OperationClass) {
			in.BudgetProfile = SemanticReadBudgetProfile
		} else if isDiscoveryOperation(in.CapabilityID, in.OperationClass) {
			in.BudgetProfile = DiscoveryBudgetProfile
		} else if isManagedSchemaPage(in) {
			in.BudgetProfile = ManagedSchemaBudgetProfile
		} else if IsOperationalControlRead(in.CapabilityID, in.OperationClass) {
			in.BudgetProfile = OperationalControlBudgetProfile
		}
	}
	if _, _, err := AdmissionWindowBudget(AdmissionRequest{BudgetProfile: in.BudgetProfile, CapabilityID: in.CapabilityID, OperationClass: in.OperationClass, ManifestChecksum: in.ManifestChecksum, Window: in.Window, RetryThreshold: in.RetryThreshold, Maximum: in.Maximum}); err != nil {
		return Result{}, err
	}

	if in.Identity.ServicePrincipalID == "" || in.Identity.PrincipalID == "" || in.Identity.TenantID == "" || in.CapabilityID == "" || in.Owner == "" || in.GatewayBindingRef == "" {
		return Result{}, fmt.Errorf("invalid governed invocation")
	}
	if s.RequireDelegation && in.Identity.Delegation == "" {
		return Result{}, &AuthorizationDeniedError{ReasonCode: "DELEGATION_REQUIRED"}
	}
	if in.CapabilityID == HandoffIssueCapability && (in.AuthorizationCapabilityID != "ouf.managed-source.file.upload" || !ValidHandoffIssue(in.Identity, in.OperationClass, in.IdempotencyKey, in.Arguments)) {
		return Result{}, errors.New("INVALID_HANDOFF_ISSUE")
	}
	if in.Upload != nil && (in.CapabilityID != "ouf.managed-source.file.upload" || in.Upload.Reader == nil || in.Upload.Size < 1 || in.Upload.Size > 10*1024*1024) {
		return Result{}, fmt.Errorf("invalid governed upload")
	}
	if in.CapabilityID == "ouf.managed-source.file.upload" && in.Upload == nil {
		var args map[string]string
		if json.Unmarshal(in.Arguments, &args) != nil || len(args) != 1 || uuid.Validate(args["handoffId"]) != nil {
			return Result{}, fmt.Errorf("invalid managed upload handoff")
		}
	}
	resource := in.Resource
	if resource.TenantID == "" {
		resource.TenantID = in.Identity.TenantID
	}
	if resource.ResourceType == "" {
		resource.ResourceType = "capability"
	}
	publicStatus := in.CapabilityID == statusview.Capability
	if publicStatus {
		// This capability has a fixed public response contract. No tool argument
		// or caller-provided attribute may raise that disclosure ceiling.
		if detail := resource.Attributes["detailLevel"]; detail != "" && detail != statusview.Public {
			return Result{}, ErrUnauthorized
		}
		resource.Attributes = maps.Clone(resource.Attributes)
		if resource.Attributes == nil {
			resource.Attributes = make(map[string]string)
		}
		resource.Attributes["detailLevel"] = statusview.Public
	}
	authorizationCapability := in.CapabilityID
	if in.AuthorizationCapabilityID != "" {
		authorizationCapability = in.AuthorizationCapabilityID
	}
	decision, err := s.Auth.Authorize(ctx, AuthorizationRequest{Identity: in.Identity, Resource: resource, CapabilityID: authorizationCapability, Owner: in.Owner, OperationClass: in.OperationClass})
	if err != nil {
		return Result{}, err
	}
	if !decision.Allowed || decision.DecisionRef == "" {
		if s.Audit != nil {
			_ = s.Audit.Audit(ctx, AuditEvent{
				EventType:                "AUTHORIZATION_DENIED_PRE_ADMISSION",
				Identity:                 in.Identity,
				ManifestChecksum:         in.ManifestChecksum,
				OutcomeCode:              decision.DecisionCode,
				AuthorizationDecisionRef: decision.DecisionRef,
			})
		}
		return Result{}, &AuthorizationDeniedError{ReasonCode: decision.DecisionCode, PolicyVersion: decision.BundleVersion, CapabilityID: authorizationCapability}
	}
	if publicStatus && (decision.PermittedDetailLevel != statusview.Public || decision.ResourceScope["tenantId"] != in.Identity.TenantID || decision.ResourceScope["resourceType"] != "capability") {
		return Result{}, ErrUnauthorized
	}
	if in.CapabilityID == "ouf.semantic.discovery" && in.OperationClass == "COMMAND" {
		var command struct {
			IdempotencyKey string `json:"idempotencyKey"`
		}
		if json.Unmarshal(in.Arguments, &command) != nil || command.IdempotencyKey == "" {
			return Result{}, errors.New("SEMANTIC_DISCOVERY_KEY_REQUIRED")
		}
		in.IdempotencyKey = command.IdempotencyKey
	}
	if in.CheckDecision != nil {
		if err := in.CheckDecision(decision); err != nil {
			return Result{}, err
		}
	}
	if in.CapabilityID == "ouf.managed-source.onboarding.review.prepare" || in.CapabilityID == "ouf.managed-source.onboarding.read" {
		in.Arguments, err = withoutDraftContext(in.Arguments)
		if err != nil {
			return Result{}, err
		}
	}
	requestHash := sha256.Sum256(in.Arguments)
	fingerprintArguments := in.Arguments
	if in.ReadPositionRef != "" {
		if !IsOperationalProducerRead(in.CapabilityID, in.OperationClass) || in.BudgetProfile != OperationalControlBudgetProfile {
			return Result{}, errors.New("unreviewed operational read position")
		}
		position, parseErr := uuid.Parse(in.ReadPositionRef)
		if parseErr != nil || position == uuid.Nil {
			return Result{}, errors.New("invalid operational read position")
		}
		fingerprintArguments, _ = json.Marshal(struct {
			Arguments json.RawMessage `json:"arguments"`
			Position  string          `json:"position"`
		}{in.Arguments, in.ReadPositionRef})
	}
	fingerprint, err := SemanticFingerprint(s.FingerprintKey, in.CapabilityID, fingerprintArguments)
	if err != nil {
		return Result{}, err
	}
	var permit *reprocessing.VerifiedPermit
	if s.Reprocessing.Matches(in.Identity.ServicePrincipalID, in.Identity.PrincipalID, in.Identity.TenantID, in.CapabilityID, in.Owner, in.OperationClass, in.IdempotencyKey) {
		permit = s.Reprocessing
		if s.ReprocessingGuard == nil {
			return Result{}, reprocessing.ErrPermit
		}
		if err := s.ReprocessingGuard(); err != nil {
			return Result{}, err
		}
	}
	statusRead := isPollRead(in)
	if in.InternalWaitRead && !statusRead {
		return Result{}, errors.New("invalid internal wait read")
	}
	admitted, err := s.Admission.Reserve(ctx, AdmissionRequest{
		CallsPerMinute: s.AdmissionCallsPerMinute, StatusRead: statusRead, InternalWaitRead: in.InternalWaitRead,
		Reprocessing: permit, AttemptTimeout: in.Timeout,
		BudgetProfile: in.BudgetProfile,
		AttemptID:     uuid.New(), Identity: in.Identity, CapabilityID: in.CapabilityID, Owner: in.Owner,
		OperationClass: in.OperationClass, ManifestChecksum: in.ManifestChecksum, AuthorizationDecisionRef: decision.DecisionRef,
		IdempotencyKey: in.IdempotencyKey, RequestHash: hex.EncodeToString(requestHash[:]), SemanticFingerprint: fingerprint,
		FingerprintVersion: "v1", CorrelationID: in.CorrelationID, Window: in.Window, RetryThreshold: in.RetryThreshold, Maximum: in.Maximum,
	})
	if err != nil {
		return Result{}, err
	}
	if admitted.Replay {
		return Result{AttemptID: admitted.AttemptID, Replay: true, AttemptState: admitted.AttemptState, ResultID: admitted.ResultID}, nil
	}
	if permit != nil {
		if err := s.ReprocessingGuard(); err != nil {
			return Result{}, err
		}
		if err := permit.CheckTime(time.Now(), in.Timeout+time.Second); err != nil {
			return Result{}, err
		}
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
		MaxResultBytes: in.Maximum.ResultBytes, MaxRequestBytes: in.MaxRequestBytes,
		Upload: in.Upload,
	}, in.Timeout)
	uncertainCommand := in.CapabilityID == "ouf.semantic.discovery.adopt" || in.CapabilityID == HandoffIssueCapability
	if uncertainCommand && callErr == nil && response.Status >= 500 {
		// A Gateway failure after forwarding cannot establish whether the owner committed.
		callErr = ErrIdempotencyOutcomeUnknown
	}
	if (permit != nil || uncertainCommand) && callErr != nil {
		// Dispatch may have reached the owner. Preserve the finite lease and
		// reservation for native UNKNOWN recovery; never infer a known failure
		// or make this one-use session available for another dispatch.
		if permit != nil && s.Audit != nil {
			_ = s.Audit.Audit(ctx, AuditEvent{EventType: "SEMANTIC_REPROCESSING_OUTCOME_UNCERTAIN", Identity: in.Identity, AttemptID: admitted.AttemptID, ManifestChecksum: in.ManifestChecksum, OutcomeCode: "OWNER_OUTCOME_UNCERTAIN", AuthorizationDecisionRef: decision.DecisionRef, PermittedDetailLevel: decision.PermittedDetailLevel})
		}
		return Result{AttemptID: admitted.AttemptID}, callErr
	}
	outcome := AttemptOutcome{Code: "UPSTREAM_ERROR", BackendRequestID: response.BackendRequestID}
	if response.BackendRequestID == "" {
		outcome.BackendRequestID = backendRequestID
	}
	if response.Problem != nil && response.Problem.Code != "" {
		outcome.Code = response.Problem.Code
	}
	if in.CapabilityID == "ouf.semantic.discovery.adopt" && response.Problem != nil {
		outcome.Code = adoptionProblemCode(response.Problem.Code)
		if response.Status == 403 && outcome.Code == "SEMANTIC_ADOPTION_REJECTED" {
			outcome.Code = SafeDenialReason(response.Problem.Code, "OWNER_AUTHORIZATION_DENIED")
		}
		response.Problem = &Problem{Status: response.Status, Code: outcome.Code}
	}
	actual := Cost{ToolCalls: 1, ResultBytes: int64(len(response.Body))}
	if in.CapabilityID == "ouf.semantic.search.batch" && callErr == nil && response.Problem == nil {
		actual.DistinctObjects = publishedRevisionCount(response.Body)
		if actual.DistinctObjects > in.Maximum.DistinctObjects {
			// Reject the whole response and consume the full reservation.
			// No excess revisions are admitted as evidence.
			actual.DistinctObjects = in.Maximum.DistinctObjects
			callErr, outcome.Code = ErrResultLimit, "PUBLISHED_OBJECT_LIMIT_EXCEEDED"
		}
	}
	if in.InternalWaitRead {
		actual.ToolCalls = 0
	}
	redacted := false
	if publicStatus && callErr == nil && response.Problem == nil && response.Status >= 200 && response.Status < 300 && actual.ResultBytes <= in.Maximum.ResultBytes {
		response.Body, callErr = statusview.Project(response.Body)
		redacted = callErr == nil
		if callErr != nil {
			outcome.Code = "INVALID_PUBLIC_STATUS"
		}
	}
	if publicStatus && (response.Problem != nil || response.Status < 200 || response.Status >= 300) {
		code := "STATUS_UNAVAILABLE"
		if response.Status == 401 || response.Status == 403 {
			code = "NOT_AUTHORIZED"
			if response.Problem != nil && response.Problem.Code == "NO_APPLICABLE_GRANT" {
				code = "NO_APPLICABLE_GRANT"
			}
		}
		response.Body = nil
		response.Problem = &Problem{Status: response.Status, Code: code, Title: code}
	}
	if callErr == nil && response.Problem == nil && response.Status >= 200 && response.Status < 300 {
		outcome.Success, outcome.Code = true, "SUCCEEDED"
	}
	if actual.ResultBytes > in.Maximum.ResultBytes || int64(len(response.Body)) > in.Maximum.ResultBytes {
		callErr, outcome.Code = ErrResultLimit, "RESULT_LIMIT_EXCEEDED"
		outcome.Success = false
	}
	if outcome.Success {
		outcome.ResultID = replayResultID(in, response.Body)
	}
	if err := s.Admission.Reconcile(ctx, admitted.AttemptID, actual, outcome); err != nil {
		return Result{}, err
	}
	if s.Audit != nil {
		if err := s.Audit.Audit(ctx, AuditEvent{EventType: "TOOL_ATTEMPT_RECONCILED", Identity: in.Identity, AttemptID: admitted.AttemptID, ManifestChecksum: in.ManifestChecksum, OutcomeCode: outcome.Code, AuthorizationDecisionRef: decision.DecisionRef, PermittedDetailLevel: decision.PermittedDetailLevel, Redacted: redacted}); err != nil {
			return Result{}, err
		}
	}
	if callErr != nil {
		return Result{AttemptID: admitted.AttemptID}, callErr
	}
	return Result{Body: response.Body, Problem: response.Problem, AttemptID: admitted.AttemptID}, nil
}

func adoptionProblemCode(code string) string {
	switch code {
	case "SEM_PROPOSAL_RECEIPT_INVALID", "SEM_CAPABILITY_REQUIRED:ouf.semantic.discovery.adopt",
		"SEMANTIC_CANDIDATE_NOT_FOUND", "CANDIDATE_EXPIRED", "IDEMPOTENCY_KEY_REUSE",
		"CANDIDATE_ALREADY_ADOPTED", "SEMANTIC_ID_CONFLICT", "CANDIDATE_PUBLISHER_MISSING",
		"CANDIDATE_METADATA_AMBIGUOUS", "CANDIDATE_METADATA_UNRESOLVED",
		"CANDIDATE_REFERENCE_UNRESOLVED", "CANDIDATE_VOCABULARY_UNRESOLVED":
		return code
	}
	return "SEMANTIC_ADOPTION_REJECTED"
}

func SemanticFingerprint(key []byte, capability string, raw json.RawMessage) (string, error) {
	if len(key) < 32 {
		return "", errors.New("fingerprint key must contain at least 32 bytes")
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	switch capability {
	case "ouf.semantic.discovery":
		if args, ok := value.(map[string]any); ok {
			delete(args, "idempotencyKey")
		}
	case HandoffIssueCapability, "ouf.semantic.discovery.adopt", "ouf.semantic.propose", "ouf.semantic.review.prepare", "ouf.semantic.approval.request", "ouf.managed-source.onboarding.review.prepare":
		if args, ok := value.(map[string]any); ok {
			delete(args, "commandKey")
		}
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
