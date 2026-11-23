package kernel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

// Charge the interactive read to the caller's ordinary bucket. The dispatch
// marker covers local SQL execution, which makes no Gateway call.
func admitSummary(ctx context.Context, service *orchestration.Service, i orchestration.Identity, checksum, decision string, input map[string]any, maximum int) (func(context.Context, int, int, bool) error, error) {
	return admitAnalysisCall(ctx, service, i, checksum, decision, input, maximum, "ouf.source.analysis.read", "READ")
}

func admitAnalysisCall(ctx context.Context, service *orchestration.Service, i orchestration.Identity, checksum, decision string, input map[string]any, maximum int, capability, operation string) (func(context.Context, int, int, bool) error, error) {
	const timeout = 3 * time.Second
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	fingerprint, err := orchestration.SemanticFingerprint(service.FingerprintKey, capability, raw)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	a, err := service.Admission.Reserve(ctx, orchestration.AdmissionRequest{
		CallsPerMinute: service.AdmissionCallsPerMinute, StatusRead: input["view"] == "STATUS",
		AttemptID: uuid.New(), AttemptTimeout: timeout, Identity: i, CapabilityID: capability, Owner: "mcp", OperationClass: operation,
		ManifestChecksum: checksum, AuthorizationDecisionRef: decision, IdempotencyKey: uuid.NewString(), CorrelationID: uuid.NewString(),
		RequestHash: hex.EncodeToString(digest[:]), SemanticFingerprint: fingerprint, FingerprintVersion: "v1",
		Window: orchestration.AdmissionWindow, RetryThreshold: orchestration.AdmissionRetryThreshold,
		Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 128, ResultBytes: int64(maximum)},
	})
	if err != nil {
		return nil, err
	}
	localReadID := uuid.NewString()
	if err = service.Admission.Dispatch(ctx, a.AttemptID, localReadID, timeout+time.Second, a.LockVersion); err != nil {
		return nil, err
	}
	return func(ctx context.Context, size, objects int, success bool) error {
		code := "ANALYSIS_READ_FAILED"
		if success {
			code = "SUCCEEDED"
		}
		outcome := orchestration.AttemptOutcome{Code: code, Success: success, BackendRequestID: localReadID}
		err := service.Admission.Reconcile(ctx, a.AttemptID, orchestration.Cost{ToolCalls: 1, ResultBytes: int64(size), DistinctObjects: int64(objects)}, outcome)
		if err != nil || service.Audit == nil {
			return err
		}
		return service.Audit.Audit(ctx, orchestration.AuditEvent{EventType: "TOOL_ATTEMPT_RECONCILED", Identity: i, AttemptID: a.AttemptID,
			ManifestChecksum: checksum, OutcomeCode: code, AuthorizationDecisionRef: decision})
	}, nil
}
