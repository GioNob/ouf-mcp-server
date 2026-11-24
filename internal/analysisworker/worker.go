// Package analysisworker executes a single durably reserved operation per turn.
package analysisworker

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type Worker struct {
	Store        *postgres.Store
	Gateway      orchestration.AnalysisMandatePort
	Manifest     *manifest.Snapshot
	Installation analysis.RunInstallation
	ID           string
}

func (w Worker) Once(ctx context.Context) error {
	if !w.Installation.Enabled || !w.Installation.Limits.OwnerDedupQualified || w.Gateway == nil || !analysis.CanonicalUUID(w.ID) {
		return analysis.ErrUnavailable
	}
	r, err := w.Store.DueRun(ctx, w.Installation.Workload)
	if errors.Is(err, analysis.ErrScope) {
		return nil
	}
	if err != nil {
		return err
	}
	if r.Manifest != w.Installation.Manifest || r.Limits != w.Installation.Limits {
		return w.Store.StopRun(ctx, r, "ANALYSIS_SCOPE_DRIFT")
	}
	work, err := w.Store.NextRunWork(ctx, r, w.ID)
	if err != nil {
		return err
	}
	if !slices.Contains(r.Mandate.Operations, work.Operation) {
		return w.Store.StopRun(ctx, r, "ANALYSIS_MANDATE_REVOKED")
	}
	c, err := w.Store.ReserveRunWork(ctx, r, work)
	switch {
	case errors.Is(err, analysis.ErrRunPressure):
		return w.Store.DelayRun(ctx, r)
	case errors.Is(err, analysis.ErrMandateExpired):
		return w.Store.StopRun(ctx, r, "ANALYSIS_MANDATE_EXPIRED")
	case err != nil:
		return err
	}
	body, _ := json.Marshal(map[string]any{"mandateId": c.MandateID, "generation": c.Generation, "analysisId": c.AnalysisID,
		"intentKey": c.IntentKey, "attemptId": c.AttemptID, "fence": c.Fence, "operation": c.Operation, "arguments": work.Arguments})
	start := time.Now()
	reply, err := w.Gateway.MandateCall(ctx, "dispatch", body, "", work.Maximum.Bytes, time.Duration(work.Maximum.Millis)*time.Millisecond)
	// The admission may have crossed the wire. Keep all maximum charges,
	// the original key and lease for the next reconciliation decision.
	if err != nil {
		if errors.Is(err, orchestration.ErrMandateTransportUncertain) && work.Operation == "semantic.discovery.request" && r.Limits.OwnerDedupQualified {
			return w.Store.ReconcileRunAttempt(ctx, c, w.ID)
		}
		return w.Store.HoldRunAttempt(ctx, c, w.ID, "ANALYSIS_EFFECT_UNCERTAIN")
	}
	receipt := postgres.RunReceipt{Body: reply.Body, Decision: reply.Decision, Correlation: reply.Correlation, Elapsed: time.Since(start)}
	if reply.RetryAfter != "" {
		receipt.RetryAfter = analysis.OwnerRetryAfter(reply.RetryAfter, time.Now())
	}
	if reply.Status/100 != 2 {
		var p struct{ Code string }
		_ = json.Unmarshal(reply.Body, &p)
		switch {
		case work.Operation == "ouf.semantic.search.batch" && (reply.Status == 413 || reply.Status == 422):
			receipt.Code = "SEM_BATCH_REQUEST_TOO_LARGE"
			if reply.Status == 422 {
				receipt.Code = "SEM_BATCH_RESULT_TOO_LARGE"
			}
		case reply.Effect == "IN_FLIGHT":
			return w.Store.RecordCrossing(ctx, c, w.ID, reply.Decision, reply.Correlation, reply.Body)
		case reply.Status == 409 && p.Code == "ANALYSIS_COMMAND_CONFLICT":
			return w.Store.HoldRunAttempt(ctx, c, w.ID, "ANALYSIS_EFFECT_UNCERTAIN")
		case reply.Status == 409 && p.Code == "ANALYSIS_STALE_EXECUTION":
			return w.Store.HoldRunAttempt(ctx, c, w.ID, "ANALYSIS_SCOPE_DRIFT")
		case reply.Status == 403 || reply.Status == 404 && p.Code == "ANALYSIS_MANDATE_UNKNOWN":
			receipt.Code = "ANALYSIS_MANDATE_REVOKED"
		case reply.Status == 410 && p.Code == "ANALYSIS_MANDATE_EXPIRED":
			receipt.Code = "ANALYSIS_MANDATE_EXPIRED"
		case (reply.Status == 429 && (p.Code == "ANALYSIS_BACKOFF" || p.Code == "SEM_BATCH_RATE_LIMITED")) ||
			(reply.Status == 503 && (p.Code == "ANALYSIS_AUTHORITY_UNAVAILABLE" || p.Code == "SEM_BATCH_UNAVAILABLE")):
			n, err := w.Store.RunAttemptCount(ctx, c)
			if err != nil {
				return err
			}
			receipt.Code = "ANALYSIS_BACKOFF"
			receipt.RetryAfter = analysis.RunRetryAt(time.Now(), n, reply.RetryAfter)
		default:
			return w.Store.HoldRunAttempt(ctx, c, w.ID, "ANALYSIS_OWNER_FAILURE")
		}
	}
	err = w.Store.FinishRunWork(ctx, c, w.ID, w.Manifest, receipt)
	if errors.Is(err, analysis.ErrInvalid) || errors.Is(err, analysis.ErrRunDrift) {
		return w.Store.HoldRunAttempt(ctx, c, w.ID, "ANALYSIS_OWNER_FAILURE")
	}
	return err
}
