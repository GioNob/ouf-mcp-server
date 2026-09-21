package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func TestRetryClassification(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		success  bool
		complete bool
	}{
		{"success", true, true},
		{"failure", false, true},
		{"in_flight", false, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			s, ctx, checksum := concurrencyFixture(t)
			req := concurrencyRequest(checksum, uuid.NewString(), "v1:hmac-sha256:retry-classification")
			req.RetryThreshold = 3
			req.Window = time.Hour
			for n := 0; n < 3; n++ {
				req.IdempotencyKey = uuid.NewString()
				req.CorrelationID = uuid.NewString()
				d, err := s.Reserve(ctx, req)
				if n == 2 && !scenario.success {
					if !errors.Is(err, orchestration.ErrToolSelectionStall) {
						t.Fatalf("third non-successful equivalent attempt: %v", err)
					}
					// Denial must remain effective without committing a blocked flag.
					if _, err = s.Reserve(ctx, req); !errors.Is(err, orchestration.ErrToolSelectionStall) {
						t.Fatalf("repeated denial: %v", err)
					}
					break
				}
				if err != nil {
					t.Fatalf("attempt %d: %v", n+1, err)
				}
				var retry bool
				if err = s.Pool().QueryRow(ctx, `select is_equivalent_retry from ouf_mcp.attempt_admission_context where attempt_id=$1`, d.AttemptID).Scan(&retry); err != nil {
					t.Fatal(err)
				}
				if retry != (n > 0 && !scenario.success) {
					t.Fatalf("attempt %d classified retry=%v", n+1, retry)
				}
				if scenario.complete {
					backend := uuid.NewString()
					if err = s.Dispatch(ctx, d.AttemptID, backend, time.Minute, d.LockVersion); err != nil {
						t.Fatal(err)
					}
					if err = s.Reconcile(ctx, d.AttemptID, orchestration.Cost{ToolCalls: 1, ResultBytes: 12}, orchestration.AttemptOutcome{Success: scenario.success, BackendRequestID: backend}); err != nil {
						t.Fatal(err)
					}
				}
				replay, err := s.Reserve(ctx, req)
				if err != nil || !replay.Replay || replay.AttemptID != d.AttemptID {
					t.Fatalf("idempotent replay: %+v %v", replay, err)
				}
				conflict := req
				conflict.RequestHash = hash64("different-payload")
				if _, err = s.Reserve(ctx, conflict); !errors.Is(err, ErrIdempotencyConflict) {
					t.Fatalf("idempotency conflict: %v", err)
				}
			}
			if scenario.success {
				req.IdempotencyKey = uuid.NewString()
				if _, err := s.Reserve(ctx, req); !errors.Is(err, ErrBudgetExhausted) {
					t.Fatalf("successful calls must still consume ordinary budget: %v", err)
				}
			}
		})
	}
}

func TestRetryClassificationMixedOutcomes(t *testing.T) {
	for _, firstSuccess := range []bool{false, true} {
		s, ctx, checksum := concurrencyFixture(t)
		req := concurrencyRequest(checksum, uuid.NewString(), "v1:hmac-sha256:mixed-outcomes")
		req.RetryThreshold = 3
		req.Window = time.Hour
		// Failure followed by success must retain the successful retry charge.
		// Success followed by failure must not charge the independent success.
		for n, success := range []bool{firstSuccess, !firstSuccess} {
			req.IdempotencyKey = uuid.NewString()
			d, err := s.Reserve(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			backend := uuid.NewString()
			if err = s.Dispatch(ctx, d.AttemptID, backend, time.Minute, d.LockVersion); err != nil {
				t.Fatal(err)
			}
			outcome := orchestration.AttemptOutcome{Success: success, BackendRequestID: backend}
			for replay := 0; replay < 2; replay++ {
				if err = s.Reconcile(ctx, d.AttemptID, orchestration.Cost{ToolCalls: 1, ResultBytes: 12}, outcome); err != nil {
					t.Fatal(err)
				}
			}
			var retry bool
			if err = s.Pool().QueryRow(ctx, `select is_equivalent_retry from ouf_mcp.attempt_admission_context where attempt_id=$1`, d.AttemptID).Scan(&retry); err != nil {
				t.Fatal(err)
			}
			if retry != (n == 1 && !firstSuccess) {
				t.Fatalf("persisted classification changed after reconcile: firstSuccess=%v n=%d retry=%v", firstSuccess, n, retry)
			}
		}
		req.IdempotencyKey = uuid.NewString()
		_, err := s.Reserve(ctx, req)
		if !firstSuccess && !errors.Is(err, orchestration.ErrToolSelectionStall) {
			t.Fatalf("successful retry lost its charge: %v", err)
		}
		if firstSuccess && err != nil {
			t.Fatalf("independent success consumed retry quota: %v", err)
		}
	}
}
