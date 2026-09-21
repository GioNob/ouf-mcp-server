package postgres

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func TestWindowBudgetSuccessfulCallsReachIndependentCap(t *testing.T) {
	s, ctx, checksum := concurrencyFixture(t)
	req := concurrencyRequest(checksum, uuid.NewString(), "v1:hmac-sha256:successful-budget")
	req.RetryThreshold = 3
	req.Window = time.Hour
	for n := 0; n < 20; n++ {
		req.IdempotencyKey = uuid.NewString()
		req.CorrelationID = uuid.NewString()
		d, err := s.Reserve(ctx, req)
		if err != nil {
			t.Fatalf("successful call %d denied: %v", n+1, err)
		}
		backend := uuid.NewString()
		if err = s.Dispatch(ctx, d.AttemptID, backend, time.Minute, d.LockVersion); err != nil {
			t.Fatal(err)
		}
		if err = s.Reconcile(ctx, d.AttemptID, orchestration.Cost{ToolCalls: 1, ResultBytes: 12}, orchestration.AttemptOutcome{Success: true, BackendRequestID: backend}); err != nil {
			t.Fatal(err)
		}
	}
	// Replay remains available at quota exhaustion and does not charge again.
	if d, err := s.Reserve(ctx, req); err != nil || !d.Replay {
		t.Fatalf("replay at cap: %+v %v", d, err)
	}
	req.IdempotencyKey = uuid.NewString()
	if _, err := s.Reserve(ctx, req); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("twenty-first single call bypassed cap: %v", err)
	}
	req.CapabilityID = "another.capability"
	req.SemanticFingerprint = "v1:hmac-sha256:other"
	req.Maximum.ToolCalls = 2
	req.RetryThreshold = 8
	if _, err := s.Reserve(ctx, req); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("cap changed with capability, envelope or retry threshold: %v", err)
	}
}

func TestWindowBudgetByteAndObjectCaps(t *testing.T) {
	for _, dimension := range []string{"bytes", "objects"} {
		t.Run(dimension, func(t *testing.T) {
			s, ctx, checksum := concurrencyFixture(t)
			req := concurrencyRequest(checksum, uuid.NewString(), "v1:hmac-sha256:bounded")
			req.Window = time.Hour
			if dimension == "bytes" {
				req.WindowBudget.ResultBytes = req.Maximum.ResultBytes
			} else {
				req.WindowBudget.DistinctObjects = req.Maximum.DistinctObjects
			}
			if _, err := s.Reserve(ctx, req); err != nil {
				t.Fatal(err)
			}
			req.IdempotencyKey = uuid.NewString()
			req.CapabilityID = "another.capability"
			req.SemanticFingerprint = "v1:hmac-sha256:another"
			if _, err := s.Reserve(ctx, req); !errors.Is(err, ErrBudgetExhausted) {
				t.Fatalf("%s reservation cap bypassed: %v", dimension, err)
			}
			if dimension == "objects" {
				req.Maximum.DistinctObjects = 0
				if _, err := s.Reserve(ctx, req); err != nil {
					t.Fatalf("zero-object diagnostics denied at object cap: %v", err)
				}
			}
		})
	}
}

func TestWindowBudgetPolicyPinnedAndLegacyFailClosed(t *testing.T) {
	s, ctx, checksum := concurrencyFixture(t)
	req := concurrencyRequest(checksum, uuid.NewString(), "v1:hmac-sha256:pinned")
	req.Window = time.Hour
	d, err := s.Reserve(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	candidate := req
	candidate.IdempotencyKey = uuid.NewString()
	candidate.WindowBudget.ToolCalls++
	if _, err = s.Reserve(ctx, candidate); !errors.Is(err, ErrBudgetPolicyMismatch) {
		t.Fatalf("window cap replaced: %v", err)
	}
	// Model a window created before migration 009 without inventing old caps.
	if _, err = s.Pool().Exec(ctx, `update ouf_mcp.budget_window set limit_tool_calls=null,limit_result_bytes=null,limit_distinct_objects=null where budget_window_id=(select budget_window_id from ouf_mcp.attempt_admission_context where attempt_id=$1)`, d.AttemptID); err != nil {
		t.Fatal(err)
	}
	candidate.WindowBudget = req.WindowBudget
	if _, err = s.Reserve(ctx, candidate); !errors.Is(err, ErrBudgetPolicyMismatch) {
		t.Fatalf("legacy window silently received new quota: %v", err)
	}
	if replay, err := s.Reserve(ctx, req); err != nil || !replay.Replay {
		t.Fatalf("legacy replay unavailable: %+v %v", replay, err)
	}
	var reserved int64
	if err = s.Pool().QueryRow(ctx, `select reserved_tool_calls from ouf_mcp.budget_window where budget_window_id=(select budget_window_id from ouf_mcp.attempt_admission_context where attempt_id=$1)`, d.AttemptID).Scan(&reserved); err != nil || reserved != 1 {
		t.Fatalf("legacy accounting changed: %d %v", reserved, err)
	}
}

func TestWindowBudgetConcurrentAdmissionsRespectCap(t *testing.T) {
	s, ctx, checksum := concurrencyFixture(t)
	req := concurrencyRequest(checksum, uuid.NewString(), "v1:hmac-sha256:concurrent-budget")
	req.Window = time.Hour
	req.WindowBudget.ToolCalls = 2
	start := make(chan struct{})
	errs := make(chan error, 6)
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		candidate := req
		candidate.IdempotencyKey = uuid.NewString()
		candidate.SemanticFingerprint = fmt.Sprintf("v1:hmac-sha256:budget-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.Reserve(ctx, candidate)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	admitted := 0
	for err := range errs {
		if err == nil {
			admitted++
		} else if !errors.Is(err, ErrBudgetExhausted) && !serializableContention(err) {
			t.Fatal(err)
		}
	}
	var reserved int64
	if err := s.Pool().QueryRow(ctx, `select reserved_tool_calls from ouf_mcp.budget_window where tenant_id=$1`, req.Identity.TenantID).Scan(&reserved); err != nil {
		t.Fatal(err)
	}
	if admitted < 1 || admitted > 2 || reserved != int64(admitted) {
		t.Fatalf("concurrent budget exceeded/lost: admitted=%d reserved=%d", admitted, reserved)
	}
}
