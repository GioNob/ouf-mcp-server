package postgres

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestDebtAdmissionReadsActiveDebt(t *testing.T) {
	s, ctx, checksum := concurrencyFixture(t)
	tenant := "tenant-debt-" + uuid.NewString()
	seed := concurrencyRequest(checksum, tenant, "v1:hmac-sha256:"+fmt.Sprintf("%064x", 21))
	seed.WindowBudget.DistinctObjects = 2
	reserved, err := s.Reserve(ctx, seed)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Dispatch(ctx, reserved.AttemptID, "backend-"+uuid.NewString(), -time.Second, reserved.LockVersion); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	claimed, err := s.MarkStaleAndClaim(ctx, now, now.Add(-time.Minute), "worker", time.Minute, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	var windowID uuid.UUID
	if err := s.Pool().QueryRow(ctx, `select budget_window_id from ouf_mcp.attempt_admission_context where attempt_id=$1`, reserved.AttemptID).Scan(&windowID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `update ouf_mcp.budget_window set current_unresolved_object_debt_total=0 where budget_window_id=$1`, windowID); err != nil {
		t.Fatal(err)
	}

	candidate := concurrencyRequest(checksum, tenant, "v1:hmac-sha256:"+fmt.Sprintf("%064x", 22))
	candidate.WindowBudget = seed.WindowBudget
	if _, err := s.Reserve(ctx, candidate); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("expected budget exhaustion from ACTIVE debt, got %v", err)
	}
}
