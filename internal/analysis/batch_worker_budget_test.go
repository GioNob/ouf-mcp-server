package analysis

import (
	"context"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type expiringBatchAuthority struct{ now *time.Time }

func (a expiringBatchAuthority) Authorize(context.Context, orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	*a.now = (*a.now).Add(2 * time.Second)
	return orchestration.AuthorizationDecision{}, context.DeadlineExceeded
}

func TestBatchBudgetExpiresDuringAuthorityCheck(t *testing.T) {
	snapshot, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	worker := BatchWorker{Manifest: snapshot, Service: &orchestration.Service{Auth: expiringBatchAuthority{&now}}, Now: func() time.Time { return now }}
	job := BatchWork{Stage: BatchPublished, Columns: []BatchWorkColumn{
		{Ordinal: 0, RetryStarted: now.Add(-batchBudget + time.Second), RetryAttempts: 1},
		{Ordinal: 1, RetryStarted: now, RetryAttempts: 1},
	}}
	result, _, err := worker.execute(t.Context(), orchestration.Identity{ActorType: "HUMAN", Delegation: "fixture", ServicePrincipalID: "s", PrincipalID: "p", TenantID: "t"}, "", job, BatchLimits{})
	if err != nil || len(result) != 2 || result[0].Outcome.Reason != "RETRY_BUDGET_EXHAUSTED" || result[1].Outcome.Reason != "ANALYSIS_AUTHORITY_UNAVAILABLE" {
		t.Fatalf("incorrect durable stop causes: %+v, %v", result, err)
	}
}
