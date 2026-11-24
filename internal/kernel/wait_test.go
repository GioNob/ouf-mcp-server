package kernel

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type revocableRead struct {
	denied atomic.Bool
	checks atomic.Int32
}

func (a *revocableRead) Authorize(context.Context, orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	a.checks.Add(1)
	return orchestration.AuthorizationDecision{Allowed: !a.denied.Load(), DecisionRef: "fixture", DecisionCode: "NO_APPLICABLE_GRANT"}, nil
}

// PET-mcp-168. Revoke after emitting a signed reference but before its next
// observation. The governed read must deny before reaching the owner.
func TestWaitReauthorizesAfterRevocation(t *testing.T) {
	owner := &nextGateway{}
	auth := &revocableRead{}
	service := &orchestration.Service{RequireDelegation: true, Auth: auth, Admission: &statusAdmission{}, Gateway: owner, FingerprintKey: make([]byte, 32)}
	session := revisionSession(t, service, "ouf.managed-source.onboarding.review.prepare ouf.managed-source.onboarding.read")
	params := &mcp.CallToolParams{Name: "source.onboarding.review.prepare", Arguments: nextstep.Object{
		"sourceId": "teatri", "onboardingVersionId": "11111111-1111-4111-8111-111111111111", "expectedVersion": 1, "commandKey": "review-fixture-1",
	}}
	result, err := session.CallTool(context.Background(), params)
	if err != nil {
		t.Fatal(err)
	}
	contract := nextContract(t)
	first := checkNext(t, contract, result)
	if first["action"] != "WAIT_FOR_HUMAN" {
		t.Fatal(first)
	}
	auth.denied.Store(true)
	before := auth.checks.Load()
	result, err = session.CallTool(context.Background(), &mcp.CallToolParams{Name: "ouf.wait", Arguments: first["arguments"]})
	if err != nil {
		t.Fatal(err)
	}
	next := checkNext(t, contract, result)
	if next["reasonCode"] != "NO_APPLICABLE_GRANT" || auth.checks.Load() <= before || owner.reads.Load() != 0 {
		t.Fatal("revocation did not fence the owner read", next)
	}
}

func TestWaitAdmissionBoundsConcurrentCalls(t *testing.T) {
	w := &waitService{principalLimit: 2, totalLimit: 100, active: map[string]int{}}
	var admitted atomic.Int32
	var group sync.WaitGroup
	for range 100 {
		group.Go(func() {
			if w.admit("same-scope") {
				admitted.Add(1)
			}
		})
	}
	group.Wait()
	if admitted.Load() != 2 {
		t.Fatal("principal limit", admitted.Load())
	}
	w.release("same-scope")
	w.release("same-scope")
	if w.total != 0 || len(w.active) != 0 {
		t.Fatal("active scope leaked")
	}
}
