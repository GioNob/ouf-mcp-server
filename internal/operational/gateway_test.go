package operational

import (
	"context"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type fakeRemote struct{ called bool }

func (f *fakeRemote) Execute(context.Context, orchestration.GatewayRequest, time.Duration) (orchestration.GatewayResponse, error) {
	f.called = true
	return orchestration.GatewayResponse{Status: 204}, nil
}

func TestRoutingGatewayAlwaysDelegatesSystemStatus(t *testing.T) {
	remote := &fakeRemote{}
	g := RoutingGateway{Remote: remote}
	res, err := g.Execute(context.Background(), orchestration.GatewayRequest{Owner: "mcp", CapabilityID: "ouf.system.status", AttemptID: "a", Identity: orchestration.Identity{TenantID: "tenant"}}, time.Second)
	if err != nil || res.Status != 204 || !remote.called {
		t.Fatalf("system status bypassed Gateway res=%+v err=%v remote=%v", res, err, remote.called)
	}
}

func TestRoutingGatewayDelegatesOtherCapabilities(t *testing.T) {
	remote := &fakeRemote{}
	g := RoutingGateway{Remote: remote}
	res, err := g.Execute(context.Background(), orchestration.GatewayRequest{Owner: "ingestion", CapabilityID: "ouf.operations.explain"}, time.Second)
	if err != nil || res.Status != 204 || !remote.called {
		t.Fatalf("remote dispatch res=%+v err=%v remote=%v", res, err, remote.called)
	}
}
