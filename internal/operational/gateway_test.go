package operational

import (
	"context"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type fakeSelf struct{ called bool }

func (f *fakeSelf) SystemStatus(context.Context, orchestration.Identity) ([]byte, error) {
	f.called = true
	return []byte(`{"status":"HEALTHY"}`), nil
}

type fakeRemote struct{ called bool }

func (f *fakeRemote) Execute(context.Context, orchestration.GatewayRequest, time.Duration) (orchestration.GatewayResponse, error) {
	f.called = true
	return orchestration.GatewayResponse{Status: 204}, nil
}

func TestRoutingGatewayKeepsMCPStatusLocal(t *testing.T) {
	self := &fakeSelf{}
	remote := &fakeRemote{}
	g := RoutingGateway{Remote: remote, Self: self}
	res, err := g.Execute(context.Background(), orchestration.GatewayRequest{Owner: "mcp", CapabilityID: "ouf.system.status", AttemptID: "a", Identity: orchestration.Identity{TenantID: "tenant"}}, time.Second)
	if err != nil || res.Status != 200 || !self.called || remote.called {
		t.Fatalf("local dispatch res=%+v err=%v self=%v remote=%v", res, err, self.called, remote.called)
	}
}

func TestRoutingGatewayDelegatesOtherCapabilities(t *testing.T) {
	self := &fakeSelf{}
	remote := &fakeRemote{}
	g := RoutingGateway{Remote: remote, Self: self}
	res, err := g.Execute(context.Background(), orchestration.GatewayRequest{Owner: "ingestion", CapabilityID: "ouf.operations.explain"}, time.Second)
	if err != nil || res.Status != 204 || self.called || !remote.called {
		t.Fatalf("remote dispatch res=%+v err=%v self=%v remote=%v", res, err, self.called, remote.called)
	}
}
