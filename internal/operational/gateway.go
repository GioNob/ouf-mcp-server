package operational

import (
	"context"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type SelfStatusProvider interface {
	SystemStatus(context.Context, orchestration.Identity) ([]byte, error)
}

type RoutingGateway struct {
	Remote orchestration.GatewayPort
	Self   SelfStatusProvider
}

func (g RoutingGateway) Execute(ctx context.Context, in orchestration.GatewayRequest, timeout time.Duration) (orchestration.GatewayResponse, error) {
	if in.Owner == "mcp" && in.CapabilityID == "ouf.system.status" {
		if g.Self == nil {
			return orchestration.GatewayResponse{}, errors.New("MCP_SELF_STATUS_NOT_BOUND")
		}
		body, err := g.Self.SystemStatus(ctx, in.Identity)
		if err != nil {
			return orchestration.GatewayResponse{}, err
		}
		return orchestration.GatewayResponse{Status: 200, Body: body, BackendRequestID: in.AttemptID}, nil
	}
	if g.Remote == nil {
		return orchestration.GatewayResponse{}, errors.New("REMOTE_GATEWAY_NOT_BOUND")
	}
	return g.Remote.Execute(ctx, in, timeout)
}
