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
	if g.Remote == nil {
		return orchestration.GatewayResponse{}, errors.New("REMOTE_GATEWAY_NOT_BOUND")
	}
	return g.Remote.Execute(ctx, in, timeout)
}
