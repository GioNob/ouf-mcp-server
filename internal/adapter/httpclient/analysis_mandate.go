package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func (c *GatewayClient) MandateCall(ctx context.Context, action string, raw json.RawMessage, delegation string, limit int64, timeout time.Duration) (orchestration.MandateResponse, error) {
	var out orchestration.MandateResponse
	if (action != "issue" && action != "dispatch" && action != "revoke") || (action == "issue" && delegation == "") || (action == "dispatch" && delegation != "") ||
		c.Endpoint.Scheme != "https" || limit < 1 || limit > 1<<20 || timeout <= 0 || timeout > 30*time.Second {
		return out, errors.New("invalid analysis mandate invocation")
	}
	call, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	u := *c.Endpoint
	u.Path = "/internal/analysis/v1/mandates/" + action
	u.RawPath = ""
	req, err := http.NewRequestWithContext(call, http.MethodPost, u.String(), bytes.NewReader(raw))
	if err != nil {
		return out, err
	}
	token, err := c.TokenSource.Token(call)
	if err != nil || token == "" {
		return out, errors.New("workload credential unavailable")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	if delegation != "" {
		req.Header.Set("X-OUF-Delegation", delegation)
	}
	res, err := c.Client.Do(req)
	if err != nil {
		return out, orchestration.ErrMandateTransportUncertain
	}
	defer res.Body.Close()
	out = orchestration.MandateResponse{Status: res.StatusCode, Decision: res.Header.Get("X-OUF-Analysis-Decision"), Correlation: res.Header.Get("X-Correlation-ID"),
		Effect: res.Header.Get("X-OUF-Analysis-Effect"), RetryAfter: res.Header.Get("Retry-After")}
	out.Body, err = io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return out, orchestration.ErrMandateTransportUncertain
	}
	if int64(len(out.Body)) > limit {
		return out, errors.New("analysis gateway receipt incomplete")
	}
	return out, nil
}
