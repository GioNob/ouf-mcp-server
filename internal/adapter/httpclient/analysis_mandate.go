package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func (c *GatewayClient) MandateCall(ctx context.Context, action string, raw json.RawMessage, delegation string, limit int64, timeout time.Duration) (orchestration.MandateResponse, error) {
	out, err := c.mandateCall(ctx, action, raw, delegation, limit, timeout)
	return out.MandateResponse, err
}

func (c *GatewayClient) DispatchMandate(ctx context.Context, raw json.RawMessage, limit int64, timeout time.Duration) (analysis.DispatchResponse, error) {
	return c.mandateCall(ctx, "dispatch", raw, "", limit, timeout)
}

func (c *GatewayClient) mandateCall(ctx context.Context, action string, raw json.RawMessage, delegation string, limit int64, timeout time.Duration) (analysis.DispatchResponse, error) {
	var out analysis.DispatchResponse
	if (action != "issue" && action != "dispatch" && action != "revoke" && action != "status" && action != "renewal-check") || ((action == "issue" || action == "renewal-check") && delegation == "") || (action == "renewal-check" && c.Endpoint.Path != analysis.RenewalCheckPath) || ((action == "dispatch" || action == "status") && delegation != "") ||
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
	out.MandateResponse = orchestration.MandateResponse{Status: res.StatusCode, Decision: res.Header.Get("X-OUF-Analysis-Decision"), Correlation: res.Header.Get("X-Correlation-ID"),
		Effect: res.Header.Get("X-OUF-Analysis-Effect"), RetryAfter: res.Header.Get("Retry-After")}
	for _, header := range res.Header.Values("X-OUF-Analysis-Stale-Cause") {
		for _, token := range strings.Split(header, ",") {
			cause := strings.TrimSpace(token)
			if cause == "REVOKED" || (cause == "SUPERSEDED" && out.StaleCause == "") {
				out.StaleCause = cause
			}
		}
	}
	out.Body, err = io.ReadAll(io.LimitReader(res.Body, limit+1))
	if err != nil {
		return out, orchestration.ErrMandateTransportUncertain
	}
	if int64(len(out.Body)) > limit {
		return out, errors.New("analysis gateway receipt incomplete")
	}
	return out, nil
}

func (c *GatewayClient) AuthorizeRunRenewal(ctx context.Context, i orchestration.Identity, check analysis.RunRenewalCheck) (analysis.RenewalDecision, error) {
	var out analysis.RenewalDecision
	if i.ActorType != "HUMAN" || i.Delegation == "" {
		return out, orchestration.ErrUnauthorized
	}
	command, err := json.Marshal(check.Command)
	if err != nil {
		return out, err
	}
	if _, err = analysis.ParseRenewalCommand(command); err != nil {
		return out, err
	}
	wire, err := check.Wire()
	if err != nil {
		return out, err
	}
	reply, err := c.MandateCall(ctx, "renewal-check", wire, i.Delegation, analysis.RenewalBodyMax, 3*time.Second)
	if err != nil {
		return out, analysis.ErrIssueUnavailable
	}
	if reply.Status != 200 {
		var p struct{ Code string }
		if !analysis.ValidEvidenceJSON(reply.Body) || json.Unmarshal(reply.Body, &p) != nil {
			return out, analysis.ErrIssueUnavailable
		}
		for _, e := range []struct {
			status int
			code   string
			cause  error
		}{
			{400, "ANALYSIS_INPUT_INVALID", analysis.ErrInvalid}, {403, "ANALYSIS_AUTHORITY_DENIED", orchestration.ErrUnauthorized},
			{404, "ANALYSIS_MANDATE_UNKNOWN", analysis.ErrScope}, {409, "ANALYSIS_STALE_EXECUTION", analysis.ErrFenced},
			{410, "ANALYSIS_MANDATE_EXPIRED", analysis.ErrMandateExpired}, {429, "ANALYSIS_BACKOFF", analysis.ErrRunPressure}, {503, "ANALYSIS_AUTHORITY_UNAVAILABLE", analysis.ErrIssueUnavailable},
		} {
			if reply.Status == e.status && p.Code == e.code {
				return out, &analysis.RenewalBackoff{Cause: e.cause, RetryAfter: reply.RetryAfter}
			}
		}
		return out, analysis.ErrIssueUnavailable
	}
	return analysis.DecodeRenewalDecision(reply.Body, wire)
}
