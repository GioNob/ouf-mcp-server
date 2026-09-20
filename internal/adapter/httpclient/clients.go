package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/GioNob/ouf-mcp-server/internal/recovery"
)

type AuthorizationClient struct {
	Endpoint    *url.URL
	Client      *http.Client
	TokenSource TokenSource
}

func NewAuthorization(endpoint string, token string) (*AuthorizationClient, error) {
	return NewAuthorizationWithTokenSource(endpoint, StaticTokenSource(token))
}
func NewAuthorizationWithTokenSource(endpoint string, source TokenSource) (*AuthorizationClient, error) {
	u, e := governed(endpoint)
	if e != nil {
		return nil, e
	}
	if source == nil {
		return nil, errors.New("workload token source is required")
	}
	return &AuthorizationClient{u, sharedClient(), source}, nil
}
func (c *AuthorizationClient) Authorize(ctx context.Context, in orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	body, _ := json.Marshal(in)
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, c.Endpoint.String(), bytes.NewReader(body))
	if e != nil {
		return orchestration.AuthorizationDecision{}, e
	}
	token, e := c.TokenSource.Token(ctx)
	if e != nil {
		return orchestration.AuthorizationDecision{}, e
	}
	headers(req, token, "", "", "")
	res, e := c.Client.Do(req)
	if e != nil {
		return orchestration.AuthorizationDecision{}, e
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		return orchestration.AuthorizationDecision{}, errors.New("authorization service denied request")
	}
	var out orchestration.AuthorizationDecision
	e = json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&out)
	return out, e
}

type GatewayClient struct {
	Endpoint    *url.URL
	Client      *http.Client
	TokenSource TokenSource
}

type RecoveryClient struct {
	Endpoint    *url.URL
	Client      *http.Client
	TokenSource TokenSource
}

func NewRecovery(endpoint, token string) (*RecoveryClient, error) {
	return NewRecoveryWithTokenSource(endpoint, StaticTokenSource(token))
}
func NewRecoveryWithTokenSource(endpoint string, source TokenSource) (*RecoveryClient, error) {
	u, e := governed(endpoint)
	if e != nil {
		return nil, e
	}
	if source == nil {
		return nil, errors.New("workload token source is required")
	}
	return &RecoveryClient{u, sharedClient(), source}, nil
}
func (c *RecoveryClient) QueryOutcome(ctx context.Context, in recovery.OwnerQuery) (recovery.OwnerEvidence, error) {
	body, _ := json.Marshal(in)
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, c.Endpoint.String(), bytes.NewReader(body))
	if e != nil {
		return recovery.OwnerEvidence{}, e
	}
	token, e := c.TokenSource.Token(ctx)
	if e != nil {
		return recovery.OwnerEvidence{}, e
	}
	headers(req, token, in.CorrelationID, "", "")
	res, e := c.Client.Do(req)
	if e != nil {
		return recovery.OwnerEvidence{}, e
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		return recovery.OwnerEvidence{}, errors.New("owner recovery query rejected")
	}
	var out recovery.OwnerEvidence
	e = json.NewDecoder(io.LimitReader(res.Body, 64<<10)).Decode(&out)
	return out, e
}

func NewGateway(endpoint string, token string) (*GatewayClient, error) {
	return NewGatewayWithTokenSource(endpoint, StaticTokenSource(token))
}
func NewGatewayWithTokenSource(endpoint string, source TokenSource) (*GatewayClient, error) {
	u, e := governed(endpoint)
	if e != nil {
		return nil, e
	}
	if source == nil {
		return nil, errors.New("workload token source is required")
	}
	return &GatewayClient{u, sharedClient(), source}, nil
}
func (c *GatewayClient) Execute(ctx context.Context, in orchestration.GatewayRequest, timeout time.Duration) (orchestration.GatewayResponse, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	body, _ := json.Marshal(in)
	endpoint := *c.Endpoint
	switch in.CapabilityID {
	case "ouf.operations.summary", "ouf.ingestion.operations.summary", "ouf.gateway.operations.summary":
		endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/" + in.CapabilityID
	case "authorization.permissions.read", "authorization.permissions.propose", "authorization.proposal.read":
		mode := map[string]string{"authorization.permissions.read": "read", "authorization.permissions.propose": "propose", "authorization.proposal.read": "status"}[in.CapabilityID]
		endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/authorization/" + mode
	}
	req, e := http.NewRequestWithContext(callCtx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if e != nil {
		return orchestration.GatewayResponse{}, e
	}
	token, e := c.TokenSource.Token(callCtx)
	if e != nil {
		return orchestration.GatewayResponse{}, e
	}
	headers(req, token, in.CorrelationID, in.IdempotencyKey, in.AttemptID)
	if in.Identity.Delegation != "" {
		req.Header.Set("X-OUF-Delegation", in.Identity.Delegation)
	}
	res, e := c.Client.Do(req)
	if e != nil {
		return orchestration.GatewayResponse{}, e
	}
	defer res.Body.Close()
	limit := in.MaxResultBytes
	if limit < 1 {
		limit = 1 << 20
	}
	payload, e := io.ReadAll(io.LimitReader(res.Body, limit+1))
	if e != nil {
		return orchestration.GatewayResponse{}, e
	}
	out := orchestration.GatewayResponse{Status: res.StatusCode, Body: payload, BackendRequestID: res.Header.Get("X-Backend-Request-ID")}
	if res.StatusCode/100 != 2 {
		var p orchestration.Problem
		_ = json.Unmarshal(payload, &p)
		p.RetryAfter = res.Header.Get("Retry-After")
		p.Status = res.StatusCode
		out.Problem = &p
	}
	return out, nil
}
func governed(raw string) (*url.URL, error) {
	u, e := url.Parse(raw)
	if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid governed service endpoint")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1")) {
		return nil, errors.New("governed endpoint requires HTTPS")
	}
	return u, nil
}
func sharedClient() *http.Client {
	return &http.Client{Transport: &http.Transport{MaxIdleConns: 32, MaxIdleConnsPerHost: 16, MaxConnsPerHost: 32, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: 10 * time.Second}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
func headers(r *http.Request, token, correlation, idempotency, attempt string) {
	r.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(token) != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	r.Header.Set("X-Correlation-ID", correlation)
	r.Header.Set("Idempotency-Key", idempotency)
	r.Header.Set("X-Tool-Attempt-ID", attempt)
}
