package httpclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

const batchMandatePath = "/internal/analysis-batch/v1/"

type BatchMandateClient struct {
	endpoint string
	client   *http.Client
}

func NewBatchMandateClient(endpoint, caFile, certFile, keyFile string) (*BatchMandateClient, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" ||
		(u.Path != "" && u.Path != "/" && u.Path != batchMandatePath) {
		return nil, errors.New("invalid batch mandate Gateway endpoint")
	}
	config, err := BatchMutualTLS(caFile, certFile, keyFile)
	if err != nil {
		return nil, err
	}
	u.Path = batchMandatePath
	return &BatchMandateClient{u.String(), &http.Client{
		Transport: &http.Transport{TLSClientConfig: config, MaxIdleConnsPerHost: 4, IdleConnTimeout: 30 * time.Second},
		Timeout:   4 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}, nil
}

func BatchMutualTLS(caFile, certFile, keyFile string) (*tls.Config, error) {
	ca, err := os.ReadFile(caFile)
	roots := x509.NewCertPool()
	if err != nil || !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid batch mandate CA")
	}
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, errors.New("invalid batch mandate TLS identity")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{pair}}, nil
}

func (c *BatchMandateClient) Prepare(ctx context.Context, proof string) (analysis.BatchMandateView, error) {
	return c.command(ctx, "prepare", map[string]string{"startProof": proof})
}

func (c *BatchMandateClient) Issue(ctx context.Context, input analysis.BatchMandateIssue) (analysis.BatchMandateView, error) {
	return c.command(ctx, "issue", input)
}

func (c *BatchMandateClient) Read(ctx context.Context, key analysis.BatchMandateKey) (analysis.BatchMandateView, error) {
	return c.command(ctx, "read", key)
}

func (c *BatchMandateClient) Expire(ctx context.Context, key analysis.BatchMandateKey, version int) (analysis.BatchMandateView, error) {
	return c.command(ctx, "expire", struct {
		analysis.BatchMandateKey
		Version int `json:"expectedVersion"`
	}{key, version})
}

func (c *BatchMandateClient) command(ctx context.Context, action string, input any) (analysis.BatchMandateView, error) {
	response, _, err := c.post(ctx, action, input, 32768)
	var view analysis.BatchMandateView
	if err == nil && response.Problem != nil {
		err = &analysis.BatchMandateError{Observation: orchestration.ObserveOwner(response, nil, time.Now().UTC())}
	}
	if err == nil && (response.Status != http.StatusOK || json.Unmarshal(response.Body, &view) != nil) {
		err = errors.New("invalid batch mandate Gateway response")
	}
	return view, err
}

func (c *BatchMandateClient) Dispatch(ctx context.Context, input analysis.BatchMandateDispatch, limit int64) (orchestration.GatewayResponse, string, error) {
	return c.post(ctx, "dispatch", input, limit)
}

func (c *BatchMandateClient) post(ctx context.Context, action string, input any, limit int64) (orchestration.GatewayResponse, string, error) {
	raw, err := json.Marshal(input)
	if err != nil || len(raw) > 65536 {
		return orchestration.GatewayResponse{}, "", analysis.ErrInvalid
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+action, bytes.NewReader(raw))
	if err != nil {
		return orchestration.GatewayResponse{}, "", analysis.ErrInvalid
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.client.Do(req)
	if err != nil {
		return batchNetworkFailure(err)
	}
	defer res.Body.Close()
	out := orchestration.GatewayResponse{Status: res.StatusCode, RetryAfter: res.Header.Get("Retry-After")}
	readLimit := limit
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		readLimit = 16384
	}
	out.Body, err = io.ReadAll(io.LimitReader(res.Body, readLimit+1))
	if err != nil {
		return batchNetworkFailure(err)
	}
	if int64(len(out.Body)) > readLimit {
		return orchestration.GatewayResponse{}, "", orchestration.ErrResultLimit
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var body struct{ Code string }
		_ = json.Unmarshal(out.Body, &body)
		out.Problem = &orchestration.Problem{Status: res.StatusCode, Code: body.Code, RetryAfter: out.RetryAfter}
	}
	var binding string
	values := res.Header.Values(analysis.BatchDiscoveryBindingHeader)
	if len(values) == 1 && len(values[0]) <= 16384 && !strings.Contains(values[0], ",") {
		binding = values[0]
	}
	return out, binding, nil
}

func batchNetworkFailure(err error) (orchestration.GatewayResponse, string, error) {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ETIMEDOUT) || errors.Is(err, syscall.EPIPE) {
		p := &orchestration.Problem{Status: 503, Code: "MCP_BATCH_GATEWAY_INTERRUPTED"}
		return orchestration.GatewayResponse{Status: p.Status, Problem: p}, "", nil
	}
	return orchestration.GatewayResponse{}, "", errors.New("batch mandate Gateway channel unavailable")
}
