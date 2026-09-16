package httpclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"

	"github.com/GioNob/ouf-mcp-server/internal/authorization"
)

type PolicyBundleClient struct {
	Endpoint      *url.URL
	Client        *http.Client
	WorkloadToken string
}

func NewPolicyBundle(endpoint, token string) (*PolicyBundleClient, error) {
	u, err := governed(endpoint)
	if err != nil {
		return nil, err
	}
	return &PolicyBundleClient{Endpoint: u, Client: sharedClient(), WorkloadToken: token}, nil
}

func (c *PolicyBundleClient) FetchActive(ctx context.Context) (authorization.ActivePolicyBundle, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.Endpoint.String(), nil)
	if err != nil {
		return authorization.ActivePolicyBundle{}, err
	}
	headers(req, c.WorkloadToken, "", "", "")
	res, err := c.Client.Do(req)
	if err != nil {
		return authorization.ActivePolicyBundle{}, err
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		return authorization.ActivePolicyBundle{}, errors.New("authorization policy bundle fetch rejected")
	}
	var out authorization.ActivePolicyBundle
	if err := json.NewDecoder(io.LimitReader(res.Body, 2<<20)).Decode(&out); err != nil {
		return authorization.ActivePolicyBundle{}, err
	}
	return out, nil
}
