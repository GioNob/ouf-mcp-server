package httpclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type TokenSource interface {
	Token(context.Context) (string, error)
}

type StaticTokenSource string

func (s StaticTokenSource) Token(context.Context) (string, error) {
	token := strings.TrimSpace(string(s))
	if token == "" {
		return "", errors.New("workload token is empty")
	}
	return token, nil
}

type ClientCredentialsTokenSource struct {
	endpoint   *url.URL
	clientID   string
	secretFile string
	client     *http.Client
	mu         sync.Mutex
	token      string
	expiresAt  time.Time
	clock      func() time.Time
}

func NewClientCredentialsTokenSource(endpoint, clientID, secretFile string) (*ClientCredentialsTokenSource, error) {
	u, err := governed(endpoint)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(clientID) == "" {
		return nil, errors.New("OIDC client id is required")
	}
	if strings.TrimSpace(secretFile) == "" {
		return nil, errors.New("OIDC client secret file is required")
	}
	return &ClientCredentialsTokenSource{
		endpoint: u, clientID: clientID, secretFile: secretFile,
		client: sharedClient(), clock: time.Now,
	}, nil
}

func (s *ClientCredentialsTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	if s.token != "" && now.Add(30*time.Second).Before(s.expiresAt) {
		return s.token, nil
	}
	secretRaw, err := os.ReadFile(s.secretFile)
	if err != nil {
		return "", errors.New("OIDC client secret cannot be read")
	}
	secret := strings.TrimSpace(string(secretRaw))
	if secret == "" {
		return "", errors.New("OIDC client secret is empty")
	}
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", s.clientID)
	form.Set("client_secret", secret)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint.String(), strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	res, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		return "", errors.New("OIDC client credentials token request rejected")
	}
	var payload struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	decoder := json.NewDecoder(io.LimitReader(res.Body, 64<<10))
	if err := decoder.Decode(&payload); err != nil {
		return "", errors.New("invalid OIDC token response")
	}
	if strings.TrimSpace(payload.AccessToken) == "" || payload.ExpiresIn <= 0 {
		return "", errors.New("incomplete OIDC token response")
	}
	if payload.TokenType != "" && !strings.EqualFold(payload.TokenType, "Bearer") {
		return "", errors.New("unsupported OIDC token type")
	}
	s.token = payload.AccessToken
	s.expiresAt = now.Add(time.Duration(payload.ExpiresIn) * time.Second)
	return s.token, nil
}
