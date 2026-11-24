package httpclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientCredentialsTokenSourceCachesAndReloadsSecretOnRefresh(t *testing.T) {
	dir := t.TempDir()
	secretFile := filepath.Join(dir, "client-secret")
	if err := os.WriteFile(secretFile, []byte("secret-one\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		n := calls.Add(1)
		wantSecret := "secret-one"
		if n > 1 {
			wantSecret = "secret-two"
		}
		if r.Form.Get("grant_type") != "client_credentials" ||
			r.Form.Get("client_id") != "ouf-mcp-server" ||
			r.Form.Get("client_secret") != wantSecret {
			t.Fatalf("unexpected token form: %#v", r.Form)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"access_token":"token-%d","expires_in":60,"token_type":"Bearer"}`, n)
	}))
	defer server.Close()

	endpoint, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	source := &ClientCredentialsTokenSource{
		endpoint:   endpoint,
		clientID:   "ouf-mcp-server",
		secretFile: secretFile,
		client:     server.Client(),
		clock:      func() time.Time { return now },
	}

	first, err := source.Token(context.Background())
	if err != nil || first != "token-1" {
		t.Fatalf("first token=%q err=%v", first, err)
	}
	second, err := source.Token(context.Background())
	if err != nil || second != "token-1" || calls.Load() != 1 {
		t.Fatalf("cached token=%q calls=%d err=%v", second, calls.Load(), err)
	}

	if err := os.WriteFile(secretFile, []byte("secret-two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	now = now.Add(31 * time.Second)
	refreshed, err := source.Token(context.Background())
	if err != nil || refreshed != "token-2" || calls.Load() != 2 {
		t.Fatalf("refreshed token=%q calls=%d err=%v", refreshed, calls.Load(), err)
	}
}

func TestMandateTokenRequests(t *testing.T) { // MCP-58E-02
	const setting = "OUF_ANALYSIS_MANDATE_SCOPES"
	t.Setenv(setting, "")
	if err := os.Unsetenv(setting); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(file, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	const endpoint = "https://issuer.invalid/token"
	for _, custom := range []string{"", "fixture.issue fixture.status"} {
		want := "ouf.analysis.mandate.issue ouf.analysis.mandate.dispatch ouf.analysis.mandate.revoke ouf.analysis.mandate.status"
		if custom != "" {
			t.Setenv(setting, custom)
			want = custom
		}
		ordinary, err := NewMandateTokenSource(endpoint, "fixture", file)
		if err != nil {
			t.Fatal(err)
		}
		renewal, err := NewRenewalTokenSource(endpoint, "fixture", file)
		if err != nil {
			t.Fatal(err)
		}
		var requested []string
		client := &http.Client{Transport: statusRoundTrip(func(r *http.Request) (*http.Response, error) {
			scope := r.FormValue("scope")
			requested = append(requested, scope)
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"access_token":"fixture","expires_in":300}`))}, nil
		})}
		ordinary.client, renewal.client = client, client
		for _, source := range []*ClientCredentialsTokenSource{ordinary, renewal, ordinary, renewal} {
			if _, err := source.Token(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if len(requested) != 2 || requested[0] != want || requested[1] != "ouf.analysis.mandate.renewal-check" {
			t.Fatal(requested)
		}
	}
}

func TestClientCredentialsTokenSourceRejectsIncompleteResponse(t *testing.T) {
	dir := t.TempDir()
	secretFile := filepath.Join(dir, "client-secret")
	if err := os.WriteFile(secretFile, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"","expires_in":0}`))
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL)
	source := &ClientCredentialsTokenSource{
		endpoint:   endpoint,
		clientID:   "ouf-mcp-server",
		secretFile: secretFile,
		client:     server.Client(),
		clock:      time.Now,
	}
	if _, err := source.Token(context.Background()); err == nil {
		t.Fatal("incomplete token response accepted")
	}
}
