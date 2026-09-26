package kernel

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func testServer(t *testing.T) *httptest.Server {
	t.Helper()
	h, err := NewHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-OUF-Gateway-Verified", "true")
		h.ServeHTTP(w, r)
	}))
}

func TestOfficialClientUsesModernStatelessDiscovery(t *testing.T) {
	ts := testServer(t)
	defer ts.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "mcp-1a-test", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: ts.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	result := session.InitializeResult()
	if result == nil || result.ProtocolVersion != ProtocolVersion {
		t.Fatalf("discovery negotiated %#v", result)
	}
	tools, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	gotNames := make([]string, 0, len(tools.Tools))
	for _, tool := range tools.Tools {
		gotNames = append(gotNames, tool.Name)
		schema, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(strings.ToLower(string(schema)), `"sql"`) {
			t.Fatalf("forbidden query surface in %s schema: %s", tool.Name, schema)
		}
	}
	sort.Strings(gotNames)
	wantNames := []string{
		"authorization.permissions.propose",
		"authorization.permissions.read",
		"authorization.proposal.read",
		"ouf.ingestion.history",
		"ouf.ingestion.status",
		"ouf.operations.explain",
		"ouf.operations.incidents",
		"ouf.operations.summary",
		"ouf.system.status",
		"source.file.preview",
		"source.file.profile",
		"source.onboarding.create",
		"urban.object.related_search",
	}
	if strings.Join(gotNames, ",") != strings.Join(wantNames, ",") {
		t.Fatalf("tools/list names=%v want=%v", gotNames, wantNames)
	}
	call, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "urban.object.related_search", Arguments: map[string]any{
		"anchorObjectId": "fd971091-2b0d-4daf-977a-81509056315a", "anchorTypeCode": "DEHOR", "relationIri": "https://example.test/relatedTo", "direction": "OUTBOUND", "targetTypeCodes": []string{"CIVICO"}, "limit": 10,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !call.IsError || len(call.Content) != 1 {
		t.Fatalf("unbound tool must fail closed: %#v", call)
	}
	invalid, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "urban.object.related_search", Arguments: map[string]any{"sql": "select 1"}})
	if err != nil {
		t.Fatal(err)
	}
	if !invalid.IsError {
		t.Fatalf("closed schema accepted undeclared input: %#v", invalid)
	}
}

func TestDiscoverResponseDoesNotCreateTransportSession(t *testing.T) {
	ts := testServer(t)
	defer ts.Close()
	body := `{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/clientCapabilities":{},"io.modelcontextprotocol/clientInfo":{"name":"raw-test","version":"1"},"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}`
	req, _ := http.NewRequest(http.MethodPost, ts.URL, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Protocol-Version", ProtocolVersion)
	req.Header.Set("Mcp-Method", "server/discover")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, b)
	}
	if got := resp.Header.Get("Mcp-Session-Id"); got != "" {
		t.Fatalf("stateless response emitted transport session %q", got)
	}
}

func TestModernProfileRejectsLegacyAndTransportSessions(t *testing.T) {
	ts := testServer(t)
	defer ts.Close()
	cases := []struct {
		name, method, version, session string
		want                           int
	}{
		{"legacy", http.MethodPost, "2025-11-25", "", http.StatusBadRequest},
		{"session", http.MethodPost, ProtocolVersion, "client-session", http.StatusBadRequest},
		{"get", http.MethodGet, ProtocolVersion, "", http.StatusMethodNotAllowed},
		{"delete", http.MethodDelete, ProtocolVersion, "", http.StatusMethodNotAllowed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, ts.URL, bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{}}`))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json")
			req.Header.Set("Mcp-Protocol-Version", tc.version)
			req.Header.Set("Mcp-Method", "server/discover")
			if tc.session != "" {
				req.Header.Set("Mcp-Session-Id", tc.session)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.want {
				b, _ := io.ReadAll(resp.Body)
				t.Fatalf("status %d, want %d: %s", resp.StatusCode, tc.want, b)
			}
		})
	}
}

func TestRequiredProtocolHeadersFailClosed(t *testing.T) {
	ts := testServer(t)
	defer ts.Close()
	for _, missing := range []string{"version", "method"} {
		t.Run(missing, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodPost, ts.URL, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{}}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Mcp-Protocol-Version", ProtocolVersion)
			req.Header.Set("Mcp-Method", "server/discover")
			if missing == "version" {
				req.Header.Del("Mcp-Protocol-Version")
			} else {
				req.Header.Del("Mcp-Method")
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status %d", resp.StatusCode)
			}
		})
	}
}
