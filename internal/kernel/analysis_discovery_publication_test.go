package kernel

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type publicationDenyAuth struct{ calls atomic.Int32 }

func (a *publicationDenyAuth) Authorize(context.Context, orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	a.calls.Add(1)
	return orchestration.AuthorizationDecision{Allowed: false, DecisionRef: "fixture-denied"}, nil
}

type publicationStore struct {
	analysisSDKStore
	reads atomic.Int32
}

func (s *publicationStore) ReadAnalysis(context.Context, analysis.Scope, string, string, int, int) (analysis.View, error) {
	s.reads.Add(1)
	return analysis.View{}, analysis.ErrScope
}

func (s *publicationStore) ReadAnalysisTask(context.Context, analysis.Scope, string, string, string) (analysis.Task, error) {
	s.reads.Add(1)
	return analysis.Task{}, analysis.ErrScope
}

// Publication exposes the reviewed descriptors, never permission to execute.
// Use the ordinary factory, including picker mode used by the deployed server.
func TestPublishedAnalysisDiscoveryDenyBeforeOwnerOrStore(t *testing.T) {
	for _, governed := range []bool{false, true} {
		name := "unbound"
		if governed {
			name = "governed-picker-denied"
		}
		t.Run(name, func(t *testing.T) {
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			auth := &publicationDenyAuth{}
			store := &publicationStore{}
			gateway := &analysisSDKGateway{}
			service := &orchestration.Service{RequireDelegation: true, Auth: auth, Admission: store, Gateway: gateway, FingerprintKey: make([]byte, 32)}
			var handler http.Handler
			var err error
			if governed {
				handler, err = NewGovernedHTTPHandlerWithFilePicker(logger, service, "https://example.test/trusted-human/managed-files/")
			} else {
				handler, err = NewHTTPHandler(logger)
			}
			if err != nil {
				t.Fatal(err)
			}
			transport := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for k, v := range map[string]string{"X-OUF-Delegation": "opaque-proof", "X-OUF-Gateway-Verified": "true", "X-OUF-Service-Principal": "mcp", "X-OUF-Principal-ID": "analyst", "X-OUF-Tenant-ID": "tenant", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1"} {
					r.Header.Set(k, v)
				}
				handler.ServeHTTP(w, r)
			}))
			defer transport.Close()
			client := mcp.NewClient(&mcp.Implementation{Name: "publication-test", Version: "1"}, nil)
			session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: transport.URL}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			listed, err := session.ListTools(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			id := "11111111-1111-4111-8111-111111111111"
			inputs := map[string]map[string]any{
				"semantic.discovery.request":    {"requestedArtifactType": "PROPERTY", "intent": "tipologia", "preferredLanguages": []string{"it"}, "idempotencyKey": "publication-test-0001"},
				"semantic.discovery.status":     {"requestId": id},
				"semantic.discovery.candidates": {"requestId": id},
				"source.analysis.plan":          {"assetId": id, "profileId": id, "expectedRevision": 0, "work": []any{map[string]any{"tool": "semantic.search", "arguments": map[string]any{"q": "tipologia"}, "columns": []int{0}}}},
				"source.analysis.read":          {"analysisId": strings.Repeat("a", 64)},
				"source.analysis.step":          {"analysisId": strings.Repeat("a", 64), "taskId": strings.Repeat("b", 64)},
			}
			for tool, args := range inputs {
				found := 0
				for _, descriptor := range listed.Tools {
					if descriptor.Name == tool {
						found++
					}
				}
				if found != 1 {
					t.Fatalf("expected one published %s, got %d", tool, found)
				}
				result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
				if err != nil || !result.IsError {
					t.Fatalf("%s did not fail closed: %v %v", tool, result, err)
				}
			}
			if governed && auth.calls.Load() != 6 {
				t.Fatalf("expected six authorization denials, got %d", auth.calls.Load())
			}
			if gateway.calls != 0 || store.plans != 0 || store.reads.Load() != 0 {
				t.Fatal("denied publication reached owner or stored analysis")
			}
		})
	}
}
