package kernel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type analysisSDKStore struct {
	statusAdmission
	plans int
}

func (s *analysisSDKStore) PlanAnalysis(_ context.Context, p analysis.Plan) (analysis.View, error) {
	s.plans++
	b, covered, done := analysis.Calculate(p.Tasks)
	return analysis.View{ID: p.ID, Profile: p.Profile, Budget: b, Tasks: p.Tasks, CoveredColumns: covered, EvidenceColumns: done}, nil
}
func (*analysisSDKStore) ReadAnalysis(context.Context, analysis.Scope, string, string, int, int) (analysis.View, error) {
	return analysis.View{}, analysis.ErrScope
}
func (*analysisSDKStore) ReadAnalysisTask(context.Context, analysis.Scope, string, string, string) (analysis.Task, error) {
	return analysis.Task{}, analysis.ErrScope
}
func (*analysisSDKStore) ClaimAnalysisTask(context.Context, analysis.Scope, string, string, string) (analysis.Task, error) {
	return analysis.Task{}, analysis.ErrScope
}
func (*analysisSDKStore) FinishAnalysisTask(context.Context, analysis.Scope, string, string, string, analysis.Finish) (analysis.Task, error) {
	return analysis.Task{}, analysis.ErrScope
}

type analysisSDKAuth struct{}

func (analysisSDKAuth) Authorize(context.Context, orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	return orchestration.AuthorizationDecision{Allowed: true, DecisionRef: "fixture"}, nil
}

type analysisSDKGateway struct {
	asset, profile string
	calls          int
}

func (g *analysisSDKGateway) Execute(_ context.Context, r orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	g.calls++
	body, _ := json.Marshal(map[string]any{"assetId": g.asset, "profileId": g.profile, "profileVersion": 1, "columnCount": 1024, "schemaHash": "sha256:" + strings.Repeat("a", 64), "view": "COLUMNS", "offset": 0, "columns": []any{map[string]any{"name": "tipologia"}}})
	return orchestration.GatewayResponse{Status: 200, Body: body}, nil
}
func TestProfileAnalysisSDKClosedInputAndAuthoritativeProfile(t *testing.T) {
	snapshot, e := manifest.Load()
	if e != nil {
		t.Fatal(e)
	}
	cap, ok := analysis.Find(snapshot, "source.analysis.plan")
	if !ok || cap.PublicationState != "INACTIVE" {
		t.Fatal("candidate exposure")
	}
	for _, c := range snapshot.ToolEligible() {
		if strings.HasPrefix(c.ToolName, "source.analysis.") {
			t.Fatal("production exposure")
		}
	}
	store := &analysisSDKStore{}
	gateway := &analysisSDKGateway{asset: uuid.NewString(), profile: uuid.NewString()}
	service := &orchestration.Service{RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: store, Gateway: gateway, FingerprintKey: make([]byte, 32)}
	server := mcp.NewServer(&mcp.Implementation{Name: "analysis-fixture", Version: "1"}, nil)
	registerGovernedTool(server, cap, snapshot, service)
	handler := modernOnly(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true}))
	transport := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, v := range map[string]string{"X-OUF-Delegation": "opaque-proof", "X-OUF-Gateway-Verified": "true", "X-OUF-Service-Principal": "mcp", "X-OUF-Principal-ID": "analyst", "X-OUF-Tenant-ID": "tenant", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1"} {
			r.Header.Set(k, v)
		}
		handler.ServeHTTP(w, r)
	}))
	defer transport.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, e := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: transport.URL}, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer session.Close()
	args := map[string]any{"assetId": gateway.asset, "profileId": gateway.profile, "expectedRevision": 0, "work": []any{map[string]any{"tool": "semantic.search", "arguments": map[string]any{"q": "tipologia", "type": "PROPERTY"}, "columns": []int{1023}}}}
	result, e := session.CallTool(context.Background(), &mcp.CallToolParams{Name: cap.ToolName, Arguments: args})
	if e != nil || result.IsError || store.plans != 1 || gateway.calls != 1 {
		t.Fatal(result, e, store.plans, gateway.calls)
	}
	var view analysis.View
	if json.Unmarshal([]byte(result.Content[0].(*mcp.TextContent).Text), &view) != nil || view.Profile.ColumnCount != 1024 || view.Budget.PlannedCalls != 1 {
		t.Fatal(view)
	}
	args["columnCount"] = 999999
	result, e = session.CallTool(context.Background(), &mcp.CallToolParams{Name: cap.ToolName, Arguments: args})
	if e == nil && !result.IsError {
		t.Fatal("accepted caller count")
	}
	if gateway.calls != 1 || store.plans != 1 {
		t.Fatal("invalid input reached owner")
	}
}
