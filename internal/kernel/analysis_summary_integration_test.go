package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type summaryAuthority struct{ revoked atomic.Bool }

func (a *summaryAuthority) Authorize(context.Context, orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	return orchestration.AuthorizationDecision{Allowed: !a.revoked.Load(), DecisionRef: "fixture:summary", DecisionCode: "NO_APPLICABLE_GRANT"}, nil
}

type summaryOwner struct{ request string }

func (o summaryOwner) Execute(_ context.Context, r orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	var body string
	switch r.CapabilityID {
	case "ouf.semantic.discovery":
		body = fmt.Sprintf(`{"state":"PENDING","requestId":%q}`, o.request)
	case "ouf.semantic.discovery.status":
		body = fmt.Sprintf(`{"state":"SUCCEEDED","requestId":%q,"candidateCount":1,"completedCandidateCount":1}`, o.request)
	case "ouf.semantic.discovery.candidates":
		body = fmt.Sprintf(`{"state":"SUCCEEDED","requestId":%q,"completedCandidateCount":1,"candidates":[{"candidate_id":"22222222-2222-4222-8222-222222222222"}]}`, o.request)
	default:
		return orchestration.GatewayResponse{}, fmt.Errorf("unexpected owner capability %s", r.CapabilityID)
	}
	if strings.Contains(string(r.Arguments), "analysisId") {
		return orchestration.GatewayResponse{}, fmt.Errorf("local selector sent to owner")
	}
	return orchestration.GatewayResponse{Status: 200, Body: []byte(body)}, nil
}

// With MCP_TEST_DATABASE_URL set, repeat against the same database:
//
//	go test ./internal/kernel/ -run '^TestSummarySDKPostgresV1EvidenceAndRevocation$' -count=20
func TestSummarySDKPostgresV1EvidenceAndRevocation(t *testing.T) {
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL required")
	}
	t.Setenv("OUF_ANALYSIS_SUMMARY_PAGE_BYTES", "4096")
	ctx := context.Background()
	store, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err = postgres.Migrate(ctx, store.Pool()); err != nil {
		t.Fatal(err)
	}
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	sum, _ := m.Checksum()
	raw, _ := m.CanonicalPayload()
	if err = store.EnsureManifest(ctx, sum, m.Version, "mcp-manifest-v1", raw); err != nil {
		t.Fatal(err)
	}
	owner := summaryOwner{uuid.NewString()}
	auth := &summaryAuthority{}
	service := &orchestration.Service{Auth: auth, RequireDelegation: true, Admission: store, Gateway: owner, FingerprintKey: make([]byte, 32)}
	scope := analysis.Scope{Service: "ouf-chatgpt", Principal: uuid.NewString(), Tenant: "tenant-a"}
	session := revisionSessionForScope(t, service, scope, "ouf.managed-source.preview ouf.semantic.discovery ouf.semantic.discovery.status ouf.semantic.discovery.candidates")
	profile := analysis.Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), Version: 1, SchemaHash: "sha256:" + strings.Repeat("a", 64), ColumnCount: 128}
	columns := make([]int, profile.ColumnCount)
	headers := make([]string, len(columns))
	for i := range columns {
		columns[i], headers[i] = i, "fixture"
	}
	work := analysis.Work{Tool: "semantic.discovery.request", Columns: columns, Arguments: json.RawMessage(`{"intent":"fixture","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`)}
	plan, err := analysis.Build(scope, profile, m, 0, []analysis.Work{work})
	if err != nil {
		t.Fatal(err)
	}
	plan.Headers, plan.Onboarding = headers, true
	if _, err = store.PlanAnalysis(ctx, plan); err != nil {
		t.Fatal(err)
	}
	call := func(tool string, args nextstep.Object, failed bool) nextstep.Object {
		t.Helper()
		r, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
		if err != nil || r.IsError != failed {
			t.Fatal(tool, r, err)
		}
		if tool == "source.analysis.read" && args["view"] != nil {
			b, _ := json.Marshal(r)
			if len(b) > 4096 {
				t.Fatal("wire limit", len(b))
			}
		}
		return ownerObject(r)
	}
	call("source.analysis.step", nextstep.Object{"analysisId": plan.ID, "taskId": plan.Tasks[0].ID}, false)
	status := nextstep.Object{"analysisId": plan.ID, "view": "STATUS"}
	if got := call("source.analysis.read", status, false); got["state"] != "RUNNING" || got["items"] != nil || len(got) != 6 {
		t.Fatal(got)
	}
	args := nextstep.Object{"analysisId": plan.ID, "requestId": owner.request}
	call("semantic.discovery.status", args, false)
	args["includeState"] = true
	call("semantic.discovery.candidates", args, false)
	if got := call("source.analysis.read", status, false); got["state"] != "COMPLETE" {
		t.Fatal(got)
	}
	first := call("source.analysis.read", nextstep.Object{"analysisId": plan.ID, "view": "SUMMARY", "limit": 128}, false)
	items := first["items"].([]any)
	if len(items) == 0 || len(items) >= 128 || first["nextCursor"] == nil || items[0].(nextstep.Object)["state"] != "CANDIDATES_READY" {
		t.Fatal(first)
	}
	next := first["next"].(nextstep.Object)["arguments"].(nextstep.Object)
	auth.revoked.Store(true)
	denied := call("source.analysis.read", next, true)
	if len(denied) != 2 || denied["code"] != "NO_APPLICABLE_GRANT" {
		t.Fatal(denied)
	}
	auth.revoked.Store(false)
	token := next["cursor"].(string)
	expired, err := analysis.OpenPosition(token, service.FingerprintKey, scope, plan.ID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	expired.Expires = time.Now().Unix() - 1
	readCursor := func(value string) nextstep.Object {
		return call("source.analysis.read", nextstep.Object{"analysisId": plan.ID, "view": "SUMMARY", "cursor": value}, true)
	}
	if got := readCursor(expired.Token(service.FingerprintKey, scope, plan.ID)); got["code"] != "ANALYSIS_CURSOR_EXPIRED" {
		t.Fatal(got)
	}
	if got := readCursor(token + "A"); got["code"] != "ANALYSIS_CURSOR_INVALID" {
		t.Fatal(got)
	}
	second := call("source.analysis.read", next, false)
	if second["changeSeq"] != first["changeSeq"] || second["items"].([]any)[0].(nextstep.Object)["ordinal"] != json.Number(fmt.Sprint(len(items))) {
		t.Fatal(second)
	}
}
