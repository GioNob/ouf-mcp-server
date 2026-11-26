package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
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

func TestLostEmptyCandidateDecisionResumesFromStatus(t *testing.T) {
	store, m := summaryTestStore(t)
	ctx := context.Background()
	const scopes = "ouf.managed-source.preview ouf.semantic.discovery ouf.semantic.discovery.status ouf.semantic.discovery.candidates ouf.semantic.propose ouf.managed-source.onboarding.create"
	for _, tc := range []struct {
		choice  string
		expired bool
	}{
		{"source.analysis.plan", false}, {"semantic.propose", false}, {"source.onboarding.create", false},
		{"semantic.propose", true}, {"source.onboarding.create", true},
	} {
		choice := tc.choice
		t.Run(fmt.Sprintf("%s/expired=%t", choice, tc.expired), func(t *testing.T) {
			scope := analysis.Scope{Service: "ouf-chatgpt", Principal: uuid.NewString(), Tenant: "tenant-a"}
			profile := analysis.Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), Version: 1,
				SchemaHash: "sha256:" + strings.Repeat("a", 64), ColumnCount: 1}
			work := analysis.Work{Tool: "semantic.discovery.request", Columns: []int{0},
				Arguments: json.RawMessage(`{"intent":"fixture","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`)}
			plan, err := analysis.Build(scope, profile, m, 0, []analysis.Work{work})
			if err != nil {
				t.Fatal(err)
			}
			plan.Headers = []string{"fixture"}
			if _, err = store.PlanAnalysis(ctx, plan); err != nil {
				t.Fatal(err)
			}
			owner := &interruptedDiscovery{}
			service := &orchestration.Service{Auth: &summaryAuthority{}, Admission: store, Gateway: owner, FingerprintKey: make([]byte, 32)}
			// Each call starts a fresh client and server. After a lost reply the
			// only input to resume is the analysis ID.
			call := func(tool string, args nextstep.Object) nextstep.Object {
				t.Helper()
				sessionService := *service
				if tool == "source.analysis.read" {
					// Keep the local STATUS window open. Owner calls, decision
					// reconciliation and every summary query use the real store.
					sessionService.Admission = publishedPETStore{store}
				}
				session := revisionSessionForScope(t, &sessionService, scope, scopes)
				r, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
				if err != nil || r.IsError {
					t.Fatal(tool, r, err)
				}
				return ownerObject(r)
			}
			status := func() nextstep.Object {
				return call("source.analysis.read", nextstep.Object{"analysisId": plan.ID, "view": "STATUS"})
			}
			resume := func(tool string) nextstep.Object {
				t.Helper()
				next := status()["next"].(nextstep.Object)
				if next["action"] != "CALL_TOOL" || next["tool"] != tool {
					t.Fatal("unexpected continuation", next)
				}
				return next["arguments"].(nextstep.Object)
			}
			request := uuid.NewString()
			owner.response = orchestration.GatewayResponse{Status: 200, Body: []byte(fmt.Sprintf(`{"state":"PENDING","requestId":%q}`, request))}
			call("source.analysis.step", resume("source.analysis.step"))
			owner.response.Body = []byte(`{"state":"SUCCEEDED","candidateCount":0}`)
			if tc.expired {
				owner.response.Body = []byte(`{"state":"SUCCEEDED","candidateCount":1,"completedCandidateCount":1}`)
			}
			call("semantic.discovery.status", resume("semantic.discovery.status"))
			owner.response.Body = []byte(`{"state":"SUCCEEDED","candidateCount":0,"candidates":[]}`)
			options := map[string]nextstep.Object{}
			for page := range 2 {
				// The first page's choices are retained only as a test oracle.
				// Recover the next call from STATUS, without using the lost reply.
				hint := status()["next"].(nextstep.Object)
				if hint["action"] != "CALL_TOOL" || hint["tool"] != "semantic.discovery.candidates" ||
					(page == 1 && hint["reasonCode"] != analysis.DiscoveryDecisionRequired) {
					t.Fatal("STATUS lost the decision", hint)
				}
				args := hint["arguments"].(nextstep.Object)
				if args["requestId"] != request || args["includeState"] != true {
					t.Fatal("lost request binding", args)
				}
				next := call("semantic.discovery.candidates", args)["next"].(nextstep.Object)
				alts, ok := next["alternatives"].([]any)
				if next["action"] != "BLOCKED" || next["actor"] != "AGENT" || !ok || len(alts) != 3 {
					t.Fatal("lost alternatives", next)
				}
				for _, item := range alts {
					alt := item.(nextstep.Object)
					tool, args := alt["tool"].(string), alt["arguments"].(nextstep.Object)
					if prior := options[tool]; prior != nil && tool != "source.analysis.plan" && args["commandKey"] != prior["commandKey"] {
						t.Fatal("decision key changed", tool, args)
					}
					options[tool] = args
				}
			}
			args := options[choice]
			if args == nil {
				t.Fatal("missing choice", choice, options)
			}
			switch choice {
			case "source.analysis.plan":
				args["intent"] = "Identificatore comunale della via"
			case "semantic.propose":
				maps.Copy(args, nextstep.Object{"semanticId": "ouf:test:Place", "artifactType": "CLASS", "namespace": "ouf",
					"localName": "Place", "ownerRef": "owner", "authorityRef": "authority", "semanticVersion": "1.0.0",
					"labels": nextstep.Object{"it": "Luogo"}, "definition": nextstep.Object{}})
			case "source.onboarding.create":
				maps.Copy(args, nextstep.Object{"assetId": profile.AssetID, "profileId": profile.ProfileID, "sourceId": "places",
					"name": "Places", "owner": "Comune", "targetClassIri": "urn:place", "semanticRefs": []string{"urn:place@1"},
					"sourceObjectKeyFields": []string{}, "fields": []nextstep.Object{{"fieldName": "fixture", "extractionDecision": "EXCLUDE", "dataAccessLabel": "OPEN"}}})
			}
			owner.response.Body = []byte(`{"state":"DRAFT"}`)
			call(choice, args) // Discard the decision receipt as well.
			after := status()
			counts := after["counts"].(nextstep.Object)
			if choice == "source.analysis.plan" {
				task := resume("source.analysis.step")["taskId"].(string)
				saved, err := store.ReadAnalysisTask(ctx, scope, plan.ID, task, plan.Manifest)
				if err != nil || after["state"] != "RUNNING" || counts["pending"] != json.Number("1") ||
					!saved.Reformulated || saved.ID == plan.Tasks[0].ID || saved.State != "READY" {
					t.Fatal(after, saved, err)
				}
			} else if after["state"] != "COMPLETE" || counts["noCandidates"] != json.Number("1") || counts["blocked"] != json.Number("0") {
				t.Fatal("confirmed choice left the column open", after)
			}
		})
	}
}

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
	t.Setenv("OUF_ANALYSIS_SUMMARY_PAGE_BYTES", "4096")
	ctx := context.Background()
	store, m := summaryTestStore(t)
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

func summaryTestStore(t *testing.T) (*postgres.Store, *manifest.Snapshot) {
	t.Helper()
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	store, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
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
	return store, m
}

type interruptedDiscovery struct {
	response orchestration.GatewayResponse
	err      error
	calls    atomic.Int32
}

func (o *interruptedDiscovery) Execute(context.Context, orchestration.GatewayRequest, time.Duration) (orchestration.GatewayResponse, error) {
	o.calls.Add(1)
	return o.response, o.err
}

func TestLostDiscoveryErrorResumesFromStoredClock(t *testing.T) {
	store, m := summaryTestStore(t)
	ctx := context.Background()
	for _, status := range []int{429, 503, 0} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			scope := analysis.Scope{Service: "ouf-chatgpt", Principal: uuid.NewString(), Tenant: "tenant-a"}
			profile := analysis.Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), Version: 1, SchemaHash: "sha256:" + strings.Repeat("a", 64), ColumnCount: 1}
			work := analysis.Work{Tool: "semantic.discovery.request", Columns: []int{0}, Arguments: json.RawMessage(`{"intent":"fixture","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`)}
			plan, err := analysis.Build(scope, profile, m, 0, []analysis.Work{work})
			if err != nil {
				t.Fatal(err)
			}
			plan.Headers = []string{"fixture"}
			if _, err = store.PlanAnalysis(ctx, plan); err != nil {
				t.Fatal(err)
			}
			task, err := store.ClaimAnalysisTask(ctx, scope, plan.ID, plan.Tasks[0].ID, plan.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			request := uuid.NewString()
			body, _ := json.Marshal(map[string]string{"requestId": request, "state": "PENDING"})
			if _, err = store.FinishAnalysisTask(ctx, scope, plan.ID, task.ID, task.Fence, analysis.Finish{State: "SUCCEEDED", Body: body}); err != nil {
				t.Fatal(err)
			}
			o := &interruptedDiscovery{}
			code, reason, seconds := "RATE_LIMITED", "TRANSIENT_OWNER_WAIT", 72
			if status == 0 {
				o.err = context.DeadlineExceeded
				code, reason, seconds = "OWNER_NETWORK_INTERRUPTED", "OWNER_TIMEOUT", orchestration.PollIntervalSeconds
			} else {
				o.response = orchestration.GatewayResponse{Status: status, Problem: &orchestration.Problem{Status: status, Code: code, RetryAfter: "72"}, Body: []byte(`{"detail":"PRIVATE_ERROR_BODY"}`)}
			}
			service := &orchestration.Service{Auth: &summaryAuthority{}, Admission: store, Gateway: o, FingerprintKey: make([]byte, 32)}
			const scopes = "ouf.managed-source.preview ouf.semantic.discovery.status ouf.semantic.discovery.candidates"
			first := revisionSessionForScope(t, service, scope, scopes)
			start := time.Now()
			res, err := first.CallTool(ctx, &mcp.CallToolParams{Name: "semantic.discovery.status", Arguments: nextstep.Object{"analysisId": plan.ID, "requestId": request}})
			if err != nil || !res.IsError {
				t.Fatal(res, err)
			}
			// Discard the complete reply. Assert the typed record independently.
			var due time.Time
			var savedReason, savedCode string
			var savedStatus int
			err = store.Pool().QueryRow(ctx, `select not_before,reason,observation_code,observation_status from ouf_mcp.analysis_owner_due
				where analysis_id=$1 and request_id=$2`, plan.ID, request).Scan(&due, &savedReason, &savedCode, &savedStatus)
			if err != nil || due.Before(start.Add(time.Duration(seconds)*time.Second)) || savedReason != reason || savedCode != code || savedStatus != status {
				t.Fatal(due, savedReason, savedCode, savedStatus, err)
			}
			restarted := revisionSessionForScope(t, service, scope, scopes)
			for _, view := range []string{"STATUS", "SUMMARY"} {
				res, err := restarted.CallTool(ctx, &mcp.CallToolParams{Name: "source.analysis.read", Arguments: nextstep.Object{"analysisId": plan.ID, "view": view}})
				if err != nil || res.IsError {
					t.Fatal(res, err)
				}
				next := checkNext(t, nextContract(t), res)
				if next["tool"] != "ouf.wait" || next["reasonCode"] != reason || o.calls.Load() != 1 {
					t.Fatal("lost reply caused premature owner read", view, next)
				}
			}
		})
	}
}
