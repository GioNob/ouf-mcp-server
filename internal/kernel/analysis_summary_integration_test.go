package kernel

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
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
	store, m := summaryTestStore(t, isolatedSummaryTestDSN(t))
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
				args["intent"] = "Codice strada comunale"
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
	store, m := summaryTestStore(t, summaryTestDSN(t))
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

func summaryTestDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL required")
	}
	return dsn
}

// The adapter package exercises concurrent SERIALIZABLE admissions during go test ./...
// Separate principals still write the same tables and can incur serialization failures.
func isolatedSummaryTestDSN(t *testing.T) string {
	t.Helper()
	dsn := summaryTestDSN(t)
	admin, err := postgres.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	name := "mcp_summary_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Pool().Exec(t.Context(), "create database "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Pool().Exec(context.Background(), "drop database "+name); err != nil {
			t.Errorf("drop fixture database: %v", err)
		}
	})
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal("invalid MCP_TEST_DATABASE_URL")
		}
		u.Path, u.RawPath = "/"+name, ""
		query := u.Query()
		query.Del("database")
		query.Del("dbname")
		u.RawQuery = query.Encode()
		return u.String()
	}
	return dsn + " dbname=" + name
}

func summaryTestStore(t *testing.T, dsn string) (*postgres.Store, *manifest.Snapshot) {
	t.Helper()
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

type failingAnalysisSuccessor struct {
	*postgres.Store
	reads atomic.Int32
}

func (s *failingAnalysisSuccessor) ReadAnalysisSuccessor(ctx context.Context, scope analysis.Scope, id, manifest string, task analysis.Task) (*analysis.TaskResume, error) {
	stored, err := s.Store.ReadAnalysisTask(ctx, scope, id, task.ID, manifest)
	if err != nil {
		return nil, err
	}
	if stored.State != "SUCCEEDED" || len(stored.Result) == 0 {
		return nil, errors.New("success not committed before the continuation read")
	}
	switch s.reads.Add(1) {
	case 1:
		return nil, errors.New("injected successor read failure")
	case 2:
		return nil, context.DeadlineExceeded
	default:
		return s.Store.ReadAnalysisSuccessor(ctx, scope, id, manifest, task)
	}
}

func (o *interruptedDiscovery) Execute(context.Context, orchestration.GatewayRequest, time.Duration) (orchestration.GatewayResponse, error) {
	o.calls.Add(1)
	return o.response, o.err
}

func TestLostDiscoveryErrorResumesFromStoredClock(t *testing.T) {
	store, m := summaryTestStore(t, summaryTestDSN(t))
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

func TestDiscoveryDeadlineSharedBetweenPublicAndPlannedReads(t *testing.T) {
	store, m := summaryTestStore(t, isolatedSummaryTestDSN(t))
	ctx := t.Context()
	key := make([]byte, 32)
	waits, err := configuredWait(key)
	if err != nil {
		t.Fatal(err)
	}
	contract := nextContract(t)
	const scopes = "ouf.managed-source.preview ouf.semantic.discovery.status"
	newScope := func() analysis.Scope {
		return analysis.Scope{Service: "ouf-chatgpt", Principal: uuid.NewString(), Tenant: "tenant-fixture"}
	}
	planFor := func(t *testing.T, scope analysis.Scope, args nextstep.Object, observations int) analysis.Plan {
		t.Helper()
		raw, _ := json.Marshal(args)
		profile := analysis.Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), Version: 1,
			SchemaHash: "sha256:" + strings.Repeat("a", 64), ColumnCount: 1}
		var work []analysis.Work
		for n := 1; n <= observations; n++ {
			work = append(work, analysis.Work{Tool: "semantic.discovery.status", Columns: []int{0}, Arguments: raw, Observation: n})
		}
		plan, err := analysis.Build(scope, profile, m, 0, work)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = store.PlanAnalysis(ctx, plan); err != nil {
			t.Fatal(err)
		}
		return plan
	}
	call := func(t *testing.T, session *mcp.ClientSession, tool string, args nextstep.Object, wantError bool) *mcp.CallToolResult {
		t.Helper()
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
		if err != nil || result == nil || result.IsError != wantError {
			t.Fatal(tool, result, err)
		}
		return result
	}
	withScope := func(ctx context.Context, scope analysis.Scope) context.Context {
		i := orchestration.Identity{Issuer: "issuer", Audience: "gateway", ActorType: "HUMAN", Delegation: "human-proof",
			ServicePrincipalID: scope.Service, PrincipalID: scope.Principal, TenantID: scope.Tenant}
		return context.WithValue(ctx, identityKey{}, requestIdentity{Identity: i})
	}
	assertWait := func(t *testing.T, scope analysis.Scope, result *mcp.CallToolResult, tool, field, value string, due time.Time) {
		t.Helper()
		next := checkNext(t, contract, result)
		args, _ := next["arguments"].(nextstep.Object)
		token, _ := args["waitRef"].(string)
		resume, _ := args["resumeArguments"].(nextstep.Object)
		binding := waitBinding(withScope(ctx, scope))
		ref, err := waits.signer.Open(token, binding)
		if next["action"] != "POLL" || next["tool"] != "ouf.wait" || err != nil || !ref.RestoreArgs(resume) || ref.Read != tool ||
			ref.Args[field] != value || ref.NotBefore != due.UTC().Format(time.RFC3339Nano) {
			t.Fatal("unexpected wait", next, ref, err)
		}
	}
	runWait := func(t *testing.T, s *workflowServer, scope analysis.Scope, next nextstep.Object) *mcp.CallToolResult {
		t.Helper()
		wait, _ := analysis.Find(m, "ouf.wait")
		s.registerWait(wait)
		raw, err := json.Marshal(nextstep.Object{"params": nextstep.Object{"name": wait.ToolName, "arguments": next["arguments"]}})
		var req mcp.CallToolRequest
		if err != nil || json.Unmarshal(raw, &req) != nil {
			t.Fatal("invalid wait call", err)
		}
		result, err := s.tools[wait.ToolName].run(withScope(t.Context(), scope), &req)
		if err != nil || result == nil || result.IsError {
			t.Fatal("wait could not resume", result, err)
		}
		return s.validateNext(wait.ToolName, result)
	}
	resumeStep := func(t *testing.T, plan analysis.Plan, taskID string, owner *interruptedDiscovery, next nextstep.Object, due time.Time) nextstep.Object {
		t.Helper()
		before := owner.calls.Load()
		var resume nextstep.Object
		// The command wait returns a continuation. It cannot dispatch the step.
		synctest.Test(t, func(t *testing.T) {
			sdk := mcp.NewServer(&mcp.Implementation{Name: "fixture", Version: "1"}, nil)
			s, err := newWorkflowServer(sdk, m, key)
			if err != nil {
				t.Fatal(err)
			}
			s.waits.duration = 2 * time.Second
			step, _ := analysis.Find(m, "source.analysis.step")
			registerAnalysisTool(s, step, m, &orchestration.Service{})
			time.Sleep(time.Until(due.Add(-time.Second)))
			follow := checkNext(t, contract, runWait(t, s, plan.Scope, next))
			resume, _ = follow["arguments"].(nextstep.Object)
			if !time.Now().Equal(due) || owner.calls.Load() != before || follow["tool"] != step.ToolName || follow["action"] != "CALL_TOOL" ||
				resume["analysisId"] != plan.ID || resume["taskId"] != taskID {
				t.Fatal("wait did not return the correct step at the deadline", follow, time.Now(), due)
			}
		})
		return resume
	}
	storedDue := func(t *testing.T, scope analysis.Scope, request string) time.Time {
		t.Helper()
		selector := orchestration.DiscoveryObservationSelector(key, request)
		obs, err := store.ReadOwnerObservation(ctx, orchestration.Identity{ServicePrincipalID: scope.Service, PrincipalID: scope.Principal, TenantID: scope.Tenant}, selector)
		if err != nil || obs.Due.IsZero() {
			t.Fatal("missing owner clock", obs, err)
		}
		return obs.Due
	}
	planDue := func(t *testing.T, plan analysis.Plan, request string) time.Time {
		t.Helper()
		var due time.Time
		if err := store.Pool().QueryRow(ctx, `select not_before from ouf_mcp.analysis_owner_due where analysis_id=$1 and request_id=$2`, plan.ID, request).Scan(&due); err != nil {
			t.Fatal(err)
		}
		return due
	}
	ageRequest := func(t *testing.T, plan analysis.Plan, request string) {
		t.Helper()
		if _, err := store.Pool().Exec(ctx, `with elapsed as (
			update ouf_mcp.analysis_owner_due set not_before=transaction_timestamp()-interval '1 second'
			where analysis_id=$5 and request_id=$6
		) update ouf_mcp.tool_attempt set owner_due=transaction_timestamp()-interval '1 second'
			where service_principal_id=$1 and principal_id=$2 and tenant_id=$3 and owner_selector=$4 and owner_due is not null`,
			plan.Scope.Service, plan.Scope.Principal, plan.Scope.Tenant, orchestration.DiscoveryObservationSelector(key, request), plan.ID, request); err != nil {
			t.Fatal(err)
		}
	}
	ageTask := func(t *testing.T, plan analysis.Plan, task, field string, seconds int) {
		t.Helper()
		if _, err := store.Pool().Exec(ctx, `update ouf_mcp.profile_analysis_task
			set task=jsonb_set(task,array[$3::text],to_jsonb(transaction_timestamp()-$4::int * interval '1 second'))
			where analysis_id=$1 and task_id=$2`, plan.ID, task, field, seconds); err != nil {
			t.Fatal(err)
		}
	}
	rateLimited := orchestration.GatewayResponse{Status: 429, RetryAfter: "72",
		Problem: &orchestration.Problem{Status: 429, Code: "RATE_LIMITED", RetryAfter: "72"}}

	t.Run("successor-read-failure-resumes-from-plan", func(t *testing.T) {
		scope := newScope()
		request := uuid.NewString()
		plan := planFor(t, scope, nextstep.Object{"requestId": request}, 2)
		first, second := plan.Tasks[0].ID, plan.Tasks[1].ID
		owner := &interruptedDiscovery{response: orchestration.GatewayResponse{
			Status: 200, Body: []byte(fmt.Sprintf(`{"state":"RUNNING","requestId":%q}`, request)),
		}}
		admission := &failingAnalysisSuccessor{Store: store}
		service := &orchestration.Service{Auth: &summaryAuthority{}, Admission: admission, Gateway: owner, FingerprintKey: key}
		session := revisionSessionForScope(t, service, scope, scopes)
		step := nextstep.Object{"analysisId": plan.ID, "taskId": first}
		for n := int32(1); n <= 2; n++ {
			succeeded := call(t, session, "source.analysis.step", step, false)
			stored, err := store.ReadAnalysisTask(ctx, scope, plan.ID, first, plan.Manifest)
			if err != nil || stored.State != "SUCCEEDED" || len(stored.Result) == 0 || stored.Attempts != 1 ||
				ownerObject(succeeded)["state"] != "SUCCEEDED" || admission.reads.Load() != n || owner.calls.Load() != 1 {
				t.Fatal("success was not saved before reading the continuation", stored, err, admission.reads.Load(), owner.calls.Load())
			}
			next := checkNext(t, contract, succeeded)
			args, _ := next["arguments"].(nextstep.Object)
			if next["action"] != "CALL_TOOL" || next["tool"] != "source.analysis.read" || len(args) != 1 || args["analysisId"] != plan.ID {
				t.Fatal("failed successor read did not return the plan", next)
			}
			page := call(t, session, "source.analysis.read", args, false)
			assertWait(t, scope, page, "source.analysis.step", "taskId", second, stored.CompletedAt.Add(31*time.Second))
		}
		stored, err := store.ReadAnalysisTask(ctx, scope, plan.ID, first, plan.Manifest)
		if err != nil {
			t.Fatal(err)
		}
		succeeded := call(t, session, "source.analysis.step", step, false)
		assertWait(t, scope, succeeded, "source.analysis.step", "taskId", second, stored.CompletedAt.Add(31*time.Second))
		if admission.reads.Load() != 3 || owner.calls.Load() != 1 {
			t.Fatal("replay redispatched the successful observation", admission.reads.Load(), owner.calls.Load())
		}
	})

	t.Run("summary-skips-exhausted-predecessor", func(t *testing.T) {
		scope := newScope()
		request, independent := uuid.NewString(), uuid.NewString()
		args := json.RawMessage(fmt.Sprintf(`{"requestId":%q}`, request))
		other := json.RawMessage(fmt.Sprintf(`{"requestId":%q}`, independent))
		profile := analysis.Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), Version: 1,
			SchemaHash: "sha256:" + strings.Repeat("a", 64), ColumnCount: 3}
		plan, err := analysis.Build(scope, profile, m, 0, []analysis.Work{
			{Tool: "semantic.discovery.status", Columns: []int{1}, Arguments: args, Observation: 1},
			{Tool: "semantic.discovery.status", Columns: []int{0}, Arguments: args, Observation: 2},
			{Tool: "semantic.discovery.status", Columns: []int{2}, Arguments: other, Observation: 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		plan.Headers = []string{"dependent", "predecessor", "independent"}
		if _, err = store.PlanAnalysis(ctx, plan); err != nil {
			t.Fatal(err)
		}
		first, second, third := plan.Tasks[0].ID, plan.Tasks[1].ID, plan.Tasks[2].ID
		owner := &interruptedDiscovery{response: rateLimited}
		service := &orchestration.Service{Auth: &summaryAuthority{}, Admission: store, Gateway: owner, FingerprintKey: key}
		session := revisionSessionForScope(t, service, scope, scopes)
		read := func(view string) *mcp.CallToolResult {
			return call(t, session, "source.analysis.read", nextstep.Object{"analysisId": plan.ID, "view": view}, false)
		}
		firstPage := call(t, session, "source.analysis.read", nextstep.Object{"analysisId": plan.ID, "view": "SUMMARY", "limit": 2}, false)
		pageNext := checkNext(t, contract, firstPage)
		pageArgs, _ := pageNext["arguments"].(nextstep.Object)
		if pageNext["reasonCode"] != "ANALYSIS_NEXT_PAGE" || pageArgs["cursor"] == nil {
			t.Fatal("missing frozen summary cursor", pageNext)
		}
		step := nextstep.Object{"analysisId": plan.ID, "taskId": first}
		waiting := ownerObject(call(t, session, "source.analysis.step", step, false))
		if waiting["state"] != "WAITING" || owner.calls.Load() != 1 {
			t.Fatal("first 429 did not suspend A", waiting, owner.calls.Load())
		}
		ageRequest(t, plan, request)
		ageTask(t, plan, first, "notBefore", 1)
		// Skip the intervening retries and let the final 429 exhaust the budget.
		if _, err = store.Pool().Exec(ctx, `update ouf_mcp.profile_analysis_task
			set task=jsonb_set(task,'{attempts}',to_jsonb($3::int)) where analysis_id=$1 and task_id=$2`,
			plan.ID, first, analysis.MaxRetryAttempts-1); err != nil {
			t.Fatal(err)
		}
		held := ownerObject(call(t, session, "source.analysis.step", step, false))
		if held["state"] != "HELD" || held["code"] != "RETRY_BUDGET_EXHAUSTED" || owner.calls.Load() != 2 {
			t.Fatal("final 429 did not exhaust A", held, owner.calls.Load())
		}
		dependent, err := store.ReadAnalysisTask(ctx, scope, plan.ID, second, plan.Manifest)
		if err != nil || dependent.State != "READY" || dependent.Attempts != 0 {
			t.Fatal("unexpected dependent state", dependent, err)
		}
		assertIndependent := func(view string, result *mcp.CallToolResult) nextstep.Object {
			t.Helper()
			next := checkNext(t, contract, result)
			if next["action"] != "CALL_TOOL" || next["tool"] != "source.analysis.step" {
				t.Fatal("independent step was not offered", view, next)
			}
			args := next["arguments"].(nextstep.Object)
			if args["analysisId"] != plan.ID || args["taskId"] != third {
				t.Fatal("wrong continuation", view, next)
			}
			return args
		}
		resume := assertIndependent("STATUS", read("STATUS"))
		assertIndependent("SUMMARY", read("SUMMARY"))
		lastPage := call(t, session, "source.analysis.read", pageArgs, false)
		assertIndependent("frozen SUMMARY", lastPage)
		if ownerObject(lastPage)["changeSeq"] != ownerObject(firstPage)["changeSeq"] {
			t.Fatal("the new continuation changed the snapshot")
		}
		owner.response = orchestration.GatewayResponse{Status: 200, Body: []byte(`{"state":"RUNNING"}`)}
		if result := ownerObject(call(t, session, "source.analysis.step", resume, false)); result["state"] != "SUCCEEDED" || owner.calls.Load() != 3 {
			t.Fatal("independent step did not complete", result, owner.calls.Load())
		}
		assertHeld := func(view string, result *mcp.CallToolResult) {
			t.Helper()
			if next := checkNext(t, contract, result); next["action"] != "BLOCKED" || next["reasonCode"] != "ANALYSIS_HELD" {
				t.Fatal("the useless continuation was offered again", view, next)
			}
		}
		for _, view := range []string{"STATUS", "SUMMARY"} {
			assertHeld(view, read(view))
		}
		assertHeld("frozen SUMMARY", call(t, session, "source.analysis.read", pageArgs, false))
	})

	for _, tc := range []struct {
		name      string
		status    int
		uppercase bool
	}{
		{name: "429", status: 429},
		{name: "200", status: 200},
		{name: "uppercase-429", status: 429, uppercase: true},
	} {
		t.Run("public-to-plan/"+tc.name, func(t *testing.T) {
			scope := newScope()
			request := uuid.NewString()
			if tc.uppercase {
				request = "abcdef01-2345-4abc-8def-0123456789ab"
			}
			args := nextstep.Object{"requestId": request}
			publicArgs := args
			if tc.uppercase {
				publicArgs = nextstep.Object{"requestId": strings.ToUpper(request)}
			}
			owner := &interruptedDiscovery{response: rateLimited}
			if tc.status == 200 {
				owner.response = orchestration.GatewayResponse{Status: 200, Body: []byte(fmt.Sprintf(`{"state":"RUNNING","requestId":%q,"pollAfterSeconds":72}`, request))}
			}
			service := &orchestration.Service{Auth: &summaryAuthority{}, Admission: store, Gateway: owner, FingerprintKey: key}
			session := revisionSessionForScope(t, service, scope, scopes)
			start := time.Now()
			call(t, session, "semantic.discovery.status", publicArgs, tc.status == 429)
			end := time.Now()
			due := storedDue(t, scope, request)
			if due.Before(start.Add(72*time.Second)) || due.After(end.Add(72*time.Second)) {
				t.Fatal("unexpected owner deadline", due, start, end)
			}
			plan := planFor(t, scope, args, 1)
			taskID := plan.Tasks[0].ID
			page := call(t, session, "source.analysis.read", nextstep.Object{"analysisId": plan.ID}, false)
			assertWait(t, scope, page, "source.analysis.step", "taskId", taskID, due)
			stepArgs := nextstep.Object{"analysisId": plan.ID, "taskId": taskID}
			waiting := ownerObject(call(t, session, "source.analysis.step", stepArgs, false))
			notBefore, err := time.Parse(time.RFC3339Nano, fmt.Sprint(waiting["notBefore"]))
			if err != nil || waiting["state"] != "WAITING" || !notBefore.Equal(due) || waiting["attempts"] != json.Number("0") || owner.calls.Load() != 1 {
				t.Fatal("direct claim ignored public observation", waiting, owner.calls.Load(), err)
			}
			other := scope
			other.Principal = uuid.NewString()
			otherPlan := planFor(t, other, args, 1)
			otherSession := revisionSessionForScope(t, service, other, scopes)
			otherPage := call(t, otherSession, "source.analysis.read", nextstep.Object{"analysisId": otherPlan.ID}, false)
			next := checkNext(t, contract, otherPage)
			if next["action"] != "CALL_TOOL" || next["tool"] != "source.analysis.step" {
				t.Fatal("another identity inherited the clock", next)
			}
			owner.response = orchestration.GatewayResponse{Status: 200, Body: []byte(`{"state":"SUCCEEDED","candidateCount":0}`)}
			otherResult := ownerObject(call(t, otherSession, "source.analysis.step", next["arguments"].(nextstep.Object), false))
			if otherResult["state"] != "SUCCEEDED" || owner.calls.Load() != 2 {
				t.Fatal("another identity could not proceed", otherResult, owner.calls.Load())
			}
			// Move only this fixture's existing deadline and its claimed task
			// to the elapsed side of the DB clock, with no real wait.
			ageRequest(t, plan, request)
			ageTask(t, plan, taskID, "notBefore", 1)
			page = call(t, session, "source.analysis.read", nextstep.Object{"analysisId": plan.ID}, false)
			next = checkNext(t, contract, page)
			if next["action"] != "CALL_TOOL" || next["tool"] != "source.analysis.step" {
				t.Fatal("elapsed clock did not reopen the plan", next)
			}
			result := ownerObject(call(t, session, "source.analysis.step", next["arguments"].(nextstep.Object), false))
			if result["state"] != "SUCCEEDED" || owner.calls.Load() != 3 {
				t.Fatal("planned read did not resume", result, owner.calls.Load())
			}
		})
	}

	t.Run("summary-predecessor-mixed-uuid-spelling", func(t *testing.T) {
		scope := newScope()
		const request = "abcdef01-2345-4abc-8def-0123456789ab"
		args := json.RawMessage(fmt.Sprintf(`{"requestId":%q}`, request))
		upperArgs := json.RawMessage(fmt.Sprintf(`{"requestId":%q}`, strings.ToUpper(request)))
		profile := analysis.Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), Version: 1,
			SchemaHash: "sha256:" + strings.Repeat("a", 64), ColumnCount: 2}
		plan, err := analysis.Build(scope, profile, m, 0, []analysis.Work{
			{Tool: "semantic.discovery.status", Columns: []int{1}, Arguments: upperArgs, Observation: 1},
			{Tool: "semantic.discovery.status", Columns: []int{0}, Arguments: args, Observation: 2},
		})
		if err != nil {
			t.Fatal(err)
		}
		if string(plan.Tasks[0].Arguments) != string(upperArgs) || string(plan.Tasks[1].Arguments) != string(args) {
			t.Fatal("the plan did not preserve the two spellings", plan.Tasks)
		}
		plan.Headers = []string{"second", "first"}
		if _, err = store.PlanAnalysis(ctx, plan); err != nil {
			t.Fatal(err)
		}
		first, second := plan.Tasks[0].ID, plan.Tasks[1].ID
		owner := &interruptedDiscovery{response: rateLimited}
		service := &orchestration.Service{Auth: &summaryAuthority{}, Admission: store, Gateway: owner, FingerprintKey: key}
		session := revisionSessionForScope(t, service, scope, scopes)
		limited := call(t, session, "source.analysis.step", nextstep.Object{"analysisId": plan.ID, "taskId": first}, false)
		if ownerObject(limited)["state"] != "WAITING" || owner.calls.Load() != 1 {
			t.Fatal("first observation did not wait after 429", limited)
		}
		due := planDue(t, plan, request)
		assertWait(t, scope, call(t, session, "source.analysis.read", nextstep.Object{
			"analysisId": plan.ID, "offset": 1, "limit": 1,
		}, false), "source.analysis.step", "taskId", first, due)
		var resume nextstep.Object
		check := func(result *mcp.CallToolResult) {
			t.Helper()
			assertWait(t, scope, result, "source.analysis.step", "taskId", first, due)
			resume = resumeStep(t, plan, first, owner, checkNext(t, contract, result), due)
		}
		for _, view := range []string{"STATUS", "SUMMARY"} {
			check(call(t, session, "source.analysis.read", nextstep.Object{"analysisId": plan.ID, "view": view}, false))
		}
		page := call(t, session, "source.analysis.read", nextstep.Object{"analysisId": plan.ID, "view": "SUMMARY", "limit": 1}, false)
		pageNext := checkNext(t, contract, page)
		var frozenTask string
		if err := store.Pool().QueryRow(ctx, `select frozen_hint->>'TaskID' from ouf_mcp.analysis_summary_read
			where analysis_id=$1 and frozen_hint is not null`, plan.ID).Scan(&frozenTask); err != nil || frozenTask != first {
			t.Fatal("snapshot froze the dependent task", frozenTask, err)
		}
		check(call(t, session, "source.analysis.read", pageNext["arguments"].(nextstep.Object), false))
		// A cursor minted before the correction can still point to B.
		if _, err := store.Pool().Exec(ctx, `update ouf_mcp.analysis_summary_read
			set frozen_hint=jsonb_set(frozen_hint,'{TaskID}',to_jsonb($2::text)) where analysis_id=$1 and frozen_hint is not null`, plan.ID, second); err != nil {
			t.Fatal(err)
		}
		check(call(t, session, "source.analysis.read", pageNext["arguments"].(nextstep.Object), false))
		ageRequest(t, plan, request)
		ageTask(t, plan, first, "notBefore", 1)
		owner.response = orchestration.GatewayResponse{Status: 200, Body: []byte(`{"state":"RUNNING"}`)}
		succeeded := call(t, session, "source.analysis.step", resume, false)
		if ownerObject(succeeded)["state"] != "SUCCEEDED" || owner.calls.Load() != 2 {
			t.Fatal("the summary continuation did not complete A", succeeded, owner.calls.Load())
		}
		completed, err := time.Parse(time.RFC3339Nano, fmt.Sprint(ownerObject(succeeded)["completedAt"]))
		if err != nil {
			t.Fatal(err)
		}
		assertWait(t, scope, succeeded, "source.analysis.step", "taskId", second, completed.Add(31*time.Second))
		early := ownerObject(call(t, session, "source.analysis.step", nextstep.Object{"analysisId": plan.ID, "taskId": second}, false))
		if early["state"] != "WAITING" || early["attempts"] != json.Number("0") || owner.calls.Load() != 2 {
			t.Fatal("mixed-spelling successor did not wait for pacing", early, owner.calls.Load())
		}
		ageRequest(t, plan, request)
		ageTask(t, plan, first, "completedAt", 32)
		ageTask(t, plan, first, "notBefore", 1)
		ageTask(t, plan, second, "notBefore", 1)
		done := ownerObject(call(t, session, "source.analysis.step", nextstep.Object{"analysisId": plan.ID, "taskId": second}, false))
		if done["state"] != "SUCCEEDED" || owner.calls.Load() != 3 {
			t.Fatal("mixed-spelling successor did not complete", done, owner.calls.Load())
		}
	})

	t.Run("historical-uppercase-public-clock", func(t *testing.T) {
		scope := newScope()
		const request = "ABCDEF01-2345-4ABC-8DEF-0123456789AB"
		args := nextstep.Object{"requestId": request}
		// 0269770 hashes the supplied requestId text, shared by both reads.
		raw, _ := json.Marshal([]any{"owner-observation-v1", "ouf.semantic.discovery.status", args})
		mac := hmac.New(sha256.New, key)
		mac.Write(raw)
		selector := fmt.Sprintf("%x", mac.Sum(nil))
		checksum, _ := m.Checksum()
		var due time.Time
		if err := store.Pool().QueryRow(ctx, `insert into ouf_mcp.tool_attempt
			(attempt_id,service_principal_id,principal_id,tenant_id,capability_id,operation_class,manifest_checksum,
			 idempotency_key,request_hash,correlation_id,state,dispatch_state,completed_at,
			 owner_selector,owner_due,owner_due_reason,owner_code,owner_status)
			values ($1,$2,$3,$4,'ouf.semantic.discovery.status','READ',$5,$6,$7,$8,'FAILED','ACKNOWLEDGED',transaction_timestamp(),
			 $9,transaction_timestamp()+interval '72 seconds','TRANSIENT_OWNER_WAIT','RATE_LIMITED',429) returning owner_due`,
			uuid.New(), scope.Service, scope.Principal, scope.Tenant, checksum, uuid.NewString(), analysis.Digest(args), uuid.NewString(), selector).Scan(&due); err != nil {
			t.Fatal(err)
		}
		owner := &interruptedDiscovery{response: rateLimited}
		service := &orchestration.Service{Auth: &summaryAuthority{}, Admission: store, Gateway: owner, FingerprintKey: key}
		session := revisionSessionForScope(t, service, scope, scopes+" ouf.semantic.discovery.candidates")
		identity := withScope(ctx, scope).Value(identityKey{}).(requestIdentity).Identity
		rawArgs, _ := json.Marshal(args)
		for _, tool := range []string{"semantic.discovery.status", "semantic.discovery.candidates"} {
			cap, _ := analysis.Find(m, tool)
			saved, err := service.SavedObservation(ctx, identity, cap.CapabilityID, cap.OperationClass, rawArgs)
			if err != nil || !saved.Due.Equal(due) {
				t.Fatal("historical observation clock was lost", tool, saved, err)
			}
			assertWait(t, scope, call(t, session, tool, args, true), tool, "requestId", request, due)
		}
		// An earlier token can be due already, so the wait must query the clock again.
		sdk := mcp.NewServer(&mcp.Implementation{Name: "fixture", Version: "1"}, nil)
		s, err := newWorkflowServer(sdk, m, key)
		if err != nil {
			t.Fatal(err)
		}
		s.analysisService, s.waits.interval = service, time.Millisecond
		status, _ := analysis.Find(m, "semantic.discovery.status")
		registerGovernedTool(s, status, m, service)
		engine := nextstep.Engine{Signer: waits.signer, Binding: waitBinding(withScope(ctx, scope)),
			Now: func() time.Time { return time.Now().Add(-2 * time.Minute) }, Valid: func(string, nextstep.Object) bool { return true }}
		old := engine.Evaluate(status, m.NextPolicies[status.NextPolicy], args, nextstep.Object{"state": "RUNNING"}, false)
		assertWait(t, scope, runWait(t, s, scope, old), status.ToolName, "requestId", request, due)
		if owner.calls.Load() != 0 {
			t.Fatal("historical wait dispatched an owner read", owner.calls.Load())
		}
	})

	t.Run("plan-to-public", func(t *testing.T) {
		scope := newScope()
		request := uuid.NewString()
		args := nextstep.Object{"requestId": request}
		plan := planFor(t, scope, args, 1)
		taskID := plan.Tasks[0].ID
		owner := &interruptedDiscovery{response: rateLimited}
		service := &orchestration.Service{Auth: &summaryAuthority{}, Admission: store, Gateway: owner, FingerprintKey: key}
		session := revisionSessionForScope(t, service, scope, scopes)
		stepArgs := nextstep.Object{"analysisId": plan.ID, "taskId": taskID}
		var earliestFourthDue time.Time
		for attempt := int32(1); attempt <= 4; attempt++ {
			if attempt == 4 {
				earliestFourthDue = time.Now().Add(5 * time.Minute)
			}
			step := call(t, session, "source.analysis.step", stepArgs, false)
			if ownerObject(step)["state"] != "WAITING" || owner.calls.Load() != attempt {
				t.Fatal("planned observation did not record its wait", step, owner.calls.Load())
			}
			if attempt == 1 {
				public := call(t, session, "semantic.discovery.status", args, true)
				assertWait(t, scope, public, "semantic.discovery.status", "requestId", request, storedDue(t, scope, request))
			}
			if attempt < 4 {
				ageRequest(t, plan, request)
				ageTask(t, plan, taskID, "notBefore", 1)
			}
		}
		due := planDue(t, plan, request)
		if due.Before(earliestFourthDue) || !due.After(storedDue(t, scope, request)) {
			t.Fatal("fourth retry did not save its local backoff", due, earliestFourthDue)
		}
		contextual := nextstep.Object{"requestId": request, "analysisId": plan.ID}
		for _, readArgs := range []nextstep.Object{args, contextual} {
			public := call(t, session, "semantic.discovery.status", readArgs, true)
			assertWait(t, scope, public, "semantic.discovery.status", "requestId", request, due)
		}
		// An older signed wait is already due. It must reread the current
		// request clock without dispatching an owner observation.
		status, _ := analysis.Find(m, "semantic.discovery.status")
		engine := nextstep.Engine{Signer: waits.signer, Binding: waitBinding(withScope(ctx, scope)),
			Now: func() time.Time { return time.Now().Add(-2 * time.Minute) }, Valid: func(string, nextstep.Object) bool { return true }}
		old := engine.Evaluate(status, m.NextPolicies[status.NextPolicy], contextual, nextstep.Object{"state": "RUNNING"}, false)
		if old["tool"] != "ouf.wait" {
			t.Fatal("missing older wait", old)
		}
		waited := call(t, session, "ouf.wait", old["arguments"].(nextstep.Object), false)
		assertWait(t, scope, waited, "semantic.discovery.status", "requestId", request, due)
		otherPlan := planFor(t, scope, args, 1)
		page := call(t, session, "source.analysis.read", nextstep.Object{"analysisId": otherPlan.ID}, false)
		assertWait(t, scope, page, "source.analysis.step", "taskId", otherPlan.Tasks[0].ID, due)
		if owner.calls.Load() != 4 {
			t.Fatal("public read dispatched during the planned wait", owner.calls.Load())
		}
		other := scope
		other.Principal = uuid.NewString()
		owner.response = orchestration.GatewayResponse{Status: 200, Body: []byte(`{"state":"SUCCEEDED","candidateCount":0}`)}
		call(t, revisionSessionForScope(t, service, other, scopes), "semantic.discovery.status", args, false)
		if owner.calls.Load() != 5 {
			t.Fatal("another identity inherited the plan clock", owner.calls.Load())
		}
		ageRequest(t, plan, request)
		call(t, session, "semantic.discovery.status", args, false)
		if owner.calls.Load() != 6 {
			t.Fatal("public read did not resume after the clock elapsed", owner.calls.Load())
		}
	})

	for _, tc := range []struct {
		name     string
		response string
	}{
		{name: "pacing", response: `{"state":"RUNNING","requestId":%q}`},
		{name: "saved-deadline", response: `{"state":"RUNNING","requestId":%q,"pollAfterSeconds":72}`},
	} {
		t.Run("step-next/"+tc.name, func(t *testing.T) {
			scope := newScope()
			request := uuid.NewString()
			plan := planFor(t, scope, nextstep.Object{"requestId": request}, 2)
			first, second := plan.Tasks[0].ID, plan.Tasks[1].ID
			owner := &interruptedDiscovery{response: rateLimited}
			service := &orchestration.Service{Auth: &summaryAuthority{}, Admission: store, Gateway: owner, FingerprintKey: key}
			session := revisionSessionForScope(t, service, scope, scopes)
			stepArgs := nextstep.Object{"analysisId": plan.ID, "taskId": first}
			limited := call(t, session, "source.analysis.step", stepArgs, false)
			if ownerObject(limited)["state"] != "WAITING" || owner.calls.Load() != 1 {
				t.Fatal("first observation did not wait after 429", limited, owner.calls.Load())
			}
			ageRequest(t, plan, request)
			ageTask(t, plan, first, "notBefore", 1)
			owner.response = orchestration.GatewayResponse{Status: 200, Body: fmt.Appendf(nil, tc.response, request)}
			succeeded := call(t, session, "source.analysis.step", stepArgs, false)
			body := ownerObject(succeeded)
			completed, err := time.Parse(time.RFC3339Nano, fmt.Sprint(body["completedAt"]))
			if err != nil || body["state"] != "SUCCEEDED" || owner.calls.Load() != 2 {
				t.Fatal("first observation did not resume", body, owner.calls.Load(), err)
			}
			due := completed.Add(31 * time.Second)
			if saved := planDue(t, plan, request); saved.After(due) {
				due = saved
			}
			assertWait(t, scope, succeeded, "source.analysis.step", "taskId", second, due)
			early := ownerObject(call(t, session, "source.analysis.step", nextstep.Object{"analysisId": plan.ID, "taskId": second}, false))
			if early["state"] != "WAITING" || early["attempts"] != json.Number("0") || owner.calls.Load() != 2 {
				t.Fatal("successor dispatched before its deadline", early, owner.calls.Load())
			}
			resume := resumeStep(t, plan, second, owner, checkNext(t, contract, succeeded), due)
			ageRequest(t, plan, request)
			ageTask(t, plan, first, "completedAt", 32)
			ageTask(t, plan, first, "notBefore", 1)
			ageTask(t, plan, second, "notBefore", 1)
			owner.response = orchestration.GatewayResponse{Status: 200, Body: fmt.Appendf(nil, `{"state":"SUCCEEDED","requestId":%q,"candidateCount":0}`, request)}
			b := ownerObject(call(t, session, "source.analysis.step", resume, false))
			if b["taskId"] != second || b["state"] != "SUCCEEDED" || owner.calls.Load() != 3 {
				t.Fatal("successor did not run after the wait", b, owner.calls.Load())
			}
			var saved time.Time
			if err := store.Pool().QueryRow(ctx, `update ouf_mcp.analysis_owner_due
				set not_before=transaction_timestamp()+interval '72 seconds' where analysis_id=$1 and request_id=$2
				returning not_before`, plan.ID, request).Scan(&saved); err != nil {
				t.Fatal(err)
			}
			// Replaying the last task keeps the discovery fallback, with the
			// current plan clock even though the receipt supplied no delay.
			last := call(t, session, "source.analysis.step", resume, false)
			assertWait(t, scope, last, "semantic.discovery.status", "requestId", request, saved)
			if owner.calls.Load() != 3 {
				t.Fatal("replaying the last task called the owner", owner.calls.Load())
			}
		})
	}
}
