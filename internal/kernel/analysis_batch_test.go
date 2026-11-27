package kernel

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type batchSDKStore struct {
	analysisSDKStore
	mu     sync.Mutex
	scope  analysis.Scope
	start  analysis.BatchStart
	key    string
	digest string
	legacy analysis.View
}

func (s *batchSDKStore) ReadAnalysis(_ context.Context, _ analysis.Scope, id, _ string, _, _ int) (analysis.View, error) {
	if id == s.legacy.ID {
		return s.legacy, nil
	}
	return analysis.View{}, analysis.ErrScope
}

func (s *batchSDKStore) ReadBatchStart(_ context.Context, scope analysis.Scope, id, _ string) (analysis.BatchStart, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if scope != s.scope || id != s.start.ID {
		return analysis.BatchStart{}, analysis.ErrScope
	}
	return s.start, nil
}

func (s *batchSDKStore) LookupBatchStartCommand(_ context.Context, scope analysis.Scope, _, key, digest string) (analysis.BatchStart, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if scope != s.scope || key != s.key {
		return analysis.BatchStart{}, false, nil
	}
	if digest != s.digest {
		return analysis.BatchStart{}, true, analysis.ErrKeyReuse
	}
	return s.start, true, nil
}

func (s *batchSDKStore) SaveBatchStart(_ context.Context, scope analysis.Scope, _, key, digest string, start analysis.BatchStart) (analysis.BatchStart, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scope, s.key, s.digest, s.start = scope, key, digest, start
	return start, nil
}

func TestBatchPreviewStartAndLegacyWire(t *testing.T) {
	const asset = "11111111-1111-4111-8111-111111111111"
	const profile = "22222222-2222-4222-8222-222222222222"
	const granted = "ouf.managed-source.preview"
	scope := analysis.Scope{Service: "ouf-chatgpt", Principal: "user-a", Tenant: "tenant-a"}
	pin := analysis.Profile{AssetID: asset, ProfileID: profile, Version: 1, SchemaHash: "sha256:" + strings.Repeat("a", 64), ColumnCount: 1024}
	legacyID := analysis.AnalysisID(scope, pin)
	store := &batchSDKStore{legacy: analysis.View{ID: legacyID, Profile: pin, Budget: analysis.Budget{PlannedCalls: 1}, Tasks: []analysis.Task{}}}
	service := &orchestration.Service{RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: store,
		FingerprintKey: make([]byte, 32), Gateway: &analysisSDKGateway{asset: asset, profile: profile}}
	session := revisionSessionForScope(t, service, scope, granted)
	contract := nextContract(t)
	call := func(session *mcp.ClientSession, tool string, args nextstep.Object) *mcp.CallToolResult {
		t.Helper()
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	preview := call(session, "source.file.preview", nextstep.Object{"mode": analysis.BatchMode, "assetId": asset, "profileId": profile})
	body := assertBatchWire(t, contract, preview)
	next := body["next"].(nextstep.Object)
	args := next["arguments"].(nextstep.Object)
	if preview.IsError || body["columnCount"] != json.Number("1024") || body["columns"] != nil ||
		next["action"] != "CALL_TOOL" || next["reasonCode"] != "PLAN_COLUMN_DISCOVERY" || next["tool"] != "source.analysis.plan" ||
		next["alternatives"] != nil || len(args) != 5 || args["mode"] != analysis.BatchMode || args["schemaHash"] != pin.SchemaHash {
		t.Fatal("batch preview did not emit a single verified start", body)
	}
	plan := call(session, "source.analysis.plan", args)
	body = assertBatchWire(t, contract, plan)
	id := analysis.BatchAnalysisID(scope, pin, "")
	if plan.IsError || id == legacyID || body["analysisId"] != id || body["phase"] != "BLOCKED" || body["mode"] != analysis.BatchMode {
		t.Fatal("batch start was not retained independently", body)
	}
	planWire, _ := json.Marshal(plan)
	replayWire, _ := json.Marshal(call(session, "source.analysis.plan", args))
	if !bytes.Equal(planWire, replayWire) {
		t.Fatal("same-key batch start changed its result")
	}
	status, _ := json.Marshal(call(revisionSessionForScope(t, service, scope, granted), "source.analysis.read", nextstep.Object{"analysisId": id}))
	if !bytes.Equal(status, planWire) {
		t.Fatal("failed to resume the batch shape with analysisId only")
	}
	for _, view := range []string{"BATCH", "SUMMARY"} {
		reply := call(session, "source.analysis.read", nextstep.Object{"analysisId": id, "view": view})
		if body := assertBatchWire(t, contract, reply); !reply.IsError || body["code"] != nextstep.BatchUnavailable || body["analysisId"] != id {
			t.Fatal(view, body)
		}
	}
	decision := nextstep.Object{"analysisId": id, "batchId": profile, "commandKey": asset, "expectedRevision": 0,
		"decisions": []any{nextstep.Object{"ordinal": 0, "action": "EXCLUDE", "reason": "Fuori dal mapping"}}}
	reply := call(session, "source.analysis.decide", decision)
	if body := assertBatchWire(t, contract, reply); !reply.IsError || body["code"] != nextstep.BatchUnavailable {
		t.Fatal(body)
	}
	decision["decisions"] = append(decision["decisions"].([]any), nextstep.Object{"ordinal": 0, "action": "REFORMULATE", "intent": "via"})
	reply = call(session, "source.analysis.decide", decision)
	if body := assertBatchWire(t, contract, reply); !reply.IsError || body["code"] != "INVALID_TOOL_ARGUMENTS" {
		t.Fatal("duplicate ordinal was accepted", body)
	}
	predecessor := predecessorLegacyAnalysisSession(t, service, scope)
	for _, compared := range []*mcp.ClientSession{session, predecessor} {
		if init := compared.InitializeResult(); init == nil || init.ProtocolVersion != "2026-07-28" {
			t.Fatalf("unexpected MCP negotiation: %#v", init)
		}
	}
	read := nextstep.Object{"analysisId": legacyID}
	legacyWire := func(compared *mcp.ClientSession) []byte {
		t.Helper()
		result := call(compared, "source.analysis.read", read)
		wire, err := json.Marshal(result)
		if err != nil || result.IsError {
			t.Fatalf("legacy read failed: %s (%v)", wire, err)
		}
		return wire
	}
	got, want := legacyWire(session), legacyWire(predecessor)
	if !bytes.Equal(got, want) {
		t.Fatalf("legacy wire changed\ngot  %s\nwant %s", got, want)
	}
}

// Reproduce the successful (analysisId) path of origin/main at 753d99f:
// profile_analysis.go reads and marshals the owner view; workflow.go decorates,
// validates next and returns *mcp.CallToolResult directly. The read and its
// legacy projection use the same implementations and policy for this fixture.
// A separate SDK server applies the predecessor's completion and metadata framing.
func predecessorLegacyAnalysisSession(t *testing.T, service *orchestration.Service, scope analysis.Scope) *mcp.ClientSession {
	t.Helper()
	snapshot, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	workflow, err := newWorkflowServer(nil, snapshot, service.FingerprintKey)
	if err != nil {
		t.Fatal(err)
	}
	const tool = "source.analysis.read"
	capability, ok := analysis.Find(snapshot, tool)
	if !ok {
		t.Fatal("missing analysis read capability")
	}
	registered := registeredTool{capability: capability, limit: 1 << 20}
	coordinator := analysis.Coordinator{Service: service, Store: service.Admission.(analysis.Store), Manifest: snapshot}
	identity := requestIdentity{Identity: orchestration.Identity{
		ServicePrincipalID: scope.Service, PrincipalID: scope.Principal, TenantID: scope.Tenant, ActorType: "HUMAN", Delegation: "human-proof",
	}}
	sdk := mcp.NewServer(&mcp.Implementation{Name: "ouf-mcp-server", Version: "0.1.0"}, nil)
	sdk.AddTool(&mcp.Tool{Name: tool, InputSchema: capability.InputSchema}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ctx = context.WithValue(ctx, identityKey{}, identity)
		var args nextstep.Object
		if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
			return nil, err
		}
		id, _ := args["analysisId"].(string)
		view, err := coordinator.Read(ctx, identity.Identity, id, 0, 128)
		if err != nil {
			return nil, err
		}
		body, err := json.Marshal(view)
		if err != nil {
			return nil, err
		}
		result := workflow.decorate(ctx, registered, args, ownerContent(body, ""))
		return workflow.validateNext(tool, result), nil
	})
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	server, err := sdk.Connect(t.Context(), serverTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	client := mcp.NewClient(&mcp.Implementation{Name: "source-revision", Version: "1"}, nil)
	session, err := client.Connect(t.Context(), clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

func assertBatchWire(t *testing.T, contract *jsonschema.Resolved, result *mcp.CallToolResult) nextstep.Object {
	t.Helper()
	wire, err := json.Marshal(result)
	body := ownerObject(result)
	if err != nil || len(wire) > analysis.BatchMaxResultBytes || result.Meta[batchResultMeta] != nil || len(result.Content) != 1 {
		t.Fatalf("invalid batch wire size=%d error=%v", len(wire), err)
	}
	if err := contract.Validate(body["next"]); err != nil {
		t.Fatal("invalid next", err)
	}
	if text, ok := result.Content[0].(*mcp.TextContent); !ok || text.Text != batchResultText {
		t.Fatal("batch result contained an unfixed text projection")
	}
	return body
}

func TestBatchResultBudgetCountsEscapingAndWaitEnvelope(t *testing.T) {
	snapshot, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	sdk := mcp.NewServer(&mcp.Implementation{Name: "batch-budget", Version: "1"}, nil)
	s, err := newWorkflowServer(sdk, snapshot, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("b", 64)
	const reason = nextstep.AnalysisRunning
	const escaped = "<>&\u2028\u2029\x01"
	identity := requestIdentity{Identity: orchestration.Identity{
		PrincipalID: "user", ServicePrincipalID: "test", TenantID: "tenant", Delegation: "fixture", ActorType: "HUMAN",
	}}
	selectors := nextstep.Object{"analysisId": id, "view": "BATCH"}
	next := s.waits.signer.Postpone(waitBinding(context.WithValue(context.Background(), identityKey{}, identity)),
		nextstep.WaitRef{Read: "source.analysis.read", Args: selectors, Terminal: []string{"COMPLETE"}, Reason: reason},
		time.Now(), time.Now().Add(time.Hour))
	raw := func(padding string) *mcp.CallToolResult {
		return &mcp.CallToolResult{StructuredContent: nextstep.Object{"analysisId": id, "padding": padding, "next": next}}
	}
	var waitPadding string
	for _, capability := range snapshot.ToolEligible() {
		switch capability.ToolName {
		case "ouf.wait":
			s.registerWait(capability)
		case "source.analysis.read":
			var schema jsonschema.Schema
			if err := json.Unmarshal(capability.InputSchema, &schema); err != nil {
				t.Fatal(err)
			}
			s.register(&mcp.Tool{Name: capability.ToolName, InputSchema: &schema}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return raw(waitPadding), nil
			})
		}
	}
	contract := nextContract(t)
	base, _ := json.Marshal(s.finalizeBatch("source.analysis.read", raw("")))
	room := analysis.BatchMaxResultBytes - len(base)
	padding := strings.Repeat(escaped, room/(6*len([]rune(escaped)))) + strings.Repeat("x", room%(6*len([]rune(escaped))))
	result := s.finalizeBatch("source.analysis.read", raw(padding))
	wire, _ := json.Marshal(result)
	if len(wire) != analysis.BatchMaxResultBytes || result.IsError {
		t.Fatalf("exact limit: bytes=%d error=%t", len(wire), result.IsError)
	}
	result = s.finalizeBatch("source.analysis.read", raw(padding+"x"))
	if body := assertBatchWire(t, contract, result); !result.IsError || body["code"] != nextstep.BatchResultLimit {
		t.Fatal("oversize batch read was emitted", body)
	}
	result = s.finalizeBatch("semantic.approval.request", workflowError(nextstep.Object{
		"analysisId": id, "code": "SEM_REVIEW_BATCH_ITEMS_NOT_READY",
		"next": nextstep.BatchBlock(nextstep.ReviewBatchItemsNotReady), "item": strings.Repeat(escaped, 256),
	}))
	if body := assertBatchWire(t, contract, result); body["code"] != nextstep.BatchResultLimit || body["item"] != nil {
		t.Fatal("oversize application error was emitted", body)
	}
	waitEnvelope := func(padding string) *mcp.CallToolResult {
		observation := nextstep.Observation("source.analysis.read", selectors, ownerObject(raw(padding)))
		result := waitResult(true, observation, next)
		result.StructuredContent.(nextstep.Object)["analysisId"] = id
		return s.finalizeBatch("ouf.wait", result)
	}
	waitBase, _ := json.Marshal(waitEnvelope(""))
	waitPadding = strings.Repeat("x", analysis.BatchMaxResultBytes-len(waitBase))
	preSDK := waitEnvelope(waitPadding)
	if wire, err := json.Marshal(preSDK); err != nil || len(wire) != analysis.BatchMaxResultBytes || preSDK.IsError {
		t.Fatalf("wait fixture before SDK framing: bytes=%d isError=%t marshalErr=%v", len(wire), preSDK.IsError, err)
	}

	stream := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return sdk }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stream.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), identityKey{}, identity)))
	}))
	defer httpServer.Close()
	ctx := context.Background()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "batch-budget", Version: "1"}, nil).Connect(ctx, &mcp.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	result, err = session.CallTool(ctx, &mcp.CallToolParams{Name: "ouf.wait", Arguments: next["arguments"]})
	if err != nil {
		t.Fatal(err)
	}
	body := assertBatchWire(t, contract, result)
	if !result.IsError || body["analysisId"] != id || body["changed"] != false || body["observation"] != nil || body["code"] != nextstep.BatchResultLimit {
		t.Fatal("ouf.wait did not fail closed on its final batch envelope", body)
	}
}
