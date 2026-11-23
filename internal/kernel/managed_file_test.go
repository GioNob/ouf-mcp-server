package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/authorization"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// D-084 r2, PET-mcp-097/099: only the owner decides which lists are disclosed.
func TestManagedFileDistinctValuesPassThrough(t *testing.T) {
	const asset = "00000000-0000-4000-8000-000000000001"
	const job = "00000000-0000-4000-8000-000000000002"
	const direct = `{"columns":[{"name":"code","distinctValues":["A","a","é"]},{"name":"legacy","distinctValues":2}]}`
	values := make([]string, 2000)
	for i := range values {
		values[i] = fmt.Sprintf("%04d", i) + strings.Repeat("a", 123) + "é"
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	profile := `{"columns":[{"name":"code","dataAccessLabel":"OPEN","distinctValues":` + string(encoded) +
		`},{"name":"restricted","dataAccessLabel":"RESTRICTED","distinctCount":2}]}`
	var calls atomic.Int32
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("X-OUF-Delegation") != "human-proof" {
			t.Error("profile response bypassed delegation")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/managed.file/profile":
			// The command may return a small owner result directly.
			_, _ = io.WriteString(w, direct)
		case "/managed.file/preview":
			_, _ = io.WriteString(w, profile)
		default:
			t.Error("unexpected profile route")
		}
	}))
	defer gateway.Close()
	endpoint, _ := url.Parse(gateway.URL)
	service := &orchestration.Service{
		RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: &statusAdmission{}, FingerprintKey: make([]byte, 32),
		Gateway: &httpclient.GatewayClient{Endpoint: endpoint, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")},
	}
	session := revisionSession(t, service, "ouf.managed-source.file.profile ouf.managed-source.preview")
	for _, tc := range []struct {
		tool string
		args map[string]string
		want string
	}{
		{"source.file.profile", map[string]string{"assetId": asset}, direct},
		{"source.file.preview", map[string]string{"assetId": asset, "jobId": job}, profile},
	} {
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tc.tool, Arguments: tc.args})
		if err != nil {
			t.Fatal(err)
		}
		if result.IsError || len(result.Content) != 1 || result.Content[0].(*mcp.TextContent).Text != tc.want {
			t.Fatalf("%s changed the owner profile disclosure", tc.tool)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("each delegated tool must make one owner call")
	}
}

func TestManagedFileProfileUsesDelegatedGatewayWithoutRawAttachment(t *testing.T) {
	const capID = "ouf.managed-source.file.profile"
	const createCap = "ouf.managed-source.onboarding.create"
	const createKey = "00000000-0000-4000-8000-000000000003"
	const asset = "00000000-0000-4000-8000-000000000001"
	now := time.Now().UTC()
	cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{
		BundleID: "managed", BundleVersion: 1, ActivatedAt: now,
		Bundle: authorization.PolicyBundle{BundleID: "managed", Version: 1, PublishedAt: now,
			Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: capID, Operation: "COMMAND", RequiredScope: capID, AllowedActors: []string{"HUMAN"}}, {CapabilityID: createCap, Operation: "COMMAND", RequiredScope: createCap, AllowedActors: []string{"HUMAN"}}},
			Grants:       []authorization.Grant{{GrantID: "profile", CapabilityID: capID, TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}, {GrantID: "draft", CapabilityID: createCap, TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}},
		},
	}})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	calls := 0
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if (r.URL.Path != "/internal/capabilities/v1/execute/managed.file/profile" && r.URL.Path != "/internal/capabilities/v1/execute/managed.file/create") ||
			r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "human-proof" {
			t.Error("managed-file profile bypassed governed transport")
		}
		var in orchestration.GatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatal(err)
		}
		var args map[string]any
		if err := json.Unmarshal(in.Arguments, &args); err != nil {
			t.Fatal(err)
		}
		if args["assetId"] != asset || in.Owner != "onboarding" || in.OperationClass != "COMMAND" || in.Identity.Delegation != "" {
			t.Error("unsafe managed-file arguments or identity")
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/internal/capabilities/v1/execute/managed.file/create" {
			if in.IdempotencyKey != createKey || in.CapabilityID != createCap || args["profileId"] != "00000000-0000-4000-8000-000000000002" || args["sourceId"] != "cinema" || len(args) != 9 {
				t.Error("unsafe onboarding draft arguments")
			}
			if len(args["fields"].([]any)) != 1024 || len(args["semanticRefs"].([]any)) != 1024 || len(args["sourceObjectKeyFields"].([]any)) != 1024 {
				t.Error("wide command lost columns, references or identity fields")
			}
			io.WriteString(w, `{"sourceId":"cinema","state":"DRAFT"}`)
			return
		}
		if len(args) != 1 {
			t.Error("profile arguments changed")
		}
		io.WriteString(w, `{"jobId":"00000000-0000-4000-8000-000000000002","status":"QUEUED"}`)
	}))
	defer gateway.Close()
	target, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
	service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: &statusAdmission{}, Gateway: &httpclient.GatewayClient{Endpoint: target, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")}, FingerprintKey: make([]byte, 32)}
	handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
	if err != nil {
		t.Fatal(err)
	}
	caller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, v := range map[string]string{"X-OUF-Delegation": "human-proof", "X-OUF-Gateway-Verified": "true", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": "user-a", "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "gateway", "X-OUF-Granted-Scopes": capID + " " + createCap} {
			r.Header.Set(k, v)
		}
		handler.ServeHTTP(w, r)
	}))
	defer caller.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "managed-file-test", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: caller.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "source.file.profile", Arguments: map[string]any{"assetId": asset}})
	if err != nil || result.IsError || calls != 1 {
		t.Fatalf("profile failed: result=%+v err=%v calls=%d", result, err, calls)
	}
	bad, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "source.file.profile", Arguments: map[string]any{"assetId": asset, "url": "https://attacker.invalid/file.csv"}})
	if err != nil {
		t.Fatal(err)
	}
	if !bad.IsError || calls != 1 {
		t.Fatal("tool accepted arbitrary file URL")
	}
	fields := make([]any, 1024)
	refs := make([]string, len(fields))
	keys := make([]string, len(fields))
	for i := range fields {
		property := fmt.Sprintf("urn:test:property:%d", i)
		refs[i] = property + "@1"
		keys[i] = fmt.Sprintf("campo_%d", i)
		fields[i] = map[string]any{"fieldName": keys[i], "extractionDecision": "INCLUDE", "dataAccessLabel": "RESTRICTED", "targetPropertyIri": property}
	}
	draftArgs := map[string]any{
		"analysisId": strings.Repeat("a", 64), "commandKey": createKey,
		"assetId": asset, "profileId": "00000000-0000-4000-8000-000000000002", "sourceId": "cinema", "name": "Cinema", "owner": "Comune", "targetClassIri": "https://example.org/Cinema", "semanticRefs": refs, "sourceObjectKeyFields": keys, "fields": fields,
	}
	draft, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "source.onboarding.create", Arguments: draftArgs})
	if err != nil || draft.IsError || calls != 2 {
		t.Fatalf("draft failed: result=%+v err=%v calls=%d", draft, err, calls)
	}
	draftArgs["fields"] = append(fields, fields[0])
	tooWide, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "source.onboarding.create", Arguments: draftArgs})
	if err != nil || !tooWide.IsError || calls != 2 {
		t.Fatalf("1025 columns dispatched: %v %v %d", tooWide, err, calls)
	}
	draftArgs["fields"] = fields
	largeRefs := make([]string, 4500)
	for i := range largeRefs {
		largeRefs[i] = fmt.Sprintf("urn:test:%d:%s@1", i, strings.Repeat("é", 240))
	}
	draftArgs["semanticRefs"] = largeRefs
	tooLarge, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "source.onboarding.create", Arguments: draftArgs})
	if err != nil || !tooLarge.IsError || calls != 2 {
		t.Fatal("oversized create did not return a tool error before dispatch", err)
	}
	if next := checkNext(t, nextContract(t), tooLarge); next["reasonCode"] != "INVALID_TOOL_ARGUMENTS" || next["action"] != "BLOCKED" {
		t.Fatal(next)
	}
}
