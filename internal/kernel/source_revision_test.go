package kernel

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/authorization"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Official SDK -> scoped policy -> admission -> TLS Gateway transport.
// The owner response is a fixture, not a real HUMAN approval or source run.
func TestSourceRevisionUsesExactParentScopeAndStableCommand(t *testing.T) {
	const version = "11111111-1111-4111-8111-111111111111"
	const executionJSON = `[{"propertyIri":"urn:name","sourceField":"urn:name","reconciliation":{"policyRef":"urn:ouf:policy:linked-object-property-reconciliation:v1","cardinality":"SINGLE","normalizerRef":"EXACT_TYPED","normalizerVersion":1,"validity":"SOURCE_INTERVAL"}}]`
	var execution []any
	if err := json.Unmarshal([]byte(executionJSON), &execution); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ tool, capability, operation, mode string }{
		{"source.onboarding.revision.prepare", "ouf.onboarding.revision.prepare", "COMMAND", "prepare"},
		{"source.onboarding.revision.read", "ouf.onboarding.revision.read", "READ", "read"},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			var calls atomic.Int32
			gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/internal/capabilities/v1/execute/source.revision/"+tc.mode || r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "human-proof" {
					t.Error("governed source review transport changed")
				}
				var in orchestration.GatewayRequest
				if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
					t.Fatal(err)
				}
				assertGatewayDispatchSchema(t, "GATEWAY_SOURCE_REVISION_PAIRWISE_ROOT", "mcp-gateway-source-revision-"+tc.mode+"-dispatch-v1.json", in)
				if in.CapabilityID != tc.capability || in.Owner != "onboarding" || in.OperationClass != tc.operation || in.MaxResultBytes != 65536 {
					t.Error("owner or result bound changed")
				}
				if tc.operation == "COMMAND" && in.IdempotencyKey != "review-theatres-1" {
					t.Error("stable command key lost")
				}
				var args map[string]any
				if err := json.Unmarshal(in.Arguments, &args); err != nil {
					t.Fatal(err)
				}
				if tc.operation == "COMMAND" && !reflect.DeepEqual(args["changes"], execution) {
					t.Error("typed execution proposal changed")
				}
				if tc.operation == "COMMAND" {
					fingerprint, err := orchestration.SemanticFingerprint(make([]byte, 32), in.CapabilityID, in.Arguments)
					if err != nil || fingerprint != "v1:hmac-sha256:765c169274a84d711d930786f71c4a4e12620288c1155912d8eb363626abe99d" ||
						in.RequestHash != "64b3d403dca4b227fc1da0cec79e3983568f5d6b5b6d62ec0707bdaa8180b351" || r.Header.Get("If-Match") != "" {
						t.Error("historical changes fingerprint, request hash or headers changed")
					}
				}
				if args["sourceId"] != "theatres" || (args["onboardingVersionId"] != version && args["parentVersionId"] != version) {
					t.Error("source/version binding lost")
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"state":"IN_REVIEW","reviewUrl":"https://ths.invalid/review/fixture"}`)
			}))
			defer gateway.Close()
			admission := &statusAdmission{}
			service := revisionService(t, gateway, admission, "theatres", version)
			for _, scope := range []string{tc.capability, "mcp.connect"} {
				session := revisionSession(t, service, scope)
				for _, source := range []string{"theatres", "other-source"} {
					args := map[string]any{"sourceId": source, "onboardingVersionId": version}
					if tc.operation == "READ" && source == "theatres" {
						delete(args, "onboardingVersionId")
						args["parentVersionId"] = version
						args["commandKey"] = "review-theatres-1"
					}
					if tc.operation == "COMMAND" {
						delete(args, "onboardingVersionId")
						args["parentVersionId"] = version
						args["configurationHash"] = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
						args["publicationEffect"] = "NEW_PUBLICATION_MAY_START_ACQUISITION"
						args["expectedVersion"] = 0
						args["commandKey"] = "review-theatres-1"
						args["changes"] = execution
					}
					before, admitted := calls.Load(), admission.calls.Load()
					res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tc.tool, Arguments: args})
					if err != nil {
						t.Fatal(err)
					}
					allowed := scope == tc.capability && source == "theatres"
					if allowed {
						if res.IsError || calls.Load() != before+1 {
							t.Fatalf("authorized review failed: %+v", res)
						}
					} else if !res.IsError || calls.Load() != before || admission.calls.Load() != admitted {
						t.Fatal("denied source review reached admission or Gateway")
					}
					args["approvedBy"] = "user-a"
					before = calls.Load()
					res, err = session.CallTool(context.Background(), &mcp.CallToolParams{Name: tc.tool, Arguments: args})
					if err == nil && !res.IsError {
						t.Fatal("tool accepted caller approval")
					}
					if calls.Load() != before {
						t.Fatal("undeclared authority dispatched")
					}
				}
			}
		})
	}
}

func revisionService(t *testing.T, gateway *httptest.Server, admission *statusAdmission, source, parent string) *orchestration.Service {
	t.Helper()
	now := time.Now().UTC()
	bundle := authorization.PolicyBundle{BundleID: "source-review", Version: 1, PublishedAt: now}
	for _, descriptor := range []struct{ capability, operation string }{
		{"ouf.onboarding.revision.prepare", "COMMAND"},
		{"ouf.onboarding.revision.read", "READ"},
	} {
		bundle.Capabilities = append(bundle.Capabilities, authorization.CapabilityDescriptor{
			CapabilityID: descriptor.capability, Operation: descriptor.operation, RequiredScope: descriptor.capability, AllowedActors: []string{"HUMAN"},
		})
		bundle.Grants = append(bundle.Grants, authorization.Grant{
			GrantID: descriptor.capability, CapabilityID: descriptor.capability, TenantID: "tenant-a", SubjectID: "user-a",
			ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour),
			Constraints: &authorization.GrantConstraints{Effect: "ALLOW", ResourceType: "source", ResourceID: source, ResourceAttributes: map[string]string{"onboardingVersionId": parent}},
		})
	}
	cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{BundleID: bundle.BundleID, BundleVersion: 1, ActivatedAt: now, Bundle: bundle}})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	endpoint, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
	return &orchestration.Service{
		RequireDelegation: true, Auth: cache, Admission: admission, FingerprintKey: make([]byte, 32),
		Gateway: &httpclient.GatewayClient{Endpoint: endpoint, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")},
	}
}

func revisionSession(t *testing.T, service *orchestration.Service, scope string) *mcp.ClientSession {
	t.Helper()
	handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, v := range map[string]string{"X-OUF-Gateway-Verified": "true", "X-OUF-Delegation": "human-proof", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": "user-a", "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "gateway", "X-OUF-Granted-Scopes": scope} {
			r.Header.Set(k, v)
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	client := mcp.NewClient(&mcp.Implementation{Name: "source-revision", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return session
}

func TestSourceRevisionReceiptSchemas(t *testing.T) {
	server := testServer(t)
	defer server.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "revision-discovery", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	list, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	columns := func(schema any) any {
		object, _ := schema.(map[string]any)
		properties, _ := object["properties"].(map[string]any)
		return properties["newColumns"]
	}
	var input, prepareReceipt, readReceipt any
	for _, tool := range list.Tools {
		switch tool.Name {
		case "source.onboarding.revision.prepare":
			input, prepareReceipt = columns(tool.InputSchema), columns(tool.OutputSchema)
		case "source.onboarding.revision.read":
			readReceipt = columns(tool.OutputSchema)
		}
	}
	if input == nil || !reflect.DeepEqual(input, prepareReceipt) || !reflect.DeepEqual(input, readReceipt) {
		t.Fatal("tools/list must expose the decision contract on prepare input and both receipts")
	}
}

func TestSourceRevisionNewAsset(t *testing.T) {
	const prepare = "source.onboarding.revision.prepare"
	const read = "source.onboarding.revision.read"
	const parent = "00000000-0000-4000-8000-000000000001"
	const asset = "00000000-0000-4000-8000-000000000002"
	const profile = "00000000-0000-4000-8000-000000000003"
	const key = "second-upload-1"
	const excluded = `{"column":"note","decision":"EXCLUDE"}`
	const mapped = `{"column":"prezzo","decision":"MAP","mapping":{"propertyIri":"urn:example:prezzo","transform":"DECIMAL_DOT_V1","reconciliation":{"policyRef":"urn:ouf:policy:linked-object-property-reconciliation:v1","cardinality":"SINGLE","normalizerRef":"EXACT_TYPED","normalizerVersion":1,"validity":"UNBOUNDED"},"accessLabel":"SENSITIVE","authorityOrder":["source-b","source-a"],"comparison":{"comparator":"DECIMAL_V1","assertionRef":"urn:example:price-comparison"}}}`
	const missing = `[{"code":"MISSING_MAPPED_COLUMN","column":"amount","expected":"DECIMAL","actual":null},{"code":"NEW_COLUMN_DECISION_REQUIRED","column":"total","expected":null,"actual":"STRING"},{"code":"NEW_COLUMN_DECISION_REQUIRED","column":"prezzo","expected":null,"actual":"DECIMAL"}]`
	const notNew = `[{"code":"COLUMN_NOT_NEW","column":"amount","expected":null,"actual":"DECIMAL"}]`
	assetRef := map[string]any{"assetId": asset, "profileId": profile}
	readArgs := map[string]any{"sourceId": "source-example", "parentVersionId": parent, "commandKey": key}
	for _, tc := range []struct{ name, decisions, diagnostics string }{
		{name: "compatible"},
		{name: "explicit empty", decisions: "[]"},
		{name: "exclude", decisions: "[" + excluded + "]"},
		{name: "map", decisions: "[" + mapped + "]"},
		{name: "mixed", decisions: "[" + excluded + "," + mapped + "]"},
		{name: "missing decisions", decisions: "[" + excluded + "]", diagnostics: missing},
		{name: "not new", decisions: `[{"column":"amount","decision":"EXCLUDE"}]`, diagnostics: notNew},
		{name: "map without mapping", decisions: `[{"column":"prezzo","decision":"MAP"}]`},
		{name: "oversize owner body", decisions: "[" + excluded + "]"},
		{name: "lost response", decisions: "[" + mapped + "]"},
		{name: "unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prepareArgs := map[string]any{
				"sourceId": "source-example", "parentVersionId": parent, "commandKey": key,
				"configurationHash": "sha256:" + strings.Repeat("a", 64), "expectedVersion": float64(7),
				"publicationEffect": "NEW_PUBLICATION_MAY_START_ACQUISITION", "assetRef": assetRef,
			}
			recorded := "[]"
			if tc.decisions != "" {
				var decisions []any
				if err := json.Unmarshal([]byte(tc.decisions), &decisions); err != nil {
					t.Fatal(err)
				}
				prepareArgs["newColumns"] = decisions
				recorded = tc.decisions
			}
			receipt := `{"state":"IN_REVIEW","assetRef":{"assetId":"` + asset + `","profileId":"` + profile + `"},"newColumns":` + recorded + `,"diagnostics":[],"onboardingVersionId":"00000000-0000-4000-8000-000000000004","challengeId":"00000000-0000-4000-8000-000000000005","humanApprovalRequired":true}`
			problem := `{"code":"ONB_REVISION_CSV_INCOMPATIBLE","diagnostics":` + tc.diagnostics + `}`
			var prepares, reads atomic.Int32
			gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var in orchestration.GatewayRequest
				var args map[string]any
				if json.NewDecoder(r.Body).Decode(&in) != nil || json.Unmarshal(in.Arguments, &args) != nil {
					t.Error("invalid dispatch envelope")
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "human-proof" || in.Owner != "onboarding" {
					t.Error("governed transport changed")
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/internal/capabilities/v1/execute/source.revision/prepare":
					prepares.Add(1)
					if !reflect.DeepEqual(args, prepareArgs) || in.CapabilityID != "ouf.onboarding.revision.prepare" || in.IdempotencyKey != key || r.Header.Get("Idempotency-Key") != key || r.Header.Get("If-Match") != `W/"ov:7"` {
						t.Error("file replacement lost its parent, intent or command binding")
					}
					assertGatewayDispatchSchema(t, "GATEWAY_SOURCE_REVISION_PAIRWISE_ROOT", "mcp-gateway-source-revision-prepare-dispatch-v1.json", in)
					if tc.diagnostics != "" {
						w.WriteHeader(http.StatusUnprocessableEntity)
						_, _ = io.WriteString(w, strings.Replace(problem, `"column":"amount"`, `"sample":"private","column":"amount"`, 1))
						return
					}
					switch tc.name {
					case "lost response":
						// The owner accepted the command, but its receipt was cut short.
						w.Header().Set("Content-Length", "100")
						_, _ = io.WriteString(w, "{")
						return
					case "unavailable":
						w.Header().Set("Retry-After", "15")
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
				case "/internal/capabilities/v1/execute/source.revision/read":
					reads.Add(1)
					if prepares.Load() != 1 || !reflect.DeepEqual(args, readArgs) || in.CapabilityID != "ouf.onboarding.revision.read" || r.Header.Get("If-Match") != "" {
						t.Error("receipt lookup did not reconcile the original command")
					}
					assertGatewayDispatchSchema(t, "GATEWAY_SOURCE_REVISION_PAIRWISE_ROOT", "mcp-gateway-source-revision-read-dispatch-v1.json", in)
				default:
					t.Error("unexpected revision route")
					w.WriteHeader(http.StatusNotFound)
					return
				}
				_, _ = io.WriteString(w, receipt)
			}))
			defer gateway.Close()
			admission := &statusAdmission{}
			service := revisionService(t, gateway, admission, "source-example", parent)
			session := revisionSession(t, service, "ouf.onboarding.revision.prepare ouf.onboarding.revision.read")
			if tc.name == "map without mapping" {
				res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: prepare, Arguments: prepareArgs})
				if (err == nil && !res.IsError) || admission.calls.Load() != 0 || prepares.Load() != 0 {
					t.Fatal("MAP without mapping must fail before admission and dispatch")
				}
				return
			}
			call := func(tool string, args map[string]any, wantError bool, want string) {
				t.Helper()
				res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
				if err != nil || res.IsError != wantError || len(res.Content) != 1 {
					t.Fatalf("unexpected %s result: %+v %v", tool, res, err)
				}
				if got := res.Content[0].(*mcp.TextContent).Text; got != want {
					t.Fatalf("%s returned %s, want %s", tool, got, want)
				}
				if !wantError {
					var owner map[string]any
					if err := json.Unmarshal([]byte(want), &owner); err != nil || !reflect.DeepEqual(res.StructuredContent, owner) {
						t.Fatalf("%s structured receipt differs from the owner: %+v", tool, res.StructuredContent)
					}
				}
			}
			if tc.name == "oversize owner body" {
				body, err := json.Marshal(map[string]any{"Arguments": prepareArgs})
				if err != nil {
					t.Fatal(err)
				}
				column := prepareArgs["newColumns"].([]any)[0].(map[string]any)
				column["column"] = "note" + strings.Repeat("a", 65537-len(body))
				call(prepare, prepareArgs, true, `{"code":"OWNER_COMMAND_ARGUMENTS_TOO_LARGE","retryable":false}`)
				if admission.calls.Load() != 0 || prepares.Load() != 0 {
					t.Fatal("oversize signed body reached admission or Gateway")
				}
				return
			}
			if tc.diagnostics != "" {
				call(prepare, prepareArgs, true, problem)
				if prepares.Load() != 1 || reads.Load() != 0 {
					t.Error("permanent incompatibility triggered another dispatch")
				}
				return
			}
			switch tc.name {
			case "lost response":
				call(prepare, prepareArgs, true, `{"code":"SOURCE_REVIEW_READ_REQUIRED","retryable":false}`)
			case "unavailable":
				unavailable, _ := json.Marshal(orchestration.Problem{Code: "SOURCE_REVIEW_READ_REQUIRED", Status: 503, RetryAfter: "15"})
				call(prepare, prepareArgs, true, string(unavailable))
			default:
				call(prepare, prepareArgs, false, receipt)
			}
			call(read, readArgs, false, receipt)
			if prepares.Load() != 1 || reads.Load() != 1 {
				t.Fatalf("unexpected dispatch count: prepare=%d read=%d", prepares.Load(), reads.Load())
			}
		})
	}
}
