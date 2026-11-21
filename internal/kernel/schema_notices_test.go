package kernel

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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

const noticeRow = `{"noticeId":"notice-1","sourceId":"source-a","sourceName":"Theatres","since":"2026-11-21T10:00:00Z","lastObservedAt":"2026-11-21T11:00:00Z","observationPolls":3,"differences":[{"field":"name","change":"ADDED"}],"nextAction":"REVISE_ONBOARDING","revision":{"tool":"source.onboarding.revision.prepare","parentVersion":7,"observedSchemaRef":"schema-2"}}`

func TestSchemaNoticesGovernedRead(t *testing.T) {
	const tool = "source.onboarding.notices.read"
	const cursor = "page+/=&%2F"
	now := time.Now().UTC()
	bundle := authorization.PolicyBundle{
		BundleID: "notices", Version: 1, PublishedAt: now,
		Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: schemaNoticesCapability, Operation: "READ", RequiredScope: schemaNoticesCapability, AllowedActors: []string{"HUMAN"}}},
		Grants:       []authorization.Grant{{GrantID: "collection", CapabilityID: schemaNoticesCapability, TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}},
	}
	cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{BundleID: bundle.BundleID, BundleVersion: 1, ActivatedAt: now, Bundle: bundle}})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	var sourceGrantRevoked atomic.Bool
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.RequestURI() != "/internal/capabilities/v1/execute/ouf.onboarding.schema-notices.read" ||
			r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "human-proof" {
			t.Error("unexpected Gateway route or delegation")
		}
		var in orchestration.GatewayRequest
		if json.NewDecoder(r.Body).Decode(&in) != nil || in.Owner != "onboarding" || in.CapabilityID != schemaNoticesCapability ||
			in.OperationClass != "READ" || in.AuthorizationDecisionRef != "notices:1:"+schemaNoticesCapability ||
			in.GatewayBindingRef != "capability://"+schemaNoticesCapability || in.MaxResultBytes != 65536 ||
			in.Identity.PrincipalID != "user-a" || in.Identity.TenantID != "tenant-a" || in.Identity.ActorType != "HUMAN" {
			t.Error("incorrect authorized envelope")
		}
		var args schemaNoticesInput
		if json.Unmarshal(in.Arguments, &args) != nil || args.State != "OPEN" {
			t.Error("missing OPEN state")
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case sourceGrantRevoked.Load():
			io.WriteString(w, emptySchemaNotices)
		case args.After == "" && args.Limit == 20:
			io.WriteString(w, `{"notices":[`+noticeRow+`],"next":"`+cursor+`","hasMore":true}`)
		case args.After == cursor && args.Limit == 1:
			io.WriteString(w, `{"notices":[`+strings.Replace(noticeRow, "notice-1", "notice-2", 1)+`],"next":null,"hasMore":false}`)
		case args.After == "temporarily-unavailable":
			w.Header().Set("Retry-After", "15")
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, `{"code":"private-id","detail":"private-id"}`)
		default:
			t.Errorf("unexpected page arguments %s", in.Arguments)
		}
	}))
	defer gateway.Close()
	client, err := httpclient.NewGateway(gateway.URL+"/internal/capabilities/v1/execute", "workload")
	if err != nil {
		t.Fatal(err)
	}
	client.Client = gateway.Client()
	admission := &statusAdmission{}
	service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: admission, Gateway: client, FingerprintKey: make([]byte, 32)}
	call := func(session *mcp.ClientSession, args map[string]any) *mcp.CallToolResult {
		t.Helper()
		res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	res := call(revisionSession(t, service, "mcp.connect"), map[string]any{})
	if !res.IsError || !strings.Contains(res.Content[0].(*mcp.TextContent).Text, "NOT_AUTHORIZED") || admission.calls.Load() != 0 || calls.Load() != 0 {
		t.Fatal("missing scope did not stop the read before admission")
	}
	session := revisionSession(t, service, schemaNoticesCapability)
	for _, args := range []map[string]any{{"limit": 0}, {"limit": 101}, {"after": ""}, {"after": strings.Repeat("x", 1025)}, {"state": "RESOLVED"}, {"sourceId": "other"}, {"tenantId": "other"}} {
		if !call(session, args).IsError || calls.Load() != 0 {
			t.Fatalf("invalid page dispatched: %v", args)
		}
	}
	readPage := func(args map[string]any) map[string]any {
		t.Helper()
		result := call(session, args)
		var page map[string]any
		if result.IsError || json.Unmarshal([]byte(result.Content[0].(*mcp.TextContent).Text), &page) != nil || !reflect.DeepEqual(page, result.StructuredContent) {
			t.Fatalf("invalid MCP page: %+v", result)
		}
		return page
	}
	first := readPage(map[string]any{})
	row := first["notices"].([]any)[0].(map[string]any)
	var expected map[string]any
	if json.Unmarshal([]byte(noticeRow), &expected) != nil || !reflect.DeepEqual(row, expected) || first["hasMore"] != true || first["next"] != cursor {
		t.Fatalf("lost revision, differences or cursor: %v", first)
	}
	nextArgs := map[string]any{"state": "OPEN", "limit": 1, "after": first["next"]}
	last := readPage(nextArgs)
	if last["notices"].([]any)[0].(map[string]any)["noticeId"] != "notice-2" || last["next"] != nil || last["hasMore"] != false {
		t.Fatalf("incorrect final page: %v", last)
	}
	sourceGrantRevoked.Store(true)
	empty := readPage(nextArgs)
	if len(empty["notices"].([]any)) != 0 || empty["next"] != nil || empty["hasMore"] != false || len(empty) != 3 || calls.Load() != 3 {
		t.Fatal("reread did not use the current owner projection")
	}
	sourceGrantRevoked.Store(false)
	res = call(session, map[string]any{"after": "temporarily-unavailable"})
	if !res.IsError || !strings.Contains(res.Content[0].(*mcp.TextContent).Text, `"retryAfterSeconds":15`) || strings.Contains(res.Content[0].(*mcp.TextContent).Text, "private-id") {
		t.Fatal("owner outage must surface as a sanitized error")
	}
}

func TestSchemaNoticesOwnerOutcomes(t *testing.T) {
	const args = `{"state":"OPEN","limit":1,"after":"previous"}`
	for _, tc := range []struct {
		name   string
		result orchestration.Result
		err    error
		want   string
	}{
		{"empty", orchestration.Result{Body: []byte(emptySchemaNotices)}, nil, emptySchemaNotices},
		{"no grant", orchestration.Result{}, &orchestration.AuthorizationDeniedError{ReasonCode: "NO_APPLICABLE_GRANT"}, emptySchemaNotices},
		{"denied", orchestration.Result{Problem: &orchestration.Problem{Status: 403}}, nil, "NOT_AUTHORIZED"},
		{"bad cursor", orchestration.Result{Problem: &orchestration.Problem{Status: 400}}, nil, "INVALID_SCHEMA_NOTICES_REQUEST"},
		{"missing route", orchestration.Result{Problem: &orchestration.Problem{Status: 404}}, nil, "SCHEMA_NOTICES_READ_FAILED"},
		{"throttled", orchestration.Result{Problem: &orchestration.Problem{Status: 429}}, nil, "SCHEMA_NOTICES_UNAVAILABLE"},
		{"oversize", orchestration.Result{}, orchestration.ErrResultLimit, "RESULT_LIMIT_EXCEEDED"},
		{"replay", orchestration.Result{Replay: true}, nil, "SCHEMA_NOTICES_REQUIRE_FRESH_READ"},
		{"malformed", orchestration.Result{Body: []byte(`{}`)}, nil, "INVALID_SCHEMA_NOTICES_PAGE"},
		{"hidden count", orchestration.Result{Body: []byte(`{"notices":[],"next":null,"hasMore":false,"total":1}`)}, nil, "INVALID_SCHEMA_NOTICES_PAGE"},
		{"over limit", orchestration.Result{Body: []byte(`{"notices":[` + noticeRow + `,` + noticeRow + `],"next":null,"hasMore":false}`)}, nil, "INVALID_SCHEMA_NOTICES_PAGE"},
		{"repeated cursor", orchestration.Result{Body: []byte(`{"notices":[` + noticeRow + `],"next":"previous","hasMore":true}`)}, nil, "INVALID_SCHEMA_NOTICES_PAGE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, structured, err := schemaNoticesResult(tc.result, tc.err, []byte(args))
			body := res.Content[0].(*mcp.TextContent).Text
			if tc.want == emptySchemaNotices {
				if err != nil || res.IsError || body != emptySchemaNotices || string(structured.(json.RawMessage)) != body {
					t.Fatal("empty or unauthorized collection is distinguishable")
				}
			} else if err != nil || !res.IsError || structured != nil || !strings.Contains(body, tc.want) {
				t.Fatalf("wrong owner outcome: %s", body)
			}
		})
	}
}

func TestSchemaNoticesProjectsOnlyValidatedValues(t *testing.T) {
	row := strings.Replace(noticeRow, `"parentVersion":7`, `"parentVersion":9007199254740993`, 1)
	owner := `{"notices":[{"private":"hidden-source"}],"notices":[` + row + `],"hasMore":false,"next":null}`
	res, structured, err := schemaNoticesResult(orchestration.Result{Body: []byte(owner)}, nil, []byte(`{"limit":1}`))
	if err != nil || res.IsError {
		t.Fatal("failed to project valid notice")
	}
	body := res.Content[0].(*mcp.TextContent).Text
	if strings.Contains(body, "hidden-source") || !strings.Contains(body, `"parentVersion":9007199254740993`) || string(structured.(json.RawMessage)) != body {
		t.Fatal("shadowed member exposed or revision changed")
	}
}
