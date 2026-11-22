package kernel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// D-084, PET-mcp-002/097/099. A structured type pin may be unknown to
// MCP. Only the owner resolves it. No external owner is used in this test.
func TestValueTypesGovernedDispatch(t *testing.T) {
	const asset = "00000000-0000-4000-8000-000000000012"
	const pin = "urn:example:place@" + asset
	const nested = `[{
		"kind":"STRUCTURED", "typeRef":"` + pin + `", "targetPropertyIri":"urn:place", "dataAccessLabel":"OPEN",
		"components":[{"component":"address","structured":{
			"typeRef":"urn:example:address@` + asset + `","components":[{
				"component":"shape", "fieldName":"shape", "reader":"WKT_V1", "transformParams":{"crs":"EPSG:3003"}
			},{
				"component":"label","constructor":{
					"constructorId":"TEXT_JOIN_V1","inputs":[
						{"role":"part","fieldName":"via","reader":"TEXT_NFC_TRIM_V1"},
						{"role":"part","fieldName":"civico","reader":"IDENTITY"}
					],"params":{"separator":" / "}
				}
			}]
		}}]
	}]`
	const create = `{
		"assetId":"` + asset + `", "profileId":"` + asset + `", "sourceId":"places",
		"name":"Places", "owner":"Comune", "targetClassIri":"urn:place",
		"semanticRefs":["` + pin + `"], "sourceObjectKeyFields":[],
		"fields":[
			{"fieldName":"via","extractionDecision":"INCLUDE","dataAccessLabel":"OPEN"},
			{"fieldName":"civico","extractionDecision":"INCLUDE","dataAccessLabel":"OPEN"},{
			"fieldName":"shape", "extractionDecision":"INCLUDE", "dataAccessLabel":"OPEN",
			"transform":"WKT_V1", "transformParams":{"crs":"EPSG:3003"}
		}], "compositions":` + nested + `
	}`
	const revision = `{
		"sourceId":"places", "parentVersionId":"` + asset + `", "expectedVersion":1,
		"configurationHash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"commandKey":"rev-1", "publicationEffect":"NEW_PUBLICATION_MAY_START_ACQUISITION",
		"assetRef":{"assetId":"` + asset + `","profileId":"` + asset + `"},
		"newColumns":[{"column":"shape","decision":"MAP","mapping":{
			"propertyIri":"urn:shape", "transform":"WKT_V1", "transformParams":{"crs":"EPSG:3003"},
			"reconciliation":{"policyRef":"urn:ouf:policy:linked-object-property-reconciliation:v1",
				"cardinality":"SINGLE","normalizerRef":"EXACT_TYPED","normalizerVersion":1,"validity":"UNBOUNDED"}
		}}], "compositions":` + nested + `
	}`
	const proposal = `{
		"commandKey":"draft-1", "semanticId":"urn:example:container", "artifactType":"DATATYPE",
		"namespace":"urn:example:", "localName":"container", "ownerRef":"owner", "authorityRef":"authority",
		"semanticVersion":"1.0.0", "labels":{"it":"Contenitore"}, "definition":{"components":[
			{"name":"location","type":"` + pin + `","cardinality":"MANY","required":true,"labels":{"en":"Locations"}},
			{"name":"price","type":"decimal","cardinality":"ONE","required":false,
			 "constraints":{"minInclusive":"0.001","maxInclusive":"9007199254740993"}}
		]}
	}`
	for _, tc := range []struct {
		name, scope, owner, route, args, replace string
	}{
		{"semantic.propose", "ouf.semantic.propose", "semantic", "semantic/create", proposal, "decimal"},
		{"source.onboarding.create", "ouf.managed-source.onboarding.create", "onboarding", "managed.file/create", create, "WKT_V1"},
		{"source.onboarding.revision.prepare", "ouf.onboarding.revision.prepare", "onboarding", "source.revision/prepare", revision, "WKT_V1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var want map[string]any
			if err := json.Unmarshal([]byte(tc.args), &want); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			var reject atomic.Bool
			gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var in orchestration.GatewayRequest
				var got map[string]any
				if json.NewDecoder(r.Body).Decode(&in) != nil || json.Unmarshal(in.Arguments, &got) != nil ||
					in.Owner != tc.owner || in.CapabilityID != tc.scope || in.OperationClass != "COMMAND" ||
					r.URL.Path != "/"+tc.route || r.Header.Get("X-OUF-Delegation") != "human-proof" ||
					!reflect.DeepEqual(got, want) {
					t.Error("owner routing or arguments changed")
				}
				if tc.name == "semantic.propose" && !strings.Contains(string(in.Arguments), "9007199254740993") {
					t.Error("owner received a rounded component bound")
				}
				w.Header().Set("Content-Type", "application/json")
				if reject.Load() {
					w.WriteHeader(http.StatusUnprocessableEntity)
					_ = json.NewEncoder(w).Encode(map[string]string{"code": "OWNER_MAPPING_INVALID", "detail": "private-source-value"})
					return
				}
				if tc.name == "source.onboarding.revision.prepare" {
					_ = json.NewEncoder(w).Encode(map[string]any{"newColumns": got["newColumns"], "compositions": got["compositions"]})
				} else {
					_ = json.NewEncoder(w).Encode(map[string]string{"state": "DRAFT"})
				}
			}))
			defer gateway.Close()
			admission := &statusAdmission{}
			endpoint, _ := url.Parse(gateway.URL)
			service := &orchestration.Service{
				RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: admission, FingerprintKey: make([]byte, 32),
				Gateway: &httpclient.GatewayClient{Endpoint: endpoint, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")},
			}
			session := revisionSession(t, service, tc.scope)
			call := func(args string) *mcp.CallToolResult {
				t.Helper()
				result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tc.name, Arguments: json.RawMessage(args)})
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			invalid := strings.Replace(tc.args, tc.replace, "private-source-value", 1)
			rejected := call(invalid)
			if !rejected.IsError || len(rejected.Content) != 1 || rejected.Content[0].(*mcp.TextContent).Text != `{"code":"INVALID_TOOL_ARGUMENTS","retryable":false}` ||
				calls.Load() != 0 || admission.calls.Load() != 0 {
				t.Fatal("invalid arguments must produce a safe code before admission")
			}
			result := call(tc.args)
			if result.IsError || calls.Load() != 1 || admission.calls.Load() != 1 {
				t.Fatal("valid arguments must reach their owner exactly once")
			}
			if tc.name == "source.onboarding.revision.prepare" {
				if !reflect.DeepEqual(result.StructuredContent, map[string]any{"newColumns": want["newColumns"], "compositions": want["compositions"]}) {
					t.Fatal("revision receipt lost mapping details")
				}
			}
			reject.Store(true)
			result = call(tc.args)
			if !result.IsError || len(result.Content) != 1 || calls.Load() != 2 {
				t.Fatal("missing owner rejection")
			}
			message := result.Content[0].(*mcp.TextContent).Text
			if !strings.Contains(message, "OWNER_MAPPING_INVALID") || strings.Contains(message, "private-source-value") {
				t.Fatal("owner rejection must preserve the code and omit source data")
			}
		})
	}
}
