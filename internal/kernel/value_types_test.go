package kernel

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// D-084, PET-mcp-002/097/099. The synthetic owner receives the proposal
// unchanged and stands in for resolution of the published references.
func TestValueTypesGovernedDispatch(t *testing.T) {
	for _, tc := range []struct {
		name, scope, owner, route, replace string
		valueMap                           bool
	}{
		{"semantic.propose", "ouf.semantic.propose", "semantic", "semantic/create", "9007199254740993", false},
		{"semantic.propose", "ouf.semantic.propose", "semantic", "semantic/create", "", true},
		{"source.onboarding.create", "ouf.managed-source.onboarding.create", "onboarding", "managed.file/create", "itemReaderParams", false},
		{"source.onboarding.revision.prepare", "ouf.onboarding.revision.prepare", "onboarding", "source.revision/prepare", "items", false},
	} {
		testName := tc.name
		if tc.valueMap {
			testName += "/VALUE_MAP"
		}
		t.Run(testName, func(t *testing.T) {
			data, err := os.ReadFile("testdata/value-types/" + tc.name + ".json")
			if err != nil {
				t.Fatal(err)
			}
			var fixture orchestration.GatewayRequest
			if err := json.Unmarshal(data, &fixture); err != nil {
				t.Fatal(err)
			}
			args := string(fixture.Arguments)
			var want map[string]any
			if err := json.Unmarshal(fixture.Arguments, &want); err != nil {
				t.Fatal(err)
			}
			ownerCode := "OWNER_MAPPING_INVALID"
			var invalid []string
			if tc.valueMap {
				ownerCode = "SEMANTIC_VALUE_MAP_INVALID"
				want["artifactType"] = "VALUE_MAP"
				want["rationale"] = map[string]any{"sourceColumn": map[string]string{"fieldName": "categoria", "sourceId": "places"}}
				want["definition"] = map[string]any{
					"targetVocabulary": "urn:example:vocabulary@00000000-0000-4000-8000-000000000001",
					"entries": []map[string]string{
						{"code": "b", "concept": "urn:concept:1"},
						{"code": "B", "concept": "urn:concept:1"},
						{"code": "a", "concept": "urn:concept:2"},
					},
					"labels": map[string]string{"it": "Codici"},
				}
				data, err := json.Marshal(want)
				if err != nil {
					t.Fatal(err)
				}
				args = string(data)
				if err := json.Unmarshal(data, &want); err != nil {
					t.Fatal(err)
				}
				invalid = []string{
					strings.Replace(args, `"it":"Codici"`, `"it":"Co\ud800dici"`, 1),
					strings.Replace(args, `"it":"Codici"`, `"it":"Codici","\u0069t":"Codici"`, 1),
					strings.Replace(args, `"code":"a"`, `"code":"b"`, 1),
					strings.Replace(args, `"code":"a"`, `"code":"private-source-value "`, 1),
					strings.Replace(args, `"code":"a"`, `"code":"A\u009fB"`, 1),
					strings.Replace(args, `"code":"a"`, `"code":"\u00a0A1\u00a0"`, 1),
					strings.Replace(args, `"code":"a"`, `"code":"`+strings.Repeat("x", 129)+`"`, 1),
					strings.Replace(args, `"concept":"urn:concept:2"`, `"concept":"urn:`+strings.Repeat("x", 509)+`"`, 1),
					strings.Replace(args, `"fieldName":"categoria"`, `"fieldName":""`, 1),
				}
			} else {
				invalid = []string{strings.Replace(args, tc.replace, "private-source-value", 1)}
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
				if tc.name == "semantic.propose" && !tc.valueMap && !strings.Contains(string(in.Arguments), "9007199254740993") {
					t.Error("owner received a rounded component bound")
				}
				w.Header().Set("Content-Type", "application/json")
				if reject.Load() {
					w.WriteHeader(http.StatusUnprocessableEntity)
					_ = json.NewEncoder(w).Encode(map[string]string{"code": ownerCode, "detail": "private-source-value"})
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
			for _, args := range invalid {
				rejected := call(args)
				if !rejected.IsError || len(rejected.Content) != 1 || rejected.Content[0].(*mcp.TextContent).Text != `{"code":"INVALID_TOOL_ARGUMENTS","retryable":false}` ||
					calls.Load() != 0 || admission.calls.Load() != 0 {
					t.Fatal("invalid arguments must produce a safe code before admission")
				}
			}
			result := call(args)
			if result.IsError || calls.Load() != 1 || admission.calls.Load() != 1 {
				t.Fatal("valid arguments must reach their owner exactly once")
			}
			body := map[string]any{"state": "DRAFT"}
			if tc.name == "source.onboarding.revision.prepare" {
				body = map[string]any{"newColumns": want["newColumns"], "compositions": want["compositions"]}
			}
			checkOwnerAndNext(t, result, body)
			reject.Store(true)
			result = call(args)
			if !result.IsError || len(result.Content) != 1 || calls.Load() != 2 {
				t.Fatal("missing owner rejection")
			}
			message := result.Content[0].(*mcp.TextContent).Text
			if !strings.Contains(message, ownerCode) || strings.Contains(message, "private-source-value") {
				t.Fatal("owner rejection must preserve the code and omit source data")
			}
		})
	}
}

// D-084 r2, PET-mcp-002/097/099: byte boundaries through SDK and the Gateway adapter.
func TestValueMapProposalByteLimits(t *testing.T) {
	const tool = "semantic.propose"
	const capability = "ouf.semantic.propose"
	const definitionLimit = 786432
	const argumentLimit = 1048576
	fixture, err := os.ReadFile("testdata/value-types/" + tool + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var request struct{ Arguments map[string]any }
	if err := json.Unmarshal(fixture, &request); err != nil {
		t.Fatal(err)
	}
	args := request.Arguments
	encode := func(value any) string {
		t.Helper()
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(encoded)
	}
	legacyLabel := strings.Repeat("&", 3000)
	args["labels"] = map[string]string{"it": legacyLabel, "en": legacyLabel, "de": legacyLabel, "sl": legacyLabel}
	legacyOversize := encode(args)
	if len(legacyOversize) <= 65536 || len(legacyOversize) >= argumentLimit {
		t.Fatal("invalid legacy fixture size")
	}
	args["artifactType"] = "VALUE_MAP"
	args["labels"] = map[string]string{"it": "/////"}
	args["rationale"] = map[string]any{"sourceColumn": map[string]string{"fieldName": "categoria"}}
	entries := make([]map[string]string, 2000)
	for i := range entries {
		entries[i] = map[string]string{"code": fmt.Sprintf("%04d", i) + strings.Repeat("a", 124), "concept": "urn:" + strings.Repeat("z", 508)}
	}
	labels := map[string]string{"it": "Codici"}
	definition := map[string]any{
		"targetVocabulary": "urn:example:vocabulary@00000000-0000-4000-8000-000000000001",
		"entries":          entries,
		"labels":           labels,
	}
	args["definition"] = definition
	allFieldsMax := encode(args)
	if len(encode(definition)) <= argumentLimit {
		t.Fatal("all fields at their maxima must exceed both byte limits")
	}
	for _, entry := range entries {
		entry["concept"] = entry["concept"][:239]
	}
	entries[len(entries)-1]["concept"] = "urn:" + strings.Repeat("z", 508)
	// The canonical definition writes these five characters directly. The
	// final literal backslash sequence still occupies seven JSON bytes.
	entries[0]["code"] = "0000&<>\u2028\u2029" + `\u2028` + strings.Repeat("a", 113)
	const extraGoEscapeBytes = 3*5 + 2*3
	labels["it"] = ""
	const controls = "\b\f\n\r\t\x00\x0b\x1f"
	const canonicalControls = `\b\f\n\r\t\u0000\u000b\u001f`
	labels["it"] = controls + strings.Repeat("A", definitionLimit-len(encode(definition))+extraGoEscapeBytes-len(canonicalControls))
	valid := encode(args)
	wantDefinitionBytes := len(encode(definition)) - extraGoEscapeBytes - (len(encode(controls)) - 2 - len(canonicalControls))
	if wantDefinitionBytes != definitionLimit || len(valid) >= argumentLimit {
		t.Fatal("invalid canonical boundary fixture")
	}
	labels["it"] += "A"
	definitionOversize := encode(args)
	// Equivalent JSON string escapes exercise the inbound argument limit
	// without changing the canonical definition or exceeding its byte cap.
	padding := argumentLimit - len(valid)
	pad := func(extra int) string {
		padded := strings.Replace(valid, "z", `\u007a`, extra/5)
		return strings.Replace(padded, "/", `\/`, extra%5)
	}
	atLimit, overLimit := pad(padding), pad(padding+1)
	if len(atLimit) != argumentLimit || len(overLimit) != argumentLimit+1 {
		t.Fatal("invalid transport boundary fixture")
	}
	var calls atomic.Int32
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var in orchestration.GatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error("invalid Gateway envelope")
			return
		}
		var sent struct {
			Definition json.RawMessage
			Rationale  struct{ SourceColumn struct{ FieldName string } }
		}
		if err := json.Unmarshal(in.Arguments, &sent); err != nil {
			t.Error("invalid proposal in Gateway envelope")
			return
		}
		if in.IdempotencyKey != args["commandKey"] ||
			r.URL.Path != "/semantic/create" || r.Header.Get("X-OUF-Delegation") != "human-proof" ||
			sent.Rationale.SourceColumn.FieldName != "categoria" {
			t.Error("governed dispatch changed the proposal metadata")
		}
		if got := len(sent.Definition); got != definitionLimit {
			t.Errorf("Gateway definition has %d bytes, want %d", got, definitionLimit)
		}
		if !strings.Contains(string(sent.Definition), canonicalControls) {
			t.Error("Gateway definition changed RFC 8785 control escapes")
		}
		if !strings.Contains(string(sent.Definition), `&<>`+"\u2028\u2029"+`\\u2028`) {
			t.Error("Gateway definition changed the UTF-8 or literal escape bytes")
		}
		if in.RequestHash != fmt.Sprintf("%x", sha256.Sum256(in.Arguments)) {
			t.Error("dispatched argument bytes do not match RequestHash")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":"DRAFT"}`))
	}))
	defer gateway.Close()
	endpoint, _ := url.Parse(gateway.URL)
	admission := &statusAdmission{}
	service := &orchestration.Service{
		RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: admission, FingerprintKey: make([]byte, 32),
		Gateway: &httpclient.GatewayClient{Endpoint: endpoint, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")},
	}
	session := revisionSession(t, service, capability)
	for _, tc := range []struct{ name, input, code string }{
		{"legacy over 64 KiB", legacyOversize, "OWNER_COMMAND_ARGUMENTS_TOO_LARGE"},
		{"2000 entries with both max-length fields", allFieldsMax, "INVALID_TOOL_ARGUMENTS"},
		{"canonical limit plus one", definitionOversize, "INVALID_TOOL_ARGUMENTS"},
		{"transport limit plus one", overLimit, "OWNER_COMMAND_ARGUMENTS_TOO_LARGE"},
		{"exact transport and canonical limits", atLimit, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: json.RawMessage(tc.input)})
			if err != nil {
				t.Fatal(err)
			}
			if tc.code != "" {
				want := fmt.Sprintf(`{"code":%q,"retryable":false}`, tc.code)
				if !result.IsError || len(result.Content) != 1 || result.Content[0].(*mcp.TextContent).Text != want ||
					calls.Load() != 0 || admission.calls.Load() != 0 {
					t.Fatal("expected a stable size error before admission and dispatch")
				}
				return
			}
			if result.IsError || calls.Load() != 1 || admission.calls.Load() != 1 {
				t.Fatal("boundary request did not reach the Gateway once")
			}
		})
	}
}
