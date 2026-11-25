package kernel

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// MCP-50P-004. Exercise owner problem projection through the SDK and consume
// only text at the client. No command is executed to interpret a refusal.
func TestOnboardingOwnerRail(t *testing.T) {
	const create = "source.onboarding.create"
	const prepare = "source.onboarding.review.prepare"
	const read = "source.onboarding.read"
	const version = "11111111-1111-4111-8111-111111111111"
	analysis := strings.Repeat("a", 64)
	raw, err := os.ReadFile("testdata/value-types/source.onboarding.create.json")
	var fixture struct{ Arguments railObject }
	if err != nil || json.Unmarshal(raw, &fixture) != nil {
		t.Fatal("missing create fixture", err)
	}
	createArgs := fixture.Arguments
	createArgs["commandKey"], createArgs["analysisId"] = uuid.NewString(), analysis
	validCreate := maps.Clone(createArgs)
	validCreate["semanticRefs"] = []string{"urn:example:place@1.0.0"}
	readArgs := railObject{"sourceId": createArgs["sourceId"], "onboardingVersionId": version, "analysisId": analysis}
	prepareArgs := maps.Clone(readArgs)
	prepareArgs["expectedVersion"], prepareArgs["commandKey"] = float64(0), "previous-prepare"
	var proposal railObject
	if err := json.Unmarshal([]byte(reviewExecutionJSON), &proposal); err != nil {
		t.Fatal(err)
	}
	mismatchArgs := maps.Clone(prepareArgs)
	mismatchArgs["executionProposal"] = proposal
	asset := railObject{"assetId": validCreate["assetId"], "profileId": validCreate["profileId"]}
	replaceArgs := maps.Clone(mismatchArgs)
	replaceArgs["draftAssetRef"] = asset
	readers := []railObject{{"fieldName": "shape", "transform": "TEXT_NFC_TRIM_V1", "allowedTransforms": []string{"IDENTITY"}}}
	draft := railObject{"reasonCode": "EXECUTION_PROPOSAL_REQUIRED", "workflowState": "DRAFT", "sourceId": readArgs["sourceId"],
		"onboardingVersionId": version, "lockVersion": 0, "allowedActions": []string{"PREPARE_REVIEW"}, "executionProposalRequired": true,
		"missingSections": []string{"udp.resolution", "limits"}}
	for _, tc := range []struct {
		tool, reason, target string
		args, owner          railObject
		status               int
	}{
		{create, "EXECUTION_PROPOSAL_REQUIRED", prepare, validCreate, draft, 200},
		{read, "EXECUTION_PROPOSAL_REQUIRED", prepare, readArgs, draft, 200},
		{prepare, "ONB_EXECUTION_PROPOSAL_REQUIRED", prepare, prepareArgs, railObject{"missingSections": []string{"udp.resolution", "limits"}}, 422},
		{create, "ONB_SEMANTIC_REF_FORM_INVALID", create, createArgs, railObject{"index": float64(0)}, 422},
		{prepare, "ONB_GOVERNED_IDENTITY_INVALID", prepare, replaceArgs, identityDiagnostic("SIGNALS_INCOMPLETE"), 422},
		{prepare, "ONB_GOVERNED_IDENTITY_INVALID", prepare, mismatchArgs, railObject{}, 422},
		{prepare, "ONB_ACQUISITION_LIMIT_BELOW_PROFILE", prepare, replaceArgs, railObject{"limit": "maxRows", "observed": 13, "ceiling": 80}, 422},
		{prepare, "ONB_ACQUISITION_LIMIT_INSTALLATION_EXCEEDED", "", replaceArgs, railObject{"limit": "maxColumns", "observed": 14, "installationMaximum": 12}, 422},
		{prepare, "ONB_SEMANTIC_REF_VERSION_MISMATCH", read, mismatchArgs, railObject{"semanticRef": "urn:example:place@1.0.0"}, 422},
		{create, "ONB_READER_UNSUPPORTED", read, validCreate, railObject{"fieldName": "shape"}, 422},
		{create, "ONB_READER_UNSUPPORTED", create, validCreate, railObject{"fields": readers}, 422},
		{prepare, "ONB_DATATYPE_UNRESOLVED", create, replaceArgs, railObject{"fields": readers, "resolutions": []string{"REPLACE_DRAFT"}}, 422},
		{create, "ONB_REVISION_ASSET_SOURCE_CONFLICT", read, validCreate, railObject{"resolutions": []string{"CREATE_REVISION"}, "boundSourceId": "readable-source"}, 409},
		{create, "ONB_REVIEW_STALE_VERSION", read, validCreate, railObject{}, 412},
		{prepare, "ONB_REVIEW_INTERNAL_ERROR", "", prepareArgs, railObject{}, 500},
	} {
		t.Run(tc.tool+"/"+tc.reason, func(t *testing.T) {
			owner := maps.Clone(tc.owner)
			if tc.status >= 400 {
				owner["code"], owner["detail"], owner["retryable"] = tc.reason, "private-owner-diagnostic", true
			}
			if rows, ok := owner["fields"].([]railObject); ok {
				row := maps.Clone(rows[0])
				row["sample"] = "private-owner-diagnostic"
				owner["fields"] = []railObject{row}
			}
			gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req orchestration.GatewayRequest
				if json.NewDecoder(r.Body).Decode(&req) != nil || strings.Contains(string(req.Arguments), "analysisId") || strings.Contains(string(req.Arguments), "draftAssetRef") {
					t.Error("invalid owner dispatch")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(owner)
			}))
			defer gateway.Close()
			u, _ := url.Parse(gateway.URL)
			service := &orchestration.Service{RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: &statusAdmission{},
				Gateway: &httpclient.GatewayClient{Endpoint: u, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("fixture")}, FingerprintKey: make([]byte, 32)}
			session := revisionSession(t, service, "ouf.managed-source.onboarding.create ouf.managed-source.onboarding.review.prepare ouf.managed-source.onboarding.read")
			res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tc.tool, Arguments: tc.args})
			if err != nil || res.IsError != (tc.status >= 400) {
				t.Fatalf("unexpected tool result %v, %v", res, err)
			}
			next := checkNext(t, nextContract(t), res)
			body := res.StructuredContent.(railObject)
			if res.IsError {
				body = textError(t, res)
				next = body["next"].(railObject)
				payload, _ := json.Marshal(res)
				if strings.Contains(string(payload), "private-owner-diagnostic") {
					t.Fatal("owner detail escaped")
				}
			}
			for k, v := range tc.owner {
				want, _ := json.Marshal(v)
				got, _ := json.Marshal(body[k])
				if string(got) != string(want) {
					t.Fatalf("owner field %s: %s, want %s", k, got, want)
				}
			}
			if next["reasonCode"] != tc.reason {
				t.Fatal(next)
			}
			if tc.target == "" {
				if next["action"] != "BLOCKED" || next["actor"] != "OPERATOR" || next["alternatives"].([]any)[0].(railObject)["tool"] != nil {
					t.Fatal("internal failure offered a retry", next)
				}
				return
			}
			step := next
			if tc.reason != "ONB_SEMANTIC_REF_VERSION_MISMATCH" {
				if next["action"] != "BLOCKED" || next["actor"] != "AGENT" {
					t.Fatal("expected agent decision", next)
				}
				step = next["alternatives"].([]any)[0].(railObject)
			}
			if step["action"] != "CALL_TOOL" || step["tool"] != tc.target {
				t.Fatal("incorrect continuation", step)
			}
			args := step["arguments"].(railObject)
			switch tc.target {
			case prepare, create:
				key, _ := args["commandKey"].(string)
				if uuid.Validate(key) != nil || key == tc.args["commandKey"] {
					t.Fatal("expected a fresh command key")
				}
				if tc.target == prepare {
					if strings.Contains(tc.reason, "EXECUTION_PROPOSAL_REQUIRED") && !strings.Contains(next["userMessage"].(string), "udp.resolution, limits") {
						t.Fatal("missing sections absent from message", next)
					}
					expected := withKey(prepareArgs, key)
					if ref := tc.args["draftAssetRef"]; ref != nil {
						expected["draftAssetRef"] = ref
					}
					encoded, _ := json.Marshal(next)
					if strings.Contains(string(encoded), "urn:fixture:materialized") || strings.Contains(string(encoded), "fixture-rule") {
						t.Fatal("diagnostic values escaped into next", next)
					}
					message := next["userMessage"].(string)
					if tc.reason == "ONB_ACQUISITION_LIMIT_BELOW_PROFILE" && !strings.Contains(message, "Ometti maxRows oppure indica un valore fra 13 e 80") {
						t.Fatal(message)
					}
					if tc.reason == "ONB_GOVERNED_IDENTITY_INVALID" {
						keyword := "comparatore"
						if tc.owner["constraint"] != nil {
							keyword = "expectedSignals"
						}
						if !strings.Contains(message, keyword) {
							t.Fatal(message)
						}
					}
					if !reflect.DeepEqual(args, expected) {
						t.Fatal("incorrect prepare arguments", args)
					}
				} else if tc.reason == "ONB_READER_UNSUPPORTED" || tc.reason == "ONB_DATATYPE_UNRESOLVED" {
					want := railObject{"assetId": validCreate["assetId"], "profileId": validCreate["profileId"], "sourceId": validCreate["sourceId"], "analysisId": analysis, "commandKey": key}
					if !reflect.DeepEqual(args, want) || !strings.Contains(next["userMessage"].(string), "fields[].allowedTransforms") {
						t.Fatal("reader correction lost its draft references", next)
					}
				} else if !reflect.DeepEqual(args, withKey(createArgs, key)) || !strings.Contains(next["userMessage"].(string), "semanticId@semanticVersion") {
					t.Fatal("create correction lost original request", step)
				}
			case read:
				want := readArgs
				if tc.tool == create {
					want = railObject{"sourceId": createArgs["sourceId"], "view": "LATEST", "analysisId": analysis}
					switch tc.reason {
					case "ONB_REVISION_ASSET_SOURCE_CONFLICT":
						want["sourceId"], want["revisionAssetRef"] = tc.owner["boundSourceId"], asset
					case "ONB_REVIEW_STALE_VERSION":
						want["draftAssetRef"] = asset
					}
				}
				if !reflect.DeepEqual(args, want) {
					t.Fatal("incorrect read arguments", args)
				}
			}
		})
	}
}

func withKey(args railObject, key string) railObject {
	out := maps.Clone(args)
	out["commandKey"] = key
	return out
}

func identityDiagnostic(constraint string) railObject {
	const iri = "urn:fixture:materialized"
	return railObject{
		"constraint": constraint, "path": "/executionProposal/udp/resolution/governedIdentity/signals",
		"propertyIris": []string{iri}, "propertyCount": 1,
		"expectedSignals": []railObject{{"id": iri, "semanticRef": iri + "@11111111-1111-4111-8111-111111111111", "comparator": "TEXT_V1"}},
		"ruleIds":         []string{"fixture-rule"},
	}
}

func TestInvalidPreparationDiagnostic(t *testing.T) {
	const identity = "ONB_GOVERNED_IDENTITY_INVALID"
	const below = "ONB_ACQUISITION_LIMIT_BELOW_PROFILE"
	const exceeds = "ONB_ACQUISITION_LIMIT_INSTALLATION_EXCEEDED"
	for _, tc := range []struct {
		code, field string
		value       any
	}{
		{identity, "constraint", nil}, {identity, "constraint", "UNKNOWN"},
		{identity, "path", "/executionProposal/privateHeader"}, {identity, "path", "/executionProposal/signals/-1"},
		{identity, "path", "/executionProposal/" + strings.Repeat("1", 512)}, {identity, "path", "/executionProposalX"},
		{identity, "propertyIris", []string{"urn:test:with space"}}, {identity, "propertyIris", []string{"relative"}},
		{identity, "propertyIris", []string{"urn:test:\u0085x"}}, {identity, "propertyIris", []string{"urn:test:" + strings.Repeat("a", 2048)}},
		{identity, "propertyIris", make([]string, 65)}, {identity, "propertyCount", 0}, {identity, "propertyCount", 2.5},
		{identity, "expectedSignals", []railObject{{"id": "urn:test:a", "semanticRef": "urn:test:b@11111111-1111-4111-8111-111111111111", "comparator": "TEXT_V1"}}},
		{identity, "expectedSignals", []railObject{{"id": "urn:test:a", "semanticRef": "urn:test:a@bad", "comparator": "TEXT_V1"}}},
		{identity, "expectedSignals", []railObject{{"id": "urn:test:a", "semanticRef": "urn:test:a@11111111-1111-4111-8111-111111111111", "comparator": "BAD"}}},
		{identity, "expectedSignals", make([]railObject, 65)}, {identity, "expectedSignals", nil},
		{identity, "ruleIds", make([]string, 33)}, {identity, "ruleIds", []int{1}},
		{below, "limit", "arbitraryText"}, {below, "observed", -1}, {below, "observed", 1.5}, {below, "ceiling", 0},
		{exceeds, "installationMaximum", nil}, {exceeds, "installationMaximum", "small"}, {exceeds, "limit", "arbitraryText"},
	} {
		t.Run(tc.code+"/"+tc.field, func(t *testing.T) {
			body := identityDiagnostic("SIGNALS_INCOMPLETE")
			if tc.code != identity {
				body = railObject{"limit": "maxRows", "ceiling": 20, "observed": 12, "installationMaximum": 10}
			}
			body[tc.field] = tc.value
			raw, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			result := onboardingProblem("source.onboarding.review.prepare", &orchestration.Problem{Code: tc.code, Status: 422}, raw)
			if !result.IsError || ownerObject(result)["code"] != "OWNER_PROBLEM_INVALID" {
				t.Fatal(ownerObject(result))
			}
		})
	}
}

func TestGovernedIdentityCorrectionByConstraint(t *testing.T) {
	snapshot, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	c, _ := analysis.Find(snapshot, "source.onboarding.review.prepare")
	e := nextstep.Engine{Errors: snapshot.NextErrors}
	var schemaDoc jsonschema.Schema
	if err := json.Unmarshal(c.InputSchema, &schemaDoc); err != nil {
		t.Fatal(err)
	}
	schema, err := schemaDoc.Resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	e.Valid = func(tool string, args railObject) bool { return tool == c.ToolName && schema.Validate(args) == nil }
	const version = "11111111-1111-4111-8111-111111111111"
	const code = "ONB_GOVERNED_IDENTITY_INVALID"
	input := railObject{"sourceId": "source", "onboardingVersionId": version, "expectedVersion": float64(2), "commandKey": "old",
		"executionProposal": railObject{"private": "fixture-data"}}
	messages := map[string]bool{}
	for _, tc := range []struct{ constraint, instruction string }{
		{"RESOLUTION_SHAPE", "GOVERNED_IDENTITY"},
		{"POLICY_FIELDS", "campi di governedIdentity"},
		{"POLICY_REFERENCE", "policyRef e strategyVersion"},
		{"POLICY_SOURCE", "sourceId"},
		{"POLICY_TENANT", "tenantId"},
		{"POLICY_CLASS", "canonicalClass"},
		{"POLICY_BOUNDS", "1–1000"},
		{"SIGNAL_SHAPE", "assertionRef"},
		{"SIGNAL_NOT_MAPPED", "propertyIris"},
		{"SIGNAL_DUPLICATE", "duplicato"},
		{"SIGNAL_FLAGS", "excludesOnDisagreement=false"},
		{"SIGNAL_COMPARATOR", "comparator"},
		{"SIGNAL_SEMANTIC_REF", "semanticRef"},
		{"SIGNAL_ASSERTION_REF", "asserzione"},
		{"SIGNALS_INCOMPLETE", "expectedSignals"},
		{"RULE_SHAPE", "signalIds"},
		{"RULE_DUPLICATE", "ruleIds"},
		{"RULE_MEMBERS", "da 1 a 32"},
		{"DISTINCT_RULE_MEMBERS", "da 2 a 32"},
	} {
		t.Run(tc.constraint, func(t *testing.T) {
			diagnostic := identityDiagnostic(tc.constraint)
			if strings.HasPrefix(tc.constraint, "POLICY_") || tc.constraint == "RESOLUTION_SHAPE" {
				diagnostic = railObject{"constraint": tc.constraint, "path": "/executionProposal/udp/resolution"}
			}
			body, _ := json.Marshal(diagnostic)
			owner := ownerObject(onboardingProblem(c.ToolName, &orchestration.Problem{Status: 422, Code: code}, body))
			n := e.Evaluate(c, snapshot.NextPolicies[c.NextPolicy], input, owner, true)
			message := n["userMessage"].(string)
			if n["reasonCode"] != code || n["action"] != "BLOCKED" || n["actor"] != "AGENT" ||
				messages[message] || !strings.Contains(message, tc.instruction) || !strings.Contains(message, "nuova commandKey") {
				t.Fatal(n)
			}
			messages[message] = true
			alts := n["alternatives"].([]railObject)
			if len(alts) != 1 || alts[0]["tool"] != c.ToolName || alts[0]["userMessage"] != message {
				t.Fatal(n)
			}
			args := alts[0]["arguments"].(railObject)
			if len(args) != 4 || args["sourceId"] != "source" || args["onboardingVersionId"] != version || args["expectedVersion"] != float64(2) ||
				args["commandKey"] == "old" || !e.Valid(c.ToolName, args) {
				t.Fatal(args)
			}
			raw, _ := json.Marshal(n)
			if strings.Contains(string(raw), "fixture") || strings.Contains(string(raw), "executionProposal") {
				t.Fatal(n)
			}
		})
	}
}

func TestInstallationMaximumOffersRegisteredUpload(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	const code = "ONB_ACQUISITION_LIMIT_INSTALLATION_EXCEEDED"
	const tool = "source.onboarding.review.prepare"
	c, _ := analysis.Find(m, tool)
	for _, registered := range []bool{true, false} {
		e := nextstep.Engine{Errors: m.NextErrors, Valid: func(name string, args railObject) bool {
			return registered && name == "source.file.upload" && args != nil && len(args) == 0
		}}
		next := e.Evaluate(c, m.NextPolicies[c.NextPolicy], nil, railObject{"code": code, "Status": 422}, true)
		if next["action"] != "BLOCKED" || next["actor"] != "OPERATOR" || next["reasonCode"] != code {
			t.Fatal(next)
		}
		alts := next["alternatives"].([]railObject)
		if len(alts) != 1 {
			t.Fatal(alts)
		}
		if registered {
			if alts[0]["action"] != "CALL_TOOL" || alts[0]["tool"] != "source.file.upload" || !reflect.DeepEqual(alts[0]["arguments"], railObject{}) {
				t.Fatal(alts)
			}
		} else if alts[0]["tool"] != nil {
			t.Fatal("unregistered upload offered", alts)
		}
	}
}
