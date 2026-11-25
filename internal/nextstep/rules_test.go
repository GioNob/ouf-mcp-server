package nextstep

import (
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
)

// Unlike the runtime fail-closed check, this test must fail when the projection
// for a declared automatic rule cannot satisfy the published target schema.
func TestEveryAutomaticRuleHasSchemaValidArguments(t *testing.T) {
	s, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	schemas := map[string]*jsonschema.Resolved{}
	decisions := map[string]*jsonschema.Resolved{}
	for _, c := range s.ToolEligible() {
		schemas[c.ToolName] = resolve(t, c.InputSchema)
		decisions[c.ToolName], err = DecisionSchema(c.InputSchema)
		if err != nil {
			t.Fatal(c.ToolName, err)
		}
		if c.ToolName == "source.file.upload" {
			schemas["source.file.upload.status"] = resolve(t, c.HandoffInputSchema)
			decisions["source.file.upload.status"] = schemas["source.file.upload.status"]
		}
	}
	const id = "11111111-1111-4111-8111-111111111111"
	fixture := Object{
		"sourceId": "teatri", "source_ref": "teatri", "type": "TEATRO", "typeCode": "FILE", "canonicalType": "urn:example:teatro",
		"lockVersion": 1, "rowVersion": 2, "revisionId": id, "currentValidationRunId": id,
		"assetId": id, "profileId": id, "jobId": id, "resultRef": id, "proposalId": id, "handoffId": id,
		"onboardingVersionId": id, "runId": id, "nextLineageId": id, "nextHandoffId": id, "requestId": id,
		"hasMore": true, "taskId": strings.Repeat("b", 64), "offset": 1, "columnCount": 3, "nextOffset": 16, "pageSize": 20, "limit": 10, "cursor": id, "nextCursor": id, "afterHandoffId": id,
		"commandKey": id, "analysisId": strings.Repeat("a", 64),
		"columns": []any{Object{"name": "tipologia", "distinctValuesComplete": false, "distinctValuesCursor": "opaque"}}, "result": Object{"requestId": id},
		// The isolated ready and resume rules each need a task in their own state.
		"tasks": []any{Object{"taskId": strings.Repeat("b", 64), "state": "READY"}, Object{"taskId": strings.Repeat("c", 64), "state": "WAITING"}},
		"items": []any{Object{"assetId": id, "latestProfileId": id, "semanticId": "urn:example:teatro", "publicationSetId": id, "revisionId": id}},
	}
	for name, p := range s.NextPolicies {
		if schemas[p.Read] == nil {
			t.Errorf("%s: alternative read unavailable", name)
		}
		for _, r := range p.Rules {
			switch r.Action {
			case "CALL_TOOL", "POLL", "WAIT_FOR_HUMAN":
				target := schemas[r.Tool]
				args := asJSON(t, (Engine{}).project(r, fixture, fixture))
				if target == nil {
					t.Fatalf("%s/%s: missing target %s", name, r.Reason, r.Tool)
				}
				if err := target.Validate(args); err != nil {
					t.Errorf("%s/%s: %s %v: %v", name, r.Reason, r.Tool, args, err)
				}
			case "BLOCKED":
				if r.Tool != "" && schemas[r.Tool] == nil {
					t.Errorf("%s/%s: missing alternative", name, r.Reason)
				}
				if len(r.Alternatives) > 4 {
					t.Fatalf("%s/%s: too many alternatives", name, r.Reason)
				}
				for _, alt := range r.Alternatives {
					if schemas[alt.Tool] == nil || alt.Message == "" {
						t.Errorf("%s/%s: invalid decision alternative %s", name, r.Reason, alt.Tool)
					}
				}
			case "DONE":
			default:
				t.Fatalf("%s: unrecognized action %s", name, r.Action)
			}
		}
	}

	// Exercise every rule in isolation, including rules normally shadowed
	// by earlier matches. Feed its own projected target arguments back as
	// the producer input (e.g. an owner repeats a pagination cursor).
	engine := Engine{Now: time.Now, Valid: func(name string, args Object) bool {
		return schemas[name] != nil && schemas[name].Validate(asJSON(t, args)) == nil
	}}
	validTarget := func(name string, args Object, decision bool) bool {
		schema := schemas[name]
		if decision {
			schema = decisions[name]
		}
		return schema != nil && schema.Validate(asJSON(t, args)) == nil
	}
	check := func(t *testing.T, next Object) {
		t.Helper()
		// CheckTargets may replace alternatives in its argument. Snapshot
		// the wire value first so a partial removal cannot mask a regression.
		before := asJSON(t, next)
		after := asJSON(t, CheckTargets(next, validTarget))
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("target validation changed the policy output\nbefore: %v\nafter:  %v", before, after)
		}
	}
	for _, c := range s.ToolEligible() {
		p := s.NextPolicies[c.NextPolicy]
		for _, rule := range p.Rules {
			t.Run(c.ToolName+"/"+rule.Reason, func(t *testing.T) {
				rule.When = nil
				input := asJSON(t, engine.project(rule, fixture, fixture))
				policy := manifest.Policy{Read: p.Read, Rules: []manifest.Rule{rule}}
				result := engine.Evaluate(c, policy, input, fixture, false)
				check(t, result)
				assertProgress(t, c.ToolName, input, asJSON(t, result))
			})
		}
		for _, rule := range s.NextErrors {
			if tool := rule.When["tool"]; tool != "" && !slices.Contains(strings.Split(tool, "|"), c.ToolName) {
				continue // The manifest restricts this error rule to its producer.
			}
			t.Run(c.ToolName+"/error/"+rule.Reason, func(t *testing.T) {
				rule.When = nil
				engine.Errors = []manifest.Rule{rule}
				input := asJSON(t, engine.project(rule, fixture, fixture))
				result := engine.Evaluate(c, p, input, Object{"code": rule.Reason}, true)
				check(t, result)
				assertProgress(t, c.ToolName, input, asJSON(t, result))
			})
		}
	}
}

// Owner enums checked against 50b ManagedMcpObservation, 50c RecoveryAction,
// 50d ProposalObservation/DiscoveryJobService and 50e SEARCH_OUTCOMES.
func TestOwnerStateVocabularyIsCovered(t *testing.T) {
	s, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range []struct{ policy, field, states string }{
		{"onboarding", "workflowState|state", "DRAFT IN_REVIEW APPROVED ACTIVE SUPERSEDED REJECTED REVOKED CHANGES_REQUESTED COMMAND_ABSENT ERROR"},
		{"discovery", "state", "PENDING RUNNING SUCCEEDED FAILED CANCELLED EXHAUSTED"},
		{"semanticDecision", "state", "DRAFT VALIDATED VALIDATION_FAILED UNDER_REVIEW APPROVED REJECTED EXPIRED ACTIVE DEPRECATED RETIRED UNKNOWN"},
		{"ingestion", "runState|run_state", "AWAITING_START READY PREFLIGHT RUNNING DRAINING RETRY_WAIT SUCCEEDED COMPLETED COMPLETED_WITH_WARNINGS FAILED PAUSED ABORTED DISABLED BLOCKED"},
		{"ingestion", "recoveryAction", "NONE AUTOMATIC_RETRY AUTOMATIC_RECONCILIATION HUMAN_RESUME_OR_ABORT HUMAN_ABORT_REQUIRED HUMAN_QUARANTINE_REVIEW HUMAN_SCHEDULE_REVIEW WAITING_FOR_PUBLICATION_VERIFICATION NEW_PUBLICATION_REQUIRED TERMINAL"},
		{"udp", "outcome", "NO_OBJECTS MATERIALIZATION_PENDING PARTIAL COMPLETE INCOMPLETE"},
		{"urban.acquisition.read", "outcome", "NO_OBJECTS MATERIALIZATION_PENDING PARTIAL COMPLETE INCOMPLETE"},
	} {
		policy := s.NextPolicies[family.policy]
		known := strings.Fields(family.states)
		seen := map[string]bool{}
		for _, state := range policy.Vocabulary[family.field] {
			seen[state] = true
		}
		if len(seen) != len(known) {
			t.Errorf("%s/%s owner vocabulary changed", family.policy, family.field)
		}
		for _, state := range known {
			if !seen[state] {
				t.Errorf("%s: missing %s", family.policy, state)
			}
		}
	}
}

func TestNoApplicableGrantRoutesEveryToolToPermissionProposal(t *testing.T) {
	s, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	engine := Engine{Errors: s.NextErrors}
	for _, c := range s.ToolEligible() {
		for _, field := range []string{"reasonCode", "code", "Code"} {
			next := engine.Evaluate(c, s.NextPolicies[c.NextPolicy], Object{"view": "ROLES"}, Object{field: "NO_APPLICABLE_GRANT"}, true)
			if next["action"] != "BLOCKED" || next["actor"] != "HUMAN_THS" || next["reasonCode"] != "NO_APPLICABLE_GRANT" ||
				!strings.Contains(next["userMessage"].(string), c.RequiredAuthorization) {
				t.Fatalf("%s/%s: %v", c.ToolName, field, next)
			}
			found := false
			for _, alt := range next["alternatives"].([]Object) {
				if strings.Contains(alt["userMessage"].(string), "authorization.permissions.propose") {
					found = true
				}
			}
			if !found {
				t.Fatalf("%s/%s: missing next step for permission proposal", c.ToolName, field)
			}
		}
	}
}

// An owner rejection has no resource to poll. Both offered alternatives
// must validate without first filling a field from conversation history.
func TestRejectedPermissionProposal(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	schemas := map[string]*jsonschema.Resolved{}
	var proposal manifest.Capability
	for _, c := range m.ToolEligible() {
		schemas[c.ToolName] = resolve(t, c.InputSchema)
		if c.ToolName == "authorization.permissions.propose" {
			proposal = c
		}
	}
	e := Engine{Errors: m.NextErrors, Valid: func(tool string, args Object) bool {
		return schemas[tool] != nil && schemas[tool].Validate(asJSON(t, args)) == nil
	}}
	input := Object{"operation": "REVOKE", "grantId": "grant-test", "reason": "review", "commandKey": uuid.NewString()}
	for _, code := range []string{"ONB_BAD_REQUEST", "AUTH_ROLE_CAPABILITY_INVALID", "AUTH_GRANT_CONSTRAINTS_INAPPLICABLE", "invalid grant reference"} {
		obs := Object{"Status": 422, "code": code, "violations": []any{Object{"field": "owner-text"}}}
		next := e.Evaluate(proposal, m.NextPolicies[proposal.NextPolicy], input, obs, true)
		want := code
		if !reasonPattern.MatchString(want) {
			want = "OWNER_REQUEST_FAILED"
		}
		alts := next["alternatives"].([]Object)
		if next["action"] != "BLOCKED" || next["actor"] != "AGENT" || next["reasonCode"] != want || len(alts) != 2 {
			t.Fatal(next)
		}
		if alts[0]["tool"] != "authorization.permissions.read" || alts[0]["arguments"].(Object)["view"] != "ROLES" || alts[1]["tool"] != proposal.ToolName {
			t.Fatal(alts)
		}
		for _, alt := range alts {
			if !e.Valid(alt["tool"].(string), alt["arguments"].(Object)) || strings.Contains(alt["userMessage"].(string), "owner-text") {
				t.Fatal(alt)
			}
		}
		copied := maps.Clone(alts[1]["arguments"].(Object))
		key, err := uuid.Parse(copied["commandKey"].(string))
		if err != nil || key.Version() != 4 || key.String() == input["commandKey"] {
			t.Fatal(copied)
		}
		copied["commandKey"] = input["commandKey"]
		if !reflect.DeepEqual(input, copied) {
			t.Fatal(copied)
		}
		if code == "AUTH_GRANT_CONSTRAINTS_INAPPLICABLE" && !strings.Contains(next["userMessage"].(string), "constraints: null") {
			t.Fatal(next)
		}
	}
}

func TestUnchangedPolicyNeverOffersAnotherPlan(t *testing.T) {
	c := manifest.Capability{ToolName: "source.analysis.plan"}
	input := Object{"commandKey": uuid.NewString()}
	next := (Engine{}).Evaluate(c, manifest.Policy{}, input, Object{"code": "POLICY_UNCHANGED"}, true)
	if next["actor"] != "HUMAN_THS" || next["action"] != "BLOCKED" || next["reasonCode"] != "POLICY_UNCHANGED" {
		t.Fatal(next)
	}
	alts := next["alternatives"].([]Object)
	if len(alts) != 2 || alts[0]["action"] != "WAIT_FOR_HUMAN" || alts[0]["tool"] != nil || alts[1]["tool"] != "authorization.permissions.read" || alts[1]["arguments"].(Object)["view"] != "ROLES" {
		t.Fatal(alts)
	}
	assertProgress(t, c.ToolName, input, asJSON(t, next))
}

func TestPlanEpochErrorOffersRecordedAnalysis(t *testing.T) {
	id, key := strings.Repeat("a", 64), uuid.NewString()
	input := Object{"assetId": uuid.NewString(), "profileId": uuid.NewString(), "commandKey": key}
	code := analysis.ErrEpochChanged.Error()
	next := (Engine{}).Evaluate(manifest.Capability{ToolName: "source.analysis.plan"}, manifest.Policy{},
		input, Object{"code": code, "analysisId": id}, true)
	if next["reasonCode"] != code || next["actor"] != "HUMAN_THS" {
		t.Fatal(next)
	}
	for _, alt := range next["alternatives"].([]Object) {
		if alt["tool"] == "source.analysis.plan" {
			args := alt["arguments"].(Object)
			fresh, _ := args["commandKey"].(string)
			parsed, err := uuid.Parse(fresh)
			if args["replanOf"] != id || len(args) != 2 || err != nil || parsed.Version() != 4 || fresh == key {
				t.Fatal(alt)
			}
			return
		}
	}
	t.Fatal("no replan for the recorded analysis", next)
}

func TestFallbackUsesAvailableReadReferences(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	schemas := map[string]*jsonschema.Resolved{}
	for _, cap := range m.ToolEligible() {
		schemas[cap.ToolName] = resolve(t, cap.InputSchema)
	}
	e := Engine{Errors: m.NextErrors, Valid: func(tool string, args Object) bool {
		return schemas[tool] != nil && schemas[tool].Validate(asJSON(t, args)) == nil
	}}
	for _, tc := range []struct {
		tool, code, actor string
		owner             Object
		failed            bool
	}{
		{"semantic.review.prepare", "OWNER_REQUEST_FAILED", "OPERATOR", Object{}, true},
		{"source.analysis.step", "INVALID_TOOL_ARGUMENTS", "OPERATOR", Object{"code": "INVALID_TOOL_ARGUMENTS"}, true},
		{"authorization.permissions.propose", "OWNER_REQUEST_FAILED", "HUMAN_THS", Object{}, true},
		{"authorization.permissions.propose", "GRANT_REVIEW", "HUMAN_THS", Object{"state": "PENDING_REVIEW"}, false},
		{"semantic.proposal.read", "REVISE_PROPOSAL", "OPERATOR", Object{"state": "REJECTED", "allowedActions": []any{"REVISE_PROPOSAL"}}, false},
	} {
		cap, ok := analysis.Find(m, tc.tool)
		if !ok {
			t.Fatal(tc.tool)
		}
		next := e.Evaluate(cap, m.NextPolicies[cap.NextPolicy], Object{}, tc.owner, tc.failed)
		if next["reasonCode"] != tc.code || next["actor"] != tc.actor || next["action"] != "BLOCKED" {
			t.Fatal(tc.tool, next)
		}
		alts := next["alternatives"].([]Object)
		if len(alts) != 1 || alts[0]["action"] != "WAIT_FOR_HUMAN" || alts[0]["tool"] != nil {
			t.Fatal(tc.tool, alts)
		}
	}
	cap, _ := analysis.Find(m, "source.analysis.step")
	id := strings.Repeat("a", 64)
	next := e.Evaluate(cap, m.NextPolicies[cap.NextPolicy], Object{"analysisId": id}, Object{"code": "INVALID_TOOL_ARGUMENTS"}, true)
	alts := next["alternatives"].([]Object)
	if next["actor"] != "AGENT" || len(alts) != 1 || alts[0]["tool"] != "source.analysis.read" ||
		!reflect.DeepEqual(alts[0]["arguments"], Object{"analysisId": id}) {
		t.Fatal(next)
	}
}

func TestDecisionAlternativesRequireStateIdentifiers(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		tool string
		args Object
		want bool
	}{
		{"authorization.proposal.read", Object{}, false},
		{"authorization.proposal.read", Object{"proposalId": ""}, false},
		{"authorization.proposal.read", Object{"proposalId": "11111111-1111-4111-8111-111111111111"}, true},
		{"semantic.discovery.adopt", Object{"commandKey": uuid.NewString()}, true},
		{"semantic.discovery.adopt", Object{}, false},
		{"source.analysis.step", Object{"analysisId": strings.Repeat("a", 64)}, false},
		{"source.analysis.step", Object{"analysisId": strings.Repeat("a", 64), "taskId": strings.Repeat("b", 64)}, true},
		{"source.onboarding.create", Object{"assetId": uuid.NewString(), "profileId": uuid.NewString()}, true},
		{"source.onboarding.create", Object{"assetId": uuid.NewString(), "profileId": uuid.NewString(), "sourceId": "place"}, true},
	} {
		c, ok := analysis.Find(m, tc.tool)
		if !ok {
			t.Fatal(tc.tool)
		}
		schema, err := DecisionSchema(c.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		if got := schema.Validate(tc.args) == nil; got != tc.want {
			t.Errorf("%s %v: valid=%t", tc.tool, tc.args, got)
		}
	}
}
