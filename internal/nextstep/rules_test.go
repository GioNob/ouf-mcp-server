package nextstep

import (
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/google/jsonschema-go/jsonschema"
)

// Unlike the runtime fail-closed check, this test must fail when the projection
// for a declared automatic rule cannot satisfy the published target schema.
func TestEveryAutomaticRuleHasSchemaValidArguments(t *testing.T) {
	s, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	schemas := map[string]*jsonschema.Resolved{
		"source.file.upload.status": resolve(t, []byte(manifest.PickerStatusInputSchema)),
	}
	for _, c := range s.ToolEligible() {
		schemas[c.ToolName] = resolve(t, c.InputSchema)
	}
	const id = "11111111-1111-4111-8111-111111111111"
	fixture := Object{
		"sourceId": "teatri", "source_ref": "teatri", "type": "TEATRO", "typeCode": "TEATRO",
		"lockVersion": 1, "rowVersion": 2, "revisionId": id, "currentValidationRunId": id,
		"assetId": id, "profileId": id, "jobId": id, "resultRef": id, "proposalId": id, "handoffId": id,
		"onboardingVersionId": id, "runId": id, "nextLineageId": id, "nextHandoffId": id, "requestId": id,
		"offset": 1, "columnCount": 3, "nextOffset": 16, "pageSize": 20, "limit": 10, "cursor": id, "nextCursor": id, "afterHandoffId": id,
		"commandKey": "fixture-review-1", "analysisId": strings.Repeat("a", 64),
		"columns": []any{Object{"name": "tipologia", "distinctValuesComplete": false, "distinctValuesCursor": "opaque"}}, "result": Object{"requestId": id},
		"tasks": []any{Object{"taskId": strings.Repeat("b", 64), "state": "READY"}},
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
			case "DONE":
			default:
				t.Fatalf("%s: unrecognized action %s", name, r.Action)
			}
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
		{"discovery", "state", "PENDING RUNNING SUCCEEDED FAILED CANCELLED"},
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
