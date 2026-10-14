package orchestration

import (
	"encoding/json"
	"testing"
)

func TestSemanticCommandKeyCannotBypassEquivalentIntentBudget(t *testing.T) {
	key := make([]byte, 32)
	for _, cap := range []string{"ouf.semantic.propose", "ouf.semantic.review.prepare", "ouf.semantic.approval.request"} {
		a, err := SemanticFingerprint(key, cap, json.RawMessage(`{"commandKey":"attempt-a","revisionId":"revision-a","expectedRowVersion":0}`))
		if err != nil {
			t.Fatal(err)
		}
		b, err := SemanticFingerprint(key, cap, json.RawMessage(`{"commandKey":"attempt-b","revisionId":"revision-a","expectedRowVersion":0}`))
		if err != nil {
			t.Fatal(err)
		}
		if a != b {
			t.Fatal("new intent key bypassed semantic equivalence")
		}
		changed, err := SemanticFingerprint(key, cap, json.RawMessage(`{"commandKey":"attempt-a","revisionId":"revision-a","expectedRowVersion":1}`))
		if err != nil {
			t.Fatal(err)
		}
		if a == changed {
			t.Fatal("changed reviewed version lost from intent")
		}
	}
	readA, _ := SemanticFingerprint(key, "ouf.semantic.proposal.read", json.RawMessage(`{"commandKey":"attempt-a"}`))
	readB, _ := SemanticFingerprint(key, "ouf.semantic.proposal.read", json.RawMessage(`{"commandKey":"attempt-b"}`))
	if readA == readB {
		t.Fatal("distinct receipt read targets conflated")
	}
}

func TestSourceReviewCommandKeyCannotResetEquivalentIntentBudget(t *testing.T) {
	key := make([]byte, 32)
	const capability = "ouf.managed-source.onboarding.review.prepare"
	a, err := SemanticFingerprint(key, capability, json.RawMessage(`{"sourceId":"theatres","onboardingVersionId":"version-a","expectedVersion":0,"commandKey":"first"}`))
	if err != nil {
		t.Fatal(err)
	}
	b, err := SemanticFingerprint(key, capability, json.RawMessage(`{"sourceId":"theatres","onboardingVersionId":"version-a","expectedVersion":0,"commandKey":"different"}`))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("new command key bypassed source review intent budget")
	}
	changed, err := SemanticFingerprint(key, capability, json.RawMessage(`{"sourceId":"theatres","onboardingVersionId":"version-a","expectedVersion":1,"commandKey":"first"}`))
	if err != nil {
		t.Fatal(err)
	}
	if a == changed {
		t.Fatal("changed source lock version lost from intent")
	}
}
