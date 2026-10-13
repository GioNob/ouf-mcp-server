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
