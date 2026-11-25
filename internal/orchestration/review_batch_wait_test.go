package orchestration

import (
	"encoding/json"
	"testing"
)

func TestReviewBatchPollRead(t *testing.T) {
	for _, tc := range []struct {
		class, args string
		want        bool
	}{
		{"READ", `{"reviewBatchId":"11111111-1111-4111-8111-111111111111","expectedVersion":"v"}`, true},
		{"READ", `{"commandKey":"single-receipt"}`, true},
		{"READ", `{"reviewContextRef":"context"}`, false},
		{"COMMAND", `{"reviewBatchId":"11111111-1111-4111-8111-111111111111"}`, false},
	} {
		in := Invocation{CapabilityID: "ouf.semantic.proposal.read", OperationClass: tc.class, Arguments: json.RawMessage(tc.args), InternalWaitRead: true}
		if got := isPollRead(in); got != tc.want {
			t.Fatalf("%s %s: poll read = %t", tc.class, tc.args, got)
		}
	}
}
