package orchestration

import "testing"

func TestWindowBudgetHardCaps(t *testing.T) {
	if !ValidWindowBudget(DefaultWindowBudget()) || !ValidWindowBudget(Cost{ToolCalls: 50, DistinctObjects: 5000, ResultBytes: 20 << 20}) {
		t.Fatal("PET budget rejected")
	}
	for _, limit := range []Cost{
		{},
		{ToolCalls: 51, ResultBytes: 1},
		{ToolCalls: 1, ResultBytes: 21 << 20},
		{ToolCalls: 1, ResultBytes: 1, DistinctObjects: 5001},
		{ToolCalls: 1, ResultBytes: 1, DistinctObjects: -1},
	} {
		if ValidWindowBudget(limit) {
			t.Fatalf("invalid budget accepted: %+v", limit)
		}
	}
}
