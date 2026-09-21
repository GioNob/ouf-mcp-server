package orchestration

// DefaultWindowBudget is the server-owned PET section 78 initial profile for
// the dimensions currently accounted by admission. It is not a tool argument.
func DefaultWindowBudget() Cost {
	return Cost{ToolCalls: 20, DistinctObjects: 1000, ResultBytes: 5 << 20}
}

func ValidWindowBudget(limit Cost) bool {
	return limit.ToolCalls > 0 && limit.ToolCalls <= 50 &&
		limit.DistinctObjects >= 0 && limit.DistinctObjects <= 5000 &&
		limit.ResultBytes > 0 && limit.ResultBytes <= 20<<20
}
