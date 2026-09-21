package postgres

// Subtract from the bounded cap instead of adding untrusted/large values.
// Negative persisted usage is corruption, never newly available quota.
func budgetWouldExceed(limit, requested int64, used ...int64) bool {
	if limit < 0 || requested < 0 || requested > limit {
		return true
	}
	remaining := limit - requested
	for _, value := range used {
		if value < 0 || value > remaining {
			return true
		}
		remaining -= value
	}
	return false
}
