package postgres

import (
	"math"
	"testing"
)

func TestBudgetArithmeticFailsClosed(t *testing.T) {
	if budgetWouldExceed(20, 1, 10, 9) {
		t.Fatal("exact cap should be admissible")
	}
	for _, value := range []int64{-1, 20, math.MaxInt64} {
		if !budgetWouldExceed(20, 1, value, 1) {
			t.Fatalf("invalid or overflowing usage admitted: %d", value)
		}
	}
	if !budgetWouldExceed(20, math.MaxInt64, 0) || !budgetWouldExceed(20, -1, 0) {
		t.Fatal("invalid request admitted")
	}
}
