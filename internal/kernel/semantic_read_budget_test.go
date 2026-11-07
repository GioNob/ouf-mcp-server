package kernel

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestSemanticReadBudgetWindowResponseIsExplicit(t *testing.T) {
	end := time.Date(2026, 11, 7, 20, 8, 0, 0, time.UTC)
	result := semanticReadAdmissionErrorResult(orchestration.NewSemanticReadBudgetExhaustedError(errors.New("budget"), "TOOL_CALLS", end.Add(-17*time.Second), end))
	if result == nil || !result.IsError || len(result.Content) != 1 {
		t.Fatal(result)
	}
	text, ok := result.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatal("text result missing")
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(text.Text), &body); err != nil {
		t.Fatal(err)
	}
	if len(body) != 5 || body["code"] != "SEMANTIC_READ_WINDOW_BUDGET_EXHAUSTED" || body["retryable"] != true ||
		body["retryAfterSeconds"] != float64(17) || body["windowEndsAt"] != "2026-11-07T20:08:00Z" || body["dimension"] != "TOOL_CALLS" {
		t.Fatal(body)
	}
}

func TestSemanticReadBudgetDoesNotMakeOtherErrorsRetryable(t *testing.T) {
	for _, err := range []error{orchestration.ErrUnauthorized, orchestration.ErrToolSelectionStall, errors.New("unknown"), errors.New("orchestration budget exhausted")} {
		if semanticReadAdmissionErrorResult(err) != nil {
			t.Fatal("untyped or permanent error became retryable", err)
		}
	}
}
