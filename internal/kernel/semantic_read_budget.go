package kernel

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func windowAdmissionErrorResult(err error) *mcp.CallToolResult {
	var exhausted *orchestration.WindowBudgetExhaustedError
	if !errors.As(err, &exhausted) {
		return nil
	}
	body, _ := json.Marshal(map[string]any{
		"code":              exhausted.Code,
		"retryable":         true,
		"retryAfterSeconds": exhausted.RetryAfterSeconds,
		"windowEndsAt":      exhausted.WindowEnd.UTC().Format(time.RFC3339Nano),
		"dimension":         exhausted.Dimension,
	})
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(body)}}, IsError: true}
}
