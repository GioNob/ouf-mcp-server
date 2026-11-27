package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestSemanticReadBudgetWindowResponseIsExplicit(t *testing.T) {
	end := time.Date(2026, 11, 7, 20, 8, 0, 0, time.UTC)
	result := windowAdmissionErrorResult(orchestration.NewSemanticReadBudgetExhaustedError(errors.New("budget"), "TOOL_CALLS", end.Add(-17*time.Second), end))
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
		if windowAdmissionErrorResult(err) != nil {
			t.Fatal("untyped or permanent error became retryable", err)
		}
	}
}

type refusedWindow struct {
	statusAdmission
	code string
}

func (a *refusedWindow) Reserve(_ context.Context, _ orchestration.AdmissionRequest) (orchestration.AdmissionDecision, error) {
	end := time.Now().UTC().Truncate(orchestration.AdmissionWindow).Add(orchestration.AdmissionWindow)
	return orchestration.AdmissionDecision{}, orchestration.NewWindowBudgetError(errors.New("window"), a.code, "TOOL_CALLS", end.Add(-time.Second), end)
}

func TestAdmissionRefusalOverMCP(t *testing.T) {
	for _, code := range []string{"TOOL_SELECTION_STALL", "ADMISSION_WINDOW_BUDGET_EXHAUSTED"} {
		t.Run(code, func(t *testing.T) {
			owner := &analysisSDKGateway{}
			service := &orchestration.Service{RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: &refusedWindow{code: code},
				Gateway: owner, FingerprintKey: make([]byte, 32)}
			session := revisionSession(t, service, "ouf.managed-source.profile")
			args := map[string]any{"assetId": "11111111-1111-4111-8111-111111111111", "commandKey": "22222222-2222-4222-8222-222222222222"}
			result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "source.file.profile", Arguments: args})
			if err != nil || !result.IsError || owner.calls != 0 {
				t.Fatal("admission error crossed the dispatch boundary", err)
			}
			next := checkNext(t, nextContract(t), result)
			var text map[string]any
			if json.Unmarshal([]byte(result.Content[0].(*mcp.TextContent).Text), &text) != nil || len(text) != 6 ||
				text["code"] != code || text["retryable"] != true || !reflect.DeepEqual(text["next"], next) ||
				next["action"] != "POLL" || next["tool"] != "ouf.wait" || next["arguments"].(map[string]any)["waitRef"] == nil ||
				next["pollAfterSeconds"].(float64) < 1 {
				t.Fatal("client lost the governed retry", result)
			}
		})
	}
}
