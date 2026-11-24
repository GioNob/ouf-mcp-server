package orchestration

import (
	"encoding/json"
	"errors"
	"time"
)

// Server-owned read capacity selected for source mapping. This does not raise
// equivalent-query retry guards, command budgets or recovery-session permits.
const SemanticReadBudgetProfile = "ouf.semantic-read-budget.v1"

func IsSemanticRead(capability, operation string) bool {
	return operation == "READ" && (capability == "ouf.semantic.search" || capability == "ouf.semantic.search.batch" || capability == "ouf.semantic.consultation.read")
}

func semanticReadWindowBudget(in AdmissionRequest) (string, Cost, error) {
	if !IsSemanticRead(in.CapabilityID, in.OperationClass) ||
		in.ManifestChecksum == "" || in.Window != AdmissionWindow || in.RetryThreshold != AdmissionRetryThreshold ||
		in.Maximum.ToolCalls != 1 || in.Maximum.ResultBytes < 1 || in.Maximum.ResultBytes > 262144 ||
		in.Maximum.DistinctObjects < 0 || in.Maximum.DistinctObjects > 100 {
		return "", Cost{}, errors.New("unsupported semantic read budget")
	}
	return in.ManifestChecksum + ":" + SemanticReadBudgetProfile, Cost{ToolCalls: 60, ResultBytes: 15 << 20, DistinctObjects: 6000}, nil
}

// Only a refusal before dispatch due to this finite window carries retry timing.
// It grants no retry/authorization and never describes an ambiguous owner effect.
type SemanticReadBudgetExhaustedError = WindowBudgetExhaustedError

func NewSemanticReadBudgetExhaustedError(cause error, dimension string, now, end time.Time) error {
	return NewWindowBudgetError(cause, "SEMANTIC_READ_WINDOW_BUDGET_EXHAUSTED", dimension, now, end)
}

// Count a published revision once even when it is evidence for two intents.
// The coordinator validates the complete evidence envelope before using it.
func publishedRevisionCount(body []byte) int64 {
	var batch struct {
		Results []struct {
			Hits []struct{ RevisionID string }
		}
	}
	if json.Unmarshal(body, &batch) != nil {
		return 0
	}
	revisions := make(map[string]struct{})
	for _, result := range batch.Results {
		for _, hit := range result.Hits {
			if hit.RevisionID != "" {
				revisions[hit.RevisionID] = struct{}{}
			}
		}
	}
	return int64(len(revisions))
}
