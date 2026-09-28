package manifest

import "testing"

func TestResolutionReviewIsReadOnlyAndHumanConfirmationIsNotATool(t *testing.T) {
	snapshot, err := Load()
	if err != nil { t.Fatal(err) }
	read := 0
	for _, c := range snapshot.Capabilities {
		if c.ToolName == "resolution.issue.read" {
			read++
			if !c.ToolEligible || c.OperationClass != "READ" || c.RequiredAuthorization != "resolution.issue.read" { t.Fatalf("review tool must be a governed read: %+v", c) }
		}
		if c.ToolName == "resolution.match.approve" && c.ToolEligible { t.Fatal("human confirmation must never be an MCP tool") }
	}
	if read != 1 { t.Fatalf("expected one review tool, got %d", read) }
}
