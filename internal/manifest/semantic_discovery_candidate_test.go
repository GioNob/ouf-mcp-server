package manifest

import (
	"strings"
	"testing"
)

func TestDiscoveryCandidateIsClosedAndNotAdvertisedBeforeRelease(t *testing.T) {
	snapshot, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, capability := range snapshot.Capabilities {
		if !strings.HasPrefix(capability.ToolName, "semantic.discovery.") {
			continue
		}
		count++
		if capability.PublicationState != "INACTIVE" || capability.Owner != "semantic" {
			t.Fatal("unaccepted discovery tool advertised")
		}
		if capability.ToolName == "semantic.discovery.request" {
			if capability.OperationClass != "COMMAND" {
				t.Fatal("discovery request mislabelled as read")
			}
		} else if capability.OperationClass != "READ" {
			t.Fatal("discovery result read mislabelled")
		}
	}
	if count != 3 {
		t.Fatalf("expected three discovery candidates, got %d", count)
	}
	for _, capability := range snapshot.ToolEligible() {
		if strings.HasPrefix(capability.ToolName, "semantic.discovery.") {
			t.Fatal("candidate exposed without coordinated release")
		}
	}
}
