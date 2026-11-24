package manifest

import (
	"strings"
	"testing"
)

func TestDiscoveryPublicationKeepsOwnerAndOperationContracts(t *testing.T) {
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
		if capability.PublicationState != "ACTIVE" || capability.Owner != "semantic" {
			t.Fatal("discovery publication or owner changed")
		}
		if capability.ToolName == "semantic.discovery.request" || capability.ToolName == "semantic.discovery.adopt" {
			if capability.OperationClass != "COMMAND" {
				t.Fatal("discovery request mislabelled as read")
			}
		} else if capability.OperationClass != "READ" {
			t.Fatal("discovery result read mislabelled")
		}
	}
	if count != 4 {
		t.Fatalf("expected four discovery candidates, got %d", count)
	}
	eligible := 0
	for _, capability := range snapshot.ToolEligible() {
		if strings.HasPrefix(capability.ToolName, "semantic.discovery.") {
			eligible++
		}
	}
	if eligible != 4 {
		t.Fatalf("expected four published discovery tools, got %d", eligible)
	}
}
