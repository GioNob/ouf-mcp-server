package manifest

import (
	"slices"
	"testing"
)

// MCP-58A-01 (D-080): analyses stored by the T1 release in esercizio
// (release/d090-t1-mcp 4b39568, manifest 1.17.0-published-batch-v1 with the
// 50s next exits) stay resumable by the T2 candidate in each of the three
// installation states of the published batch. The literal checksums are the
// ones the T1 generator emitted for 4b39568.
func TestT1DeployedAnalysesStayResumable(t *testing.T) {
	deployed := map[string]string{
		"ACTIVE":   "0c624454825ebc571690961be309c38d27d78e96ab64e7903ea99c359c4f8daa",
		"INACTIVE": "42cdc29936d851ef039488b4ba3488be3aadcb4e03b45906e4de9ae22b424e2f",
		"ABSENT":   "e7ee7a2c2ed3030133a96f20a7c416bd158c38cc9775cb1b59503842c2f008ab",
	}
	for state, t1 := range deployed {
		m, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for index, c := range m.Capabilities {
			if c.CapabilityID != "ouf.semantic.search.batch" {
				continue
			}
			found = true
			if state == "ABSENT" {
				m.Capabilities = append(m.Capabilities[:index], m.Capabilities[index+1:]...)
			} else {
				m.Capabilities[index].PublicationState = state
			}
			break
		}
		if !found {
			t.Fatal("published batch capability missing")
		}
		current, err := m.Checksum()
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Contains(AnalysisPredecessors(current), t1) {
			t.Fatalf("%s: T1 deployed manifest %s is not an accepted predecessor of %s", state, t1, current)
		}
	}
}
