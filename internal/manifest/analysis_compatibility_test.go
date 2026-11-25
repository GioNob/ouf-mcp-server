package manifest

import (
	"slices"
	"testing"
)

// Checksum of the manifest in service before 1.19.0: 1.18.0 (cefa37c) with the
// published batch ACTIVE. An analysis planned under it must stay readable and
// resumable after the release (regenerate with go generate when the manifest changes).
const servicePredecessorActive = "08862439ab1b90c615bbaa8bad44e9bf4ebcdfbbdd650a8c6cc5abc60a75c984"

func TestAnalysisPredecessorResumesPlansOfTheManifestInService(t *testing.T) {
	m, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(m.Capabilities, func(c Capability) bool { return c.CapabilityID == "ouf.semantic.search.batch" })
	if i < 0 || m.Capabilities[i].PublicationState != "ACTIVE" {
		t.Fatal("missing active published batch baseline")
	}
	for _, tc := range []struct{ state, predecessor string }{
		{"ACTIVE", servicePredecessorActive},
		{"INACTIVE", "3af54401e8228fec468659aee03591e45183c1e3d54f7ab52d973b94f442fb0e"},
		{"REMOVED", "8b12709d6a4bcac49a53c5bb53cb8f9b668529d48a9ae61bd8da35d1bc295c45"},
	} {
		if tc.state == "REMOVED" {
			m.Capabilities = slices.Delete(m.Capabilities, i, i+1)
		} else {
			m.Capabilities[i].PublicationState = tc.state
		}
		sum, err := m.Checksum()
		if err != nil {
			t.Fatal(err)
		}
		if got := AnalysisPredecessor(sum); got != tc.predecessor || AnalysisPredecessor(got) != "" {
			t.Fatalf("%s: manifest %s resumes %q, want only its 1.18.0 predecessor %q", tc.state, sum, got, tc.predecessor)
		}
	}
}
