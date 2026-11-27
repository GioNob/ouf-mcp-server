package manifest

import "testing"

// The 1.19.0 service manifest (de648f13) must accept candidate plans on rollback.
const servicePredecessorActive = "a5daebe093a64669ccd8213a5222f0e7488c2278b6b0614ac29e2db449b3a1e6"

func TestAnalysisPredecessorResumesPlansOfTheManifestInService(t *testing.T) {
	m, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	sum, err := m.Checksum()
	if err != nil {
		t.Fatal(err)
	}
	if got := AnalysisPredecessor(sum); got != servicePredecessorActive {
		t.Fatalf("manifest %s selects %q, want the 1.19.0 manifest in service", sum, got)
	}
	if AnalysisPredecessor(servicePredecessorActive) != "" {
		t.Fatal("the predecessor checksum must not chain to an older manifest")
	}
}
