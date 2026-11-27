package manifest

import "testing"

// The 1.19.1 service manifest (753d99f) must accept legacy candidate plans on rollback.
const servicePredecessorActive = "3be35916aed3ecbd914764ccc0650a17d718c49f810e2c4818ac6cf2428c981a"

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
		t.Fatalf("manifest %s selects %q, want the 1.19.1 manifest in service", sum, got)
	}
	if AnalysisPredecessor(servicePredecessorActive) != "" {
		t.Fatal("the predecessor checksum must not chain to an older manifest")
	}
}
