package manifest

import "testing"

// Plans created with the batch ACTIVE on the 1.18.2 service manifest (2a25eb4)
// must remain readable and resumable after the release.
const servicePredecessorActive = "7172623aaf380a14a7833b0ca57e814534afe6a427674f34955f2be606066b19"

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
		t.Fatalf("manifest %s resumes %q, want the 1.18.2 manifest in service", sum, got)
	}
	if AnalysisPredecessor(servicePredecessorActive) != "" {
		t.Fatal("the predecessor checksum must not chain to an older manifest")
	}
}
