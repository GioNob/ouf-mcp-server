package manifest

import "testing"

// Plans created with the batch ACTIVE on the 1.17.0 service manifest (4b39568)
// must remain readable and resumable after the release.
const servicePredecessorActive = "0c624454825ebc571690961be309c38d27d78e96ab64e7903ea99c359c4f8daa"

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
		t.Fatalf("manifest %s resumes %q, want the 1.17.0 manifest in service", sum, got)
	}
	if AnalysisPredecessor(servicePredecessorActive) != "" {
		t.Fatal("the predecessor checksum must not chain to an older manifest")
	}
}
