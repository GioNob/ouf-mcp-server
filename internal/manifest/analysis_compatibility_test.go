package manifest

import "testing"

// Checksum of the manifest in service before 1.18.0: 1.17.0 (4b39568) with the
// published batch ACTIVE. An analysis planned under it must stay readable and
// resumable after the release (regenerate with go generate when the manifest changes).
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
