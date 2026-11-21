package orchestration

import (
	"testing"

	"github.com/google/uuid"
)

func TestOwnerReplayReferenceRetainsOnlyPublicUUID(t *testing.T) {
	id := uuid.New()
	for _, tc := range []struct {
		capability string
		field      string
	}{
		{"ouf.managed-source.file.profile", "jobId"},
		{"ouf.managed-source.onboarding.create", "onboardingVersionId"},
		{"ouf.semantic.discovery", "requestId"},
		{"ouf.managed-source.file.upload", "assetId"},
	} {
		in := Invocation{CapabilityID: tc.capability}
		if tc.field == "assetId" {
			in.Upload = &UploadStream{}
		}
		body := []byte(`{"` + tc.field + `":"` + id.String() + `","status":"STAGED","detail":"private"}`)
		if got := replayResultID(in, body); got == nil || *got != id {
			t.Fatal("missing receipt", tc.capability)
		}
		if got := replayResultID(in, []byte(`{"`+tc.field+`":"private","status":"STAGED"}`)); got != nil {
			t.Fatal("unverified reference persisted", tc.capability)
		}
	}
}
