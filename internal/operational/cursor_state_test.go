package operational

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func TestIncidentCursorIdentityCanonicalAndSecretFree(t *testing.T) {
	identity := orchestration.Identity{PrincipalID: "operator", TenantID: "tenant", ServicePrincipalID: "mcp", ActorType: "HUMAN",
		AuthenticationContextRef: "authn:1", Issuer: "issuer", Audience: "audience", Scopes: []string{"b", "a", "a"}, Delegation: "DO_NOT_STORE_DELEGATION",
		Claims: &orchestration.IdentityClaims{ExternalRoleRefs: []string{"r2", "r1"}, Amr: []string{"otp", "pwd"}, AuthenticatedAt: time.Date(2026, 10, 11, 8, 0, 0, 0, time.FixedZone("offset", 7200))}}
	first, err := BindIncidentCursor(identity, "policy:1", strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	identity.Scopes = []string{"a", "b"}
	identity.Claims.ExternalRoleRefs = []string{"r1", "r2"}
	identity.Claims.Amr = []string{"pwd", "otp"}
	identity.Claims.AuthenticatedAt = identity.Claims.AuthenticatedAt.UTC()
	second, err := BindIncidentCursor(identity, "policy:1", strings.Repeat("a", 64))
	if err != nil || first != second {
		t.Fatal("equivalent identity order changed binding")
	}
	encoded, _ := json.Marshal(first)
	if strings.Contains(string(encoded), "DO_NOT_STORE") || strings.Contains(string(encoded), "otp") || strings.Contains(string(encoded), "authn:1") {
		t.Fatal("private verification material persisted in binding")
	}
	identity.AuthenticationContextRef = "authn:2"
	third, _ := BindIncidentCursor(identity, "policy:1", strings.Repeat("a", 64))
	if third.Fingerprint == first.Fingerprint {
		t.Fatal("changed authentication reused cursor")
	}
	identity.AuthenticationContextRef = "authn:1"
	identity.Claims.ExternalRoleRefs = append(identity.Claims.ExternalRoleRefs, "additional")
	fourth, _ := BindIncidentCursor(identity, "policy:1", strings.Repeat("a", 64))
	if fourth.Fingerprint == first.Fingerprint {
		t.Fatal("changed claims reused cursor")
	}
}

func TestIncidentCursorFrameContainsBoundedPositionsOnly(t *testing.T) {
	frame := CursorFrame{Profile: CursorProfile, Filters: []byte(`{"limit":1}`), PageLimit: 1, SelfStatus: "HEALTHY", Owners: []OwnerPosition{
		{Name: "INGESTION", Cursor: uuid.NewString(), SnapshotRef: uuid.NewString(), ItemsDigest: strings.Repeat("a", 64), OwnerDigest: strings.Repeat("b", 64)},
		{Name: "GATEWAY", Cursor: uuid.NewString(), SnapshotRef: uuid.NewString(), ItemsDigest: strings.Repeat("c", 64), OwnerDigest: strings.Repeat("d", 64)},
	}}
	if err := ValidateCursorFrame(frame); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*CursorFrame){
		func(f *CursorFrame) { f.Filters = []byte(`{"incidentPayload":{"secret":"hidden"}}`) },
		func(f *CursorFrame) { f.PageNumber = 201 },
		func(f *CursorFrame) { f.Owners[0].Cursor = uuid.Nil.String() },
		func(f *CursorFrame) { f.Owners[0].PageIndex = 201 },
		func(f *CursorFrame) { f.Owners[0].ItemsDigest = "untrusted" },
	} {
		copyFrame := frame
		copyFrame.Owners = append([]OwnerPosition(nil), frame.Owners...)
		mutate(&copyFrame)
		if err := ValidateCursorFrame(copyFrame); err == nil {
			t.Fatal("invalid cursor metadata accepted")
		}
	}
}
