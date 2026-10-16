package reprocessing

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
 "os"
 "path/filepath"
	"github.com/google/uuid"
	"testing"
	"time"
)

func fixture(t *testing.T, change func(*Permit)) ([]byte, []byte, string, string) {
	t.Helper()
	pub1, key1, _ := ed25519.GenerateKey(rand.Reader)
	pub2, key2, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now().Unix()
	profile, _ := json.Marshal(KeyProfile{MaxSessionsPerTenant:64,Schema: "ouf.mcp-reprocessing-key-profile.v1", InstallationRef: "fixture-installation", InstallerKeyRef: "existing-fixture-installer", AttestorKeyRef: "existing-fixture-attestor", InstallerPublicKey: pub1, AttestorPublicKey: pub2, NotBefore: now - 3600, ExpiresAt: now + 3600})
	manifest := Hash([]byte("fixture-manifest"))
	p := Permit{Schema: "ouf.mcp-semantic-reprocessing-session.v1", SessionID: uuid.NewString(), DecisionRef: "fixture-explicit-decision", Reason: "FIXTURE_AUTHORIZED_REPROCESSING_AFTER_SCOPE_RECOVERY", InstallationRef: "fixture-installation", KeyProfileHash: Hash(profile), ManifestChecksum: manifest, ServicePrincipalID: "fixture-workload", HumanSubject: "fixture-human", TenantID: "fixture-tenant", Owner: "semantic", CapabilityID: "ouf.semantic.propose", OperationClass: "COMMAND", CommandKey: "same-owner-command", RequestHash: Hash([]byte("fixture-request")), PredecessorAttemptID: uuid.NewString(), PredecessorLockVersion: 2, PredecessorOutcomeCode: "SEM_CAPABILITY_REQUIRED:ouf.semantic.propose", PriorToolCalls: 1, MaximumNewToolCalls: 1, MaximumCumulativeToolCalls: 2, OwnerReceiptAbsentAt: now - 1, IssuedAt: now, ExpiresAt: now + 600}
	if change != nil {
		change(&p)
	}
	raw, _ := json.Marshal(p)
	env, _ := json.Marshal(Envelope{Payload: raw, InstallerSignature: ed25519.Sign(key1, RoleFrame("DEPLOYMENT_INTENT", raw)), AttestorSignature: ed25519.Sign(key2, RoleFrame("FINAL_DEPLOYMENT_APPROVAL", raw))})
	return profile, env, Hash(profile), manifest
}
func TestSemanticReprocessingDualRoleAndExactScope(t *testing.T) {
	profile, env, pin, manifest := fixture(t, nil)
	v, e := Verify(profile, env, pin, manifest, time.Now())
	if e != nil {
		t.Fatal(e)
	}
	p := v.Value()
	if !v.Matches(p.ServicePrincipalID, p.HumanSubject, p.TenantID, p.CapabilityID, p.Owner, p.OperationClass, p.CommandKey) {
		t.Fatal("scope lost")
	}
	if v.Matches("another-workload", p.HumanSubject, p.TenantID, p.CapabilityID, p.Owner, p.OperationClass, p.CommandKey) {
		t.Fatal("identity widened")
	}
	copy := v.Value()
	copy.MaximumNewToolCalls = 100
	if v.Value().MaximumNewToolCalls != 1 {
		t.Fatal("mutable permit")
	}
}
func TestSemanticReprocessingTamperAndUnknownOrDuplicateFields(t *testing.T) {
	profile, raw, pin, manifest := fixture(t, nil)
	var env Envelope
	_ = json.Unmarshal(raw, &env)
	for _, alter := range []func(){func() { env.InstallerSignature[0] ^= 1 }, func() { env.AttestorSignature = nil }, func() { env.Payload = append(env.Payload, byte(' ')) }} {
		_ = json.Unmarshal(raw, &env)
		alter()
		bad, _ := json.Marshal(env)
		if _, e := Verify(profile, bad, pin, manifest, time.Now()); e == nil {
			t.Fatal("tamper accepted")
		}
	}
	for _, bad := range [][]byte{append([]byte(`{"schema":"duplicate",`), profile[1:]...), append([]byte(`{"unknown":true,`), profile[1:]...)} {
		if _, e := Verify(bad, raw, Hash(bad), manifest, time.Now()); e == nil {
			t.Fatal("profile ambiguity accepted")
		}
	}
	if _, e := Verify(profile, raw, Hash([]byte("other-profile")), manifest, time.Now()); e == nil {
		t.Fatal("untrusted profile accepted")
	}
	if _, e := Verify(profile, raw, pin, Hash([]byte("other-manifest")), time.Now()); e == nil {
		t.Fatal("manifest substitution")
	}
}
func TestSemanticReprocessingFiniteAndNoApprovalOrPermanentUnknownOverride(t *testing.T) {
	changes := []func(*Permit){func(p *Permit) { p.MaximumNewToolCalls = 2 }, func(p *Permit) { p.PriorToolCalls = 0 }, func(p *Permit) { p.MaximumCumulativeToolCalls = 3 }, func(p *Permit) { p.ExpiresAt = p.IssuedAt + 901 }, func(p *Permit) { p.CapabilityID = "ouf.semantic.approve" }, func(p *Permit) { p.PredecessorOutcomeCode = "UNKNOWN" }, func(p *Permit) { p.OwnerReceiptAbsentAt = p.IssuedAt - 301 }, func(p *Permit) { p.OwnerReceiptAbsentAt = p.IssuedAt + 1 }}
	for i, change := range changes {
		profile, env, pin, manifest := fixture(t, change)
		if _, e := Verify(profile, env, pin, manifest, time.Now()); e == nil {
			t.Fatalf("invalid permit %d", i)
		}
	}
	profile, env, pin, manifest := fixture(t, nil)
	v, e := Verify(profile, env, pin, manifest, time.Now().Add(time.Hour))
	if v == nil || !errors.Is(e, ErrExpired) {
		t.Fatal("expired scope must remain cryptographically identified but blocked")
	}
}
func TestSemanticReprocessingPermitNeverAcceptsUnsignedOperatorAssertion(t *testing.T) {
	profile, raw, pin, manifest := fixture(t, nil)
	var env Envelope
	_ = json.Unmarshal(raw, &env)
	env.InstallerSignature = env.AttestorSignature
	raw, _ = json.Marshal(env)
	if _, e := Verify(profile, raw, pin, manifest, time.Now()); e == nil {
		t.Fatal("roles collapsed")
	}
}

func TestSemanticReprocessingDeploymentFileCustody(t *testing.T){
 if os.Geteuid()!=0{t.Skip("native root fixture required; explicit CI step runs this test")}
 dir,e:=os.MkdirTemp("/var/lib","ouf-mcp-reprocessing-custody-");if e!=nil{t.Fatal(e)};t.Cleanup(func(){_ = os.RemoveAll(dir)})
 path:=filepath.Join(dir,"public-profile.json");raw:=[]byte(`{"public":"metadata-only"}`)
 if e=os.WriteFile(path,raw,0440);e!=nil{t.Fatal(e)}
 if got,e:=ReadDeploymentFile(path);e!=nil||string(got)!=string(raw){t.Fatalf("private public metadata failed: %v",e)}
 link:=filepath.Join(dir,"link");if e=os.Symlink(path,link);e!=nil{t.Fatal(e)}
 if _,e=ReadDeploymentFile(link);e==nil{t.Fatal("symlink accepted")}
 hard:=filepath.Join(dir,"hard");if e=os.Link(path,hard);e!=nil{t.Fatal(e)}
 if _,e=ReadDeploymentFile(path);e==nil{t.Fatal("multiple links accepted")};_=os.Remove(hard)
 _=os.Chmod(path,0660);if _,e=ReadDeploymentFile(path);e==nil{t.Fatal("writable profile accepted")};_=os.Chmod(path,0440)
 _=os.Chmod(dir,0770);if _,e=ReadDeploymentFile(path);e==nil{t.Fatal("writable ancestry accepted")};_=os.Chmod(dir,0700)
}
