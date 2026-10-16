package reprocessing

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
 "encoding/hex"
	"errors"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fixture(t *testing.T,change func(*Permit))([]byte,[]byte,[]byte,string,string){
 t.Helper();pub1,key1,_:=ed25519.GenerateKey(rand.Reader);now:=time.Now().Unix()
 policy,_:=json.Marshal(TrustPolicy{Schema:"ouf.semantic-deployment-trust-policy.v1",InstallationRef:"fixture-installation",EntityRef:"fixture-entity",Keys:[]TrustKey{{KeyRef:"existing-fixture-installer",IssuerRef:"fixture-installer-issuer",Roles:[]string{"DEPLOYMENT_INTENT","FINAL_DEPLOYMENT_APPROVAL"},PublicKey:hex.EncodeToString(pub1),NotBefore:now-3600,ExpiresAt:now+3600,State:"ACTIVE"}}})
 configured:=KeyProfile{Schema:"ouf.mcp-reprocessing-key-profile.v1",MaxSessionsPerTenant:64,InstallationRef:"fixture-installation",EntityRef:"fixture-entity",InstallerKeyRef:"existing-fixture-installer",IntentIssuerRef:"fixture-installer-issuer",ApprovalIssuerRef:"fixture-installer-issuer",TrustPolicyHash:Hash(policy)}
 profile,_:=json.Marshal(configured);manifest:=Hash([]byte("fixture-manifest"))
 p := Permit{Schema: "ouf.mcp-semantic-reprocessing-session.v1", SessionID: uuid.NewString(), DecisionRef: "fixture-explicit-decision", Reason: "FIXTURE_AUTHORIZED_REPROCESSING_AFTER_SCOPE_RECOVERY", InstallationRef: "fixture-installation", KeyProfileHash: Hash(profile), ManifestChecksum: manifest, ServicePrincipalID: "fixture-workload", HumanSubject: "fixture-human", TenantID: "fixture-tenant", Owner: "semantic", CapabilityID: "ouf.semantic.propose", OperationClass: "COMMAND", CommandKey: "same-owner-command", RequestHash: Hash([]byte("fixture-request")), PredecessorAttemptID: uuid.NewString(), PredecessorLockVersion: 2, PredecessorOutcomeCode: "SEM_CAPABILITY_REQUIRED:ouf.semantic.propose", PriorToolCalls: 1, MaximumNewToolCalls: 1, MaximumCumulativeToolCalls: 2, OwnerReceiptAbsentAt: now - 1, IssuedAt: now, ExpiresAt: now + 600}
 if change!=nil{change(&p)};raw,_:=json.Marshal(p)
 intent:=SignatureHeader(configured,raw,"DEPLOYMENT_INTENT");frame,_:=SigningBytes(raw,intent);intent.Signature=hex.EncodeToString(ed25519.Sign(key1,frame))
 approval:=SignatureHeader(configured,raw,"FINAL_DEPLOYMENT_APPROVAL");frame,_=SigningBytes(raw,approval);approval.Signature=hex.EncodeToString(ed25519.Sign(key1,frame))
 env,_:=json.Marshal(Envelope{Payload:raw,Intent:intent,Approval:approval})
 return profile,policy,env,Hash(profile),manifest
}
func TestSemanticReprocessingDualRoleAndExactScope(t *testing.T) {
	profile, policy,env, pin, manifest := fixture(t, nil)
	v, e := Verify(profile,policy, env, pin, manifest, time.Now())
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
	profile, policy,raw, pin, manifest := fixture(t, nil)
	var env Envelope
	_ = json.Unmarshal(raw, &env)
	for _, alter := range []func(){func() { env.Intent.Signature="00" }, func() { env.Approval.Signature="" }, func() { env.Payload = append(env.Payload, byte(' ')) }} {
		_ = json.Unmarshal(raw, &env)
		alter()
		bad, _ := json.Marshal(env)
		if _, e := Verify(profile,policy, bad, pin, manifest, time.Now()); e == nil {
			t.Fatal("tamper accepted")
		}
	}
	for _, bad := range [][]byte{append([]byte(`{"schema":"duplicate",`), profile[1:]...), append([]byte(`{"unknown":true,`), profile[1:]...)} {
		if _, e := Verify(bad,policy, raw, Hash(bad), manifest, time.Now()); e == nil {
			t.Fatal("profile ambiguity accepted")
		}
	}
	if _, e := Verify(profile,policy, raw, Hash([]byte("other-profile")), manifest, time.Now()); e == nil {
		t.Fatal("untrusted profile accepted")
	}
	if _, e := Verify(profile,policy, raw, pin, Hash([]byte("other-manifest")), time.Now()); e == nil {
		t.Fatal("manifest substitution")
	}
}
func TestSemanticReprocessingFiniteAndNoApprovalOrPermanentUnknownOverride(t *testing.T) {
	changes := []func(*Permit){func(p *Permit) { p.MaximumNewToolCalls = 2 }, func(p *Permit) { p.PriorToolCalls = 0 }, func(p *Permit) { p.MaximumCumulativeToolCalls = 3 }, func(p *Permit) { p.ExpiresAt = p.IssuedAt + 901 }, func(p *Permit) { p.CapabilityID = "ouf.semantic.approve" }, func(p *Permit) { p.PredecessorOutcomeCode = "UNKNOWN" }, func(p *Permit) { p.OwnerReceiptAbsentAt = p.IssuedAt - 301 }, func(p *Permit) { p.OwnerReceiptAbsentAt = p.IssuedAt + 1 }}
	for i, change := range changes {
		profile, policy,env, pin, manifest := fixture(t, change)
		if _, e := Verify(profile,policy, env, pin, manifest, time.Now()); e == nil {
			t.Fatalf("invalid permit %d", i)
		}
	}
	profile, policy,env, pin, manifest := fixture(t, nil)
	v, e := Verify(profile,policy, env, pin, manifest, time.Now().Add(time.Hour))
	if v == nil || !errors.Is(e, ErrExpired) {
		t.Fatal("expired scope must remain cryptographically identified but blocked")
	}
}
func TestSemanticReprocessingPermitNeverAcceptsUnsignedOperatorAssertion(t *testing.T) {
	profile, policy,raw, pin, manifest := fixture(t, nil)
	var env Envelope
	_ = json.Unmarshal(raw, &env)
	env.Approval=env.Intent
	raw, _ = json.Marshal(env)
	if _, e := Verify(profile,policy, raw, pin, manifest, time.Now()); e == nil {
		t.Fatal("roles collapsed")
	}
}

func TestSemanticReprocessingDeploymentFileCustody(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("native root fixture required; explicit CI step runs this test")
	}
	dir, e := os.MkdirTemp("/var/lib", "ouf-mcp-reprocessing-custody-")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "public-profile.json")
	raw := []byte(`{"public":"metadata-only"}`)
	if e = os.WriteFile(path, raw, 0440); e != nil {
		t.Fatal(e)
	}
	if got, e := ReadDeploymentFile(path); e != nil || string(got) != string(raw) {
		t.Fatalf("private public metadata failed: %v", e)
	}
	link := filepath.Join(dir, "link")
	if e = os.Symlink(path, link); e != nil {
		t.Fatal(e)
	}
	if _, e = ReadDeploymentFile(link); e == nil {
		t.Fatal("symlink accepted")
	}
	hard := filepath.Join(dir, "hard")
	if e = os.Link(path, hard); e != nil {
		t.Fatal(e)
	}
	if _, e = ReadDeploymentFile(path); e == nil {
		t.Fatal("multiple links accepted")
	}
	_ = os.Remove(hard)
	_ = os.Chmod(path, 0660)
	if _, e = ReadDeploymentFile(path); e == nil {
		t.Fatal("writable profile accepted")
	}
	_ = os.Chmod(path, 0440)
	_ = os.Chmod(dir, 0770)
	if _, e = ReadDeploymentFile(path); e == nil {
		t.Fatal("writable ancestry accepted")
	}
	_ = os.Chmod(dir, 0700)
}

func TestSemanticReprocessingFrameMatchesExistingPythonProducer(t *testing.T){
 raw:=[]byte(`{"fixture":"permit"}`)
 profile:=KeyProfile{InstallationRef:"fixture",EntityRef:"fixture-entity",InstallerKeyRef:"fixture-installer",IntentIssuerRef:"fixture-issuer",ApprovalIssuerRef:"fixture-issuer"}
 frame,e:=SigningBytes(raw,SignatureHeader(profile,raw,"DEPLOYMENT_INTENT"))
 if e!=nil||Hash(frame)!="e3078a249df0f94d348869156c28136013902c084f372ecde828e307b6e8fa66"{t.Fatal("existing Python producer wire frame changed")}
}
