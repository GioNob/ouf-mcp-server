package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/GioNob/ouf-mcp-server/internal/reprocessing"
	"github.com/google/uuid"
)

func TestSemanticReprocessingDeploymentManifestUpgrade(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("explicit root-owned metadata CI fixture")
	}
	root, err := os.MkdirTemp("/var/lib", "ouf-mcp-upgrade-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	for _, scenario := range []struct {
		name    string
		expired bool
		tamper  bool
	}{
		{"expired", true, false},
		{"active", false, false},
		{"tampered-expired", true, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := filepath.Join(root, scenario.name)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			encode := func(value any) []byte {
				raw, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				return raw
			}
			pub, key, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().Unix()
			trust := reprocessing.TrustPolicy{Schema: "ouf.semantic-deployment-trust-policy.v1",
				InstallationRef: "fixture", EntityRef: "fixture",
				Keys: []reprocessing.TrustKey{{KeyRef: "installer", IssuerRef: "issuer",
					Roles:     []string{"DEPLOYMENT_INTENT", "FINAL_DEPLOYMENT_APPROVAL"},
					PublicKey: hex.EncodeToString(pub), NotBefore: now - 7200, ExpiresAt: now + 7200, State: "ACTIVE"}}}
			policy := encode(trust)
			profile := reprocessing.KeyProfile{Schema: "ouf.mcp-reprocessing-key-profile.v1", MaxSessionsPerTenant: 1,
				InstallationRef: "fixture", EntityRef: "fixture", InstallerKeyRef: "installer",
				IntentIssuerRef: "issuer", ApprovalIssuerRef: "issuer", TrustPolicyHash: reprocessing.Hash(policy)}
			profileRaw := encode(profile)
			issued := now
			if scenario.expired {
				issued -= 601
			}
			permit := reprocessing.Permit{Schema: "ouf.mcp-semantic-reprocessing-session.v1",
				SessionID: uuid.NewString(), DecisionRef: "fixture-decision", Reason: "fixture-recovery",
				InstallationRef: "fixture", KeyProfileHash: reprocessing.Hash(profileRaw),
				ManifestChecksum: reprocessing.Hash([]byte("old-catalog")), ServicePrincipalID: "service",
				HumanSubject: "human", TenantID: "tenant", Owner: "semantic",
				CapabilityID: "ouf.semantic.propose", OperationClass: "COMMAND", CommandKey: "same-command",
				RequestHash: reprocessing.Hash([]byte("same-request")), PredecessorAttemptID: uuid.NewString(),
				PredecessorLockVersion: 1, PredecessorOutcomeCode: "SEM_CAPABILITY_REQUIRED:ouf.semantic.propose",
				PriorToolCalls: 1, MaximumNewToolCalls: 1, MaximumCumulativeToolCalls: 2,
				OwnerReceiptAbsentAt: issued - 1, IssuedAt: issued, ExpiresAt: issued + 600}
			payload := encode(permit)
			sign := func(role string) reprocessing.DetachedSignature {
				header := reprocessing.SignatureHeader(profile, payload, role)
				frame, err := reprocessing.SigningBytes(payload, header)
				if err != nil {
					t.Fatal(err)
				}
				header.Signature = hex.EncodeToString(ed25519.Sign(key, frame))
				return header
			}
			envelope := reprocessing.Envelope{Payload: payload,
				Intent: sign("DEPLOYMENT_INTENT"), Approval: sign("FINAL_DEPLOYMENT_APPROVAL")}
			if scenario.tamper {
				envelope.Approval.Signature = ""
			}
			files := map[string][]byte{"profile.json": profileRaw, "policy.json": policy, "permit.json": encode(envelope)}
			for name, raw := range files {
				if err := os.WriteFile(filepath.Join(dir, name), raw, 0440); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("MCP_REPROCESSING_KEY_PROFILE_FILE", filepath.Join(dir, "profile.json"))
			t.Setenv("MCP_REPROCESSING_KEY_PROFILE_SHA256", reprocessing.Hash(profileRaw))
			t.Setenv("MCP_REPROCESSING_PERMIT_FILE", filepath.Join(dir, "permit.json"))
			t.Setenv("MCP_REPROCESSING_EXISTING_POLICY_FILE", filepath.Join(dir, "policy.json"))
			service := &orchestration.Service{}
			err = configureReprocessing(service, reprocessing.Hash([]byte("new-catalog")))
			if !scenario.expired || scenario.tamper {
				if !errors.Is(err, reprocessing.ErrPermit) {
					t.Fatal("unproven cross-manifest metadata accepted")
				}
				return
			}
			if err != nil || service.Reprocessing == nil || service.ReprocessingGuard == nil {
				t.Fatal("expired historical metadata blocks bootstrap", err)
			}
			if !errors.Is(service.ReprocessingGuard(), reprocessing.ErrPermit) {
				t.Fatal("strict current-manifest dispatch guard bypassed")
			}
			if !errors.Is(service.Reprocessing.CheckTime(time.Now(), 0), reprocessing.ErrExpired) {
				t.Fatal("expired authority renewed")
			}
			for name, before := range files {
				after, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil || reprocessing.Hash(before) != reprocessing.Hash(after) {
					t.Fatal("retained metadata modified")
				}
			}
		})
	}
}
