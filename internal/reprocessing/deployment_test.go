package reprocessing

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestDeploymentRetainsExpiredOldManifestWithoutDispatchAuthority(t *testing.T) {
	profile, policy, envelope, pin, oldManifest := fixture(t, nil)
	newManifest := Hash([]byte("upgraded-manifest"))
	now := time.Now().Add(time.Hour)
	if _, err := Verify(profile, policy, envelope, pin, newManifest, now); !errors.Is(err, ErrPermit) {
		t.Fatal("strict operation verification must reject a changed manifest")
	}
	verified, err := VerifyDeployment(profile, policy, envelope, pin, newManifest, now)
	if verified == nil || !errors.Is(err, ErrExpired) || verified.Value().ManifestChecksum != oldManifest {
		t.Fatal("expired signed history was not retained as blocked")
	}
	if !errors.Is(verified.CheckTime(now, 0), ErrExpired) {
		t.Fatal("historical session gained authority")
	}
}

func TestDeploymentNeverCarriesActiveOrFuturePermitAcrossManifests(t *testing.T) {
	for _, future := range []bool{false, true} {
		profile, policy, envelope, pin, _ := fixture(t, func(p *Permit) {
			if future {
				p.IssuedAt += 300
				p.ExpiresAt += 300
				p.OwnerReceiptAbsentAt += 300
			}
		})
		value, err := VerifyDeployment(profile, policy, envelope, pin, Hash([]byte("different")), time.Now())
		if value != nil || !errors.Is(err, ErrPermit) {
			t.Fatal("non-expired manifest substitution accepted")
		}
	}
}

func TestDeploymentExpiredCompatibilityStillChecksSignaturesAndLimits(t *testing.T) {
	profile, policy, raw, pin, _ := fixture(t, nil)
	var envelope Envelope
	_ = json.Unmarshal(raw, &envelope)
	envelope.Approval.Signature = ""
	tampered, _ := json.Marshal(envelope)
	value, err := VerifyDeployment(profile, policy, tampered, pin, Hash([]byte("new")), time.Now().Add(time.Hour))
	if value != nil || !errors.Is(err, ErrPermit) {
		t.Fatal("invalid historical signature accepted")
	}
	for _, change := range []func(*Permit){
		func(p *Permit) { p.MaximumNewToolCalls = 2 },
		func(p *Permit) { p.ExpiresAt = p.IssuedAt + 901 },
		func(p *Permit) { p.ManifestChecksum = "invalid" },
	} {
		profile, policy, raw, pin, _ := fixture(t, change)
		value, err := VerifyDeployment(profile, policy, raw, pin, Hash([]byte("new")), time.Now().Add(time.Hour))
		if value != nil || !errors.Is(err, ErrPermit) {
			t.Fatal("invalid historical scope accepted")
		}
	}
}

func TestDeploymentCurrentManifestBehaviorIsUnchanged(t *testing.T) {
	profile, policy, raw, pin, manifest := fixture(t, nil)
	for _, now := range []time.Time{time.Now(), time.Now().Add(time.Hour)} {
		expected, expectedErr := Verify(profile, policy, raw, pin, manifest, now)
		actual, actualErr := VerifyDeployment(profile, policy, raw, pin, manifest, now)
		if !errors.Is(actualErr, expectedErr) || expected.Hash() != actual.Hash() {
			t.Fatal("current-manifest semantics changed")
		}
	}
	if value, err := VerifyDeployment(profile, policy, raw, pin, "", time.Now().Add(time.Hour)); value != nil || !errors.Is(err, ErrPermit) {
		t.Fatal("invalid current manifest accepted")
	}
}
