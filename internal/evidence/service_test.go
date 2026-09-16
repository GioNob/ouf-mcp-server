package evidence

import (
	"context"
	"errors"
	"testing"
)

type fakeStore struct{ received []Verified }

func (f *fakeStore) InsertVerifiedEvidence(_ context.Context, in Verified) (Result, error) {
	f.received = append(f.received, in)
	return Result{EvidenceRef: in.EvidenceRef}, nil
}

func TestIngressDerivesAuthorityAndCanonicalEvidence(t *testing.T) {
	store := &fakeStore{}
	service := Service{Store: store}
	identity := TrustedIdentity{Authenticated: true, ServicePrincipalID: "udp-workload", BackendOwner: "udp"}
	input := Input{ClaimedOwner: "udp", BackendRequestID: "backend-1", Kind: "OWNER_RESULT", TerminalState: "SUCCEEDED", OutcomeCode: "SUCCEEDED", ResultRef: "result://1", ActualDistinctObjects: 2, ObjectHashes: []string{"b", "a"}}
	first, err := service.Ingest(context.Background(), identity, input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Ingest(context.Background(), identity, input)
	if err != nil || first.EvidenceRef != second.EvidenceRef || store.received[0].PayloadHash != store.received[1].PayloadHash {
		t.Fatalf("non-deterministic ingress: %+v %+v %v", first, second, err)
	}
	if store.received[0].ServicePrincipalID != "udp-workload" || store.received[0].BackendOwner != "udp" || store.received[0].ObjectHashes[0] != "a" {
		t.Fatalf("trusted derivation/canonicalization failed: %+v", store.received[0])
	}
}

func TestIngressRejectsMissingIdentityAndClaimedOwnerMismatch(t *testing.T) {
	service := Service{Store: &fakeStore{}}
	input := Input{BackendRequestID: "backend-1", Kind: "OWNER_RESULT"}
	if _, err := service.Ingest(context.Background(), TrustedIdentity{}, input); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("missing identity accepted: %v", err)
	}
	input.ClaimedOwner = "attacker"
	identity := TrustedIdentity{Authenticated: true, ServicePrincipalID: "udp-workload", BackendOwner: "udp"}
	if _, err := service.Ingest(context.Background(), identity, input); !errors.Is(err, ErrOwnerMismatch) {
		t.Fatalf("owner mismatch accepted: %v", err)
	}
}
