package recovery

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"testing"
	"time"
)

type fakeStore struct {
	candidate Candidate
	applied   []OwnerEvidence
}

func (f *fakeStore) MarkStaleAndClaim(context.Context, time.Time, time.Time, string, time.Duration, int) ([]Candidate, error) {
	return []Candidate{f.candidate}, nil
}
func (f *fakeStore) ApplyOwnerEvidence(_ context.Context, _ uuid.UUID, _ string, e OwnerEvidence) error {
	f.applied = append(f.applied, e)
	return nil
}

type fakeOwner struct {
	e   OwnerEvidence
	err error
}

func (f fakeOwner) QueryOutcome(context.Context, OwnerQuery) (OwnerEvidence, error) {
	return f.e, f.err
}
func service(store *fakeStore, owner fakeOwner) Service {
	return Service{Store: store, Owner: owner, WorkerID: "worker-1", Lease: time.Minute, AdmissionGrace: time.Minute, Batch: 10}
}
func TestDeterministicOwnerOutcomeIsApplied(t *testing.T) {
	id := uuid.New()
	store := &fakeStore{candidate: Candidate{id, "backend-1", "udp", "cap", "corr"}}
	result, err := service(store, fakeOwner{e: OwnerEvidence{BackendRequestID: "backend-1", Outcome: "SUCCEEDED", OutcomeCode: "OK", ActualToolCalls: 1}}).RunOnce(context.Background(), time.Now())
	if err != nil || result.Reconciled != 1 || len(store.applied) != 1 {
		t.Fatalf("%+v %v", result, err)
	}
}
func TestUnavailableOwnerPreservesUnknown(t *testing.T) {
	store := &fakeStore{candidate: Candidate{uuid.New(), "backend-1", "udp", "cap", "corr"}}
	result, err := service(store, fakeOwner{err: errors.New("down")}).RunOnce(context.Background(), time.Now())
	if err != nil || result.StillUnknown != 1 || store.applied[0].Outcome != "UNKNOWN" {
		t.Fatalf("%+v %v", result, err)
	}
}
func TestMismatchedEvidenceCannotResolveAttempt(t *testing.T) {
	store := &fakeStore{candidate: Candidate{uuid.New(), "backend-1", "udp", "cap", "corr"}}
	_, err := service(store, fakeOwner{e: OwnerEvidence{BackendRequestID: "other", Outcome: "SUCCEEDED"}}).RunOnce(context.Background(), time.Now())
	if !errors.Is(err, ErrInvalidOwnerEvidence) || len(store.applied) != 0 {
		t.Fatalf("err=%v", err)
	}
}
