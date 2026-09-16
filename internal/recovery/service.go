package recovery

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrInvalidOwnerEvidence = errors.New("invalid owner recovery evidence")

type Candidate struct {
	AttemptID                                            uuid.UUID
	BackendRequestID, Owner, CapabilityID, CorrelationID string
}
type OwnerQuery struct{ BackendRequestID, Owner, CapabilityID, CorrelationID string }
type OwnerEvidence struct {
	BackendRequestID                   string
	Outcome                            string // SUCCEEDED, FAILED, NOT_DISPATCHED, UNKNOWN
	OutcomeCode, ResultRef             string
	ActualToolCalls, ActualResultBytes int64
}
type Store interface {
	MarkStaleAndClaim(context.Context, time.Time, time.Time, string, time.Duration, int) ([]Candidate, error)
	ApplyOwnerEvidence(context.Context, uuid.UUID, string, OwnerEvidence) error
	ExpireUnknown(context.Context, time.Time, time.Time, int) (int, error)
}
type OwnerPort interface {
	QueryOutcome(context.Context, OwnerQuery) (OwnerEvidence, error)
}
type AuditPort interface {
	AppendRecoveryAudit(context.Context, uuid.UUID, string, string) error
}
type Result struct{ Claimed, Reconciled, StillUnknown, Unresolved int }
type Service struct {
	Store          Store
	Owner          OwnerPort
	Audit          AuditPort
	WorkerID       string
	Lease          time.Duration
	AdmissionGrace time.Duration
	MaxUnknownHold time.Duration
	Batch          int
}

func (s Service) RunOnce(ctx context.Context, now time.Time) (Result, error) {
	if s.WorkerID == "" || s.Lease <= 0 || s.AdmissionGrace <= 0 || s.MaxUnknownHold <= 0 || s.Batch < 1 {
		return Result{}, errors.New("invalid recovery worker configuration")
	}
	candidates, err := s.Store.MarkStaleAndClaim(ctx, now, now.Add(-s.AdmissionGrace), s.WorkerID, s.Lease, s.Batch)
	if err != nil {
		return Result{}, err
	}
	result := Result{Claimed: len(candidates)}
	for _, candidate := range candidates {
		evidence, queryErr := s.Owner.QueryOutcome(ctx, OwnerQuery{candidate.BackendRequestID, candidate.Owner, candidate.CapabilityID, candidate.CorrelationID})
		if queryErr != nil {
			evidence = OwnerEvidence{BackendRequestID: candidate.BackendRequestID, Outcome: "UNKNOWN", OutcomeCode: "OWNER_QUERY_UNAVAILABLE"}
		}
		if evidence.BackendRequestID != candidate.BackendRequestID {
			return result, ErrInvalidOwnerEvidence
		}
		if evidence.Outcome != "SUCCEEDED" && evidence.Outcome != "FAILED" && evidence.Outcome != "NOT_DISPATCHED" && evidence.Outcome != "UNKNOWN" {
			return result, ErrInvalidOwnerEvidence
		}
		if err = s.Store.ApplyOwnerEvidence(ctx, candidate.AttemptID, s.WorkerID, evidence); err != nil {
			return result, err
		}
		if s.Audit != nil {
			if err = s.Audit.AppendRecoveryAudit(ctx, candidate.AttemptID, evidence.Outcome, evidence.OutcomeCode); err != nil {
				return result, err
			}
		}
		if evidence.Outcome == "UNKNOWN" {
			result.StillUnknown++
		} else {
			result.Reconciled++
		}
	}
	result.Unresolved, err = s.Store.ExpireUnknown(ctx, now, now.Add(-s.MaxUnknownHold), s.Batch)
	if err != nil {
		return result, err
	}
	return result, nil
}
