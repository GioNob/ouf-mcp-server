package evidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"

	"github.com/google/uuid"
)

var (
	ErrUnauthenticated = errors.New("authenticated evidence identity required")
	ErrOwnerMismatch   = errors.New("evidence owner differs from trusted identity")
	ErrIngressConflict = errors.New("evidence ingress conflict")
)

type TrustedIdentity struct {
	Authenticated      bool
	ServicePrincipalID string
	BackendOwner       string
}

type Input struct {
	ClaimedOwner, BackendRequestID, Kind, TerminalState, OutcomeCode, ResultRef string
	ActualDistinctObjects                                                       int64
	ObjectHashes                                                                []string
}

type Verified struct {
	EvidenceRef uuid.UUID
	TrustedIdentity
	Input
	PayloadHash string
}

type Result struct {
	EvidenceRef uuid.UUID
	Replay      bool
}

type CompensationResult struct {
	Disposition, AttemptState, OutcomeCode string
	OutcomeRevision                        int
}

type CompensationStore interface {
	LateCompensate(context.Context, uuid.UUID, uuid.UUID, int) (CompensationResult, error)
}

type CompensationService struct{ Store CompensationStore }

func (s CompensationService) Apply(ctx context.Context, attemptID, evidenceRef uuid.UUID, expectedRevision int) (CompensationResult, error) {
	if attemptID == uuid.Nil || evidenceRef == uuid.Nil || expectedRevision < 0 {
		return CompensationResult{}, errors.New("invalid late compensation request")
	}
	return s.Store.LateCompensate(ctx, attemptID, evidenceRef, expectedRevision)
}

type Store interface {
	InsertVerifiedEvidence(context.Context, Verified) (Result, error)
}

type Service struct{ Store Store }

func (s Service) Ingest(ctx context.Context, identity TrustedIdentity, input Input) (Result, error) {
	if !identity.Authenticated || identity.ServicePrincipalID == "" || identity.BackendOwner == "" {
		return Result{}, ErrUnauthenticated
	}
	if input.ClaimedOwner != "" && input.ClaimedOwner != identity.BackendOwner {
		return Result{}, ErrOwnerMismatch
	}
	if input.BackendRequestID == "" || (input.Kind != "OWNER_RESULT" && input.Kind != "OWNER_PROVES_NO_DISPATCH") {
		return Result{}, errors.New("invalid evidence input")
	}
	input.ObjectHashes = append([]string(nil), input.ObjectHashes...)
	sort.Strings(input.ObjectHashes)
	evidenceRef := uuid.NewSHA1(uuid.NameSpaceOID, []byte(identity.BackendOwner+"\x00"+input.BackendRequestID))
	canonical, err := json.Marshal(struct {
		EvidenceRef  uuid.UUID
		BackendOwner string
		Input
	}{evidenceRef, identity.BackendOwner, input})
	if err != nil {
		return Result{}, err
	}
	digest := sha256.Sum256(canonical)
	return s.Store.InsertVerifiedEvidence(ctx, Verified{evidenceRef, identity, input, hex.EncodeToString(digest[:])})
}
