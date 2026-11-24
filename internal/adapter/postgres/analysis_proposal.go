package postgres

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/jackc/pgx/v5"
)

type proposalRow struct {
	analysis.RenewalProposal
	Feedback []analysis.RenewalItem
}

func scanProposal(row pgx.Row) (proposalRow, error) {
	var p proposalRow
	var raw, feedback []byte
	var state string
	err := row.Scan(&raw, &state, &feedback)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, analysis.ErrScope
	}
	if err != nil {
		return p, err
	}
	if json.Unmarshal(raw, &p.RenewalProposal) != nil || json.Unmarshal(feedback, &p.Feedback) != nil {
		return p, analysis.ErrRunDrift
	}
	hash := p.Hash
	p.Hash = ""
	if analysis.Digest(p.RenewalProposal) != hash {
		return p, analysis.ErrRunDrift
	}
	p.Hash = hash
	p.State = state
	return p, nil
}

func latestProposal(ctx context.Context, tx pgx.Tx, id string) (proposalRow, error) {
	return scanProposal(tx.QueryRow(ctx, `select proposal,state,feedback from ouf_mcp.analysis_renewal_proposal where analysis_id=$1 order by version desc limit 1`, id))
}

func observeProposal(ctx context.Context, tx pgx.Tx, p *proposalRow, now time.Time) error {
	// A rollback to 020 can consume the right without writing a 021 receipt.
	if p.State != "OPEN" && p.State != "CHANGES_REQUESTED" {
		return nil
	}
	var settled string
	increment, err := json.Marshal(p.Increment)
	if err != nil {
		return err
	}
	err = tx.QueryRow(ctx, `select case when expected_seq=$3 and proposal_version=$4 and proposal_hash=$5
		and increment=$6::jsonb and ends_at <= $7 then 'APPROVED' else 'SUPERSEDED' end
		from ouf_mcp.analysis_run_budget_session where analysis_id=$1 and incident_id=$2 and expected_seq >= $3 and renewal_used`,
		p.AnalysisID, p.IncidentID, p.ExpectedSeq, p.Version, p.Hash, increment, p.Expires).Scan(&settled)
	if err == nil {
		_, err = tx.Exec(ctx, "update ouf_mcp.analysis_renewal_proposal set state=$2 where review_ref=$1", p.ReviewRef, settled)
		p.State = settled
		return err
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if !p.Expires.After(now) {
		_, err := tx.Exec(ctx, "update ouf_mcp.analysis_renewal_proposal set state='EXPIRED' where review_ref=$1", p.ReviewRef)
		p.State = "EXPIRED"
		return err
	}
	return nil
}

func newProposal(ctx context.Context, tx pgx.Tx, r analysis.RunRecord, previous *proposalRow, increment analysis.RunCost, now time.Time) (analysis.RenewalProposal, error) {
	var p analysis.RenewalProposal
	var cp analysis.RunCheckpoint
	if err := tx.QueryRow(ctx, "select checkpoint from ouf_mcp.analysis_run where analysis_id=$1", r.ID()).Scan(&cp); err != nil {
		return p, err
	}
	if len(cp.Outcomes) != r.Profile.ColumnCount {
		return p, analysis.ErrRunDrift
	}
	p.Remaining = []int{}
	for ordinal, outcome := range cp.Outcomes {
		if outcome.State == "PENDING" {
			p.Remaining = append(p.Remaining, ordinal)
		}
	}
	p.Initial = r.Limits.Maximum
	p.Increment = increment
	if increment == (analysis.RunCost{}) || !increment.Fits(r.Limits.RenewalCeiling()) {
		return p, analysis.ErrInvalid
	}
	p.Total = p.Initial.Add(increment)
	var err error
	p.Consumed, err = runSpend(ctx, tx, r.ID(), 0)
	if err != nil {
		return p, err
	}
	p.Contract = analysis.RenewalContract
	p.Version = 1
	p.State = "OPEN"
	p.RequestedChanges = []analysis.RenewalItem{}
	if previous != nil {
		p.Previous = &previous.ProposalVersion
		p.Version = previous.Version + 1
		if previous.State == "CHANGES_REQUESTED" {
			p.RequestedChanges = previous.Feedback
		}
	}
	p.Reason = analysis.ErrRunExhausted.Error()
	p.AnalysisID, p.IncidentID = r.ID(), analysis.Digest([]string{analysis.RunBudgetProfile, r.ID()})
	p.ExpectedSeq, p.MandateID, p.Generation = r.Seq, r.Mandate.ID, r.Mandate.Generation
	p.PlanLabel = analysis.PlanLabel(r.SourceFile, r.Profile.ColumnCount)
	seconds := r.ProposalSeconds
	if seconds == 0 {
		seconds = 3600
	}
	end := analysis.RunRenewal{Seconds: seconds, ExpiresAt: r.Mandate.Expires}
	p.Expires = end.SessionEnd(r, now).UTC()
	if !p.Expires.After(now) {
		p.State = "EXPIRED"
	}
	var nonce [24]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return p, err
	}
	p.ReviewRef = base64.RawURLEncoding.EncodeToString(nonce[:])
	p.Hash = analysis.Digest(p)
	raw, err := json.Marshal(p)
	if err != nil {
		return p, err
	}
	_, err = tx.Exec(ctx, `insert into ouf_mcp.analysis_renewal_proposal(review_ref,analysis_id,incident_id,version,proposal,state,expires_at) values($1,$2,$3,$4,$5,$6,$7)`, p.ReviewRef, r.ID(), p.IncidentID, p.Version, raw, p.State, p.Expires)
	return p, err
}

// Called under the run lock, also materializes a predecessor's open incident on first observation.
func ensureProposal(ctx context.Context, tx pgx.Tx, r analysis.RunRecord, now time.Time) (proposalRow, error) {
	p, err := latestProposal(ctx, tx, r.ID())
	if err == nil {
		if err = observeProposal(ctx, tx, &p, now); err != nil {
			return p, err
		}
		if (p.State != "APPROVED" && p.State != "SUPERSEDED") || r.Seq <= p.ExpectedSeq {
			return p, nil
		}
	}
	if err != nil && !errors.Is(err, analysis.ErrScope) {
		return p, err
	}
	var open bool
	if err = tx.QueryRow(ctx, `select exists(select 1 from ouf_mcp.analysis_run_incident where analysis_id=$1 and state='OPEN')`, r.ID()).Scan(&open); err != nil {
		return p, err
	}
	if !open || r.Mandate.ID == "" || r.Limits.RenewalCeiling() == (analysis.RunCost{}) {
		return p, analysis.ErrScope
	}
	var previous *proposalRow
	if p.Version > 0 {
		previous = &p
	}
	created, err := newProposal(ctx, tx, r, previous, r.Limits.RenewalCeiling(), now)
	return proposalRow{RenewalProposal: created}, err
}

func (s *Store) CurrentRenewal(ctx context.Context, expected analysis.RunRecord) (analysis.RenewalProposal, error) {
	var out analysis.RenewalProposal
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	r, err := scanRun(tx.QueryRow(ctx, "select "+runColumns+" from ouf_mcp.analysis_run where analysis_id=$1 for update", expected.ID()))
	if err != nil {
		return out, err
	}
	if r.RunSpec != expected.RunSpec || r.Seq != expected.Seq || r.State != "BLOCKED" || r.Reason != analysis.ErrRunExhausted.Error() {
		return out, analysis.ErrFenced
	}
	now, err := s.runClock(ctx, tx)
	if err != nil {
		return out, err
	}
	p, err := ensureProposal(ctx, tx, r, now)
	if err != nil {
		return out, err
	}
	if p.State == "CHANGES_REQUESTED" {
		p.RequestedChanges = p.Feedback
	}
	return p.RenewalProposal, tx.Commit(ctx)
}

func (s *Store) ReadRenewal(ctx context.Context, i orchestration.Identity, ref string) (analysis.RenewalProposal, error) {
	var out analysis.RenewalProposal
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	p, err := scanProposal(tx.QueryRow(ctx, `select p.proposal,p.state,p.feedback from ouf_mcp.analysis_renewal_proposal p join ouf_mcp.analysis_run r using(analysis_id)
		where p.review_ref=$1 and r.issuer=$2 and r.spec->>'Workload'=$3 and r.tenant=$4 and r.subject=$5
		and convert_from(p.proposal,'UTF8')::jsonb->>'mandateId'=r.mandate->>'mandateId'
		and convert_from(p.proposal,'UTF8')::jsonb->>'generation'=r.mandate->>'generation' for update of p`,
		ref, i.Issuer, i.ServicePrincipalID, i.TenantID, i.PrincipalID))
	if err != nil {
		return out, err
	}
	now, err := s.runClock(ctx, tx)
	if err != nil {
		return out, err
	}
	if err = observeProposal(ctx, tx, &p, now); err != nil {
		return out, err
	}
	return p.RenewalProposal, tx.Commit(ctx)
}

func openRenewalIncident(ctx context.Context, tx pgx.Tx, p analysis.RenewalProposal) error {
	var open bool
	err := tx.QueryRow(ctx, `select exists(select 1 from ouf_mcp.analysis_run_incident
		where analysis_id=$1 and incident_id=$2 and state='OPEN' for share)`, p.AnalysisID, p.IncidentID).Scan(&open)
	if err == nil && !open {
		return analysis.ErrFenced
	}
	return err
}

func (s *Store) ReviseRenewal(ctx context.Context, i orchestration.Identity, v analysis.RenewalRevision) (analysis.RenewalProposal, error) {
	var out analysis.RenewalProposal
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	r, err := lockRenewalRun(ctx, tx, i, v.AnalysisID)
	if err != nil {
		return out, err
	}
	if i.ActorType != "HUMAN" || i.Delegation == "" {
		return out, orchestration.ErrUnauthorized
	}
	if r.Scope != analysis.IdentityScope(i) {
		return out, analysis.ErrScope
	}
	fingerprint := analysis.Digest(v)
	raw, err := renewalReplay(ctx, tx, r, v.Key, fingerprint)
	if err != nil {
		return out, err
	}
	if raw != nil {
		err = json.Unmarshal(raw, &out)
		return out, err
	}
	p, err := latestProposal(ctx, tx, r.ID())
	if err != nil {
		return out, err
	}
	now, err := s.runClock(ctx, tx)
	if err != nil {
		return out, err
	}
	if p.IncidentID != v.IncidentID || p.Version != v.Version || p.State != "CHANGES_REQUESTED" || !p.Expires.After(now) ||
		r.State != "BLOCKED" || r.Reason != analysis.ErrRunExhausted.Error() || r.Seq != p.ExpectedSeq+1 || r.MandateState != "ACTIVE" ||
		!slices.Equal(p.Feedback, v.RequestedChanges) {
		return out, analysis.ErrFenced
	}
	if err = openRenewalIncident(ctx, tx, p.RenewalProposal); err != nil {
		return out, err
	}
	increment := p.Increment
	if v.Increment != nil {
		increment = *v.Increment
	}
	_, err = tx.Exec(ctx, "update ouf_mcp.analysis_renewal_proposal set state='SUPERSEDED' where review_ref=$1", p.ReviewRef)
	if err != nil {
		return out, err
	}
	if err = advanceRenewalSeq(ctx, tx, &r); err != nil {
		return out, err
	}
	out, err = newProposal(ctx, tx, r, &p, increment, now)
	if err != nil {
		return out, err
	}
	raw, err = json.Marshal(out)
	if err != nil {
		return out, err
	}
	if err = recordRenewal(ctx, tx, r, v.Key, fingerprint, out.ReviewRef, raw, nil); err != nil {
		return out, err
	}
	now, err = s.runClock(ctx, tx)
	if err != nil {
		return out, err
	}
	if !p.Expires.After(now) || !out.Expires.After(now) {
		return out, analysis.ErrFenced
	}
	return out, tx.Commit(ctx)
}
