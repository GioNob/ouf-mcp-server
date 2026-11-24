package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type StatusClaim struct {
	Run     analysis.RunRecord
	Token   string
	Attempt int
}

// Commit the lease before contacting the Gateway. A crashed observer counts
// as an attempt. A late observer cannot release a newer lease or a stopped run.
func (s *Store) ClaimPausedStatus(ctx context.Context, workload string) (StatusClaim, error) {
	var claim StatusClaim
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return claim, err
	}
	defer tx.Rollback(ctx)
	now, err := s.runClock(ctx, tx)
	if err != nil {
		return claim, err
	}
	r, err := scanRun(tx.QueryRow(ctx, "select "+runColumns+` from ouf_mcp.analysis_run
		where state='RUNNING' and spec->>'Workload'=$1 and status_due is not null
		and least(status_due,budget_end,(mandate->>'expiresAt')::timestamptz)<=$2
		order by status_due,analysis_id limit 1 for update skip locked`, workload, now))
	if err != nil {
		return claim, err
	}
	var failures int
	if err = tx.QueryRow(ctx, "select status_failures from ouf_mcp.analysis_run where analysis_id=$1", r.ID()).Scan(&failures); err != nil {
		return claim, err
	}
	switch {
	case !now.Before(r.Mandate.Expires):
		_, err = tx.Exec(ctx, "update ouf_mcp.analysis_run set state='BLOCKED',reason='ANALYSIS_MANDATE_EXPIRED',mandate_state='EXPIRED',seq=seq+1 where analysis_id=$1", r.ID())
	case !now.Before(r.BudgetEnd):
		err = exhaustRun(ctx, tx, r)
	case failures > 0 && !now.Before(r.Deadline):
		_, err = tx.Exec(ctx, "update ouf_mcp.analysis_run set state='BLOCKED',reason='WAIT_DEADLINE_EXPIRED',seq=seq+1 where analysis_id=$1", r.ID())
	case failures >= analysis.MaxRetryAttempts:
		_, err = tx.Exec(ctx, "update ouf_mcp.analysis_run set status_due=deadline,status_token=null where analysis_id=$1", r.ID())
	default:
		claim = StatusClaim{Run: r, Token: uuid.NewString(), Attempt: failures + 1}
		lease := now.Add(5 * time.Second)
		if r.Deadline.After(now) && r.Deadline.Before(lease) {
			lease = r.Deadline
		}
		_, err = tx.Exec(ctx, "update ouf_mcp.analysis_run set status_token=$2,status_due=$3,status_failures=status_failures+1 where analysis_id=$1", r.ID(), claim.Token, lease)
	}
	if err != nil {
		return claim, err
	}
	return claim, tx.Commit(ctx)
}

func (s *Store) FinishPausedStatus(ctx context.Context, c StatusClaim, state, decision, retryAfter string, interval time.Duration) error {
	if interval < 5*time.Second || interval > 300*time.Second {
		return analysis.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	r := c.Run
	var end time.Time
	err = tx.QueryRow(ctx, `select status_due from ouf_mcp.analysis_run where analysis_id=$1 and seq=$2 and status_token=$3
		and state='RUNNING' and mandate->>'mandateId'=$4 and mandate->>'generation'=$5 for update`,
		r.ID(), r.Seq, c.Token, r.Mandate.ID, r.Mandate.Generation).Scan(&end)
	if errors.Is(err, pgx.ErrNoRows) {
		return analysis.ErrFenced
	}
	if err != nil {
		return err
	}
	now, err := s.runClock(ctx, tx)
	if err != nil {
		return err
	}
	if !now.Before(end) {
		return analysis.ErrFenced
	}
	if !now.Before(r.Mandate.Expires) {
		state = "EXPIRED"
	}
	next := now.Add(interval)
	var validUntil *time.Time
	runState, reason, mandate := r.State, r.Reason, r.MandateState
	failures := 0
	switch state {
	case "ACTIVE":
		if !now.Before(r.NotBefore) {
			validUntil = &next
			reason = "ANALYSIS_RUNNING"
		}
	case "REVOKED", "EXPIRED":
		runState, reason, mandate = "BLOCKED", "ANALYSIS_MANDATE_"+state, state
	case "DRIFT":
		runState, reason = "BLOCKED", "ANALYSIS_SCOPE_DRIFT"
	case "UNAVAILABLE":
		failures = c.Attempt
		retry := analysis.RunRetryAt(now, failures, retryAfter)
		if retry.After(next) {
			next = retry
		}
		if failures >= analysis.MaxRetryAttempts || next.After(r.Deadline) {
			next = r.Deadline
		}
		if !now.Before(r.Deadline) {
			runState, reason = "BLOCKED", "WAIT_DEADLINE_EXPIRED"
		} else {
			reason = "ANALYSIS_BACKOFF"
		}
	default:
		return analysis.ErrInvalid
	}
	version := ""
	if r.State != runState || r.Reason != reason {
		version = ",seq=seq+1"
	}
	_, err = tx.Exec(ctx, `update ouf_mcp.analysis_run set status_due=$2,status_valid_until=$8,status_token=null,status_failures=$3,
		status_decision=coalesce(nullif($4,''),status_decision),state=$5,reason=$6,mandate_state=$7`+version+` where analysis_id=$1`,
		r.ID(), next, failures, decision, runState, reason, mandate, validUntil)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
