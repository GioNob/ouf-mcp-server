package postgres

import (
	"context"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
)

// Only an interactive request can resume issue. The first command and the
// finite incident budget survive both a missing receipt and a client restart.
func (s *Store) ClaimRunIssue(ctx context.Context, r analysis.RunRecord) (analysis.RunRecord, int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return r, 0, err
	}
	defer tx.Rollback(ctx)
	current, err := scanRun(tx.QueryRow(ctx, "select "+runColumns+" from ouf_mcp.analysis_run where analysis_id=$1 for update", r.ID()))
	if err != nil {
		return r, 0, err
	}
	if current.RunSpec != r.RunSpec || current.State != "ISSUING" {
		return r, 0, analysis.ErrFenced
	}
	now, err := s.runClock(ctx, tx)
	if err != nil {
		return r, 0, err
	}
	var n int
	if err = tx.QueryRow(ctx, "select issue_attempts from ouf_mcp.analysis_run where analysis_id=$1", r.ID()).Scan(&n); err != nil {
		return r, 0, err
	}
	if n >= analysis.MaxRetryAttempts || !now.Add(3*time.Second).Before(current.BudgetEnd) {
		if err = s.exhaustRun(ctx, tx, current); err != nil {
			return r, 0, err
		}
		if err = tx.Commit(ctx); err != nil {
			return r, 0, err
		}
		return r, 0, analysis.ErrRunExhausted
	}
	if current.NotBefore.After(now) {
		return current, 0, analysis.ErrRunPressure
	}
	current, err = scanRun(tx.QueryRow(ctx, `update ouf_mcp.analysis_run set issue_attempts=issue_attempts+1,seq=seq+1,
		not_before=$2 where analysis_id=$1 returning `+runColumns, r.ID(), now.Add(5*time.Second)))
	if err != nil {
		return r, 0, err
	}
	return current, n + 1, tx.Commit(ctx)
}

func (s *Store) RecordIssueFailure(ctx context.Context, r analysis.RunRecord, code string, retry time.Time) error {
	state := "BLOCKED"
	if code == "ANALYSIS_BACKOFF" && !retry.IsZero() {
		state = "ISSUING"
	} else {
		retry = r.NotBefore
	}
	tag, err := s.pool.Exec(ctx, `update ouf_mcp.analysis_run set state=$3,reason=$4,not_before=greatest(not_before,$5),seq=seq+1,
		mandate_state=case when $4='ANALYSIS_AUTHORITY_DENIED' then 'DENIED' else mandate_state end
		where analysis_id=$1 and seq=$2 and state='ISSUING'`, r.ID(), r.Seq, state, code, retry)
	if err == nil && tag.RowsAffected() != 1 {
		return analysis.ErrFenced
	}
	return err
}
