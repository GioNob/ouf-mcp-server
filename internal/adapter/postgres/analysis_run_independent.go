package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/jackc/pgx/v5"
)

// Before exposing a plan-wide renewal, finish disjoint intents that can still
// reserve their next call from the existing session. Only reorder the durable
// queue. Each moved item still needs normal admission and owner proof.
func prioritizeIndependent(ctx context.Context, tx pgx.Tx, r analysis.RunRecord, now time.Time) (bool, error) {
	if !now.Before(r.BudgetEnd) || !now.Before(r.Mandate.Expires) {
		return false, nil
	}
	var cp analysis.RunCheckpoint
	if err := tx.QueryRow(ctx, "select checkpoint from ouf_mcp.analysis_run where analysis_id=$1", r.ID()).Scan(&cp); err != nil {
		return false, err
	}
	if len(cp.Queue) < 2 {
		return false, nil
	}
	maximum, fence, err := runSession(ctx, tx, r)
	if err != nil {
		return false, err
	}
	spent, err := runSpend(ctx, tx, r.ID(), fence)
	if err != nil {
		return false, err
	}
	affordable := func(item analysis.RunItem) bool {
		cost := item.Work("").Maximum
		start := now
		if r.NotBefore.After(start) {
			start = r.NotBefore
		}
		if item.Task.NotBefore.After(start) {
			start = item.Task.NotBefore
		}
		end := start.Add(time.Duration(cost.Millis) * time.Millisecond)
		return spent.Add(cost).Fits(maximum) && end.Before(r.BudgetEnd) && end.Before(r.Mandate.Expires)
	}
	if affordable(cp.Queue[0]) {
		return false, nil
	}
	var outstanding bool
	err = tx.QueryRow(ctx, `select exists(select 1 from ouf_mcp.analysis_run_attempt where analysis_id=$1 and
		((finished_at is null and superseded_at is null) or proof_until>$2))`, r.ID(), now).Scan(&outstanding)
	if err != nil || outstanding {
		return false, err
	}
	ordinals := make(map[int]bool)
	intents := make(map[string]bool)
	for index, item := range cp.Queue {
		// A preview page produces later headers and is never independent.
		if item.Task.Tool == "source.file.preview" {
			return false, nil
		}
		disjoint := !intents[item.Intent]
		for _, n := range item.Task.Columns {
			if ordinals[n] {
				disjoint = false
			}
			ordinals[n] = true
		}
		intents[item.Intent] = true
		if index == 0 || !disjoint || !affordable(item) {
			continue
		}
		copy(cp.Queue[1:index+1], cp.Queue[:index])
		cp.Queue[0] = item
		raw, err := json.Marshal(cp)
		if err != nil {
			return false, err
		}
		_, err = tx.Exec(ctx, `update ouf_mcp.analysis_run set checkpoint=$2,seq=seq+1,not_before=greatest(not_before,$3),
			status_due=case when $3::timestamptz>$4 then $4 else status_due end where analysis_id=$1`, r.ID(), raw, item.Task.NotBefore, now)
		return err == nil, err
	}
	return false, nil
}

func (s *Store) finishExhaustedReservation(ctx context.Context, tx pgx.Tx, r analysis.RunRecord, now time.Time) error {
	advanced, err := prioritizeIndependent(ctx, tx, r, now)
	if err != nil {
		return err
	}
	if !advanced {
		if err = s.exhaustRun(ctx, tx, r); err != nil {
			return err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	if advanced {
		return analysis.ErrFenced
	}
	return analysis.ErrRunExhausted
}
