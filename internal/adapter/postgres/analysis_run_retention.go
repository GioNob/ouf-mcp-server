package postgres

import (
	"context"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
)

// One bounded batch. The maintenance role has EXECUTE on the purge
// function, while direct deletes remain private to its NOLOGIN owner.
func (s *Store) CleanRunHistory(ctx context.Context, workload string, p analysis.RunRetention) error {
	for _, ttl := range []time.Duration{p.Reservations, p.Attempts, p.Snapshots, p.Reads} {
		if ttl < 24*time.Hour || ttl > 3650*24*time.Hour {
			return analysis.ErrInvalid
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	now, err := s.runClock(ctx, tx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "select ouf_mcp.purge_analysis_run_history($1,$2,$3,$4)",
		workload, now.Add(-max(p.Reservations, p.Attempts)), now.Add(-p.Snapshots), now.Add(-p.Reads))
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
