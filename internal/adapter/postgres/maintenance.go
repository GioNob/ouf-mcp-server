package postgres

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type DebtCacheReconciliationResult struct {
	Checked  int64
	Repaired int64
	Delta    int64
}

func debtCacheDelta(cached, authoritative int64) (int64, error) {
	if cached < 0 || authoritative < 0 {
		return 0, errors.New("negative debt total violates persistence invariants")
	}
	return authoritative - cached, nil
}

// ReconcileDebtCaches repairs only the non-authoritative cache. ACTIVE
// budget_object_debt rows remain authoritative and are never forgiven by TTL.
// Each window is processed in its own short transaction under the window lock.
func (s *Store) ReconcileDebtCaches(ctx context.Context, limit int) (DebtCacheReconciliationResult, error) {
	if limit < 1 {
		return DebtCacheReconciliationResult{}, errors.New("maintenance batch limit must be positive")
	}
	rows, err := s.pool.Query(ctx, `
		select w.budget_window_id
		  from ouf_mcp.budget_window w
		 where w.current_unresolved_object_debt_total <> coalesce((
		       select sum(d.debt_amount) from ouf_mcp.budget_object_debt d
		        where d.budget_window_id=w.budget_window_id and d.debt_state='ACTIVE'),0)
		 order by w.window_start,w.budget_window_id limit $1`, limit)
	if err != nil {
		return DebtCacheReconciliationResult{}, err
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return DebtCacheReconciliationResult{}, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return DebtCacheReconciliationResult{}, err
	}

	result := DebtCacheReconciliationResult{Checked: int64(len(ids))}
	for _, id := range ids {
		tx, beginErr := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if beginErr != nil {
			return result, beginErr
		}
		var cached, authoritative int64
		err = tx.QueryRow(ctx, `select current_unresolved_object_debt_total from ouf_mcp.budget_window where budget_window_id=$1 for update`, id).Scan(&cached)
		if err == nil {
			err = tx.QueryRow(ctx, `select coalesce(sum(debt_amount),0) from ouf_mcp.budget_object_debt where budget_window_id=$1 and debt_state='ACTIVE'`, id).Scan(&authoritative)
		}
		delta := int64(0)
		if err == nil {
			delta, err = debtCacheDelta(cached, authoritative)
		}
		if err == nil && delta != 0 {
			tag, execErr := tx.Exec(ctx, `update ouf_mcp.budget_window set current_unresolved_object_debt_total=$2 where budget_window_id=$1 and current_unresolved_object_debt_total=$3`, id, authoritative, cached)
			err = execErr
			if err == nil && tag.RowsAffected() != 1 {
				err = errors.New("debt cache repair CAS affected an unexpected row count")
			}
			if err == nil {
				_, err = tx.Exec(ctx, `insert into ouf_mcp.audit_event(audit_event_id,event_type,actor_type,safe_detail) values($1,'BUDGET_DEBT_CACHE_REPAIRED','MAINTENANCE_WORKER',jsonb_build_object('budgetWindowId',$2::text,'priorCachedDebt',$3,'authoritativeActiveDebt',$4,'delta',$5))`, uuid.New(), id, cached, authoritative, delta)
			}
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return result, err
		}
		if err = tx.Commit(ctx); err != nil {
			return result, err
		}
		if delta != 0 {
			result.Repaired++
			result.Delta += delta
		}
	}
	return result, nil
}
