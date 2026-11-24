package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/jackc/pgx/v5"
)

const runColumns = "spec,mandate,started,deadline,budget_end,not_before,state,reason,seq,mandate_state"

func scanRun(row pgx.Row) (analysis.RunRecord, error) {
	var r analysis.RunRecord
	var spec, mandate []byte
	err := row.Scan(&spec, &mandate, &r.Started, &r.Deadline, &r.BudgetEnd, &r.NotBefore, &r.State, &r.Reason, &r.Seq, &r.MandateState)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, analysis.ErrScope
	}
	if err != nil {
		return r, err
	}
	if json.Unmarshal(spec, &r.RunSpec) != nil || !r.RunSpec.Valid() {
		return r, analysis.ErrRunDrift
	}
	if mandate != nil {
		r.Mandate, err = analysis.DecodeMandate(mandate)
	}
	return r, err
}

func (s *Store) ReadRun(ctx context.Context, scope analysis.Scope, issuer, id string) (analysis.RunRecord, error) {
	return scanRun(s.pool.QueryRow(ctx, "select "+runColumns+` from ouf_mcp.analysis_run
		where analysis_id=$1 and issuer=$2 and workload=$3 and tenant=$4 and subject=$5`,
		id, issuer, scope.Service, scope.Tenant, scope.Principal))
}

func (s *Store) CheckRunCommand(ctx context.Context, scope analysis.Scope, issuer, key, id string) error {
	var previous string
	err := s.pool.QueryRow(ctx, `select analysis_id from ouf_mcp.analysis_run_command
		where issuer=$1 and workload=$2 and tenant=$3 and subject=$4 and command_key=$5`,
		issuer, scope.Service, scope.Tenant, scope.Principal, key).Scan(&previous)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err == nil && previous != id {
		return analysis.ErrCommandConflict
	}
	return err
}

// Register the command and the logical run BEFORE issue. Concurrent aliases
// and a crash after Gateway issue reuse the first command key at the Gateway.
func (s *Store) BeginRun(ctx context.Context, p analysis.RunSpec) (analysis.RunRecord, error) {
	var r analysis.RunRecord
	if !p.Valid() {
		return r, analysis.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return r, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "select pg_advisory_xact_lock(hashtextextended($1,5018))",
		analysis.Digest([]string{p.Issuer, p.Scope.Tenant})); err != nil {
		return r, err
	}
	if err = lockAnalysisMode(ctx, tx, p.ID()); err != nil {
		return r, err
	}
	var legacy bool
	if err = tx.QueryRow(ctx, "select exists(select 1 from ouf_mcp.profile_analysis where analysis_id=$1)", p.ID()).Scan(&legacy); err != nil {
		return r, err
	}
	var bound string
	err = tx.QueryRow(ctx, `select analysis_id from ouf_mcp.analysis_run_command
		where issuer=$1 and workload=$2 and tenant=$3 and subject=$4 and command_key=$5`,
		p.Issuer, p.Scope.Service, p.Scope.Tenant, p.Scope.Principal, p.CommandKey).Scan(&bound)
	if err == nil && bound != p.ID() {
		return r, analysis.ErrCommandConflict
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return r, err
	}
	if legacy {
		return r, analysis.ErrRunDrift
	}
	r, err = scanRun(tx.QueryRow(ctx, "select "+runColumns+" from ouf_mcp.analysis_run where analysis_id=$1 for update", p.ID()))
	if errors.Is(err, analysis.ErrScope) {
		var active, tenantActive, sourceActive int
		err = tx.QueryRow(ctx, `select count(*) filter(where subject=$3),count(*),count(*) filter(where spec->'Profile'->>'assetId'=$4) from ouf_mcp.analysis_run where issuer=$1 and tenant=$2
			and state in ('ISSUING','RUNNING') and budget_end>clock_timestamp()
			and (mandate is null or (mandate->>'expiresAt')::timestamptz>clock_timestamp())`, p.Issuer, p.Scope.Tenant, p.Scope.Principal, p.Profile.AssetID).Scan(&active, &tenantActive, &sourceActive)
		if err != nil {
			return r, err
		}
		if active >= p.Limits.ActivePlans || tenantActive >= p.Limits.Shared.TenantQueue || sourceActive >= p.Limits.Shared.SourceQueue {
			return r, analysis.ErrRunPressure
		}
		initial, err := analysis.NewRunCheckpoint(p.Profile, p.Limits.PreviewPageSize)
		if err != nil {
			return r, err
		}
		cp, _ := json.Marshal(initial)
		counts, _ := json.Marshal(initial.Counts())
		raw, err := json.Marshal(p)
		if err != nil {
			return r, err
		}
		r, err = scanRun(tx.QueryRow(ctx, `insert into ouf_mcp.analysis_run
			(analysis_id,issuer,workload,tenant,subject,spec,started,not_before,budget_end,deadline,state,checkpoint,counts)
			select $1,$2,$3,$4,$5,$6,t,t,t+$7::integer*interval '1 second',
			t+least($7::integer,$8::integer)*interval '1 second','ISSUING',$9,$10 from (select clock_timestamp() t) x returning `+runColumns,
			p.ID(), p.Issuer, p.Scope.Service, p.Scope.Tenant, p.Scope.Principal, raw, p.Limits.LifetimeSeconds, p.Limits.WaitSeconds, cp, counts))
		if err != nil {
			return r, err
		}
	} else if err != nil {
		return r, err
	}
	if r.Issuer != p.Issuer || r.Scope != p.Scope {
		return analysis.RunRecord{}, analysis.ErrScope
	}
	if r.Workload != p.Workload || r.Profile != p.Profile || r.Policy != p.Policy || r.Manifest != p.Manifest || r.Limits != p.Limits {
		return analysis.RunRecord{}, analysis.ErrRunDrift
	}
	_, err = tx.Exec(ctx, `insert into ouf_mcp.analysis_run_command values ($1,$2,$3,$4,$5,$6) on conflict do nothing`,
		p.Issuer, p.Scope.Service, p.Scope.Tenant, p.Scope.Principal, p.CommandKey, p.ID())
	if err != nil {
		return r, err
	}
	return r, tx.Commit(ctx)
}

func (s *Store) AttachMandate(ctx context.Context, spec analysis.RunSpec, raw []byte) (analysis.RunRecord, error) {
	m, err := analysis.DecodeMandate(raw)
	if err != nil {
		return analysis.RunRecord{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return analysis.RunRecord{}, err
	}
	defer tx.Rollback(ctx)
	r, err := scanRun(tx.QueryRow(ctx, "select "+runColumns+" from ouf_mcp.analysis_run where analysis_id=$1 for update", spec.ID()))
	if err != nil {
		return r, err
	}
	if r.RunSpec != spec {
		return analysis.RunRecord{}, analysis.ErrRunDrift
	}
	if r.State == "ISSUING" {
		r, err = scanRun(tx.QueryRow(ctx, `update ouf_mcp.analysis_run set mandate=$2,not_before=clock_timestamp(),seq=seq+1,mandate_state=case when $3<=clock_timestamp() then 'EXPIRED' else 'ACTIVE' end,
			deadline=least(deadline,$3),state=case when least($3,budget_end)<=clock_timestamp() then 'BLOCKED' else 'RUNNING' end,
			reason=case when $3<=clock_timestamp() then 'ANALYSIS_MANDATE_EXPIRED'
			when budget_end<=clock_timestamp() then 'ANALYSIS_BUDGET_EXHAUSTED' else 'ANALYSIS_RUNNING' end
			where analysis_id=$1 returning `+runColumns, spec.ID(), raw, m.Expires))
		if err != nil {
			return r, err
		}
		if r.Reason == "ANALYSIS_BUDGET_EXHAUSTED" {
			if err = exhaustRun(ctx, tx, r); err != nil {
				return r, err
			}
			r.Seq++
		}
	} else if analysis.Digest(m) != analysis.Digest(r.Mandate) {
		return analysis.RunRecord{}, analysis.ErrRunDrift
	}
	return r, tx.Commit(ctx)
}

// Cancellation is immediately visible to new admissions. Existing fences
// are immutable until their lease ends, including during cancellation.
func (s *Store) StopRun(ctx context.Context, r analysis.RunRecord, reason string) error {
	switch reason {
	case "ANALYSIS_MANDATE_EXPIRED", "ANALYSIS_MANDATE_REVOKED", "ANALYSIS_CANCELLED", "ANALYSIS_SCOPE_DRIFT":
	default:
		return analysis.ErrInvalid
	}
	tag, err := s.pool.Exec(ctx, `update ouf_mcp.analysis_run set state='BLOCKED',reason=$2,seq=seq+1,mandate_state=case $2
		when 'ANALYSIS_MANDATE_EXPIRED' then 'EXPIRED' when 'ANALYSIS_MANDATE_REVOKED' then 'REVOKED' else mandate_state end
		where analysis_id=$1 and issuer=$3 and workload=$4 and tenant=$5 and subject=$6 and seq=$7 and state in ('ISSUING','RUNNING')`,
		r.ID(), reason, r.Issuer, r.Scope.Service, r.Scope.Tenant, r.Scope.Principal, r.Seq)
	if err == nil && tag.RowsAffected() != 1 {
		return analysis.ErrFenced
	}
	return err
}

func (s *Store) runClock(ctx context.Context, tx interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (time.Time, error) {
	if s.analysisClock != nil {
		return s.analysisClock(), nil
	}
	var now time.Time
	err := tx.QueryRow(ctx, "select clock_timestamp()").Scan(&now)
	return now, err
}

func lockAnalysisMode(ctx context.Context, tx pgx.Tx, id string) error {
	_, err := tx.Exec(ctx, "select pg_advisory_xact_lock(hashtextextended($1,5021))", id)
	return err
}
