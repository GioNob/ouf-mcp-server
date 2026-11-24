package postgres

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/jackc/pgx/v5"
)

// Predecessor specs already pin the configured service Workload. The workload
// column holds the interactive client and must not partition installation quota.
const runInstallationSQL = "coalesce(spec->'Limits'->>'InstallationRef',spec->>'Workload')"

// Admission always takes transaction locks from widest to narrowest:
// installation, (issuer, tenant), plan mode, then the plan row if it exists.
// Read Committed gives the subsequent count a fresh snapshot after any wait.
func lockRunAdmission(ctx context.Context, tx pgx.Tx, installation, issuer, tenant, id string) error {
	if _, err := tx.Exec(ctx, "select pg_advisory_xact_lock(hashtextextended($1,5022))", installation); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "select pg_advisory_xact_lock(hashtextextended($1,5018))", analysis.Digest([]string{issuer, tenant})); err != nil {
		return err
	}
	return lockAnalysisMode(ctx, tx, id)
}

// The slot is the live logical plan row. Issue retries, leases and worker
// fences never allocate another slot. Both transitions into active capacity
// call this under lockRunAdmission, in the transaction that publishes them.
func admitRun(ctx context.Context, tx pgx.Tx, p analysis.RunSpec, now time.Time) error {
	var installation, subject, tenant, source int
	err := tx.QueryRow(ctx, `select count(*) filter (where installation=$1),
		count(*) filter (where installation=$1 and issuer=$2 and subject=$3),
		count(*) filter (where issuer=$2 and tenant=$4),
		count(*) filter (where issuer=$2 and tenant=$4 and asset=$5)
		from (select `+runInstallationSQL+` as installation, issuer, subject, tenant, spec->'Profile'->>'assetId' as asset
			from ouf_mcp.analysis_run where state in ('ISSUING','RUNNING') and budget_end>$6
			and (mandate is null or (mandate->>'expiresAt')::timestamptz>$6)) live
		where installation=$1 or (issuer=$2 and tenant=$4)`,
		p.Installation(), p.Issuer, p.Scope.Principal, p.Scope.Tenant, p.Profile.AssetID, now).Scan(&installation, &subject, &tenant, &source)
	if err != nil {
		return err
	}
	if installation >= p.Limits.InstallationPlanLimit() || subject >= p.Limits.ActivePlans ||
		tenant >= p.Limits.Shared.TenantQueue || source >= p.Limits.Shared.SourceQueue {
		return runAdmissionBackoff(now, analysis.RunRetryAt(now, 1, "").Truncate(time.Microsecond))
	}
	return nil
}

func runAdmissionBackoff(now, notBefore time.Time) *analysis.RenewalBackoff {
	seconds := (notBefore.Sub(now) + time.Second - 1) / time.Second
	return &analysis.RenewalBackoff{Cause: analysis.ErrRunPressure, NotBefore: notBefore, RetryAfter: strconv.FormatInt(int64(seconds), 10)}
}

// All admission refusals in renewRunBudgetTx precede its first write.
// Commit only the wait on the existing blocked plan, keeping the proposal
// sequence, the open incident and the one-shot right available for a retry.
func persistRenewalWait(ctx context.Context, tx pgx.Tx, current analysis.RunRecord, refusal error) error {
	var wait *analysis.RenewalBackoff
	if !errors.As(refusal, &wait) || !wait.NotBefore.After(current.NotBefore) {
		return refusal
	}
	if _, err := tx.Exec(ctx, "update ouf_mcp.analysis_run set not_before=$2 where analysis_id=$1", current.ID(), wait.NotBefore); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return refusal
}
