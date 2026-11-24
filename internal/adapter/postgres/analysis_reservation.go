package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// A claim is also the budget reservation. Never return another worker's
// attempt from this method, including on concurrent delivery of the same work.
func (s *Store) ReserveRunWork(ctx context.Context, expected analysis.RunRecord, work analysis.RunWork) (analysis.ReservationCheck, error) {
	var check analysis.ReservationCheck
	hash, err := analysis.MandateHash(work.Arguments)
	if err != nil || !analysis.CanonicalUUID(work.Worker) || analysis.ValidateID(work.Intent) != nil ||
		!analysis.AllowedOperation(work.Operation) || work.Maximum.Calls != 1 || work.Maximum.Attempts != 1 ||
		work.Maximum.Millis < 1 || work.Maximum.Millis > 30000 || work.Maximum.Bytes < 1 || work.Maximum.Objects < 0 {
		return check, analysis.ErrInvalid
	}
	var args struct{ RequestID, AssetID, ProfileID, View, IdempotencyKey string }
	if json.Unmarshal(work.Arguments, &args) != nil || len(work.Ordinals) == 0 || !slices.IsSorted(work.Ordinals) {
		return check, analysis.ErrInvalid
	}
	for i, n := range work.Ordinals {
		if n < 0 || n >= expected.Profile.ColumnCount || (i > 0 && n == work.Ordinals[i-1]) {
			return check, analysis.ErrInvalid
		}
	}
	provider := "semantic"
	if work.Operation == "source.file.preview" {
		provider = "onboarding"
		if args.AssetID != expected.Profile.AssetID || args.ProfileID != expected.Profile.ProfileID || args.View != "COLUMNS" {
			return check, analysis.ErrRunDrift
		}
	}
	if work.Operation == "semantic.discovery.request" && args.IdempotencyKey != "analysis:"+analysis.Digest([]string{expected.ID(), work.Intent}) {
		return check, analysis.ErrRunDrift
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return check, err
	}
	defer tx.Rollback(ctx)
	// A shared subject/provider slot spans every plan and replica.
	_, err = tx.Exec(ctx, "select pg_advisory_xact_lock(hashtextextended($1,5019))",
		provider)
	if err != nil {
		return check, err
	}
	if _, err = tx.Exec(ctx, "select pg_advisory_xact_lock(hashtextextended($1,5020))", analysis.Digest([]string{expected.Issuer, expected.Scope.Tenant})); err != nil {
		return check, err
	}
	r, err := scanRun(tx.QueryRow(ctx, "select "+runColumns+" from ouf_mcp.analysis_run where analysis_id=$1 for update", expected.ID()))
	if err != nil {
		return check, err
	}
	if r.RunSpec != expected.RunSpec || r.Seq != expected.Seq || r.MandateState != "ACTIVE" || r.State != "RUNNING" ||
		r.Mandate.ID != expected.Mandate.ID || !slices.Contains(r.Mandate.Operations, work.Operation) {
		return check, analysis.ErrFenced
	}
	now, err := s.runClock(ctx, tx)
	if err != nil {
		return check, err
	}
	duration := time.Duration(work.Maximum.Millis) * time.Millisecond
	if !now.Add(duration).Before(r.Mandate.Expires) {
		return check, analysis.ErrMandateExpired
	}
	if !now.Add(duration).Before(r.BudgetEnd) {
		return check, s.finishExhaustedReservation(ctx, tx, r, now)
	}
	var paused bool
	if err = tx.QueryRow(ctx, "select status_due is not null and (status_valid_until is null or status_valid_until<=$2) from ouf_mcp.analysis_run where analysis_id=$1", r.ID(), now).Scan(&paused); err != nil {
		return check, err
	}
	if paused || now.Before(r.NotBefore) {
		return check, analysis.ErrRunPressure
	}
	var busy bool
	var active int
	err = tx.QueryRow(ctx, `select exists(select 1 from ouf_mcp.analysis_run_attempt where analysis_id=$1
		and ((finished_at is null and lease_end>$2) or proof_until>$2)),
		(select count(*) from ouf_mcp.analysis_run_attempt where issuer=$3 and tenant=$4 and subject=$5 and provider=$6
		and finished_at is null and lease_end>$2)`, r.ID(), now, r.Issuer, r.Scope.Tenant, r.Scope.Principal, provider).Scan(&busy, &active)
	if err != nil {
		return check, err
	}
	if busy || active >= r.Limits.ProviderSlots {
		return check, analysis.ErrRunPressure
	}
	if work.Operation == "semantic.discovery.request" {
		var ownerJobs int
		// Uncertain submissions keep their subject-provider slot. Only an
		// identical same-plan reconciliation can use its existing slot.
		err = tx.QueryRow(ctx, `select count(*) from (
			select q.analysis_id,q.intent_key from ouf_mcp.analysis_run_request q join ouf_mcp.analysis_run r using(analysis_id)
			where r.issuer=$1 and r.tenant=$2 and r.subject=$3 and q.finished_at is null
			union select analysis_id,intent_key from ouf_mcp.analysis_run_attempt where issuer=$1 and tenant=$2 and subject=$3
			and operation='semantic.discovery.request' and finished_at is null and superseded_at is null
			and (analysis_id<>$4 or intent_key<>$5)) open_jobs`, r.Issuer, r.Scope.Tenant, r.Scope.Principal, r.ID(), work.Intent).Scan(&ownerJobs)
		if err != nil {
			return check, err
		}
		if ownerJobs >= r.Limits.ProviderSlots {
			return check, analysis.ErrRunPressure
		}
	}
	var tenantSlots, sourceSlots, depSlots, tenantRate, sourceRate, depRate int
	err = tx.QueryRow(ctx, `with recent as (select
		(a.finished_at is null and a.lease_end>$1) live,a.reserved_at>$1::timestamptz-interval '1 minute' recent,
		a.issuer=$2 and a.tenant=$3 tenant_scope,r.spec->'Profile'->>'assetId'=$4 source_scope,a.provider=$5 dep
		from ouf_mcp.analysis_run_attempt a join ouf_mcp.analysis_run r using(analysis_id)
		where (a.reserved_at>$1::timestamptz-interval '1 minute' or (a.finished_at is null and a.lease_end>$1))
		  and ((a.issuer=$2 and a.tenant=$3) or a.provider=$5))
		select count(*) filter(where tenant_scope and live),count(*) filter(where tenant_scope and source_scope and live),
		count(*) filter(where dep and live),count(*) filter(where tenant_scope and recent),
		count(*) filter(where tenant_scope and source_scope and recent),count(*) filter(where dep and recent) from recent`,
		now, r.Issuer, r.Scope.Tenant, r.Profile.AssetID, provider).Scan(&tenantSlots, &sourceSlots, &depSlots, &tenantRate, &sourceRate, &depRate)
	if err != nil {
		return check, err
	}
	l := r.Limits.Shared
	if tenantSlots >= l.TenantSlots || sourceSlots >= l.SourceSlots || depSlots >= l.DependencySlots ||
		tenantRate >= l.TenantPerMinute || sourceRate >= l.SourcePerMinute || depRate >= l.DependencyPerMinute {
		return check, analysis.ErrRunPressure
	}
	// Conservative retained-payload bound: until installation archival,
	// count even bytes already reduced to a compact outcome. Unknown effects
	// keep their full ceiling. Tenant lock serializes all providers.
	var tenantBytes, sourceBytes int64
	err = tx.QueryRow(ctx, `select coalesce(sum(charged),0),coalesce(sum(charged) filter(where asset=$3),0) from
		(select coalesce((a.actual->>'Bytes')::bigint,a.bytes) charged,r.spec->'Profile'->>'assetId' asset
		from ouf_mcp.analysis_run_attempt a join ouf_mcp.analysis_run r using(analysis_id) where a.issuer=$1 and a.tenant=$2) q`,
		r.Issuer, r.Scope.Tenant, r.Profile.AssetID).Scan(&tenantBytes, &sourceBytes)
	if err != nil {
		return check, err
	}
	if tenantBytes+work.Maximum.Bytes > l.TenantBytes || sourceBytes+work.Maximum.Bytes > l.SourceBytes {
		return check, analysis.ErrRunPressure
	}
	var itemRaw []byte
	if err = tx.QueryRow(ctx, "select checkpoint->'Queue'->0 from ouf_mcp.analysis_run where analysis_id=$1", r.ID()).Scan(&itemRaw); err != nil {
		return check, err
	}
	var item analysis.RunItem
	if json.Unmarshal(itemRaw, &item) != nil {
		return check, analysis.ErrRunDrift
	}
	if now.Before(item.Task.NotBefore) {
		return check, analysis.ErrRunPressure
	}
	planned := item.Work(work.Worker)
	plannedHash, err := analysis.MandateHash(planned.Arguments)
	if err != nil || work.Intent != planned.Intent || hash != plannedHash || work.Operation != planned.Operation || !slices.Equal(work.Ordinals, planned.Ordinals) || work.Maximum != planned.Maximum {
		return check, analysis.ErrRunDrift
	}
	sessionMaximum, firstFence, err := runSession(ctx, tx, r)
	if err != nil {
		return check, err
	}
	var attemptCount int
	err = tx.QueryRow(ctx, "select count(*) from ouf_mcp.analysis_run_attempt where analysis_id=$1 and intent_key=$2 and operation=$3 and fence>$4", r.ID(), work.Intent, work.Operation, firstFence).Scan(&attemptCount)
	if err != nil {
		return check, err
	}
	// An expired lease can still represent an owner effect. Retain its
	// full charge; a new attempt is restricted to the identical operation.
	var uncertain, identical bool
	err = tx.QueryRow(ctx, `select count(*)>0,coalesce(bool_and(operation=$2 and intent_key=$3 and arguments_hash=$4
		and generation=$5 and mandate_id=$6),false) from ouf_mcp.analysis_run_attempt where analysis_id=$1 and finished_at is null and superseded_at is null
		and not exists(select 1 from ouf_mcp.analysis_group_quarantine q where q.analysis_id=$1 and q.intent_key=analysis_run_attempt.intent_key)`,
		r.ID(), work.Operation, work.Intent, hash, r.Mandate.Generation, r.Mandate.ID).Scan(&uncertain, &identical)
	if err != nil {
		return check, err
	}
	if uncertain {
		if !identical || (work.Operation == "semantic.discovery.request" && !r.Limits.OwnerDedupQualified) {
			if _, err = tx.Exec(ctx, "update ouf_mcp.analysis_run set state='BLOCKED',reason='ANALYSIS_EFFECT_UNCERTAIN',seq=seq+1 where analysis_id=$1", r.ID()); err != nil {
				return check, err
			}
			if err = tx.Commit(ctx); err != nil {
				return check, err
			}
			return check, analysis.ErrFenced
		}
	}
	if work.Operation == "semantic.discovery.status" || work.Operation == "semantic.discovery.candidates" {
		var owned bool
		if !analysis.CanonicalUUID(args.RequestID) {
			return check, analysis.ErrInvalid
		}
		if err = tx.QueryRow(ctx, `select exists(select 1 from ouf_mcp.analysis_run_request
			where analysis_id=$1 and request_id=$2 and intent_key=$3)`, r.ID(), args.RequestID, work.Intent).Scan(&owned); err != nil {
			return check, err
		}
		if !owned {
			return check, analysis.ErrScope
		}
	} else if args.RequestID != "" {
		return check, analysis.ErrInvalid
	}
	spent, err := runSpend(ctx, tx, r.ID(), firstFence)
	if err != nil {
		return check, err
	}
	maximum := work.Maximum
	total := spent.Add(maximum)
	if !total.Fits(sessionMaximum) || now.Add(duration).After(r.BudgetEnd) {
		return check, s.finishExhaustedReservation(ctx, tx, r, now)
	}
	if attemptCount >= analysis.MaxRetryAttempts {
		if err := quarantineRunWork(ctx, tx, r, item, "ANALYSIS_BUDGET_EXHAUSTED", now); err != nil {
			return check, err
		}
		if err := tx.Commit(ctx); err != nil {
			return check, err
		}
		return check, analysis.ErrRunExhausted
	}
	if uncertain {
		if _, err = tx.Exec(ctx, "update ouf_mcp.analysis_run_attempt set superseded_at=$2 where analysis_id=$1 and finished_at is null and superseded_at is null and intent_key=$3", r.ID(), now, work.Intent); err != nil {
			return check, err
		}
	}
	var fence int64
	err = tx.QueryRow(ctx, "update ouf_mcp.analysis_run set fence=fence+1,seq=seq+1,status_due=null,status_valid_until=null,status_token=null,reason=case when $2 then 'ANALYSIS_RECONCILING' else 'ANALYSIS_RUNNING' end where analysis_id=$1 returning fence", r.ID(), uncertain).Scan(&fence)
	if err != nil {
		return check, err
	}
	attempt := uuid.NewString()
	end := now.Add(duration + 15*time.Second)
	if r.Mandate.Expires.Before(end) {
		end = r.Mandate.Expires
	}
	if r.BudgetEnd.Before(end) {
		end = r.BudgetEnd
	}
	ownerHash := ""
	if args.IdempotencyKey != "" {
		ownerHash = analysis.BytesDigest([]byte(args.IdempotencyKey))
	}
	_, err = tx.Exec(ctx, `insert into ouf_mcp.analysis_run_attempt
		(attempt_id,analysis_id,fence,generation,mandate_id,intent_key,operation,arguments_hash,ordinals_ref,ordinals,request_id,
		provider,issuer,tenant,subject,worker,reserved_at,lease_end,calls,attempts,objects,bytes,millis,owner_key_hash)
		values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,nullif($11,'')::uuid,$12,$13,$14,$15,$16,$17,$18,1,1,$19,$20,$21,nullif($22,''))`,
		attempt, r.ID(), fence, r.Mandate.Generation, r.Mandate.ID, work.Intent, work.Operation, hash, analysis.Digest(work.Ordinals),
		work.Ordinals, args.RequestID, provider, r.Issuer, r.Scope.Tenant, r.Scope.Principal, work.Worker, now, end, maximum.Objects, maximum.Bytes, maximum.Millis, ownerHash)
	if err != nil {
		return check, err
	}
	check = analysis.ReservationCheck{Contract: analysis.ReservationContract, Workload: r.Workload,
		MandateID: r.Mandate.ID, AnalysisID: r.ID(), Generation: r.Mandate.Generation, IntentKey: work.Intent, AttemptID: attempt,
		Fence: strconv.FormatInt(fence, 10), Operation: work.Operation, ArgumentsHash: hash, RequestID: args.RequestID}
	return check, tx.Commit(ctx)
}

func (s *Store) CheckReservation(ctx context.Context, c analysis.ReservationCheck) (analysis.ReservationProof, error) {
	var p analysis.ReservationProof
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return p, err
	}
	defer tx.Rollback(ctx)
	// Every admission, cancellation and lease extension takes the run lock
	// first. Persist the proof bound before returning 200 to the Gateway.
	r, err := scanRun(tx.QueryRow(ctx, "select "+runColumns+` from ouf_mcp.analysis_run
		where analysis_id=$1 and spec->>'Workload'=$2 and mandate->>'mandateId'=$3 for update`, c.AnalysisID, c.Workload, c.MandateID))
	if err != nil {
		return p, err
	}
	var matches bool
	var request, ordinals string
	var end time.Time
	err = tx.QueryRow(ctx, `select r.status_due is null and not exists(select 1 from ouf_mcp.analysis_group_quarantine q
		where q.analysis_id=a.analysis_id and q.intent_key=a.intent_key) and a.intent_key=$3 and a.operation=$4 and a.arguments_hash=$5 and a.generation=$6
		and a.fence::text=$7 and a.fence=r.fence and a.finished_at is null
		and (a.request_id is null or exists(select 1 from ouf_mcp.analysis_run_request q
		where q.analysis_id=a.analysis_id and q.request_id=a.request_id and q.intent_key=a.intent_key)),
		coalesce(a.request_id::text,''),a.ordinals_ref,a.lease_end
		from ouf_mcp.analysis_run_attempt a join ouf_mcp.analysis_run r using(analysis_id)
		where a.attempt_id=$1 and a.analysis_id=$2 and a.mandate_id=$8`, c.AttemptID, c.AnalysisID, c.IntentKey, c.Operation, c.ArgumentsHash, c.Generation, c.Fence, c.MandateID).Scan(&matches, &request, &ordinals, &end)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && request != c.RequestID) {
		return p, analysis.ErrScope
	}
	if err != nil {
		return p, err
	}
	now, err := s.runClock(ctx, tx)
	if err != nil {
		return p, err
	}
	until := min(now.Add(time.Duration(r.Limits.ProofSeconds)*time.Second).Unix(), end.Unix(), r.BudgetEnd.Unix(), r.Mandate.Expires.Unix())
	if !matches || r.MandateState != "ACTIVE" || r.State != "RUNNING" || r.Mandate.Generation != c.Generation || !slices.Contains(r.Mandate.Operations, c.Operation) || until <= now.Unix() {
		return p, analysis.ErrFenced
	}
	_, err = tx.Exec(ctx, "update ouf_mcp.analysis_run_attempt set proof_until=greatest(proof_until,$2) where attempt_id=$1", c.AttemptID, time.Unix(until, 0))
	if err != nil {
		return p, err
	}
	raw, _ := json.Marshal(r.Profile)
	profileHash, err := analysis.MandateHash(raw)
	if err != nil {
		return p, err
	}
	p = analysis.ReservationProof{Current: true, Issuer: r.Issuer, Tenant: r.Scope.Tenant, Subject: r.Scope.Principal,
		ReservationRef: c.AttemptID, OrdinalsRef: ordinals, ChangeSeq: strconv.FormatInt(r.Seq, 10), ManifestChecksum: r.Manifest,
		ProfileHash: profileHash, ValidUntil: until}
	if request != "" {
		p.RequestID = &request
	}
	return p, tx.Commit(ctx)
}
