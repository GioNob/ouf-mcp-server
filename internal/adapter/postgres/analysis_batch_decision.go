package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/jackc/pgx/v5"
)

func (s *Store) DecideBatch(ctx context.Context, scope analysis.Scope, checksum string, authorized analysis.BatchStart, command analysis.BatchDecide) (analysis.BatchReceipt, error) {
	var result analysis.BatchReceipt
	if err := command.Normalize(); err != nil || analysis.ValidateID(command.InputFingerprint) != nil {
		return result, analysis.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	// A key belongs to the caller's scope, including across analyses. Take
	// this lock first so even concurrent cross-analysis reuse has one answer.
	lock := analysis.Digest([]any{"ouf.analysis.batch.decide.v1", scope, command.CommandKey})
	if _, err = tx.Exec(ctx, `select pg_advisory_xact_lock(hashtextextended($1,0))`, lock); err != nil {
		return result, err
	}
	if err = lockBatchAnalysis(ctx, tx, scope, command.AnalysisID); err != nil {
		return result, err
	}
	start, err := readBatchStart(ctx, tx, scope, command.AnalysisID, checksum)
	if err != nil {
		return result, err
	}
	if start.ID != authorized.ID || start.Epoch != authorized.Epoch || start.Authority != authorized.Authority || !start.Seeded {
		return result, analysis.ErrScope
	}
	var fingerprint string
	var raw []byte
	err = tx.QueryRow(ctx, `select fingerprint,receipt from ouf_mcp.analysis_batch_receipt
		where service_principal_id=$1 and principal_id=$2 and tenant_id=$3 and command_key=$4`,
		scope.Service, scope.Principal, scope.Tenant, command.CommandKey).Scan(&fingerprint, &raw)
	if err == nil {
		if fingerprint != command.InputFingerprint {
			return result, analysis.ErrKeyReuse
		}
		if err = json.Unmarshal(raw, &result); err != nil {
			return result, err
		}
		return result, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return result, err
	}
	var held bool
	err = tx.QueryRow(ctx, `select stopped or integrity_hold from ouf_mcp.analysis_counts
		where analysis_id=$1 order by seq desc limit 1`, start.ID).Scan(&held)
	if err != nil {
		return result, err
	}
	if held {
		return result, analysis.ErrScope
	}

	offered, err := batchDecisionMembership(ctx, tx, start, command)
	if err != nil {
		return result, err
	}
	result = analysis.BatchReceipt{AnalysisID: start.ID, BatchID: command.BatchID, Revision: start.Revision}
	plan := analysis.Plan{Scope: scope, Profile: start.Profile}
	var used map[int]int
	recorded := make([]batchRecordedDecision, 0, len(command.Decisions))
	for _, d := range command.Decisions {
		member := offered[d.Ordinal]
		if d.Rejection == "" && d.Action == "REFORMULATE" && used == nil {
			used, err = profileUsage(ctx, tx, plan)
			if err != nil {
				return result, err
			}
		}
		choice, reason, err := prepareBatchChoice(ctx, tx, plan, command, d, member, used[d.Ordinal])
		if err != nil {
			return result, err
		}
		item := analysis.BatchDecisionResult{Ordinal: d.Ordinal, Action: d.Action, Status: "REJECTED", ReasonCode: reason}
		if reason == "" {
			item.Status = "RECORDED"
			recorded = append(recorded, choice)
		}
		result.Decisions = append(result.Decisions, item)
	}
	if len(recorded) > 0 {
		result.Revision++
	}
	raw, err = json.Marshal(result)
	if err != nil {
		return result, err
	}
	_, err = tx.Exec(ctx, `insert into ouf_mcp.analysis_batch_receipt
		(service_principal_id,principal_id,tenant_id,command_key,analysis_id,batch_id,fingerprint,receipt)
		values($1,$2,$3,$4,$5,$6,$7,$8)`, scope.Service, scope.Principal, scope.Tenant, command.CommandKey,
		start.ID, command.BatchID, command.InputFingerprint, raw)
	if err != nil {
		return result, err
	}
	var reformulated []int
	for _, choice := range recorded {
		if err = saveBatchChoice(ctx, tx, command, choice); err != nil {
			return result, err
		}
		if choice.Action == "REFORMULATE" {
			reformulated = append(reformulated, choice.Ordinal)
		}
	}
	if len(reformulated) > 0 {
		if _, err = tx.Exec(ctx, reopenBatchDiscoverySQL, start.ID, reformulated); err != nil {
			return result, err
		}
	}
	if len(recorded) > 0 {
		tag, err := tx.Exec(ctx, `update ouf_mcp.profile_analysis set revision=$2 where analysis_id=$1 and revision=$3`,
			start.ID, result.Revision, start.Revision)
		if err != nil {
			return result, err
		}
		if tag.RowsAffected() != 1 {
			return result, analysis.ErrConflict
		}
	}
	// An all-rejected command also consumes the offer and key, without
	// advancing the decision revision. A corrected choice gets a new offer.
	if _, err = tx.Exec(ctx, `update ouf_mcp.analysis_batch_offer set retired_at=transaction_timestamp()
		where analysis_id=$1 and batch_id=$2 and retired_at is null`, start.ID, command.BatchID); err != nil {
		return result, err
	}
	if _, err = tx.Exec(ctx, `update ouf_mcp.analysis_batch_column set offered_batch_id=null
		where analysis_id=$1 and offered_batch_id=$2 and state='READY_FOR_DECISION'`, start.ID, command.BatchID); err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

func batchDecisionMembership(ctx context.Context, tx pgx.Tx, start analysis.BatchStart, command analysis.BatchDecide) (map[int]analysis.BatchOfferItem, error) {
	var raw []byte
	var current bool
	var revision int64
	err := tx.QueryRow(ctx, `select revision,retired_at is null,items from ouf_mcp.analysis_batch_offer
		where analysis_id=$1 and batch_id=$2`, start.ID, command.BatchID).Scan(&revision, &current, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, analysis.ErrScope
	}
	if err != nil {
		return nil, err
	}
	var items []analysis.BatchOfferItem
	if json.Unmarshal(raw, &items) != nil {
		return nil, analysis.ErrProfile
	}
	members := make(map[int]analysis.BatchOfferItem, len(items))
	for _, item := range items {
		members[item.Item.Ordinal] = item
	}
	for _, d := range command.Decisions {
		if _, ok := members[d.Ordinal]; !ok {
			return nil, analysis.ErrScope
		}
	}
	if !current || revision != command.ExpectedRevision || start.Revision != command.ExpectedRevision {
		return nil, analysis.ErrConflict
	}
	return members, nil
}

type batchRecordedDecision struct {
	analysis.BatchDecision
	attempt, round    int
	key, stage, state string
	arguments         []byte
}

func prepareBatchChoice(ctx context.Context, tx pgx.Tx, plan analysis.Plan, cmd analysis.BatchDecide, d analysis.BatchDecision, member analysis.BatchOfferItem, used int) (batchRecordedDecision, string, error) {
	var choice batchRecordedDecision
	if d.Rejection != "" {
		return choice, d.Rejection, nil
	}
	var intent string
	var candidateFound bool
	err := tx.QueryRow(ctx, `select b.attempt,b.round,b.intent,
		exists(select from jsonb_array_elements(b.candidates) c where c->>'candidateId'=$5
		  and not ($5=any(b.refused_candidates)))
		from ouf_mcp.analysis_batch_column b join ouf_mcp.analysis_column c using(analysis_id,ordinal)
		where b.analysis_id=$1 and b.ordinal=$2 and b.offered_batch_id=$3 and b.state='READY_FOR_DECISION'
		and c.request_id=$4 and b.evidence_version=$6 and b.attempt=$7`,
		cmd.AnalysisID, d.Ordinal, cmd.BatchID, member.Item.RequestID, d.CandidateID, member.EvidenceVersion, member.Item.Attempt).
		Scan(&choice.attempt, &choice.round, &intent, &candidateFound)
	if errors.Is(err, pgx.ErrNoRows) {
		return choice, "", analysis.ErrConflict
	}
	if err != nil {
		return choice, "", err
	}
	choice.BatchDecision = d
	choice.RequestID = member.Item.RequestID
	choice.stage, choice.state = d.Action, "PENDING"
	choice.key = cmd.EffectKey(plan.Scope, d)
	switch d.Action {
	case "ADOPT":
		if d.RequestID != member.Item.RequestID || !candidateFound {
			return choice, "ANALYSIS_CANDIDATE_NOT_OFFERED", nil
		}
		choice.arguments, err = json.Marshal(map[string]string{"requestId": d.RequestID,
			"candidateId": d.CandidateID, "commandKey": choice.key})
	case "REFORMULATE":
		used = max(used, choice.attempt-1)
		if used >= analysis.MaxColumnReformulations || choice.round > analysis.MaxColumnReformulations {
			return choice, analysis.ErrReformulationExhausted.Error(), nil
		}
		if strings.EqualFold(d.Intent, analysis.NormalizeBatchIntent(intent)) {
			return choice, "REFORMULATION_INTENT_UNCHANGED", nil
		}
		var allowed bool
		allowed, err = takeReformulation(ctx, tx, plan, d.Ordinal, used, analysis.MaxColumnReformulations)
		if err == nil && !allowed {
			return choice, analysis.ErrReformulationExhausted.Error(), nil
		}
		choice.attempt, choice.round = used+2, choice.round+1
	case "EXCLUDE":
		choice.key, choice.state, choice.stage = "", "APPLIED", ""
	case "PROPOSE":
		choice.arguments = d.Proposal
		if d.RevisionID != "" {
			var key string
			key, err = ownedBatchDraftKey(ctx, tx, plan.Scope, d.RevisionID)
			if errors.Is(err, pgx.ErrNoRows) {
				return choice, "SEMANTIC_DRAFT_UNVERIFIED", nil
			}
			if err == nil {
				choice.stage = "DRAFT"
				choice.arguments, err = json.Marshal(map[string]string{"commandKey": key})
			}
		}
	default:
		return choice, "", analysis.ErrInvalid
	}
	return choice, "", err
}

func saveBatchChoice(ctx context.Context, tx pgx.Tx, cmd analysis.BatchDecide, d batchRecordedDecision) error {
	_, err := tx.Exec(ctx, `insert into ouf_mcp.analysis_batch_choice
		(analysis_id,command_key,ordinal,action,attempt,request_id,candidate_id,effect_key,effect_arguments,intent,reason,effect_state,requested_revision_id)
		values($1,$2,$3,$4,$5,$6,nullif($7,'')::uuid,nullif($8,'')::uuid,$9,nullif($10,''),nullif($11,''),$12,nullif($13,'')::uuid)`,
		cmd.AnalysisID, cmd.CommandKey, d.Ordinal, d.Action, d.attempt, d.RequestID, d.CandidateID, d.key, d.arguments, d.Intent,
		d.Reason, d.state, d.RevisionID)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `update ouf_mcp.analysis_batch_column set choice_key=$2,offered_batch_id=null,
		state=case when $4='' then 'CLOSED' else 'PENDING' end,
		work_stage=coalesce(nullif($4,''),work_stage),due_at=case when $4<>'' then transaction_timestamp() end,
		fence=null,dispatch_fence=null,retry_attempts=0,retry_started_at=null,
		attempt=$5,round=$6,intent=coalesce(nullif($7,''),intent),decision_reason=null,
		completed_count=case when $4<>'REFORMULATE' then completed_count end,
		owner_deadline=null,refused_candidates=case when $4='REFORMULATE' then '{}' else refused_candidates end,
		candidates=case when $4='REFORMULATE' then '[]' else candidates end,
		evidence_version=case when $4='REFORMULATE' then '' else evidence_version end
		where analysis_id=$1 and ordinal=$3 and state='READY_FOR_DECISION' and offered_batch_id=$8`,
		cmd.AnalysisID, cmd.CommandKey, d.Ordinal, d.stage, d.attempt, d.round, d.Intent, cmd.BatchID)
	if err == nil && tag.RowsAffected() != 1 {
		return analysis.ErrConflict
	}
	return err
}

func ownedBatchDraftKey(ctx context.Context, tx pgx.Tx, scope analysis.Scope, revision string) (string, error) {
	var key string
	err := tx.QueryRow(ctx, `select effect_key::text from ouf_mcp.analysis_batch_choice b
		join ouf_mcp.profile_analysis p using(analysis_id)
		where p.service_principal_id=$1 and p.principal_id=$2 and p.tenant_id=$3 and b.owner_ref=$4
		and b.action='PROPOSE' and b.effect_state='APPLIED' and b.effect_arguments is not null
		and b.effect_code='CREATED' order by effect_key limit 1`, scope.Service, scope.Principal, scope.Tenant, revision).Scan(&key)
	if !errors.Is(err, pgx.ErrNoRows) {
		return key, err
	}
	err = tx.QueryRow(ctx, `select idempotency_key from ouf_mcp.tool_attempt
		where service_principal_id=$1 and principal_id=$2 and tenant_id=$3 and replay_result_id=$4
		and capability_id='ouf.semantic.propose' and state='SUCCEEDED' order by attempt_id limit 1`,
		scope.Service, scope.Principal, scope.Tenant, revision).Scan(&key)
	return key, err
}

const reopenBatchDiscoverySQL = `with head as materialized (
	select * from ouf_mcp.analysis_counts where analysis_id=$1 order by seq desc limit 1
), old as materialized (
	select ordinal,outcome from ouf_mcp.analysis_column where analysis_id=$1 and ordinal=any($2::int[])
), changed as (
	update ouf_mcp.analysis_column c set request_id=null,outcome='{"state":"PENDING"}'
	from old where c.analysis_id=$1 and c.ordinal=old.ordinal returning c.ordinal,c.outcome
), versions as (
	insert into ouf_mcp.analysis_column_version(analysis_id,ordinal,seq,outcome)
	select $1,c.ordinal,h.seq+1,c.outcome from changed c cross join head h
), delta as (
	select count(*) n,count(*) filter(where outcome->>'state'='CANDIDATES_READY') ready,
		count(*) filter(where outcome->>'state'='NO_CANDIDATES') zero_count from old
) insert into ouf_mcp.analysis_counts(analysis_id,seq,total,matched,ready,zero_count,blocked,pending,onboarding,stopped,integrity_hold)
	select $1,h.seq+1,h.total,h.matched,h.ready-d.ready,h.zero_count-d.zero_count,h.blocked,h.pending+d.n,h.onboarding,h.stopped,h.integrity_hold
	from head h cross join delta d`
