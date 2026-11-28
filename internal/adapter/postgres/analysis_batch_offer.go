package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const batchActiveOfferSQL = `select batch_id::text,revision,items
	from ouf_mcp.analysis_batch_offer where analysis_id=$1 and retired_at is null limit 1`

const batchAvailableCandidates = `cross join lateral (
	select case when cardinality(b.refused_candidates)=0 then b.candidates else (
		select coalesce(jsonb_agg(candidate order by n),'[]')
		from jsonb_array_elements(b.candidates) with ordinality as i(candidate,n)
		where not ((candidate->>'candidateId')=any(b.refused_candidates))
	) end as items
) available`

const batchReadyColumnsSQL = `select b.ordinal,c.header,b.metadata,b.attempt,b.intent,c.request_id::text,c.outcome->>'requestId',
	(c.outcome->>'candidateCount')::int,c.outcome->>'state',
	jsonb_path_query_array(available.items,'$[0 to 7]'),jsonb_array_length(b.candidates),
	jsonb_array_length(available.items),b.evidence_version,b.round,coalesce(b.decision_reason,''),coalesce(budget.used,0)
	from (
		select ordinal,metadata,attempt,intent,candidates,evidence_version,round,decision_reason,refused_candidates
		from ouf_mcp.analysis_batch_column
		where analysis_id=$1 and state='READY_FOR_DECISION' and offered_batch_id is null
		order by round,ordinal limit 8
	) b cross join lateral (
		select header,request_id,outcome from ouf_mcp.analysis_column
		where analysis_id=$1 and ordinal=b.ordinal limit 1
	) c
	` + batchAvailableCandidates + `
	join ouf_mcp.profile_analysis p on p.analysis_id=$1
	left join lateral (
		select used from ouf_mcp.analysis_reformulation_budget
		where (service_principal_id,principal_id,tenant_id,asset_id,profile_id,ordinal)=
			(p.service_principal_id,p.principal_id,p.tenant_id,(p.profile->>'assetId')::uuid,(p.profile->>'profileId')::uuid,b.ordinal)
		limit 1
	) budget on true
	order by b.round,b.ordinal`

const batchCountsSQL = `select total,matched,ready,zero_count,blocked,pending,stopped,integrity_hold
	from ouf_mcp.analysis_counts where analysis_id=$1 order by seq desc limit 1`

func (s *Store) ReadBatch(ctx context.Context, scope analysis.Scope, id, checksum string, pack func(analysis.BatchOffer) ([]analysis.BatchOfferItem, error)) (analysis.BatchRead, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return analysis.BatchRead{}, err
	}
	defer tx.Rollback(ctx)
	// Serialize publication of an offer with other readers and the
	// decision transaction. Keep authorization and owner calls outside the lock.
	if pack != nil {
		if err = lockBatchAnalysis(ctx, tx, scope, id); err != nil {
			return analysis.BatchRead{}, err
		}
	}
	out, err := readBatch(ctx, tx, scope, id, checksum, pack)
	if err == nil {
		err = tx.Commit(ctx)
	}
	return out, err
}

func readBatch(ctx context.Context, tx pgx.Tx, scope analysis.Scope, id, checksum string, pack func(analysis.BatchOffer) ([]analysis.BatchOfferItem, error)) (analysis.BatchRead, error) {
	start, err := readBatchStart(ctx, tx, scope, id, checksum)
	if err != nil {
		return analysis.BatchRead{}, err
	}
	out := analysis.BatchRead{BatchStart: start, Offer: analysis.BatchOffer{AnalysisID: id}}
	if !start.Initialized {
		out.Counts = analysis.Counts{Total: start.Profile.ColumnCount, Pending: start.Profile.ColumnCount}
		out.Phase, out.Reason = "BLOCKED", "ANALYSIS_BATCH_NOT_AVAILABLE"
		return out, nil
	}
	var stopped, integrity bool
	counts := &out.Counts
	err = tx.QueryRow(ctx, batchCountsSQL, id).
		Scan(&counts.Total, &counts.Matched, &counts.Ready, &counts.Zero, &counts.Blocked, &counts.Pending, &stopped, &integrity)
	if err != nil {
		return out, err
	}
	if counts.Total != start.Profile.ColumnCount {
		return out, analysis.ErrProfile
	}
	if stopped || integrity {
		out.Phase, out.Reason = "BLOCKED", "ANALYSIS_HELD"
		return out, nil
	}
	if pack != nil {
		if err = readOrCreateBatchOffer(ctx, tx, &out.Offer, start.Revision, pack); err != nil {
			return out, err
		}
	}
	if out.Offer.ID != "" {
		out.Phase, out.Reason = "RUNNING", analysis.DiscoveryDecisionRequired
		return out, nil
	}
	var ready, working bool
	err = tx.QueryRow(ctx, `select exists(select from ouf_mcp.analysis_batch_column
		where analysis_id=$1 and state='READY_FOR_DECISION' limit 1),
		exists(select from ouf_mcp.analysis_batch_column where analysis_id=$1 and state='PENDING' limit 1)
		or exists(select from ouf_mcp.analysis_batch_column where analysis_id=$1 and state='RUNNING' limit 1)
		or exists(select from ouf_mcp.analysis_batch_column where analysis_id=$1 and state='DECIDED' limit 1)`, id).Scan(&ready, &working)
	if err != nil {
		return out, err
	}
	var decisionHold string
	if !ready && !working {
		err = tx.QueryRow(ctx, `select reason_code from ouf_mcp.analysis_batch_hold
			where analysis_id=$1 and work_stage in ('ADOPT','PROPOSE','DRAFT','REFORMULATE')
			order by reason_code,ordinal limit 1`, id).Scan(&decisionHold)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return out, err
		}
		err = nil
	}
	switch {
	case ready:
		out.Phase, out.Reason = "RUNNING", "ANALYSIS_CONTINUE"
	case working:
		out.Phase, out.Reason = "RUNNING", "ANALYSIS_RUNNING"
	case decisionHold != "":
		out.Phase, out.Reason = "BLOCKED", decisionHold
	case counts.Blocked == counts.Total:
		out.Phase, out.Reason = "BLOCKED", "ANALYSIS_HELD"
		err = tx.QueryRow(ctx, `select outcome->>'reasonCode' from ouf_mcp.analysis_column
			where analysis_id=$1 and outcome->>'state'='BLOCKED' order by ordinal limit 1`, id).Scan(&out.Reason)
	case counts.Pending != 0:
		out.Phase, out.Reason = "BLOCKED", "ANALYSIS_HELD"
	case counts.Blocked != 0:
		out.Phase, out.Reason = "PARTIAL", "ANALYSIS_SUMMARY"
		var hold string
		err = tx.QueryRow(ctx, `select reason_code from ouf_mcp.analysis_batch_hold
			where analysis_id=$1 and reason_code in ('RETRY_BUDGET_EXHAUSTED','OUTCOME_REQUIRES_RECONCILIATION',
			'AUTHORIZATION_DENIED','ANALYSIS_AUTHORITY_UNAVAILABLE','ANALYSIS_POLICY_EPOCH_CHANGED','RETRY_WINDOW_UNAVAILABLE')
			order by reason_code,ordinal limit 1`, id).Scan(&hold)
		if err == nil {
			out.Phase, out.Reason = "BLOCKED", hold
		} else if errors.Is(err, pgx.ErrNoRows) {
			err = nil
		}
	default:
		out.Phase, out.Reason = "COMPLETE", "ANALYSIS_SUMMARY"
	}
	return out, err
}

func readOrCreateBatchOffer(ctx context.Context, tx pgx.Tx, offer *analysis.BatchOffer, revision int64, pack func(analysis.BatchOffer) ([]analysis.BatchOfferItem, error)) error {
	var raw []byte
	id := offer.AnalysisID
	err := tx.QueryRow(ctx, batchActiveOfferSQL, id).Scan(&offer.ID, &offer.Revision, &raw)
	if err == nil {
		if json.Unmarshal(raw, &offer.Items) != nil || len(offer.Items) == 0 || len(offer.Items) > analysis.BatchMaxItems {
			return analysis.ErrProfile
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	items, err := batchReadyColumns(ctx, tx, id)
	if len(items) == 0 || err != nil {
		return err
	}
	offer.ID, offer.Revision, offer.Items = uuid.NewString(), revision, items
	offer.Items, err = pack(*offer)
	if err != nil {
		return err
	}
	if len(offer.Items) == 0 || len(offer.Items) > len(items) {
		return analysis.ErrProfile
	}
	ordinals := make([]int, len(offer.Items))
	for n, item := range offer.Items {
		if item.Item.Ordinal != items[n].Item.Ordinal {
			return analysis.ErrProfile
		}
		ordinals[n] = item.Item.Ordinal
	}
	raw, err = json.Marshal(offer.Items)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `insert into ouf_mcp.analysis_batch_offer(analysis_id,batch_id,revision,items)
		values($1,$2,$3,$4)`, id, offer.ID, offer.Revision, raw); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `update ouf_mcp.analysis_batch_column set offered_batch_id=$2
		where analysis_id=$1 and ordinal=any($3::int[]) and state='READY_FOR_DECISION' and offered_batch_id is null`, id, offer.ID, ordinals)
	if err == nil && tag.RowsAffected() != int64(len(ordinals)) {
		err = analysis.ErrConflict
	}
	return err
}

func batchReadyColumns(ctx context.Context, tx pgx.Tx, id string) ([]analysis.BatchOfferItem, error) {
	rows, err := tx.Query(ctx, batchReadyColumnsSQL, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]analysis.BatchOfferItem, 0, analysis.BatchMaxItems)
	firstRound := 0
	for rows.Next() {
		var item analysis.BatchOfferItem
		var metadata, candidates []byte
		var request, evidenceRequest *string
		var count *int
		var state, decisionReason string
		var round, cached, available, used int
		col := &item.Item
		if err = rows.Scan(&col.Ordinal, &col.Name, &metadata, &col.Attempt, &col.Intent, &request, &evidenceRequest,
			&count, &state, &candidates, &cached, &available, &item.EvidenceVersion, &round, &decisionReason, &used); err != nil {
			return nil, err
		}
		if len(items) == 0 {
			firstRound = round
		} else if round != firstRound {
			break
		}
		if request == nil || evidenceRequest == nil || *request != *evidenceRequest ||
			count == nil || *count != cached || item.EvidenceVersion == "" ||
			json.Unmarshal(metadata, &col.BatchColumnMetadata) != nil ||
			json.Unmarshal(candidates, &col.Candidates) != nil ||
			!((state == "NO_CANDIDATES" && *count == 0) || (state == "CANDIDATES_READY" && *count > 0)) {
			return nil, analysis.ErrProfile
		}
		col.RequestID, col.CandidateCount = *request, available
		if !col.Valid() {
			return nil, analysis.ErrProfile
		}
		col.RemainingAttempts = max(0, analysis.MaxColumnReformulations-max(used, col.Attempt-1))
		if col.CandidateCount > 0 {
			col.Allowed = append(col.Allowed, "ADOPT")
		} else {
			col.ReasonCode = "DISCOVERY_NO_CANDIDATES"
		}
		if col.RemainingAttempts > 0 {
			col.Allowed = append(col.Allowed, "REFORMULATE")
		} else {
			col.ReasonCode = "REFORMULATION_BUDGET_EXHAUSTED"
		}
		if decisionReason != "" {
			col.ReasonCode = decisionReason
		}
		col.Allowed = append(col.Allowed, "EXCLUDE", "PROPOSE")
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) ReadBatchColumn(ctx context.Context, scope analysis.Scope, id, checksum, batchID string, ordinal, offset int) (analysis.BatchOffer, analysis.BatchOfferItem, error) {
	var item analysis.BatchOfferItem
	out := analysis.BatchOffer{AnalysisID: id}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return out, item, err
	}
	defer tx.Rollback(ctx)
	start, err := readBatchStart(ctx, tx, scope, id, checksum)
	if err != nil {
		return out, item, err
	}
	if !start.Initialized {
		return out, item, analysis.ErrScope
	}
	var raw []byte
	var held bool
	err = tx.QueryRow(ctx, `select stopped or integrity_hold from ouf_mcp.analysis_counts
		where analysis_id=$1 order by seq desc limit 1`, id).Scan(&held)
	if err != nil {
		return out, item, err
	}
	if held {
		return out, item, analysis.ErrScope
	}
	err = tx.QueryRow(ctx, batchActiveOfferSQL, id).Scan(&out.ID, &out.Revision, &raw)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && out.ID != batchID) {
		return out, item, analysis.ErrScope
	}
	if err != nil {
		return out, item, err
	}
	if json.Unmarshal(raw, &out.Items) != nil {
		return out, item, analysis.ErrProfile
	}
	for _, offered := range out.Items {
		if offered.Item.Ordinal == ordinal {
			item = offered
			break
		}
	}
	if item.Item.RequestID == "" {
		return out, item, analysis.ErrScope
	}
	var metadata, candidates []byte
	col := &item.Item
	err = tx.QueryRow(ctx, `select c.header,b.intent,b.metadata,
		jsonb_path_query_array(available.items,'$[$start to $end]',jsonb_build_object('start',$6::int,'end',$6::int+8))
		from ouf_mcp.analysis_batch_column b join ouf_mcp.analysis_column c using(analysis_id,ordinal)
		`+batchAvailableCandidates+`
		where b.analysis_id=$1 and b.ordinal=$2 and b.offered_batch_id=$3 and b.state='READY_FOR_DECISION'
		and c.request_id=$4 and b.evidence_version=$5 and b.attempt=$7 limit 1`,
		id, ordinal, batchID, col.RequestID, item.EvidenceVersion, offset, col.Attempt).
		Scan(&col.Name, &col.Intent, &metadata, &candidates)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, item, analysis.ErrScope
	}
	if err != nil {
		return out, item, err
	}
	if json.Unmarshal(metadata, &col.BatchColumnMetadata) != nil || json.Unmarshal(candidates, &col.Candidates) != nil {
		return out, item, analysis.ErrProfile
	}
	return out, item, tx.Commit(ctx)
}
