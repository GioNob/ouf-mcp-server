package postgres

import (
	"context"
	"encoding/json"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/jackc/pgx/v5"
)

// Batch rows share the summary sequence and its MVCC snapshots. The discovery
// counters remain the same when a decision or a publication advances it.
func appendBatchSummary(ctx context.Context, tx pgx.Tx, id string, changes []analysis.BatchSummaryChange) error {
	if len(changes) == 0 {
		return nil
	}
	raw, err := json.Marshal(changes)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `with head as (
		select * from ouf_mcp.analysis_counts where analysis_id=$1 order by seq desc limit 1
	), advanced as (
		insert into ouf_mcp.analysis_counts
		(analysis_id,seq,total,matched,ready,zero_count,blocked,pending,onboarding,stopped,integrity_hold)
		select analysis_id,seq+1,total,matched,ready,zero_count,blocked,pending,onboarding,stopped,integrity_hold from head
		returning seq
	)
	insert into ouf_mcp.analysis_column_version(analysis_id,ordinal,seq,outcome,batch_outcome)
	select $1,r.ordinal,a.seq,c.outcome,r.fields from advanced a
	cross join jsonb_to_recordset($2::jsonb) as r(ordinal int, fields jsonb)
	join ouf_mcp.analysis_column c on c.analysis_id=$1 and c.ordinal=r.ordinal`, id, raw)
	return err
}

func writeBatchChoiceVersion(ctx context.Context, tx pgx.Tx, job analysis.BatchWork, r analysis.BatchWorkResult) error {
	if r.EffectState == "PENDING" {
		return nil
	}
	var action, key, request, revision, semanticID string
	err := tx.QueryRow(ctx, `select d.action,d.command_key::text,d.request_id::text,coalesce(d.owner_ref::text,''),coalesce(d.semantic_id,'')
		from ouf_mcp.analysis_batch_column b join ouf_mcp.analysis_batch_choice d
		on d.analysis_id=b.analysis_id and d.command_key=b.choice_key and d.ordinal=b.ordinal
		where b.analysis_id=$1 and b.ordinal=$2`, job.AnalysisID, r.Ordinal).Scan(&action, &key, &request, &revision, &semanticID)
	if err != nil {
		return err
	}
	state := "PROPOSED"
	if action == "ADOPT" {
		state = "ADOPTED"
	}
	switch r.EffectState {
	case "HELD":
		state = "BLOCKED"
	case "REFUSED":
		state = "CANDIDATES_READY"
	}
	fields := map[string]any{"state": state, "action": action, "requestId": request, "reasonCode": r.EffectCode, "receiptRef": key}
	if revision != "" {
		fields["revisionId"] = revision
	}
	if semanticID != "" {
		fields["semanticId"] = semanticID
	}
	if r.ReviewBatchID != "" {
		fields["reviewBatchId"], fields["publicationState"] = r.ReviewBatchID, "COLLECTING"
	}
	if r.PublicationSetID != "" {
		fields["publicationSetId"], fields["publicationState"] = r.PublicationSetID, "PUBLISHED"
	}
	return appendBatchSummary(ctx, tx, job.AnalysisID, []analysis.BatchSummaryChange{{Ordinal: r.Ordinal, Fields: fields}})
}

func batchSinglePublication(ctx context.Context, tx pgx.Tx, id string, item analysis.BatchReviewItem) ([]analysis.BatchSummaryChange, error) {
	if item.State != "PUBLISHED" {
		return nil, analysis.ErrInvalid
	}
	rows, err := tx.Query(ctx, `update ouf_mcp.analysis_batch_choice d
		set effect_code='SEMANTIC_SINGLE_REVIEW_PUBLISHED'
		from ouf_mcp.analysis_batch_column b
		where b.analysis_id=$1 and b.ordinal=d.ordinal and b.choice_key=d.command_key and d.analysis_id=b.analysis_id
		and d.owner_ref=$2 and d.effect_code='SEM_REVIEW_BATCH_ITEM_TYPE_UNSUPPORTED' and b.state='CLOSED'
		returning d.ordinal,d.action,d.request_id::text,d.command_key::text,coalesce(d.semantic_id,'')`, id, item.RevisionID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (analysis.BatchSummaryChange, error) {
		var change analysis.BatchSummaryChange
		var action, request, key, semanticID string
		err := row.Scan(&change.Ordinal, &action, &request, &key, &semanticID)
		state := "PROPOSED"
		if action == "ADOPT" {
			state = "ADOPTED"
		}
		change.Fields = map[string]any{"state": state, "action": action, "requestId": request, "receiptRef": key, "revisionId": item.RevisionID, "publicationState": "PUBLISHED"}
		if semanticID != "" {
			change.Fields["semanticId"] = semanticID
		}
		if analysis.CanonicalUUID(item.PublicationSetID) {
			change.Fields["publicationSetId"] = item.PublicationSetID
		}
		return change, err
	})
}

func (s *Store) ObserveBatchReview(ctx context.Context, scope analysis.Scope, id, batch, version string, items []analysis.BatchReviewItem) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockBatchAnalysis(ctx, tx, scope, id); err != nil {
		return err
	}
	var changes []analysis.BatchSummaryChange
	for _, item := range items {
		if batch == "" {
			single, err := batchSinglePublication(ctx, tx, id, item)
			if err != nil {
				return err
			}
			changes = append(changes, single...)
			continue
		}
		rows, err := tx.Query(ctx, `with saved as (
			insert into ouf_mcp.analysis_batch_review_item as r
				(analysis_id,revision_id,review_batch_id,version,state,publication_set_id)
			select $1,$2,$3,$4,$5,nullif($6,'')::uuid
			where exists(select 1 from ouf_mcp.analysis_batch_choice where analysis_id=$1 and owner_ref=$2)
			on conflict (analysis_id,revision_id,review_batch_id) do update
			set version=excluded.version,state=excluded.state,publication_set_id=excluded.publication_set_id
			where (r.state not in ('PUBLISHED','CHANGES_REQUESTED','REJECTED','EXPIRED','REMOVED')
				or ($7 and r.state='REMOVED' and excluded.state='COLLECTING'))
			and r.state is distinct from excluded.state
			and not(r.state='UNDER_REVIEW' and excluded.state='COLLECTING')
			returning revision_id
		)
		select b.ordinal,d.action,d.command_key::text,d.request_id::text,
			coalesce(nullif($8,''),d.semantic_id,''),b.state='READY_FOR_DECISION',coalesce(b.offered_batch_id::text,'')
		from saved join ouf_mcp.analysis_batch_choice d on d.analysis_id=$1 and d.owner_ref=saved.revision_id
		join ouf_mcp.analysis_batch_column b on b.analysis_id=$1 and b.ordinal=d.ordinal and b.choice_key=d.command_key
		where (b.state='CLOSED' or ($7 and b.state='READY_FOR_DECISION' and b.decision_reason='SEMANTIC_REVIEW_REMOVED'))
		and d.effect_state='APPLIED'
		and not exists(select 1 from ouf_mcp.analysis_batch_review_item r
			where r.analysis_id=$1 and r.revision_id=saved.revision_id and r.state='PUBLISHED' and r.review_batch_id<>$3)`,
			id, item.RevisionID, batch, version, item.State, item.PublicationSetID, item.ConfirmedAdd, item.SemanticID)
		if err != nil {
			return err
		}
		type target struct {
			Ordinal                                   int
			Action, Key, Request, SemanticID, OfferID string
			Restore                                   bool
		}
		targets, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (target, error) {
			var t target
			err := r.Scan(&t.Ordinal, &t.Action, &t.Key, &t.Request, &t.SemanticID, &t.Restore, &t.OfferID)
			return t, err
		})
		if err != nil {
			return err
		}
		for _, t := range targets {
			if t.Restore {
				if err := restoreBatchReviewChoice(ctx, tx, id, t.Ordinal, t.OfferID); err != nil {
					return err
				}
			}
			state := "PROPOSED"
			if t.Action == "ADOPT" {
				state = "ADOPTED"
			}
			fields := map[string]any{"state": state, "action": t.Action, "requestId": t.Request, "receiptRef": t.Key,
				"revisionId": item.RevisionID, "reviewBatchId": batch, "publicationState": item.State}
			if t.SemanticID != "" {
				fields["semanticId"] = t.SemanticID
			}
			if item.PublicationSetID != "" {
				fields["publicationSetId"] = item.PublicationSetID
			}
			if item.State == "REMOVED" {
				delete(fields, "publicationState")
				fields["reasonCode"] = "SEMANTIC_REVIEW_REMOVED"
			}
			if item.State == "CHANGES_REQUESTED" || item.State == "REJECTED" || item.State == "REMOVED" {
				reason := "SEMANTIC_REVIEW_" + item.State
				_, err = tx.Exec(ctx, `update ouf_mcp.analysis_batch_column b
					set state='READY_FOR_DECISION',decision_reason=$3,
					refused_candidates=case when d.candidate_id is null then b.refused_candidates
					else array_append(b.refused_candidates,d.candidate_id::text) end
					from ouf_mcp.analysis_batch_choice d
					where b.analysis_id=$1 and b.ordinal=$2 and d.analysis_id=b.analysis_id
					and d.ordinal=b.ordinal and d.command_key=b.choice_key`, id, t.Ordinal, reason)
				if err != nil {
					return err
				}
			}
			changes = append(changes, analysis.BatchSummaryChange{Ordinal: t.Ordinal, Fields: fields})
		}
	}
	if batch != "" && len(items) > 0 && items[0].State != "COLLECTING" && items[0].State != "REMOVED" {
		// The rejected ADD had no owner effect. Once the full lot leaves
		// collection, it can use the same key and the remaining retry budget.
		_, err = tx.Exec(ctx, `with admitted as (
			update ouf_mcp.analysis_batch_choice set review_blocked_by=null
			where analysis_id=$1 and review_blocked_by=$2 returning ordinal,command_key
		) update ouf_mcp.analysis_batch_column b set state='PENDING',due_at=clock_timestamp()
		from admitted d where b.analysis_id=$1 and b.ordinal=d.ordinal and b.choice_key=d.command_key and b.state='DECIDED'`, id, batch)
		if err != nil {
			return err
		}
	}
	if err := appendBatchSummary(ctx, tx, id, changes); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func restoreBatchReviewChoice(ctx context.Context, tx pgx.Tx, id string, ordinal int, offer string) error {
	_, err := tx.Exec(ctx, `update ouf_mcp.analysis_batch_column b
		set state='CLOSED',decision_reason=null,offered_batch_id=null,
			refused_candidates=array_remove(b.refused_candidates,d.candidate_id::text)
		from ouf_mcp.analysis_batch_choice d
		where b.analysis_id=$1 and b.ordinal=$2 and d.analysis_id=b.analysis_id
		and d.ordinal=b.ordinal and d.command_key=b.choice_key`, id, ordinal)
	if err != nil || offer == "" {
		return err
	}
	_, err = tx.Exec(ctx, `with retired as (
		update ouf_mcp.analysis_batch_offer set retired_at=transaction_timestamp()
		where analysis_id=$1 and batch_id=$2 and retired_at is null returning batch_id
	) update ouf_mcp.analysis_batch_column b set offered_batch_id=null from retired r
	where b.analysis_id=$1 and b.offered_batch_id=r.batch_id and b.state='READY_FOR_DECISION'`, id, offer)
	return err
}
