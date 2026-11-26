package postgres

import (
	"context"
	"errors"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Store) DecisionKeys(ctx context.Context, scope analysis.Scope, id, checksum, request string) (string, string, error) {
	var proposal, exclusion string
	err := s.pool.QueryRow(ctx, `select c.proposal_choice::text,c.exclusion_choice::text
		from ouf_mcp.analysis_column c join ouf_mcp.profile_analysis p using(analysis_id)
		where p.analysis_id=$1 and p.service_principal_id=$2 and p.principal_id=$3 and p.tenant_id=$4
		and p.manifest_checksum=any($5::char(64)[]) and c.request_id=$6 and c.empty_candidate_page
		and c.decision_attempt is null and c.proposal_choice is not null order by c.ordinal limit 1`,
		id, scope.Service, scope.Principal, scope.Tenant, manifest.AnalysisManifests(checksum), request).Scan(&proposal, &exclusion)
	if errors.Is(err, pgx.ErrNoRows) {
		// Legacy evidence may be unbound. It cannot close an analysis column.
		return uuid.NewString(), uuid.NewString(), nil
	}
	return proposal, exclusion, err
}

// Runs inside the successful command's reconciliation transaction. Serialize
// with replans and recheck the current choice after acquiring the header lock.
func finishColumnDecision(ctx context.Context, tx pgx.Tx, attempt uuid.UUID, d orchestration.ColumnDecision) error {
	key := "exclusion_choice"
	if d.Proposal {
		key = "proposal_choice"
	}
	var id string
	err := tx.QueryRow(ctx, `select p.analysis_id from ouf_mcp.analysis_column c
		join ouf_mcp.profile_analysis p using(analysis_id) join ouf_mcp.tool_attempt a
		using(service_principal_id,principal_id,tenant_id) where a.attempt_id=$1 and c.`+key+`=$2 and c.decision_attempt is null
		and a.state='SUCCEEDED' and a.idempotency_key=c.`+key+`::text
		and a.capability_id=case when $3 then 'ouf.semantic.propose' else 'ouf.managed-source.onboarding.create' end
		and ($3 or (p.profile->>'assetId'=$4 and p.profile->>'profileId'=$5 and c.header=any($6::text[])))
		for update of p`, attempt, d.Key, d.Proposal, d.Asset, d.Profile, d.Excluded).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = updateOutcomes(ctx, tx, id, `select ordinal,outcome as old,request_id,'' as next_tool,
		jsonb_build_object('state','NO_CANDIDATES','requestId',request_id,
		'observedAt',transaction_timestamp(),'candidateCount',0) as outcome
		from ouf_mcp.analysis_column where analysis_id=$1 and `+key+`=$2 and empty_candidate_page
		and decision_attempt is null and outcome->>'reasonCode' in ('DISCOVERY_DECISION_REQUIRED','DISCOVERY_EVIDENCE_UNAVAILABLE')`, d.Key); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `update ouf_mcp.analysis_column set decision_attempt=$3 where analysis_id=$1
		and `+key+`=$2 and decision_attempt is null and outcome->>'state'='NO_CANDIDATES'`, id, d.Key, attempt)
	return err
}
