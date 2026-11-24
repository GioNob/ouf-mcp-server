package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/jackc/pgx/v5"
)

func lockRenewalRun(ctx context.Context, tx pgx.Tx, i orchestration.Identity, id string) (analysis.RunRecord, error) {
	_, err := tx.Exec(ctx, "select pg_advisory_xact_lock(hashtextextended($1,5018))", analysis.Digest([]string{i.Issuer, i.TenantID}))
	if err != nil {
		return analysis.RunRecord{}, err
	}
	return scanRun(tx.QueryRow(ctx, "select "+runColumns+` from ouf_mcp.analysis_run where analysis_id=$1 and issuer=$2 and tenant=$3 and subject=$4 for update`, id, i.Issuer, i.TenantID, i.PrincipalID))
}

func renewalReplay(ctx context.Context, tx pgx.Tx, r analysis.RunRecord, key, fingerprint string) (json.RawMessage, error) {
	var previous string
	var receipt []byte
	err := tx.QueryRow(ctx, `select fingerprint,receipt from ouf_mcp.analysis_renewal_receipt where issuer=$1 and workload=$2 and tenant=$3 and subject=$4 and command_key=$5`,
		r.Issuer, r.Scope.Service, r.Scope.Tenant, r.Scope.Principal, key).Scan(&previous, &receipt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err == nil && previous != fingerprint {
		return nil, analysis.ErrCommandConflict
	}
	return receipt, err
}

func recordRenewal(ctx context.Context, tx pgx.Tx, r analysis.RunRecord, key, hash, ref string, receipt []byte, proof *analysis.RenewalDecision) error {
	var evidence []byte
	if proof != nil {
		evidence, _ = json.Marshal(proof)
	}
	_, err := tx.Exec(ctx, `insert into ouf_mcp.analysis_renewal_receipt(issuer,workload,tenant,subject,command_key,fingerprint,review_ref,receipt,authority)
		values($1,$2,$3,$4,$5,$6,$7,$8,$9)`, r.Issuer, r.Scope.Service, r.Scope.Tenant, r.Scope.Principal, key, hash, ref, receipt, evidence)
	return err
}

func advanceRenewalSeq(ctx context.Context, tx pgx.Tx, r *analysis.RunRecord) error {
	return tx.QueryRow(ctx, "update ouf_mcp.analysis_run set seq=seq+1 where analysis_id=$1 returning seq", r.ID()).Scan(&r.Seq)
}

func (s *Store) DecideRenewal(ctx context.Context, i orchestration.Identity, c analysis.RenewalCommand, authority analysis.RunRenewalAuthority) (json.RawMessage, error) {
	if i.ActorType != "HUMAN" || i.Delegation == "" {
		return nil, orchestration.ErrUnauthorized
	}
	if authority == nil {
		return nil, analysis.ErrIssueUnavailable
	}
	p, err := s.ReadRenewal(ctx, i, c.ReviewRef)
	if err != nil {
		return nil, err
	}
	var initial analysis.RunRecord
	var check analysis.RunRenewalCheck
	var decision analysis.RenewalDecision
	fingerprint := analysis.Digest(c)
	// Check twice under the same lock order. A competing replica may have
	// committed the command (or superseded the link) during the Gateway call.
	local := func(commit bool) (json.RawMessage, error) {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback(ctx)
		r, err := lockRenewalRun(ctx, tx, i, p.AnalysisID)
		if err != nil {
			return nil, err
		}
		if r.Workload != i.ServicePrincipalID {
			return nil, analysis.ErrScope
		}
		now, err := s.runClock(ctx, tx)
		if err != nil {
			return nil, err
		}
		if r.MandateState == "REVOKED" || r.MandateState == "DENIED" {
			return nil, orchestration.ErrUnauthorized
		}
		receipt, err := renewalReplay(ctx, tx, r, c.Key, fingerprint)
		if err != nil || receipt != nil {
			return receipt, err
		}
		current, err := latestProposal(ctx, tx, r.ID())
		if err != nil {
			return nil, err
		}
		if !current.Expires.After(now) || current.State != "OPEN" || current.ReviewRef != p.ReviewRef || current.Version != c.Version || current.Hash != c.Hash ||
			strconv.FormatInt(current.ExpectedSeq, 10) != c.ExpectedSeq || r.Seq != p.ExpectedSeq || r.State != "BLOCKED" || r.Reason != analysis.ErrRunExhausted.Error() ||
			r.Mandate.ID != p.MandateID || r.Mandate.Generation != p.Generation {
			return nil, analysis.ErrFenced
		}
		if r.MandateState == "EXPIRED" || !r.Mandate.Expires.After(now) {
			return nil, analysis.ErrMandateExpired
		}
		if r.MandateState != "ACTIVE" {
			return nil, orchestration.ErrUnauthorized
		}
		if err = openRenewalIncident(ctx, tx, p); err != nil {
			return nil, err
		}
		if !commit {
			initial = r
			check = analysis.RunRenewalCheck{Command: c, Scope: r.Scope, Issuer: r.Issuer, Action: "renew-budget", Contract: "ouf.analysis.authority-check.v1",
				Workload: r.Workload, Fence: r.Fence, MandateID: p.MandateID, Generation: p.Generation,
				RunRenewal: analysis.RunRenewal{AnalysisID: r.ID(), IncidentID: p.IncidentID, ExpectedSeq: p.ExpectedSeq, ProposalVersion: p.Version,
					ProposalHash: p.Hash, Increment: p.Increment, Reason: p.Reason, ExpiresAt: p.Expires, Seconds: r.Limits.RenewalDuration()}}
			return nil, nil
		}
		if r.RunSpec != initial.RunSpec || r.Fence != check.Fence {
			return nil, analysis.ErrFenced
		}
		if !decision.ValidUntil.After(now) {
			return nil, orchestration.ErrUnauthorized
		}
		var state string
		switch c.Decision {
		case "APPROVE":
			state = "APPROVED"
			err = s.renewRunBudgetTx(ctx, tx, initial, r, check, decision)
		case "CHANGES_REQUESTED":
			state = c.Decision
			err = advanceRenewalSeq(ctx, tx, &r)
		case "CLOSE_PARTIAL":
			state = "CLOSED_PARTIAL"
			err = closeRenewal(ctx, tx, r, p, now)
		default:
			return nil, analysis.ErrInvalid
		}
		if err != nil {
			return nil, err
		}
		items, _ := json.Marshal(c.Items)
		_, err = tx.Exec(ctx, "update ouf_mcp.analysis_renewal_proposal set state=$2,feedback=$3 where review_ref=$1", p.ReviewRef, state, items)
		if err != nil {
			return nil, err
		}
		receipt, err = json.Marshal(map[string]string{"proposalState": state, "decisionRef": decision.DecisionRef})
		if err != nil {
			return nil, err
		}
		if err = recordRenewal(ctx, tx, r, c.Key, fingerprint, p.ReviewRef, receipt, &decision); err != nil {
			return nil, err
		}
		now, err = s.runClock(ctx, tx)
		if err != nil {
			return nil, err
		}
		if !p.Expires.After(now) {
			return nil, analysis.ErrFenced
		}
		if !decision.ValidUntil.After(now) {
			return nil, orchestration.ErrUnauthorized
		}
		return receipt, tx.Commit(ctx)
	}
	receipt, err := local(false)
	if err != nil || receipt != nil {
		return receipt, err
	}
	hash, err := check.Hash()
	if err != nil {
		return nil, err
	}
	decision, err = authority.AuthorizeRunRenewal(ctx, i, check)
	if err != nil {
		return nil, err
	}
	if decision.RequestHash != hash || strings.TrimSpace(decision.DecisionRef) == "" || len(decision.DecisionRef) > 256 ||
		strings.TrimSpace(decision.GrantRef) == "" || len(decision.GrantRef) > 256 {
		return nil, orchestration.ErrUnauthorized
	}
	return local(true)
}

func closeRenewal(ctx context.Context, tx pgx.Tx, r analysis.RunRecord, p analysis.RenewalProposal, now time.Time) error {
	var unsettled bool
	err := tx.QueryRow(ctx, `select exists(select 1 from ouf_mcp.analysis_run_attempt a where a.analysis_id=$1 and (a.proof_until>$2 or
		(a.finished_at is null and a.superseded_at is null and not exists(select 1 from ouf_mcp.analysis_group_quarantine q
		where q.analysis_id=a.analysis_id and q.intent_key=a.intent_key and q.state<>'RESOLVED'))))`, r.ID(), now).Scan(&unsettled)
	if err != nil {
		return err
	}
	if unsettled {
		return analysis.ErrFenced
	}
	var cp analysis.RunCheckpoint
	if err := tx.QueryRow(ctx, "select checkpoint from ouf_mcp.analysis_run where analysis_id=$1", r.ID()).Scan(&cp); err != nil {
		return err
	}
	before := append([]analysis.Outcome(nil), cp.Outcomes...)
	for _, ordinal := range p.Remaining {
		if ordinal < 0 || ordinal >= len(cp.Outcomes) || cp.Outcomes[ordinal].State != "PENDING" {
			return analysis.ErrRunDrift
		}
		cp.Outcomes[ordinal].State = "BLOCKED"
		cp.Outcomes[ordinal].Reason = analysis.ErrRunExhausted.Error()
	}
	if cp.Counts().Pending != 0 {
		return analysis.ErrFenced
	}
	cp.Queue = nil
	state := cp.Counts().State()
	reason := "ANALYSIS_PARTIAL"
	if state == "BLOCKED" {
		reason = p.Reason
	}
	if err := saveRunCheckpoint(ctx, tx, r, cp, before, state, reason, now, now); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, "update ouf_mcp.analysis_run_incident set state='RESOLVED' where analysis_id=$1 and incident_id=$2 and state='OPEN'", r.ID(), p.IncidentID)
	if err == nil && tag.RowsAffected() != 1 {
		return analysis.ErrFenced
	}
	return err
}
