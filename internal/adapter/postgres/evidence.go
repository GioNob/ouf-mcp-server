package postgres

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/evidence"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var objectHashPattern = regexp.MustCompile(`^v[0-9]+:(hmac-sha256|hmac-sha384):[A-Za-z0-9_-]{32,512}$`)

func (s *Store) InsertVerifiedEvidence(ctx context.Context, in evidence.Verified) (evidence.Result, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return evidence.Result{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var existingRef uuid.UUID
	var existingHash string
	err = tx.QueryRow(ctx, `select evidence_ref,evidence_payload_hash from ouf_mcp.owner_evidence_inbox where backend_owner=$1 and backend_request_id=$2`, in.BackendOwner, in.BackendRequestID).Scan(&existingRef, &existingHash)
	if err == nil {
		if existingHash != in.PayloadHash {
			return evidence.Result{}, evidence.ErrIngressConflict
		}
		return evidence.Result{EvidenceRef: existingRef, Replay: true}, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return evidence.Result{}, err
	}
	var attemptID, windowID uuid.UUID
	err = tx.QueryRow(ctx, `select ta.attempt_id,ac.budget_window_id from ouf_mcp.tool_attempt ta join ouf_mcp.attempt_admission_context ac using(attempt_id) where ta.backend_request_id=$1 and ac.owner=$2`, in.BackendRequestID, in.BackendOwner).Scan(&attemptID, &windowID)
	if errors.Is(err, pgx.ErrNoRows) {
		return evidence.Result{}, evidence.ErrOwnerMismatch
	}
	if err != nil {
		return evidence.Result{}, err
	}
	_, err = tx.Exec(ctx, `insert into ouf_mcp.owner_evidence_inbox(evidence_ref,attempt_id,budget_window_id,backend_owner,backend_request_id,evidence_kind,terminal_state,terminal_outcome_code,result_ref,actual_distinct_objects,object_hashes,evidence_payload_hash,verified_at,verified_by) values($1,$2,$3,$4,$5,$6,$7,$8,nullif($9,''),$10,$11,$12,transaction_timestamp(),$13)`, in.EvidenceRef, attemptID, windowID, in.BackendOwner, in.BackendRequestID, in.Kind, in.TerminalState, in.OutcomeCode, in.ResultRef, in.ActualDistinctObjects, in.ObjectHashes, in.PayloadHash, in.ServicePrincipalID)
	if err != nil {
		return evidence.Result{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return evidence.Result{}, err
	}
	return evidence.Result{EvidenceRef: in.EvidenceRef}, nil
}

func (s *Store) ExpireUnknown(ctx context.Context, now, unknownBefore time.Time, limit int) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `select ta.attempt_id,ac.budget_window_id,ac.equivalence_group_id from ouf_mcp.tool_attempt ta join ouf_mcp.attempt_admission_context ac using(attempt_id) where ta.state='UNKNOWN' and ta.unknown_since<$1 and (ta.recovery_lease_until is null or ta.recovery_lease_until<$2) order by ta.unknown_since,ta.attempt_id limit $3`, unknownBefore, now, limit)
	if err != nil {
		return 0, err
	}
	type candidate struct{ attempt, window, group uuid.UUID }
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err = rows.Scan(&c.attempt, &c.window, &c.group); err != nil {
			rows.Close()
			return 0, err
		}
		candidates = append(candidates, c)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return 0, err
	}
	count := 0
	for _, c := range candidates {
		if _, err = tx.Exec(ctx, `select 1 from ouf_mcp.budget_window where budget_window_id=$1 for update`, c.window); err != nil {
			return 0, err
		}
		if _, err = tx.Exec(ctx, `select 1 from ouf_mcp.retry_guard where equivalence_group_id=$1 for update`, c.group); err != nil {
			return 0, err
		}
		var state string
		if err = tx.QueryRow(ctx, `select state from ouf_mcp.tool_attempt where attempt_id=$1 for update`, c.attempt).Scan(&state); err != nil {
			return 0, err
		}
		if state != "UNKNOWN" {
			continue
		}
		var calls, bytes int64
		var reservationState string
		if err = tx.QueryRow(ctx, `select reserved_tool_calls,reserved_result_bytes,state from ouf_mcp.budget_reservation where attempt_id=$1 for update`, c.attempt).Scan(&calls, &bytes, &reservationState); err != nil {
			return 0, err
		}
		if reservationState != "UNKNOWN" {
			return 0, errors.New("unresolved reservation state mismatch")
		}
		if _, err = tx.Exec(ctx, `update ouf_mcp.budget_window set reserved_tool_calls=reserved_tool_calls-$2,reserved_result_bytes=reserved_result_bytes-$3,consumed_tool_calls=consumed_tool_calls+$2,consumed_result_bytes=consumed_result_bytes+$3 where budget_window_id=$1`, c.window, calls, bytes); err != nil {
			return 0, err
		}
		if err = execOne(ctx, tx, `update ouf_mcp.budget_reservation set state='RECONCILED',actual_tool_calls=reserved_tool_calls,actual_result_bytes=reserved_result_bytes,actual_distinct_objects=0,reconciled_at=transaction_timestamp() where attempt_id=$1 and state='UNKNOWN'`, c.attempt); err != nil {
			return 0, err
		}
		if err = execOne(ctx, tx, `update ouf_mcp.idempotency_claim set state='COMPLETED',lock_version=lock_version+1 where attempt_id=$1 and state='UNKNOWN'`, c.attempt); err != nil {
			return 0, err
		}
		if err = execOne(ctx, tx, `update ouf_mcp.tool_attempt set state='UNRESOLVED',outcome_code='MAX_UNKNOWN_HOLD',completed_at=$2,recovery_owner=null,recovery_lease_until=null,lock_version=lock_version+1 where attempt_id=$1 and state='UNKNOWN'`, c.attempt, now); err != nil {
			return 0, err
		}
		count++
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Store) LateCompensate(ctx context.Context, attemptID, evidenceRef uuid.UUID, expectedRevision int) (evidence.CompensationResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return evidence.CompensationResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var preliminaryState string
	var preliminaryAttempt uuid.UUID
	if err = tx.QueryRow(ctx, `select attempt_id,evidence_state from ouf_mcp.owner_evidence_inbox where evidence_ref=$1`, evidenceRef).Scan(&preliminaryAttempt, &preliminaryState); err != nil {
		return evidence.CompensationResult{}, err
	}
	if preliminaryAttempt == attemptID && (preliminaryState == "CONSUMED" || preliminaryState == "QUARANTINED") {
		var currentState, currentCode string
		var currentRevision int
		if err = tx.QueryRow(ctx, `select state,coalesce(outcome_code,''),outcome_revision from ouf_mcp.tool_attempt where attempt_id=$1`, attemptID).Scan(&currentState, &currentCode, &currentRevision); err != nil {
			return evidence.CompensationResult{}, err
		}
		disposition := "IDEMPOTENT_REPLAY"
		if preliminaryState == "QUARANTINED" {
			disposition = "DOMAIN_VIOLATION_REPLAY"
		}
		return evidence.CompensationResult{Disposition: disposition, AttemptState: currentState, OutcomeCode: currentCode, OutcomeRevision: currentRevision}, tx.Commit(ctx)
	}
	var windowID, groupID uuid.UUID
	if err = tx.QueryRow(ctx, `select budget_window_id,equivalence_group_id from ouf_mcp.attempt_admission_context where attempt_id=$1`, attemptID).Scan(&windowID, &groupID); err != nil {
		return evidence.CompensationResult{}, err
	}
	var allowedVersions []string
	if err = tx.QueryRow(ctx, `select allowed_object_hash_versions from ouf_mcp.budget_window where budget_window_id=$1 for update`, windowID).Scan(&allowedVersions); err != nil {
		return evidence.CompensationResult{}, err
	}
	if _, err = tx.Exec(ctx, `select 1 from ouf_mcp.retry_guard where equivalence_group_id=$1 for update`, groupID); err != nil {
		return evidence.CompensationResult{}, err
	}
	var state, backendID, owner, outcomeCode string
	var revision int
	if err = tx.QueryRow(ctx, `select ta.state,ta.backend_request_id,ac.owner,ta.outcome_revision,coalesce(ta.outcome_code,'') from ouf_mcp.tool_attempt ta join ouf_mcp.attempt_admission_context ac using(attempt_id) where ta.attempt_id=$1 for update`, attemptID).Scan(&state, &backendID, &owner, &revision, &outcomeCode); err != nil {
		return evidence.CompensationResult{}, err
	}
	if revision != expectedRevision || (state != "UNRESOLVED" && state != "UNKNOWN") {
		return evidence.CompensationResult{}, errors.New("attempt revision CAS mismatch")
	}
	var reservationState string
	var reservedCalls, reservedBytes int64
	if err = tx.QueryRow(ctx, `select state,reserved_tool_calls,reserved_result_bytes from ouf_mcp.budget_reservation where attempt_id=$1 for update`, attemptID).Scan(&reservationState, &reservedCalls, &reservedBytes); err != nil {
		return evidence.CompensationResult{}, err
	}
	var debt int64
	if err = tx.QueryRow(ctx, `select debt_amount from ouf_mcp.budget_object_debt where attempt_id=$1 and budget_window_id=$2 and debt_state='ACTIVE' for update`, attemptID, windowID).Scan(&debt); err != nil {
		return evidence.CompensationResult{}, err
	}
	var evAttempt, evWindow uuid.UUID
	var evOwner, evBackend, kind, terminalState, terminalCode, resultRef, payloadHash, evidenceState string
	var actual int64
	var hashes []string
	if err = tx.QueryRow(ctx, `select attempt_id,budget_window_id,backend_owner,backend_request_id,evidence_kind,terminal_state,terminal_outcome_code,coalesce(result_ref,''),actual_distinct_objects,object_hashes,evidence_payload_hash,evidence_state from ouf_mcp.owner_evidence_inbox where evidence_ref=$1 for update`, evidenceRef).Scan(&evAttempt, &evWindow, &evOwner, &evBackend, &kind, &terminalState, &terminalCode, &resultRef, &actual, &hashes, &payloadHash, &evidenceState); err != nil {
		return evidence.CompensationResult{}, err
	}
	if evidenceState == "CONSUMED" {
		return evidence.CompensationResult{Disposition: "IDEMPOTENT_REPLAY", AttemptState: state, OutcomeCode: outcomeCode, OutcomeRevision: revision}, tx.Commit(ctx)
	}
	if evidenceState == "QUARANTINED" {
		return evidence.CompensationResult{Disposition: "DOMAIN_VIOLATION_REPLAY", AttemptState: state, OutcomeCode: outcomeCode, OutcomeRevision: revision}, tx.Commit(ctx)
	}
	if evAttempt != attemptID {
		_, _ = tx.Exec(ctx, `insert into ouf_mcp.security_incident(incident_key,attempt_id,evidence_ref,category,detail_code,evidence_payload_hash) values($1,$2,$3,'EVIDENCE_SCOPE_VIOLATION','CALLER_ATTEMPT_MISMATCH',$4) on conflict do nothing`, evidenceRef.String()+":scope", evAttempt, evidenceRef, payloadHash)
		if err = tx.Commit(ctx); err != nil {
			return evidence.CompensationResult{}, err
		}
		return evidence.CompensationResult{Disposition: "REJECTED_SCOPE", AttemptState: state, OutcomeCode: outcomeCode, OutcomeRevision: revision}, nil
	}
	if evWindow != windowID || evOwner != owner || evBackend != backendID {
		if err = quarantineEvidence(ctx, tx, attemptID, evidenceRef, payloadHash, "EVIDENCE_BINDING_VIOLATION", "OWNER_OR_REQUEST_BINDING_MISMATCH"); err != nil {
			return evidence.CompensationResult{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return evidence.CompensationResult{}, err
		}
		return evidence.CompensationResult{Disposition: "REJECTED_BINDING", AttemptState: state, OutcomeCode: outcomeCode, OutcomeRevision: revision}, nil
	}
	unique := map[string]struct{}{}
	violation := ""
	allowed := map[string]bool{}
	for _, version := range allowedVersions {
		allowed[version] = true
	}
	for _, hash := range hashes {
		unique[hash] = struct{}{}
		if !objectHashPattern.MatchString(hash) || !allowed[strings.SplitN(hash, ":", 2)[0]] {
			violation = "CRYPTO_VIOLATION"
		}
	}
	if violation == "" && (int64(len(unique)) != actual || actual > debt || (kind == "OWNER_PROVES_NO_DISPATCH" && (actual != 0 || len(hashes) != 0))) {
		violation = "CONTRACT_VIOLATION"
	}
	if violation != "" {
		if err = execOne(ctx, tx, `update ouf_mcp.tool_attempt set state='FAILED',dispatch_state='ACKNOWLEDGED',outcome_code=$3,result_ref=nullif($4,''),completed_at=coalesce(completed_at,transaction_timestamp()),outcome_revision=outcome_revision+1,lock_version=lock_version+1 where attempt_id=$1 and outcome_revision=$2 and state in('UNKNOWN','UNRESOLVED')`, attemptID, expectedRevision, violation, resultRef); err != nil {
			return evidence.CompensationResult{}, err
		}
		if err = quarantineEvidence(ctx, tx, attemptID, evidenceRef, payloadHash, violation, "AUTHORITATIVE_EVIDENCE_REJECTED"); err != nil {
			return evidence.CompensationResult{}, err
		}
		if err = tx.Commit(ctx); err != nil {
			return evidence.CompensationResult{}, err
		}
		return evidence.CompensationResult{Disposition: "COMMITTED_DOMAIN_VIOLATION", AttemptState: "FAILED", OutcomeCode: violation, OutcomeRevision: expectedRevision + 1}, nil
	}
	inserted := int64(0)
	for hash := range unique {
		tag, insertErr := tx.Exec(ctx, `insert into ouf_mcp.budget_distinct_object(budget_window_id,object_hash_key_version,object_id_hash) values($1,$2,$3) on conflict do nothing`, windowID, strings.SplitN(hash, ":", 2)[0], hash)
		if insertErr != nil {
			return evidence.CompensationResult{}, insertErr
		}
		inserted += tag.RowsAffected()
	}
	if err = execOne(ctx, tx, `update ouf_mcp.budget_object_debt set debt_state='RESOLVED',owner_evidence_ref=$3,resolved_at=transaction_timestamp(),lock_version=lock_version+1 where attempt_id=$1 and budget_window_id=$2 and debt_state='ACTIVE'`, attemptID, windowID, evidenceRef); err != nil {
		return evidence.CompensationResult{}, err
	}
	if err = execOne(ctx, tx, `update ouf_mcp.budget_window set consumed_distinct_objects=consumed_distinct_objects+$2,current_unresolved_object_debt_total=current_unresolved_object_debt_total-$3 where budget_window_id=$1 and current_unresolved_object_debt_total>=$3`, windowID, inserted, debt); err != nil {
		return evidence.CompensationResult{}, err
	}
	if err = execOne(ctx, tx, `update ouf_mcp.retry_guard set blocking_attempts=blocking_attempts-1,updated_at=transaction_timestamp() where equivalence_group_id=$1 and blocking_attempts>0`, groupID); err != nil {
		return evidence.CompensationResult{}, err
	}
	if state == "UNKNOWN" {
		if kind != "OWNER_PROVES_NO_DISPATCH" || reservationState != "UNKNOWN" {
			return evidence.CompensationResult{}, errors.New("unknown late recovery requires no-dispatch proof")
		}
		if err = execOne(ctx, tx, `update ouf_mcp.budget_window set reserved_tool_calls=reserved_tool_calls-$2,reserved_result_bytes=reserved_result_bytes-$3 where budget_window_id=$1`, windowID, reservedCalls, reservedBytes); err != nil {
			return evidence.CompensationResult{}, err
		}
		if err = execOne(ctx, tx, `update ouf_mcp.budget_reservation set state='RELEASED',actual_tool_calls=0,actual_result_bytes=0,actual_distinct_objects=0,reconciled_at=transaction_timestamp() where attempt_id=$1 and state='UNKNOWN'`, attemptID); err != nil {
			return evidence.CompensationResult{}, err
		}
		if err = execOne(ctx, tx, `update ouf_mcp.idempotency_claim set state='COMPLETED',lock_version=lock_version+1 where attempt_id=$1 and state='UNKNOWN'`, attemptID); err != nil {
			return evidence.CompensationResult{}, err
		}
	}
	if _, err = tx.Exec(ctx, `insert into ouf_mcp.budget_adjustment(adjustment_id,adjustment_key,budget_window_id,attempt_id,owner_evidence_ref,prior_outcome_revision,new_outcome_revision,distinct_object_delta,unresolved_debt_delta) values($1,$2,$3,$4,$5,$6,$7,$8,$9)`, uuid.New(), payloadHash, windowID, attemptID, evidenceRef, expectedRevision, expectedRevision+1, inserted, -debt); err != nil {
		return evidence.CompensationResult{}, err
	}
	dispatchState := "ACKNOWLEDGED"
	if kind == "OWNER_PROVES_NO_DISPATCH" {
		dispatchState = "NOT_DISPATCHED"
	}
	if err = execOne(ctx, tx, `update ouf_mcp.tool_attempt set state=$3,dispatch_state=$4,outcome_code=$5,result_ref=nullif($6,''),completed_at=coalesce(completed_at,transaction_timestamp()),outcome_revision=outcome_revision+1,lock_version=lock_version+1 where attempt_id=$1 and outcome_revision=$2 and state in('UNKNOWN','UNRESOLVED')`, attemptID, expectedRevision, terminalState, dispatchState, terminalCode, resultRef); err != nil {
		return evidence.CompensationResult{}, err
	}
	if err = execOne(ctx, tx, `update ouf_mcp.owner_evidence_inbox set evidence_state='CONSUMED',consumed_at=transaction_timestamp() where evidence_ref=$1 and evidence_state='VERIFIED'`, evidenceRef); err != nil {
		return evidence.CompensationResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return evidence.CompensationResult{}, err
	}
	return evidence.CompensationResult{Disposition: "COMPENSATED", AttemptState: terminalState, OutcomeCode: terminalCode, OutcomeRevision: expectedRevision + 1}, nil
}

func quarantineEvidence(ctx context.Context, tx pgx.Tx, attemptID, evidenceRef uuid.UUID, payloadHash, category, detail string) error {
	if err := execOne(ctx, tx, `update ouf_mcp.owner_evidence_inbox set evidence_state='QUARANTINED',consumed_at=transaction_timestamp() where evidence_ref=$1 and evidence_state='VERIFIED'`, evidenceRef); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `insert into ouf_mcp.security_incident(incident_key,attempt_id,evidence_ref,category,detail_code,evidence_payload_hash) values($1,$2,$3,$4,$5,$6) on conflict do nothing`, evidenceRef.String()+":"+strings.ToLower(category), attemptID, evidenceRef, category, detail, payloadHash)
	return err
}
