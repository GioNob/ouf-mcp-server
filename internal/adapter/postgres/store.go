package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/GioNob/ouf-mcp-server/internal/recovery"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrIdempotencyConflict = errors.New("idempotency key reused with different request")
var ErrBudgetExhausted = errors.New("orchestration budget exhausted")
var ErrIdempotencyOutcomeUnknown = errors.New("idempotency outcome unknown")
var ErrGroupLocked = errors.New("equivalence group locked by uncertain attempt")

type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 5
	config.MaxConnIdleTime = 5 * time.Minute
	config.MaxConnLifetime = 30 * time.Minute
	config.ConnConfig.ConnectTimeout = 500 * time.Millisecond
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err = pool.Ping(checkCtx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}
func (s *Store) Close()              { s.pool.Close() }
func (s *Store) Pool() *pgxpool.Pool { return s.pool }
func (s *Store) Ready(ctx context.Context) error {
	var version int
	return s.pool.QueryRow(ctx, "select max(version) from ouf_mcp.schema_migration").Scan(&version)
}

func (s *Store) EnsureManifest(ctx context.Context, checksum, version, schemaVersion string, payload []byte) error {
	if fmt.Sprintf("%x", sha256.Sum256(payload)) != checksum {
		return fmt.Errorf("manifest checksum does not match payload")
	}
	command, err := s.pool.Exec(ctx, `insert into ouf_mcp.manifest_snapshot(manifest_checksum,manifest_version,schema_version,payload) values($1,$2,$3,$4::jsonb) on conflict(manifest_checksum) do nothing`, checksum, version, schemaVersion, string(payload))
	if err != nil {
		return err
	}
	if command.RowsAffected() == 1 {
		return nil
	}
	var existingVersion, existingSchema string
	if err = s.pool.QueryRow(ctx, "select manifest_version,schema_version from ouf_mcp.manifest_snapshot where manifest_checksum=$1", checksum).Scan(&existingVersion, &existingSchema); err != nil {
		return err
	}
	if existingVersion != version || existingSchema != schemaVersion {
		return fmt.Errorf("manifest checksum collision")
	}
	return nil
}

type Session struct {
	ID                                                                           uuid.UUID
	ConversationRef, ServicePrincipalID, PrincipalID, TenantID, ManifestChecksum string
	ExpiresAt                                                                    time.Time
}

func (s *Store) CreateSession(ctx context.Context, in Session) (Session, error) {
	if in.ID == uuid.Nil {
		in.ID = uuid.New()
	}
	err := s.pool.QueryRow(ctx, `insert into ouf_mcp.application_session(application_session_id,application_conversation_ref,service_principal_id,principal_id,tenant_id,created_manifest_checksum,expires_at) values($1,nullif($2,''),$3,$4,$5,$6,$7) returning expires_at`, in.ID, in.ConversationRef, in.ServicePrincipalID, in.PrincipalID, in.TenantID, in.ManifestChecksum, in.ExpiresAt).Scan(&in.ExpiresAt)
	return in, err
}

type Admission struct {
	AttemptID                                                                                                                             uuid.UUID
	SessionID                                                                                                                             *uuid.UUID
	ServicePrincipalID, PrincipalID, TenantID, CapabilityID, OperationClass, ManifestChecksum, IdempotencyKey, RequestHash, CorrelationID string
}
type Attempt struct {
	ID                                uuid.UUID
	State, DispatchState, RequestHash string
	LockVersion                       int64
}

func (s *Store) Admit(ctx context.Context, in Admission) (Attempt, error) {
	if in.AttemptID == uuid.Nil {
		in.AttemptID = uuid.New()
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Attempt{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var out Attempt
	err = tx.QueryRow(ctx, `insert into ouf_mcp.tool_attempt(attempt_id,application_session_id,service_principal_id,principal_id,tenant_id,capability_id,operation_class,manifest_checksum,idempotency_key,request_hash,correlation_id,state,dispatch_state) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'ADMITTED','NOT_DISPATCHED') on conflict(service_principal_id,principal_id,tenant_id,capability_id,operation_class,idempotency_key) do nothing returning attempt_id,state,dispatch_state,request_hash,lock_version`, in.AttemptID, in.SessionID, in.ServicePrincipalID, in.PrincipalID, in.TenantID, in.CapabilityID, in.OperationClass, in.ManifestChecksum, in.IdempotencyKey, in.RequestHash, in.CorrelationID).Scan(&out.ID, &out.State, &out.DispatchState, &out.RequestHash, &out.LockVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `select attempt_id,state,dispatch_state,request_hash,lock_version from ouf_mcp.tool_attempt where service_principal_id=$1 and principal_id=$2 and tenant_id=$3 and capability_id=$4 and operation_class=$5 and idempotency_key=$6 for update`, in.ServicePrincipalID, in.PrincipalID, in.TenantID, in.CapabilityID, in.OperationClass, in.IdempotencyKey).Scan(&out.ID, &out.State, &out.DispatchState, &out.RequestHash, &out.LockVersion)
		if err == nil && out.RequestHash != in.RequestHash {
			return Attempt{}, ErrIdempotencyConflict
		}
	}
	if err != nil {
		return Attempt{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Attempt{}, err
	}
	return out, nil
}

// Retry only SQLSTATE 40001: PostgreSQL has aborted the transaction, and
// reserveOnce has rolled it back. No dispatch or other external side effect
// occurs here. Transport errors and uncertain commit outcomes are never retried.
func (s *Store) Reserve(ctx context.Context, in orchestration.AdmissionRequest) (orchestration.AdmissionDecision, error) {
	for attempt := 0; ; attempt++ {
		decision, err := s.reserveOnce(ctx, in)
		var conflict *pgconn.PgError
		if err == nil || attempt >= 2 || !errors.As(err, &conflict) || conflict.Code != "40001" {
			return decision, err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 2 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return orchestration.AdmissionDecision{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Store) reserveOnce(ctx context.Context, in orchestration.AdmissionRequest) (orchestration.AdmissionDecision, error) {
	if in.Window <= 0 || in.RetryThreshold < 1 || in.Maximum.ToolCalls < 1 {
		return orchestration.AdmissionDecision{}, errors.New("invalid admission policy")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var existingID uuid.UUID
	var existingHash string
	err = tx.QueryRow(ctx, `select attempt_id,request_hash from ouf_mcp.idempotency_claim where service_principal_id=$1 and principal_id=$2 and tenant_id=$3 and owner=$4 and capability_id=$5 and operation_class=$6 and idempotency_key=$7 for update`, in.Identity.ServicePrincipalID, in.Identity.PrincipalID, in.Identity.TenantID, in.Owner, in.CapabilityID, in.OperationClass, in.IdempotencyKey).Scan(&existingID, &existingHash)
	if err == nil {
		if existingHash != in.RequestHash {
			return orchestration.AdmissionDecision{}, ErrIdempotencyConflict
		}
		var state string
		if err = tx.QueryRow(ctx, `select state from ouf_mcp.tool_attempt where attempt_id=$1`, existingID).Scan(&state); err != nil {
			return orchestration.AdmissionDecision{}, err
		}
		if state == "UNKNOWN" || state == "UNRESOLVED" {
			return orchestration.AdmissionDecision{}, ErrIdempotencyOutcomeUnknown
		}
		return orchestration.AdmissionDecision{AttemptID: existingID, Replay: true}, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return orchestration.AdmissionDecision{}, err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "select transaction_timestamp()").Scan(&now); err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	windowStart := now.Truncate(in.Window)
	windowEnd := windowStart.Add(in.Window)
	windowID := uuid.New()
	err = tx.QueryRow(ctx, `insert into ouf_mcp.budget_window(budget_window_id,service_principal_id,principal_id,tenant_id,policy_ref,window_start,window_end) values($1,$2,$3,$4,$5,$6,$7) on conflict(service_principal_id,principal_id,tenant_id,policy_ref,window_start) do update set policy_ref=excluded.policy_ref returning budget_window_id`, windowID, in.Identity.ServicePrincipalID, in.Identity.PrincipalID, in.Identity.TenantID, in.ManifestChecksum, windowStart, windowEnd).Scan(&windowID)
	if err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	var reservedCalls, consumedCalls, reservedBytes, consumedBytes, reservedObjects, consumedObjects int64
	if err = tx.QueryRow(ctx, `select reserved_tool_calls,consumed_tool_calls,reserved_result_bytes,consumed_result_bytes,reserved_distinct_objects,consumed_distinct_objects from ouf_mcp.budget_window where budget_window_id=$1 for update`, windowID).Scan(&reservedCalls, &consumedCalls, &reservedBytes, &consumedBytes, &reservedObjects, &consumedObjects); err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	var objectDebt int64
	if err = tx.QueryRow(ctx, `select coalesce(sum(debt_amount),0) from ouf_mcp.budget_object_debt where budget_window_id=$1 and debt_state='ACTIVE'`, windowID).Scan(&objectDebt); err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	objectBudgetExceeded := in.Maximum.DistinctObjects > 0 && reservedObjects+consumedObjects+objectDebt+in.Maximum.DistinctObjects > in.Maximum.DistinctObjects*int64(in.RetryThreshold)
	if reservedCalls+consumedCalls+in.Maximum.ToolCalls > in.Maximum.ToolCalls*int64(in.RetryThreshold) || reservedBytes+consumedBytes+in.Maximum.ResultBytes > in.Maximum.ResultBytes*int64(in.RetryThreshold) || objectBudgetExceeded {
		return orchestration.AdmissionDecision{}, ErrBudgetExhausted
	}
	groupID := uuid.New()
	err = tx.QueryRow(ctx, `insert into ouf_mcp.retry_equivalence_group(equivalence_group_id,budget_window_id,capability_id,fingerprint_version,semantic_fingerprint) values($1,$2,$3,$4,$5) on conflict(budget_window_id,capability_id,fingerprint_version,semantic_fingerprint) do update set semantic_fingerprint=excluded.semantic_fingerprint returning equivalence_group_id`, groupID, windowID, in.CapabilityID, in.FingerprintVersion, in.SemanticFingerprint).Scan(&groupID)
	if err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	var blocking int
	var blocked bool
	err = tx.QueryRow(ctx, `insert into ouf_mcp.retry_guard(equivalence_group_id,equivalent_attempts) values($1,1) on conflict(equivalence_group_id) do update set equivalent_attempts=ouf_mcp.retry_guard.equivalent_attempts+1,updated_at=transaction_timestamp() returning blocked,blocking_attempts`, groupID).Scan(&blocked, &blocking)
	if err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	if blocking > 0 {
		return orchestration.AdmissionDecision{}, ErrGroupLocked
	}
	// Independent successful outcomes do not establish an equivalent retry.
	// Once classified as a retry, an attempt remains charged even on success
	// (MCP-A43). Retain failures and in-flight/uncertain attempts as well.
	// Read authoritative attempt states under the existing serializable
	// admission transaction; recovery success is handled by the same rule.
	var retryRelevantAttempts int
	err = tx.QueryRow(ctx, `select count(*) from (select 1 from ouf_mcp.attempt_admission_context ac join ouf_mcp.tool_attempt ta using(attempt_id) where ac.equivalence_group_id=$1 and (ta.state<>'SUCCEEDED' or ac.is_equivalent_retry is true) limit $2) pending`, groupID, in.RetryThreshold).Scan(&retryRelevantAttempts)
	if err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	if blocked || retryRelevantAttempts+1 >= in.RetryThreshold {
		return orchestration.AdmissionDecision{}, orchestration.ErrToolSelectionStall
	}
	if in.AttemptID == uuid.Nil {
		in.AttemptID = uuid.New()
	}
	_, err = tx.Exec(ctx, `insert into ouf_mcp.tool_attempt(attempt_id,service_principal_id,principal_id,tenant_id,capability_id,operation_class,manifest_checksum,idempotency_key,request_hash,correlation_id,state,dispatch_state) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'ADMITTED','NOT_DISPATCHED')`, in.AttemptID, in.Identity.ServicePrincipalID, in.Identity.PrincipalID, in.Identity.TenantID, in.CapabilityID, in.OperationClass, in.ManifestChecksum, in.IdempotencyKey, in.RequestHash, in.CorrelationID)
	if err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	_, err = tx.Exec(ctx, `insert into ouf_mcp.idempotency_claim values($1,$2,$3,$4,$5,$6,$7,$8,$9)`, in.Identity.ServicePrincipalID, in.Identity.PrincipalID, in.Identity.TenantID, in.Owner, in.CapabilityID, in.OperationClass, in.IdempotencyKey, in.RequestHash, in.AttemptID)
	if err == nil {
		_, err = tx.Exec(ctx, `insert into ouf_mcp.attempt_admission_context(attempt_id,budget_window_id,equivalence_group_id,owner,actor_type,authentication_context_ref,authorization_decision_ref,semantic_fingerprint,fingerprint_version,is_equivalent_retry) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, in.AttemptID, windowID, groupID, in.Owner, in.Identity.ActorType, in.Identity.AuthenticationContextRef, in.AuthorizationDecisionRef, in.SemanticFingerprint, in.FingerprintVersion, retryRelevantAttempts > 0)
	}
	if err == nil {
		_, err = tx.Exec(ctx, `insert into ouf_mcp.budget_reservation(reservation_id,attempt_id,budget_window_id,reserved_tool_calls,reserved_result_bytes,state,created_at,reserved_distinct_objects_max) values($1,$2,$3,$4,$5,'RESERVED',transaction_timestamp(),$6)`, uuid.New(), in.AttemptID, windowID, in.Maximum.ToolCalls, in.Maximum.ResultBytes, in.Maximum.DistinctObjects)
	}
	if err == nil {
		_, err = tx.Exec(ctx, `update ouf_mcp.budget_window set reserved_tool_calls=reserved_tool_calls+$2,reserved_result_bytes=reserved_result_bytes+$3,reserved_distinct_objects=reserved_distinct_objects+$4 where budget_window_id=$1`, windowID, in.Maximum.ToolCalls, in.Maximum.ResultBytes, in.Maximum.DistinctObjects)
	}
	if err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	return orchestration.AdmissionDecision{AttemptID: in.AttemptID}, nil
}

func (s *Store) Reconcile(ctx context.Context, id uuid.UUID, actual orchestration.Cost, outcome orchestration.AttemptOutcome) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var windowID uuid.UUID
	var calls, bytes, objects int64
	var state string
	if err = tx.QueryRow(ctx, `select budget_window_id,reserved_tool_calls,reserved_result_bytes,reserved_distinct_objects_max,state from ouf_mcp.budget_reservation where attempt_id=$1 for update`, id).Scan(&windowID, &calls, &bytes, &objects, &state); err != nil {
		return err
	}
	if state == "RECONCILED" {
		return tx.Commit(ctx)
	}
	final := "FAILED"
	if outcome.Success {
		final = "SUCCEEDED"
	}
	if actual.DistinctObjects < 0 || actual.DistinctObjects > objects {
		return errors.New("actual distinct objects exceed reservation")
	}
	_, err = tx.Exec(ctx, `update ouf_mcp.budget_window set reserved_tool_calls=reserved_tool_calls-$2,reserved_result_bytes=reserved_result_bytes-$3,reserved_distinct_objects=reserved_distinct_objects-$4,consumed_tool_calls=consumed_tool_calls+$5,consumed_result_bytes=consumed_result_bytes+$6,consumed_distinct_objects=consumed_distinct_objects+$7 where budget_window_id=$1`, windowID, calls, bytes, objects, actual.ToolCalls, actual.ResultBytes, actual.DistinctObjects)
	if err == nil {
		_, err = tx.Exec(ctx, `update ouf_mcp.budget_reservation set state='RECONCILED',actual_tool_calls=$2,actual_result_bytes=$3,actual_distinct_objects=$4,reconciled_at=transaction_timestamp() where attempt_id=$1`, id, actual.ToolCalls, actual.ResultBytes, actual.DistinctObjects)
	}
	if err == nil {
		_, err = tx.Exec(ctx, `update ouf_mcp.tool_attempt set state=$2,dispatch_state='ACKNOWLEDGED',backend_request_id=coalesce(backend_request_id,$3),lease_until=null,completed_at=transaction_timestamp(),lock_version=lock_version+1 where attempt_id=$1`, id, final, outcome.BackendRequestID)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Dispatch(ctx context.Context, id uuid.UUID, backendRequestID string, lease time.Duration, expected int64) error {
	_, err := s.MarkRunning(ctx, id, backendRequestID, lease, expected)
	return err
}
func (s *Store) MarkRunning(ctx context.Context, id uuid.UUID, backendRequestID string, lease time.Duration, expected int64) (Attempt, error) {
	var out Attempt
	err := s.pool.QueryRow(ctx, `update ouf_mcp.tool_attempt set state='RUNNING',dispatch_state='DISPATCHED',backend_request_id=$2,started_at=transaction_timestamp(),lease_until=transaction_timestamp()+$3::interval,lock_version=lock_version+1 where attempt_id=$1 and state='ADMITTED' and dispatch_state='NOT_DISPATCHED' and lock_version=$4 returning attempt_id,state,dispatch_state,request_hash,lock_version`, id, backendRequestID, lease.String(), expected).Scan(&out.ID, &out.State, &out.DispatchState, &out.RequestHash, &out.LockVersion)
	return out, err
}
func (s *Store) AppendAudit(ctx context.Context, eventType, actorType, servicePrincipal string, attemptID *uuid.UUID, manifestChecksum string, safeDetail []byte) error {
	_, err := s.pool.Exec(ctx, `insert into ouf_mcp.audit_event(audit_event_id,event_type,actor_type,service_principal_id,attempt_id,manifest_checksum,safe_detail) values($1,$2,$3,nullif($4,''),$5,nullif($6,''),$7::jsonb)`, uuid.New(), eventType, actorType, servicePrincipal, attemptID, manifestChecksum, string(safeDetail))
	return err
}
func (s *Store) Audit(ctx context.Context, event orchestration.AuditEvent) error {
	detail, err := json.Marshal(map[string]any{"outcomeCode": event.OutcomeCode, "authorizationDecisionRef": event.AuthorizationDecisionRef, "permittedDetailLevel": event.PermittedDetailLevel, "redacted": event.Redacted})
	if err != nil {
		return err
	}
	return s.AppendAudit(ctx, event.EventType, event.Identity.ActorType, event.Identity.ServicePrincipalID, &event.AttemptID, event.ManifestChecksum, detail)
}

type MaintenanceResult struct{ OrphansMarkedUnknown, ExpiredSessionsDeleted int64 }

func (s *Store) Maintain(ctx context.Context, now time.Time) (MaintenanceResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return MaintenanceResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `update ouf_mcp.tool_attempt ta set state='UNKNOWN',dispatch_state='UNKNOWN',unknown_since=coalesce(unknown_since,$1),lease_until=null,lock_version=lock_version+1 where state='RUNNING' and lease_until<$1 and not exists(select 1 from ouf_mcp.attempt_admission_context ac where ac.attempt_id=ta.attempt_id) returning attempt_id`, now)
	if err != nil {
		return MaintenanceResult{}, err
	}
	var stale []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return MaintenanceResult{}, err
		}
		stale = append(stale, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return MaintenanceResult{}, err
	}
	expired, err := tx.Exec(ctx, `delete from ouf_mcp.application_session s where expires_at<$1 and not exists(select 1 from ouf_mcp.tool_attempt a where a.application_session_id=s.application_session_id)`, now)
	if err != nil {
		return MaintenanceResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return MaintenanceResult{}, err
	}
	return MaintenanceResult{int64(len(stale)), expired.RowsAffected()}, nil
}

func (s *Store) MarkStaleAndClaim(ctx context.Context, now, admittedBefore time.Time, worker string, lease time.Duration, limit int) ([]recovery.Candidate, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	admittedRows, err := tx.Query(ctx, `select ta.attempt_id,ac.budget_window_id,ac.equivalence_group_id,br.reserved_tool_calls,br.reserved_result_bytes,br.reserved_distinct_objects_max,ta.service_principal_id,ta.manifest_checksum from ouf_mcp.tool_attempt ta join ouf_mcp.attempt_admission_context ac using(attempt_id) join ouf_mcp.budget_reservation br using(attempt_id) where ta.state='ADMITTED' and ta.dispatch_state='NOT_DISPATCHED' and ta.admitted_at<$1 order by ta.admitted_at,ta.attempt_id limit $2`, admittedBefore, limit)
	if err != nil {
		return nil, err
	}
	type expiredAdmission struct {
		id, window, group     uuid.UUID
		calls, bytes, objects int64
		service, manifest     string
	}
	var expiredAdmissions []expiredAdmission
	for admittedRows.Next() {
		var a expiredAdmission
		if err = admittedRows.Scan(&a.id, &a.window, &a.group, &a.calls, &a.bytes, &a.objects, &a.service, &a.manifest); err != nil {
			admittedRows.Close()
			return nil, err
		}
		expiredAdmissions = append(expiredAdmissions, a)
	}
	admittedRows.Close()
	if err = admittedRows.Err(); err != nil {
		return nil, err
	}
	for _, a := range expiredAdmissions {
		if _, err = tx.Exec(ctx, `select 1 from ouf_mcp.budget_window where budget_window_id=$1 for update`, a.window); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `select 1 from ouf_mcp.retry_guard where equivalence_group_id=$1 for update`, a.group); err != nil {
			return nil, err
		}
		var attemptState string
		if err = tx.QueryRow(ctx, `select state from ouf_mcp.tool_attempt where attempt_id=$1 for update`, a.id).Scan(&attemptState); err != nil {
			return nil, err
		}
		if attemptState != "ADMITTED" {
			continue
		}
		if _, err = tx.Exec(ctx, `update ouf_mcp.budget_window set reserved_tool_calls=reserved_tool_calls-$2,reserved_result_bytes=reserved_result_bytes-$3,reserved_distinct_objects=reserved_distinct_objects-$4 where budget_window_id=$1`, a.window, a.calls, a.bytes, a.objects); err != nil {
			return nil, err
		}
		if err = execOne(ctx, tx, `update ouf_mcp.tool_attempt set state='FAILED',dispatch_state='NOT_DISPATCHED',outcome_code='DISPATCH_NOT_STARTED',completed_at=transaction_timestamp(),lock_version=lock_version+1 where attempt_id=$1 and state='ADMITTED' and dispatch_state='NOT_DISPATCHED'`, a.id); err != nil {
			return nil, err
		}
		if err = execOne(ctx, tx, `update ouf_mcp.budget_reservation set state='RELEASED',actual_tool_calls=0,actual_result_bytes=0,reconciled_at=transaction_timestamp() where attempt_id=$1 and state='RESERVED'`, a.id); err != nil {
			return nil, err
		}
		if err = execOne(ctx, tx, `update ouf_mcp.idempotency_claim set state='COMPLETED',lock_version=lock_version+1 where attempt_id=$1 and state='BOUND'`, a.id); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `insert into ouf_mcp.audit_event(audit_event_id,event_type,actor_type,service_principal_id,attempt_id,manifest_checksum,safe_detail) values($1,'DISPATCH_NOT_STARTED','MAINTENANCE_WORKER',$2,$3,$4,'{}')`, uuid.New(), a.service, a.id, a.manifest); err != nil {
			return nil, err
		}
	}
	rows, err := tx.Query(ctx, `select ta.attempt_id,ac.budget_window_id,ac.equivalence_group_id,br.reserved_distinct_objects_max from ouf_mcp.tool_attempt ta join ouf_mcp.attempt_admission_context ac using(attempt_id) join ouf_mcp.budget_reservation br using(attempt_id) where ta.state='RUNNING' and ta.dispatch_state='DISPATCHED' and ta.lease_until<$1 order by ta.lease_until,ta.attempt_id limit $2`, now, limit)
	if err != nil {
		return nil, err
	}
	type staleDispatch struct {
		id, window, group uuid.UUID
		objects           int64
	}
	var staleDispatches []staleDispatch
	for rows.Next() {
		var stale staleDispatch
		if err = rows.Scan(&stale.id, &stale.window, &stale.group, &stale.objects); err != nil {
			rows.Close()
			return nil, err
		}
		staleDispatches = append(staleDispatches, stale)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for _, stale := range staleDispatches {
		if _, err = tx.Exec(ctx, `select 1 from ouf_mcp.budget_window where budget_window_id=$1 for update`, stale.window); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `select 1 from ouf_mcp.retry_guard where equivalence_group_id=$1 for update`, stale.group); err != nil {
			return nil, err
		}
		var state string
		var leaseUntil *time.Time
		if err = tx.QueryRow(ctx, `select state,lease_until from ouf_mcp.tool_attempt where attempt_id=$1 for update`, stale.id).Scan(&state, &leaseUntil); err != nil {
			return nil, err
		}
		if state != "RUNNING" || leaseUntil == nil || !leaseUntil.Before(now) {
			continue
		}
		if err = execOne(ctx, tx, `update ouf_mcp.tool_attempt set state='UNKNOWN',dispatch_state='UNKNOWN',unknown_since=coalesce(unknown_since,$2),lease_until=null,lock_version=lock_version+1 where attempt_id=$1 and state='RUNNING'`, stale.id, now); err != nil {
			return nil, err
		}
		if err = execOne(ctx, tx, `update ouf_mcp.retry_guard set blocking_attempts=blocking_attempts+1,updated_at=transaction_timestamp() where equivalence_group_id=$1`, stale.group); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `update ouf_mcp.budget_window set reserved_distinct_objects=reserved_distinct_objects-$2,current_unresolved_object_debt_total=current_unresolved_object_debt_total+$2 where budget_window_id=$1`, stale.window, stale.objects); err != nil {
			return nil, err
		}
		if stale.objects > 0 {
			if _, err = tx.Exec(ctx, `insert into ouf_mcp.budget_object_debt(attempt_id,budget_window_id,debt_amount,debt_state) values($1,$2,$3,'ACTIVE')`, stale.id, stale.window, stale.objects); err != nil {
				return nil, err
			}
		}
		if err = execOne(ctx, tx, `update ouf_mcp.budget_reservation set state='UNKNOWN',actual_unresolved_object_debt_max=$2 where attempt_id=$1 and state='RESERVED'`, stale.id, stale.objects); err != nil {
			return nil, err
		}
		if err = execOne(ctx, tx, `update ouf_mcp.idempotency_claim set state='UNKNOWN',lock_version=lock_version+1 where attempt_id=$1 and state='BOUND'`, stale.id); err != nil {
			return nil, err
		}
	}
	rows, err = tx.Query(ctx, `select ta.attempt_id,ta.backend_request_id,ac.owner,ta.capability_id,ta.correlation_id from ouf_mcp.tool_attempt ta join ouf_mcp.attempt_admission_context ac on ac.attempt_id=ta.attempt_id where ta.state='UNKNOWN' and (ta.recovery_lease_until is null or ta.recovery_lease_until<$1) order by ta.unknown_since,ta.attempt_id for update of ta skip locked limit $2`, now, limit)
	if err != nil {
		return nil, err
	}
	var out []recovery.Candidate
	for rows.Next() {
		var c recovery.Candidate
		if err = rows.Scan(&c.AttemptID, &c.BackendRequestID, &c.Owner, &c.CapabilityID, &c.CorrelationID); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, c)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for _, c := range out {
		tag, e := tx.Exec(ctx, `update ouf_mcp.tool_attempt set recovery_owner=$2,recovery_lease_until=$3 where attempt_id=$1 and state='UNKNOWN'`, c.AttemptID, worker, now.Add(lease))
		if e != nil {
			return nil, e
		}
		if tag.RowsAffected() != 1 {
			return nil, errors.New("recovery claim CAS failed")
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) ApplyOwnerEvidence(ctx context.Context, id uuid.UUID, worker string, evidence recovery.OwnerEvidence) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var windowID, groupID uuid.UUID
	if err = tx.QueryRow(ctx, `select budget_window_id,equivalence_group_id from ouf_mcp.attempt_admission_context where attempt_id=$1`, id).Scan(&windowID, &groupID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `select 1 from ouf_mcp.budget_window where budget_window_id=$1 for update`, windowID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `select 1 from ouf_mcp.retry_guard where equivalence_group_id=$1 for update`, groupID); err != nil {
		return err
	}
	var backendRequestID, state, currentOwner string
	if err = tx.QueryRow(ctx, `select backend_request_id,state,coalesce(recovery_owner,'') from ouf_mcp.tool_attempt where attempt_id=$1 for update`, id).Scan(&backendRequestID, &state, &currentOwner); err != nil {
		return err
	}
	if state != "UNKNOWN" {
		return tx.Commit(ctx)
	}
	if currentOwner != worker || backendRequestID != evidence.BackendRequestID {
		return recovery.ErrInvalidOwnerEvidence
	}
	if evidence.Outcome == "UNKNOWN" {
		err = execOne(ctx, tx, `update ouf_mcp.tool_attempt set recovery_owner=null,recovery_lease_until=null where attempt_id=$1 and recovery_owner=$2 and state='UNKNOWN'`, id, worker)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	var reservedCalls, reservedBytes, reservedObjects int64
	var reservationState string
	if err = tx.QueryRow(ctx, `select reserved_tool_calls,reserved_result_bytes,reserved_distinct_objects_max,state from ouf_mcp.budget_reservation where attempt_id=$1 for update`, id).Scan(&reservedCalls, &reservedBytes, &reservedObjects, &reservationState); err != nil {
		return err
	}
	if reservationState != "UNKNOWN" {
		return errors.New("recovery reservation is not UNKNOWN")
	}
	if evidence.ActualToolCalls < 0 || evidence.ActualResultBytes < 0 || evidence.ActualToolCalls > reservedCalls || evidence.ActualResultBytes > reservedBytes {
		return errors.New("owner actual cost exceeds reservation")
	}
	if reservedObjects > 0 {
		return errors.New("object-bearing uncertainty requires verified inbox evidence")
	}
	finalState, dispatchState, reservationFinal := "FAILED", "ACKNOWLEDGED", "RECONCILED"
	consumeCalls, consumeBytes := evidence.ActualToolCalls, evidence.ActualResultBytes
	if evidence.Outcome == "SUCCEEDED" {
		finalState = "SUCCEEDED"
	}
	if evidence.Outcome == "NOT_DISPATCHED" {
		dispatchState = "NOT_DISPATCHED"
		reservationFinal = "RELEASED"
		consumeCalls = 0
		consumeBytes = 0
	}
	if _, err = tx.Exec(ctx, `update ouf_mcp.budget_window set reserved_tool_calls=reserved_tool_calls-$2,reserved_result_bytes=reserved_result_bytes-$3,consumed_tool_calls=consumed_tool_calls+$4,consumed_result_bytes=consumed_result_bytes+$5 where budget_window_id=$1`, windowID, reservedCalls, reservedBytes, consumeCalls, consumeBytes); err != nil {
		return err
	}
	if err = execOne(ctx, tx, `update ouf_mcp.retry_guard set blocking_attempts=blocking_attempts-1,updated_at=transaction_timestamp() where equivalence_group_id=$1 and blocking_attempts>0`, groupID); err != nil {
		return err
	}
	if err = execOne(ctx, tx, `update ouf_mcp.tool_attempt set state=$3,dispatch_state=$4,outcome_code=$5,result_ref=nullif($6,''),completed_at=transaction_timestamp(),recovery_owner=null,recovery_lease_until=null,lock_version=lock_version+1 where attempt_id=$1 and recovery_owner=$2 and state='UNKNOWN'`, id, worker, finalState, dispatchState, evidence.OutcomeCode, evidence.ResultRef); err != nil {
		return err
	}
	if err = execOne(ctx, tx, `update ouf_mcp.budget_reservation set state=$2,actual_tool_calls=$3,actual_result_bytes=$4,actual_distinct_objects=0,reconciled_at=transaction_timestamp() where attempt_id=$1 and state='UNKNOWN'`, id, reservationFinal, consumeCalls, consumeBytes); err != nil {
		return err
	}
	if err = execOne(ctx, tx, `update ouf_mcp.idempotency_claim set state='COMPLETED',lock_version=lock_version+1 where attempt_id=$1 and state='UNKNOWN'`, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func execOne(ctx context.Context, tx pgx.Tx, sql string, arguments ...any) error {
	tag, err := tx.Exec(ctx, sql, arguments...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("recovery CAS affected an unexpected row count")
	}
	return nil
}

func (s *Store) AppendRecoveryAudit(ctx context.Context, id uuid.UUID, outcome, code string) error {
	return s.AppendAudit(ctx, "ATTEMPT_RECOVERY_"+outcome, "MAINTENANCE_WORKER", "", &id, "", []byte(fmt.Sprintf(`{"outcomeCode":%q}`, code)))
}
