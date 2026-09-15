package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrIdempotencyConflict = errors.New("idempotency key reused with different request")
var ErrBudgetExhausted = errors.New("orchestration budget exhausted")

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

func (s *Store) Reserve(ctx context.Context, in orchestration.AdmissionRequest) (orchestration.AdmissionDecision, error) {
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
	var reservedCalls, consumedCalls, reservedBytes, consumedBytes int64
	if err = tx.QueryRow(ctx, `select reserved_tool_calls,consumed_tool_calls,reserved_result_bytes,consumed_result_bytes from ouf_mcp.budget_window where budget_window_id=$1 for update`, windowID).Scan(&reservedCalls, &consumedCalls, &reservedBytes, &consumedBytes); err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	if reservedCalls+consumedCalls+in.Maximum.ToolCalls > in.Maximum.ToolCalls*int64(in.RetryThreshold) || reservedBytes+consumedBytes+in.Maximum.ResultBytes > in.Maximum.ResultBytes*int64(in.RetryThreshold) {
		return orchestration.AdmissionDecision{}, ErrBudgetExhausted
	}
	groupID := uuid.New()
	err = tx.QueryRow(ctx, `insert into ouf_mcp.retry_equivalence_group(equivalence_group_id,budget_window_id,capability_id,fingerprint_version,semantic_fingerprint) values($1,$2,$3,$4,$5) on conflict(budget_window_id,capability_id,fingerprint_version,semantic_fingerprint) do update set semantic_fingerprint=excluded.semantic_fingerprint returning equivalence_group_id`, groupID, windowID, in.CapabilityID, in.FingerprintVersion, in.SemanticFingerprint).Scan(&groupID)
	if err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	var attempts int
	var blocked bool
	err = tx.QueryRow(ctx, `insert into ouf_mcp.retry_guard(equivalence_group_id,equivalent_attempts) values($1,1) on conflict(equivalence_group_id) do update set equivalent_attempts=ouf_mcp.retry_guard.equivalent_attempts+1,updated_at=transaction_timestamp() returning equivalent_attempts,blocked`, groupID).Scan(&attempts, &blocked)
	if err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	if blocked || attempts >= in.RetryThreshold {
		_, _ = tx.Exec(ctx, `update ouf_mcp.retry_guard set blocked=true where equivalence_group_id=$1`, groupID)
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
		_, err = tx.Exec(ctx, `insert into ouf_mcp.attempt_admission_context values($1,$2,$3,$4,$5,$6,$7,$8,$9)`, in.AttemptID, windowID, groupID, in.Owner, in.Identity.ActorType, in.Identity.AuthenticationContextRef, in.AuthorizationDecisionRef, in.SemanticFingerprint, in.FingerprintVersion)
	}
	if err == nil {
		_, err = tx.Exec(ctx, `insert into ouf_mcp.budget_reservation values($1,$2,$3,$4,$5,'RESERVED',transaction_timestamp(),null)`, uuid.New(), in.AttemptID, windowID, in.Maximum.ToolCalls, in.Maximum.ResultBytes)
	}
	if err == nil {
		_, err = tx.Exec(ctx, `update ouf_mcp.budget_window set reserved_tool_calls=reserved_tool_calls+$2,reserved_result_bytes=reserved_result_bytes+$3 where budget_window_id=$1`, windowID, in.Maximum.ToolCalls, in.Maximum.ResultBytes)
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
	var calls, bytes int64
	var state string
	if err = tx.QueryRow(ctx, `select budget_window_id,reserved_tool_calls,reserved_result_bytes,state from ouf_mcp.budget_reservation where attempt_id=$1 for update`, id).Scan(&windowID, &calls, &bytes, &state); err != nil {
		return err
	}
	if state == "RECONCILED" {
		return tx.Commit(ctx)
	}
	final := "FAILED"
	if outcome.Success {
		final = "SUCCEEDED"
	}
	_, err = tx.Exec(ctx, `update ouf_mcp.budget_window set reserved_tool_calls=reserved_tool_calls-$2,reserved_result_bytes=reserved_result_bytes-$3,consumed_tool_calls=consumed_tool_calls+$4,consumed_result_bytes=consumed_result_bytes+$5 where budget_window_id=$1; update ouf_mcp.budget_reservation set state='RECONCILED',reconciled_at=transaction_timestamp() where attempt_id=$6; update ouf_mcp.tool_attempt set state=$7,dispatch_state='CONFIRMED',backend_request_id=coalesce(backend_request_id,$8),lease_until=null,completed_at=transaction_timestamp(),lock_version=lock_version+1 where attempt_id=$6`, windowID, calls, bytes, actual.ToolCalls, actual.ResultBytes, id, final, outcome.BackendRequestID)
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
	detail := fmt.Sprintf(`{"outcomeCode":%q}`, event.OutcomeCode)
	return s.AppendAudit(ctx, event.EventType, event.Identity.ActorType, event.Identity.ServicePrincipalID, &event.AttemptID, event.ManifestChecksum, []byte(detail))
}

type MaintenanceResult struct{ OrphansMarkedUnknown, ExpiredSessionsDeleted int64 }

func (s *Store) Maintain(ctx context.Context, now time.Time) (MaintenanceResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return MaintenanceResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	orphans, err := tx.Exec(ctx, `update ouf_mcp.tool_attempt set state='UNKNOWN',dispatch_state='UNKNOWN',lease_until=null,lock_version=lock_version+1 where state='RUNNING' and lease_until<$1`, now)
	if err != nil {
		return MaintenanceResult{}, err
	}
	expired, err := tx.Exec(ctx, `delete from ouf_mcp.application_session s where expires_at<$1 and not exists(select 1 from ouf_mcp.tool_attempt a where a.application_session_id=s.application_session_id)`, now)
	if err != nil {
		return MaintenanceResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return MaintenanceResult{}, err
	}
	return MaintenanceResult{orphans.RowsAffected(), expired.RowsAffected()}, nil
}
