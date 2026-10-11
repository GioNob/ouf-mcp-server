package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/operational"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var cursorDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validCursorBinding(binding operational.CursorBinding) bool {
	return binding.PrincipalID != "" && len(binding.PrincipalID) <= 200 && binding.TenantID != "" && len(binding.TenantID) <= 200 &&
		cursorDigestPattern.MatchString(binding.Fingerprint) && binding.PolicyRef != "" && len(binding.PolicyRef) <= 512 && cursorDigestPattern.MatchString(binding.ManifestChecksum)
}

func (s *Store) CreateIncidentCursor(ctx context.Context, binding operational.CursorBinding, frame operational.CursorFrame) (operational.CursorNode, error) {
	if !validCursorBinding(binding) || frame.PageNumber != 0 {
		return operational.CursorNode{}, operational.ErrCursorConflict
	}
	if err := operational.ValidateCursorFrame(frame); err != nil {
		return operational.CursorNode{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return operational.CursorNode{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err = cursorCapacityLock(ctx, tx); err != nil {
		return operational.CursorNode{}, err
	}
	// One expired session per admission; never erase operator or incident history.
	_, err = tx.Exec(ctx, `delete from ouf_mcp.operational_cursor_session where session_id in
	 (select session_id from ouf_mcp.operational_cursor_session where expires_at<=clock_timestamp() order by expires_at limit 1 for update skip locked)`)
	if err != nil {
		return operational.CursorNode{}, err
	}
	var total, tenant, principal int
	if err = tx.QueryRow(ctx, `select count(*),count(*) filter(where tenant_id=$1),count(*) filter(where tenant_id=$1 and principal_id=$2)
	 from ouf_mcp.operational_cursor_session`, binding.TenantID, binding.PrincipalID).Scan(&total, &tenant, &principal); err != nil {
		return operational.CursorNode{}, err
	}
	if total >= 4096 || tenant >= 128 || principal >= 16 {
		return operational.CursorNode{}, operational.ErrCursorCapacity
	}
	if err = cursorNodeCapacity(ctx, tx, binding); err != nil {
		return operational.CursorNode{}, err
	}
	node := operational.CursorNode{Token: uuid.New(), Session: uuid.New(), Frame: frame}
	err = tx.QueryRow(ctx, `insert into ouf_mcp.operational_cursor_session(session_id,principal_id,tenant_id,identity_fingerprint,policy_ref,manifest_checksum,profile)
	 values($1,$2,$3,$4,$5,$6,$7) returning expires_at`, node.Session, binding.PrincipalID, binding.TenantID, binding.Fingerprint, binding.PolicyRef, binding.ManifestChecksum, operational.CursorProfile).Scan(&node.ExpiresAt)
	if err != nil {
		return operational.CursorNode{}, err
	}
	metadata, err := json.Marshal(frame)
	if err != nil {
		return operational.CursorNode{}, err
	}
	_, err = tx.Exec(ctx, `insert into ouf_mcp.operational_cursor_node(token,session_id,page_number,metadata) values($1,$2,0,$3::jsonb)`, node.Token, node.Session, string(metadata))
	if err != nil {
		return operational.CursorNode{}, err
	}
	return node, tx.Commit(ctx)
}

func (s *Store) LoadIncidentCursor(ctx context.Context, token uuid.UUID, binding operational.CursorBinding) (operational.CursorNode, error) {
	if !validCursorBinding(binding) || token == uuid.Nil {
		return operational.CursorNode{}, operational.ErrCursorNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	node := operational.CursorNode{Token: token}
	var metadata []byte
	var valid bool
	err := s.pool.QueryRow(ctx, `select n.session_id,n.metadata,s.expires_at,n.next_token,coalesce(n.page_digest,''),s.expires_at>clock_timestamp()
	 from ouf_mcp.operational_cursor_node n join ouf_mcp.operational_cursor_session s using(session_id)
	 where n.token=$1 and s.principal_id=$2 and s.tenant_id=$3 and s.identity_fingerprint=$4 and s.policy_ref=$5 and s.manifest_checksum=$6`,
		token, binding.PrincipalID, binding.TenantID, binding.Fingerprint, binding.PolicyRef, binding.ManifestChecksum).Scan(&node.Session, &metadata, &node.ExpiresAt, &node.NextToken, &node.PageDigest, &valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return operational.CursorNode{}, operational.ErrCursorNotFound
	}
	if err != nil {
		return operational.CursorNode{}, err
	}
	if !valid {
		return operational.CursorNode{}, operational.ErrCursorExpired
	}
	if len(metadata) > 16<<10 || json.Unmarshal(metadata, &node.Frame) != nil {
		return operational.CursorNode{}, operational.ErrCursorConflict
	}
	if err = operational.ValidateCursorFrame(node.Frame); err != nil {
		return operational.CursorNode{}, err
	}
	return node, nil
}

func (s *Store) CommitIncidentCursorPage(ctx context.Context, token uuid.UUID, binding operational.CursorBinding, digest string, next *operational.CursorFrame) (*uuid.UUID, error) {
	if len(digest) != 64 || !validCursorBinding(binding) {
		return nil, operational.ErrCursorConflict
	}
	if next != nil {
		if err := operational.ValidateCursorFrame(*next); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err = cursorCapacityLock(ctx, tx); err != nil {
		return nil, err
	}
	var session uuid.UUID
	var number int
	var existing string
	var nextToken *uuid.UUID
	var metadata []byte
	err = tx.QueryRow(ctx, `select n.session_id,n.page_number,coalesce(n.page_digest,''),n.next_token,n.metadata
	 from ouf_mcp.operational_cursor_node n join ouf_mcp.operational_cursor_session s using(session_id)
	 where n.token=$1 and s.principal_id=$2 and s.tenant_id=$3 and s.identity_fingerprint=$4 and s.policy_ref=$5 and s.manifest_checksum=$6
	 and s.expires_at>clock_timestamp() for update of n`, token, binding.PrincipalID, binding.TenantID, binding.Fingerprint, binding.PolicyRef, binding.ManifestChecksum).Scan(&session, &number, &existing, &nextToken, &metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, operational.ErrCursorNotFound
	}
	if err != nil {
		return nil, err
	}
	if existing != "" {
		if existing != digest || (next == nil) != (nextToken == nil) {
			return nil, operational.ErrCursorConflict
		}
		if next != nil {
			var retained []byte
			if err = tx.QueryRow(ctx, "select metadata from ouf_mcp.operational_cursor_node where token=$1", nextToken).Scan(&retained); err != nil {
				return nil, err
			}
			provided, marshalErr := json.Marshal(next)
			if marshalErr != nil || !sameMetadataJSON(retained, provided) {
				return nil, operational.ErrCursorConflict
			}
		}
		return nextToken, tx.Commit(ctx)
	}
	if next != nil {
		var old operational.CursorFrame
		if json.Unmarshal(metadata, &old) != nil || next.PageNumber != number+1 || !sameCursorScope(old, *next) {
			return nil, operational.ErrCursorConflict
		}
		if err = cursorNodeCapacity(ctx, tx, binding); err != nil {
			return nil, err
		}
		candidate := uuid.New()
		nextToken = &candidate
		payload, marshalErr := json.Marshal(next)
		if marshalErr != nil {
			return nil, marshalErr
		}
		_, err = tx.Exec(ctx, `insert into ouf_mcp.operational_cursor_node(token,session_id,page_number,metadata) values($1,$2,$3,$4::jsonb)`, candidate, session, next.PageNumber, string(payload))
		if err != nil {
			return nil, err
		}
	}
	_, err = tx.Exec(ctx, `update ouf_mcp.operational_cursor_node set page_digest=$2,next_token=$3 where token=$1 and page_digest is null`, token, digest, nextToken)
	if err != nil {
		return nil, err
	}
	return nextToken, tx.Commit(ctx)
}

func cursorCapacityLock(ctx context.Context, tx pgx.Tx) error {
	var locked bool
	if err := tx.QueryRow(ctx, `select pg_try_advisory_xact_lock(73012028008)`).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return operational.ErrCursorCapacity
	}
	return nil
}

func cursorNodeCapacity(ctx context.Context, tx pgx.Tx, binding operational.CursorBinding) error {
	var total, tenant, principal int
	err := tx.QueryRow(ctx, `select count(*),count(*) filter(where s.tenant_id=$1),count(*) filter(where s.tenant_id=$1 and s.principal_id=$2)
	 from ouf_mcp.operational_cursor_node n join ouf_mcp.operational_cursor_session s using(session_id)`, binding.TenantID, binding.PrincipalID).Scan(&total, &tenant, &principal)
	if err != nil {
		return err
	}
	if total >= 32768 || tenant >= 4096 || principal >= 800 {
		return operational.ErrCursorCapacity
	}
	return nil
}

func sameCursorScope(old, next operational.CursorFrame) bool {
	var a, b any
	if json.Unmarshal(old.Filters, &a) != nil || json.Unmarshal(next.Filters, &b) != nil {
		return false
	}
	encodedA, _ := json.Marshal(a)
	encodedB, _ := json.Marshal(b)
	if string(encodedA) != string(encodedB) || old.Profile != next.Profile || old.PageLimit != next.PageLimit || old.SelfStatus != next.SelfStatus || old.SelfActionRequired != next.SelfActionRequired || old.Partial && !next.Partial || old.SelfConsumed && !next.SelfConsumed {
		return false
	}
	for _, previous := range old.Owners {
		found := false
		for _, current := range next.Owners {
			if previous.Name == current.Name {
				found = current.SnapshotRef == previous.SnapshotRef && (!previous.Done || current.Done) &&
					current.PageIndex >= previous.PageIndex && current.PageIndex <= previous.PageIndex+1
				if current.PageIndex == previous.PageIndex {
					found = found && current.Cursor == previous.Cursor && current.ItemsDigest == previous.ItemsDigest && current.OwnerDigest == previous.OwnerDigest && current.Offset >= previous.Offset
				}
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func sameMetadataJSON(a, b []byte) bool {
	var first, second any
	if json.Unmarshal(a, &first) != nil || json.Unmarshal(b, &second) != nil {
		return false
	}
	left, errA := json.Marshal(first)
	right, errB := json.Marshal(second)
	return errA == nil && errB == nil && string(left) == string(right)
}
