package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/operational"
	"github.com/google/uuid"
)

func cursorFixture(t *testing.T) (*Store, operational.CursorBinding, operational.CursorFrame) {
	t.Helper()
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = Migrate(ctx, store.Pool()); err != nil {
		store.Close()
		t.Fatal(err)
	}
	binding := operational.CursorBinding{PrincipalID: "operator", TenantID: "cursor-" + uuid.NewString(), Fingerprint: hash64("identity"), PolicyRef: "policy:1", ManifestChecksum: hash64("manifest")}
	frame := operational.CursorFrame{Profile: operational.CursorProfile, Filters: []byte(`{"sourceId":"source-a","limit":2}`),
		PageLimit: 2, SelfStatus: "HEALTHY", Owners: []operational.OwnerPosition{
			{Name: "INGESTION", Cursor: uuid.NewString(), ItemsDigest: hash64("ingestion-items"), OwnerDigest: hash64("ingestion-owner"), SnapshotRef: uuid.NewString()},
			{Name: "GATEWAY", Cursor: uuid.NewString(), ItemsDigest: hash64("gateway-items"), OwnerDigest: hash64("gateway-owner"), SnapshotRef: uuid.NewString()},
		}}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), "delete from ouf_mcp.operational_cursor_session where tenant_id=$1", binding.TenantID)
		store.Close()
	})
	return store, binding, frame
}

func nextCursorFrame(frame operational.CursorFrame) operational.CursorFrame {
	next := frame
	next.Owners = append([]operational.OwnerPosition(nil), frame.Owners...)
	next.PageNumber++
	next.Owners[0].Offset++
	return next
}

func TestIncidentCursorMetadataDurableAndIdentityBound(t *testing.T) {
	store, binding, frame := cursorFixture(t)
	ctx := context.Background()
	node, err := store.CreateIncidentCursor(ctx, binding, frame)
	if err != nil {
		t.Fatal(err)
	}
	other, err := Open(ctx, os.Getenv("MCP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	loaded, err := other.LoadIncidentCursor(ctx, node.Token, binding)
	if err != nil || loaded.Session != node.Session || loaded.Frame.PageLimit != 2 || loaded.NextToken != nil || loaded.PageDigest != "" {
		t.Fatalf("durable cursor: %+v %v", loaded, err)
	}
	for _, mutate := range []func(*operational.CursorBinding){
		func(b *operational.CursorBinding) { b.PrincipalID = "other" },
		func(b *operational.CursorBinding) { b.TenantID = "other" },
		func(b *operational.CursorBinding) { b.Fingerprint = hash64("changed-authentication") },
		func(b *operational.CursorBinding) { b.PolicyRef = "policy:2" },
		func(b *operational.CursorBinding) { b.ManifestChecksum = hash64("changed-manifest") },
	} {
		changed := binding
		mutate(&changed)
		if _, err = store.LoadIncidentCursor(ctx, node.Token, changed); !errors.Is(err, operational.ErrCursorNotFound) {
			t.Fatalf("binding accepted: %v", err)
		}
	}
}

func TestIncidentCursorConcurrentReplayAndChangedResultFenced(t *testing.T) {
	store, binding, frame := cursorFixture(t)
	ctx := context.Background()
	node, err := store.CreateIncidentCursor(ctx, binding, frame)
	if err != nil {
		t.Fatal(err)
	}
	next := nextCursorFrame(frame)
	digest := hash64("delivered-page-and-next-state")
	var wg sync.WaitGroup
	results := make(chan *uuid.UUID, 12)
	failures := make(chan error, 12)
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, err := store.CommitIncidentCursorPage(ctx, node.Token, binding, digest, &next)
			if err != nil {
				failures <- err
			} else {
				results <- token
			}
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	var winner *uuid.UUID
	for token := range results {
		if token == nil {
			t.Fatal("missing next cursor")
		}
		if winner != nil && *winner != *token {
			t.Fatal("concurrent replay diverged")
		}
		winner = token
	}
	if winner == nil {
		t.Fatal("no committed cursor")
	}
	for err := range failures {
		if !errors.Is(err, operational.ErrCursorCapacity) {
			t.Fatal(err)
		}
	}
	replay, err := store.CommitIncidentCursorPage(ctx, node.Token, binding, digest, &next)
	if err != nil || replay == nil || *replay != *winner {
		t.Fatalf("replay=%v err=%v", replay, err)
	}
	if _, err = store.CommitIncidentCursorPage(ctx, node.Token, binding, hash64("changed-visible-items"), &next); !errors.Is(err, operational.ErrCursorConflict) {
		t.Fatal("changed digest accepted")
	}
	changed := nextCursorFrame(frame)
	changed.Owners[0].Offset++
	if _, err = store.CommitIncidentCursorPage(ctx, node.Token, binding, digest, &changed); !errors.Is(err, operational.ErrCursorConflict) {
		t.Fatal("different position replay accepted")
	}
	var count int
	if err = store.Pool().QueryRow(ctx, "select count(*) from ouf_mcp.operational_cursor_node where session_id=$1", node.Session).Scan(&count); err != nil || count != 2 {
		t.Fatalf("node count=%d err=%v", count, err)
	}
}

func TestIncidentCursorFinalPageAndScopeMutationCannotReopen(t *testing.T) {
	store, binding, frame := cursorFixture(t)
	ctx := context.Background()
	node, err := store.CreateIncidentCursor(ctx, binding, frame)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*operational.CursorFrame){
		func(f *operational.CursorFrame) { f.Filters = []byte(`{"sourceId":"different"}`) },
		func(f *operational.CursorFrame) { f.PageLimit = 3 },
		func(f *operational.CursorFrame) { f.SelfStatus = "DEGRADED" },
		func(f *operational.CursorFrame) { f.Owners[0].SnapshotRef = uuid.NewString() },
		func(f *operational.CursorFrame) { f.Owners[0].Offset = -1 },
		func(f *operational.CursorFrame) { f.Owners[0].PageIndex = 2 },
	} {
		next := nextCursorFrame(frame)
		mutate(&next)
		if _, err = store.CommitIncidentCursorPage(ctx, node.Token, binding, hash64("page"), &next); !errors.Is(err, operational.ErrCursorConflict) {
			t.Fatalf("scope change accepted: %v", err)
		}
	}
	if token, err := store.CommitIncidentCursorPage(ctx, node.Token, binding, hash64("final"), nil); err != nil || token != nil {
		t.Fatalf("final=%v err=%v", token, err)
	}
	if _, err = store.CommitIncidentCursorPage(ctx, node.Token, binding, hash64("final"), nil); err != nil {
		t.Fatal(err)
	}
	next := nextCursorFrame(frame)
	if _, err = store.CommitIncidentCursorPage(ctx, node.Token, binding, hash64("final"), &next); !errors.Is(err, operational.ErrCursorConflict) {
		t.Fatal("closed page reopened")
	}
}

func TestIncidentCursorSQLMetadataAndExpiryImmutable(t *testing.T) {
	store, binding, frame := cursorFixture(t)
	ctx := context.Background()
	node, err := store.CreateIncidentCursor(ctx, binding, frame)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Pool().Exec(ctx, "update ouf_mcp.operational_cursor_node set metadata=metadata||'{\"producerPayload\":\"forged\"}'::jsonb where token=$1", node.Token); err == nil {
		t.Fatal("metadata changed")
	}
	if _, err = store.Pool().Exec(ctx, "update ouf_mcp.operational_cursor_session set expires_at=expires_at+interval '1 day' where session_id=$1", node.Session); err == nil {
		t.Fatal("snapshot expiry renewed")
	}
	next := nextCursorFrame(frame)
	token, err := store.CommitIncidentCursorPage(ctx, node.Token, binding, hash64("page"), &next)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Pool().Exec(ctx, "update ouf_mcp.operational_cursor_node set page_digest=$2 where token=$1", node.Token, hash64("replace")); err == nil {
		t.Fatal("result overwritten")
	}
	if _, err = store.Pool().Exec(ctx, "delete from ouf_mcp.operational_cursor_node where token=$1", token); err == nil {
		t.Fatal("continuation target deleted")
	}
}

func TestIncidentCursorExpiryCleanupAndFinitePrincipalCapacity(t *testing.T) {
	store, binding, frame := cursorFixture(t)
	ctx := context.Background()
	var first operational.CursorNode
	for range 16 {
		node, err := store.CreateIncidentCursor(ctx, binding, frame)
		if err != nil {
			t.Fatal(err)
		}
		first = node
	}
	if _, err := store.CreateIncidentCursor(ctx, binding, frame); !errors.Is(err, operational.ErrCursorCapacity) {
		t.Fatal("principal capacity missing")
	}
	var count int
	if err := store.Pool().QueryRow(ctx, "select count(*) from ouf_mcp.operational_cursor_session where tenant_id=$1", binding.TenantID).Scan(&count); err != nil || count != 16 {
		t.Fatal("partial failed admission persisted")
	}
	if _, err := store.LoadIncidentCursor(ctx, first.Token, binding); err != nil {
		t.Fatal("capacity erased existing state")
	}
	// Seed a historical expired session at creation; immutable expiry is never bypassed.
	expiredSession, expiredToken := uuid.New(), uuid.New()
	metadata, _ := json.Marshal(frame)
	_, err := store.Pool().Exec(ctx, `insert into ouf_mcp.operational_cursor_session(session_id,principal_id,tenant_id,identity_fingerprint,policy_ref,manifest_checksum,profile,created_at,expires_at)
	 values($1,$2,$3,$4,$5,$6,$7,clock_timestamp()-interval '2 hours',clock_timestamp()-interval '1 hour')`, expiredSession, binding.PrincipalID, binding.TenantID, binding.Fingerprint, binding.PolicyRef, binding.ManifestChecksum, operational.CursorProfile)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Pool().Exec(ctx, "insert into ouf_mcp.operational_cursor_node(token,session_id,page_number,metadata) values($1,$2,0,$3::jsonb)", expiredToken, expiredSession, string(metadata))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.LoadIncidentCursor(ctx, expiredToken, binding); !errors.Is(err, operational.ErrCursorExpired) {
		t.Fatalf("expired accepted: %v", err)
	}
	// Another principal can admit and clean the one expired session, preserving the first 16.
	other := binding
	other.PrincipalID = "independent"
	if _, err = store.CreateIncidentCursor(ctx, other, frame); err != nil {
		t.Fatal(err)
	}
	if _, err = store.LoadIncidentCursor(ctx, expiredToken, binding); !errors.Is(err, operational.ErrCursorNotFound) {
		t.Fatal("expired metadata retained after bounded cleanup")
	}
}

func TestIncidentCursorMetadataRejectsUnsupportedPayloadAndPageBudgets(t *testing.T) {
	store, binding, frame := cursorFixture(t)
	ctx := context.Background()
	for _, mutate := range []func(*operational.CursorFrame){
		func(f *operational.CursorFrame) { f.Filters = []byte(`{"producerPayload":"forbidden"}`) },
		func(f *operational.CursorFrame) { f.Profile = "unknown" },
		func(f *operational.CursorFrame) { f.Owners[0].ItemsDigest = "raw-secret" },
		func(f *operational.CursorFrame) { f.Owners[1].Name = "INGESTION" },
		func(f *operational.CursorFrame) { f.PageLimit = 101 },
		func(f *operational.CursorFrame) { f.PageNumber = 201 },
		func(f *operational.CursorFrame) {
			f.Filters = []byte(`{"sourceId":"` + strings.Repeat("x", 201) + `"}`)
		},
	} {
		changed := nextCursorFrame(frame)
		changed.PageNumber = 0
		mutate(&changed)
		if _, err := store.CreateIncidentCursor(ctx, binding, changed); err == nil {
			t.Fatal("unsupported metadata accepted")
		}
	}
}
