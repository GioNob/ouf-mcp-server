package postgres

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
)

func TestReferentialRetentionChainPurgesTerminalGraph(t *testing.T) {
	s, ctx := maintenanceStore(t)
	checksum := fmt.Sprintf("%064x", 71)
	window, group, attempt := uuid.New(), uuid.New(), uuid.New()
	principal := uuid.NewString()
	_, _ = s.Pool().Exec(ctx, `insert into ouf_mcp.manifest_snapshot(manifest_checksum,manifest_version,schema_version,payload) values($1,'retention','mcp-manifest-v1','{}') on conflict do nothing`, checksum)
	_, err := s.Pool().Exec(ctx, `insert into ouf_mcp.budget_window(budget_window_id,service_principal_id,principal_id,tenant_id,policy_ref,window_start,window_end) values($1,'retention',$2,'tenant',$3,transaction_timestamp()-interval '3 days',transaction_timestamp()-interval '2 days')`, window, principal, checksum)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Pool().Exec(ctx, `insert into ouf_mcp.retry_equivalence_group(equivalence_group_id,budget_window_id,capability_id,fingerprint_version,semantic_fingerprint) values($1,$2,'test','v1',$3)`, group, window, fmt.Sprintf("%064x", 72))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Pool().Exec(ctx, `insert into ouf_mcp.retry_guard(equivalence_group_id) values($1)`, group)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Pool().Exec(ctx, `insert into ouf_mcp.tool_attempt(attempt_id,service_principal_id,principal_id,tenant_id,capability_id,operation_class,manifest_checksum,idempotency_key,request_hash,correlation_id,state,dispatch_state,completed_at) values($1,'retention',$2,'tenant','test','READ',$3,$4,$5,$6,'FAILED','NOT_DISPATCHED',transaction_timestamp()-interval '2 days')`, attempt, principal, checksum, uuid.NewString(), fmt.Sprintf("%064x", 73), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Pool().Exec(ctx, `insert into ouf_mcp.attempt_admission_context(attempt_id,budget_window_id,equivalence_group_id,owner,actor_type,authentication_context_ref,authorization_decision_ref,semantic_fingerprint,fingerprint_version) values($1,$2,$3,'udp','SERVICE','authn','authz',$4,'v1')`, attempt, window, group, fmt.Sprintf("%064x", 72))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Pool().Exec(ctx, `insert into ouf_mcp.idempotency_claim(service_principal_id,principal_id,tenant_id,owner,capability_id,operation_class,idempotency_key,request_hash,attempt_id,state) values('retention',$1,'tenant','udp','test','READ',$2,$3,$4,'COMPLETED')`, principal, uuid.NewString(), fmt.Sprintf("%064x", 73), attempt)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Pool().Exec(ctx, `insert into ouf_mcp.budget_reservation(reservation_id,attempt_id,budget_window_id,reserved_tool_calls,reserved_result_bytes,state,reconciled_at) values($1,$2,$3,1,0,'RELEASED',transaction_timestamp()-interval '2 days')`, uuid.New(), attempt, window)
	if err != nil {
		t.Fatal(err)
	}

	var n int
	if err := s.Pool().QueryRow(ctx, `select ouf_mcp.purge_terminal_attempt_graph(transaction_timestamp()-interval '1 day',10)`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("purged=%d want=1", n)
	}

	for name, query := range map[string]string{
		"attempt": `select count(*) from ouf_mcp.tool_attempt where attempt_id=$1`,
		"window":  `select count(*) from ouf_mcp.budget_window where budget_window_id=$1`,
		"group":   `select count(*) from ouf_mcp.retry_equivalence_group where equivalence_group_id=$1`,
	} {
		var count int
		arg := any(attempt)
		if name == "window" {
			arg = window
		}
		if name == "group" {
			arg = group
		}
		if err := s.Pool().QueryRow(ctx, query, arg).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("%s still present", name)
		}
	}
	var manifests int
	if err := s.Pool().QueryRow(ctx, `select count(*) from ouf_mcp.manifest_snapshot where manifest_checksum=$1`, checksum).Scan(&manifests); err != nil {
		t.Fatal(err)
	}
	if manifests != 1 {
		t.Fatal("manifest snapshot immutable retention boundary was violated")
	}
}

func TestReferentialRetentionChainRetainsRecentOrphanParents(t *testing.T) {
	s, ctx := maintenanceStore(t)
	checksum := fmt.Sprintf("%064x", 81)
	window, group := uuid.New(), uuid.New()
	principal := uuid.NewString()
	_, _ = s.Pool().Exec(ctx, `insert into ouf_mcp.manifest_snapshot(manifest_checksum,manifest_version,schema_version,payload) values($1,'retention-age','mcp-manifest-v1','{}') on conflict do nothing`, checksum)
	if _, err := s.Pool().Exec(ctx, `insert into ouf_mcp.budget_window(budget_window_id,service_principal_id,principal_id,tenant_id,policy_ref,window_start,window_end) values($1,'retention-age',$2,'tenant',$3,transaction_timestamp()-interval '1 hour',transaction_timestamp()+interval '1 hour')`, window, principal, checksum); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `insert into ouf_mcp.retry_equivalence_group(equivalence_group_id,budget_window_id,capability_id,fingerprint_version,semantic_fingerprint) values($1,$2,'recent','v1',$3)`, group, window, fmt.Sprintf("%064x", 82)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `insert into ouf_mcp.retry_guard(equivalence_group_id) values($1)`, group); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool().Exec(ctx, `insert into ouf_mcp.budget_distinct_object(budget_window_id,object_hash_key_version,object_id_hash) values($1,'v1','v1:hmac-sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA')`, window); err != nil {
		t.Fatal(err)
	}

	var n int
	if err := s.Pool().QueryRow(ctx, `select ouf_mcp.purge_terminal_attempt_graph(transaction_timestamp()-interval '1 day',10)`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("purged attempts=%d want=0", n)
	}

	for name, query := range map[string]string{
		"guard":  `select count(*) from ouf_mcp.retry_guard where equivalence_group_id=$1`,
		"group":  `select count(*) from ouf_mcp.retry_equivalence_group where equivalence_group_id=$1`,
		"object": `select count(*) from ouf_mcp.budget_distinct_object where budget_window_id=$1`,
		"window": `select count(*) from ouf_mcp.budget_window where budget_window_id=$1`,
	} {
		var count int
		arg := any(group)
		if name == "object" || name == "window" {
			arg = window
		}
		if err := s.Pool().QueryRow(ctx, query, arg).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("recent %s was deleted", name)
		}
	}
}

func TestReferentialRetentionChainRejectsUnboundedBatch(t *testing.T) {
	s, ctx := maintenanceStore(t)
	if _, err := s.Pool().Exec(ctx, `select ouf_mcp.purge_terminal_attempt_graph(transaction_timestamp()-interval '1 day',1001)`); err == nil {
		t.Fatal("unbounded referential retention batch accepted")
	}
}
