package postgres

import (
	"sync"
	"testing"
)

func TestAuditRetentionDirectDeleteDeniedAndDefinerCallable(t *testing.T) {
	s, ctx := maintenanceStore(t)
	if _, err := s.Pool().Exec(ctx, `delete from ouf_mcp.audit_event where false`); err == nil {
		t.Fatal("direct audit delete unexpectedly allowed")
	}
	var execute bool
	if err := s.Pool().QueryRow(ctx, `select has_function_privilege('ouf_mcp_maintenance_role','ouf_mcp.purge_audit_events(timestamptz,integer)','EXECUTE')`).Scan(&execute); err != nil {
		t.Fatal(err)
	}
	if !execute {
		t.Fatal("maintenance role lacks governed audit retention execute")
	}
	var login bool
	if err := s.Pool().QueryRow(ctx, `select rolcanlogin from pg_roles where rolname='ouf_mcp_retention_owner'`).Scan(&login); err != nil {
		t.Fatal(err)
	}
	if login {
		t.Fatal("retention owner must remain NOLOGIN")
	}
}

func TestAuditRetentionConcurrentWorkersAreBounded(t *testing.T) {
	s, ctx := maintenanceStore(t)
	for i := 0; i < 20; i++ {
		if _, err := s.Pool().Exec(ctx, `insert into ouf_mcp.audit_event(audit_event_id,event_type,actor_type,safe_detail,occurred_at) values(gen_random_uuid(),'RETENTION_FIXTURE','SYSTEM','{}',transaction_timestamp()-interval '2 days')`); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	counts := make(chan int, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var n int
			err := s.Pool().QueryRow(ctx, `select ouf_mcp.purge_audit_events(transaction_timestamp()-interval '1 day',10)`).Scan(&n)
			counts <- n
			errs <- err
		}()
	}
	wg.Wait()
	close(counts)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	total := 0
	for n := range counts {
		if n < 0 || n > 10 {
			t.Fatalf("worker purge count=%d outside bound", n)
		}
		total += n
	}
	if total != 20 {
		t.Fatalf("total purged=%d want=20", total)
	}
	var remaining int
	if err := s.Pool().QueryRow(ctx, `select count(*) from ouf_mcp.audit_event where event_type='RETENTION_FIXTURE'`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Fatalf("remaining retention fixtures=%d", remaining)
	}
}
