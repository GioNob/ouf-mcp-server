package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func TestSystemStatusIsTenantScopedAndSafe(t *testing.T) {
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := Migrate(ctx, store.Pool()); err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	tenant := "tenant-self-" + id
	payload := []byte(`{"capabilities":[]}`)
	manifestHash := fmt.Sprintf("%x", sha256.Sum256(payload))
	if err := store.EnsureManifest(ctx, manifestHash, "self-status-v1", "mcp-manifest-v1", payload); err != nil {
		t.Fatal(err)
	}
	session, err := store.CreateSession(ctx, Session{ConversationRef: "self-status", ServicePrincipalID: "service", PrincipalID: "principal", TenantID: tenant, ManifestChecksum: manifestHash, ExpiresAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := store.Admit(ctx, Admission{SessionID: &session.ID, ServicePrincipalID: "service", PrincipalID: "principal", TenantID: tenant, CapabilityID: "fixture.status", OperationClass: "READ", ManifestChecksum: manifestHash, IdempotencyKey: id, RequestHash: fmt.Sprintf("%x", sha256.Sum256([]byte(id))), CorrelationID: id})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, "update ouf_mcp.tool_attempt set state='UNRESOLVED',completed_at=transaction_timestamp() where attempt_id=$1", attempt.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, "insert into ouf_mcp.security_incident(incident_key,attempt_id,category,detail_code,evidence_payload_hash) values($1,$2,'CONTRACT_VIOLATION','SELF_STATUS_FIXTURE',$3)", "self-status-"+id, attempt.ID, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	body, err := store.SystemStatus(ctx, orchestration.Identity{TenantID: tenant})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got["status"] != "DEGRADED" || got["actionRequired"] != true {
		t.Fatalf("unexpected self status: %s", body)
	}
	if strings.Contains(string(body), "evidence_payload_hash") || strings.Contains(string(body), strings.Repeat("a", 64)) {
		t.Fatalf("protected evidence leaked: %s", body)
	}
	other, err := store.SystemStatus(ctx, orchestration.Identity{TenantID: "other-" + id})
	if err != nil {
		t.Fatal(err)
	}
	var otherStatus map[string]any
	if err := json.Unmarshal(other, &otherStatus); err != nil {
		t.Fatal(err)
	}
	if otherStatus["status"] != "HEALTHY" || otherStatus["securityIncidentCount"] != float64(0) {
		t.Fatalf("tenant isolation failed: %s", other)
	}
}
