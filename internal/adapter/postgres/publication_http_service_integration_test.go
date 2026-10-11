package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/GioNob/ouf-mcp-server/internal/operational"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type publicationHTTPPolicyFixture struct{ sourceReads atomic.Int64 }

func (p *publicationHTTPPolicyFixture) Authorize(ctx context.Context, in orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	if in.CapabilityID == "ouf.ingestion.operations.incidents" {
		if in.Resource.Attributes["sourceRef"] != "source-a" {
			return orchestration.AuthorizationDecision{}, fmt.Errorf("source grant lost")
		}
		p.sourceReads.Add(1)
	}
	return (controlPolicyFixture{}).Authorize(ctx, in)
}

type publicationHTTPGatewayFixture struct {
	root      controlGatewayFixture
	producers *cursorServiceGatewayFixture
	books     map[string]string
}

func (g *publicationHTTPGatewayFixture) Execute(ctx context.Context, in orchestration.GatewayRequest, timeout time.Duration) (orchestration.GatewayResponse, error) {
	if in.Owner == "mcp" {
		return g.root.Execute(ctx, in, timeout)
	}
	name := "INGESTION"
	if in.Owner == "gateway" {
		name = "GATEWAY"
	}
	var query map[string]any
	if json.Unmarshal(in.Arguments, &query) != nil {
		return orchestration.GatewayResponse{}, fmt.Errorf("bad producer query")
	}
	resume := query["resumeFrom"] != nil
	if resume {
		if query["resumeFrom"] != g.books[name] || len(query) != 2 {
			return orchestration.GatewayResponse{}, fmt.Errorf("owner boundary changed")
		}
	} else if query["coverage"] != true || query["snapshot"] != true {
		return orchestration.GatewayResponse{}, fmt.Errorf("coverage lost")
	}
	reply, err := g.producers.Execute(ctx, in, timeout)
	if err != nil {
		return reply, err
	}
	var body map[string]any
	if json.Unmarshal(reply.Body, &body) != nil {
		return reply, fmt.Errorf("bad fixture page")
	}
	body["bookmarkProfile"] = "ouf.owner-publication-bookmark.v1"
	body["publicationBookmark"] = g.books[name]
	coverage := map[string]any{"noPublicationProgress": resume}
	if resume {
		coverage["parentBookmark"] = g.books[name]
		body["items"] = []any{}
	}
	body["coverage"] = coverage
	reply.Body, _ = json.Marshal(body)
	return reply, nil
}
func TestOperationalControlPublicationHTTPReopenLoginAndIdleKeepsFiniteBudget(t *testing.T) {
	ctx, store, manifest := controlStore(t)
	tenant := "control-publication-http-" + uuid.NewString()
	producers := &cursorServiceGatewayFixture{tokens: map[string]string{"GATEWAY": uuid.NewString(), "INGESTION": uuid.NewString()}, snapshots: map[string]string{"GATEWAY": uuid.NewString(), "INGESTION": uuid.NewString()}}
	gateway := &publicationHTTPGatewayFixture{producers: producers, books: map[string]string{"GATEWAY": uuid.NewString(), "INGESTION": uuid.NewString()}}
	policy := &publicationHTTPPolicyFixture{}
	service := &orchestration.Service{RequireDelegation: true, Auth: policy, Admission: store, Gateway: gateway, Audit: store, FingerprintKey: make([]byte, 32)}
	self := controlSelfFixture{}
	aggregate := &operational.Aggregator{Caller: service, Self: self, ManifestChecksum: manifest}
	gateway.root.owner = operational.NewOwnerAPI(self, aggregate).WithAuthorization(controlPolicyFixture{}).WithOperationalDelegation("workload").WithIncidentCursors(store).WithPublicationBookmarks(store)
	in := orchestration.Invocation{
		Identity:     orchestration.Identity{ServicePrincipalID: "workload", PrincipalID: "operator", TenantID: tenant, ActorType: "HUMAN", AuthenticationContextRef: "first-login", Delegation: "e30." + strings.Repeat("a", 43)},
		Resource:     orchestration.ResourceContext{ResourceType: "capability", TenantID: tenant, Attributes: map[string]string{"detailLevel": "TENANT_OPERATIONAL"}},
		CapabilityID: "ouf.operations.incidents", Owner: "mcp", OperationClass: "READ", GatewayBindingRef: "capability://ouf.operations.incidents", ManifestChecksum: manifest,
		Arguments: json.RawMessage(`{"snapshot":true,"coverage":true,"sourceId":"source-a","limit":100}`), IdempotencyKey: uuid.NewString(), CorrelationID: uuid.NewString(),
		Window: time.Minute, Timeout: 5 * time.Second, RetryThreshold: 3, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 100, ResultBytes: 512 << 10},
	}
	first, err := service.Call(ctx, in)
	var page map[string]any
	if err != nil || first.Problem != nil || json.Unmarshal(first.Body, &page) != nil || page["publicationBookmark"] == nil || page["consultationRecorded"] != false {
		t.Fatalf("first: %v %s", err, first.Body)
	}
	token := page["publicationBookmark"].(string)
	reopened, err := Open(ctx, os.Getenv("MCP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	gateway.root.owner = operational.NewOwnerAPI(self, aggregate).WithAuthorization(controlPolicyFixture{}).WithOperationalDelegation("workload").WithIncidentCursors(reopened).WithPublicationBookmarks(reopened)
	in.Identity.AuthenticationContextRef = "new-login"
	for index := 0; index < 2; index++ {
		in.Arguments = json.RawMessage(`{"resumeFrom":"` + token + `","limit":100}`)
		in.IdempotencyKey = uuid.NewString()
		resumed, err := service.Call(ctx, in)
		if err != nil || resumed.Problem != nil || json.Unmarshal(resumed.Body, &page) != nil || page["publicationBookmark"] != token || len(page["items"].([]any)) != 0 || page["consultationRecorded"] != false {
			t.Fatalf("resume %d: %v %s", index, err, resumed.Body)
		}
	}
	var windows int
	var consumed, reserved int64
	if err = store.Pool().QueryRow(ctx, `select count(*),coalesce(sum(consumed_tool_calls),0),coalesce(sum(reserved_tool_calls),0) from ouf_mcp.budget_window where tenant_id=$1 and policy_ref=$2`, tenant, manifest+":"+orchestration.OperationalControlBudgetProfile).Scan(&windows, &consumed, &reserved); err != nil {
		t.Fatal(err)
	}
	var books int
	if err = store.Pool().QueryRow(ctx, `select count(*) from ouf_mcp.operational_publication_bookmark where tenant_id=$1`, tenant).Scan(&books); err != nil {
		t.Fatal(err)
	}
	if windows != 1 || consumed != 9 || reserved != 0 || gateway.root.calls != 3 || producers.calls != 6 || books != 1 || policy.sourceReads.Load() != 3 {
		t.Fatal("idle reset/bypass/capacity growth", windows, consumed, reserved, gateway.root.calls, producers.calls, books)
	}
}

