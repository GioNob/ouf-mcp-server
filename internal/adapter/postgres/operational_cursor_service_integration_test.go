package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/operational"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

type cursorServiceGatewayFixture struct{mu sync.Mutex;tokens,snapshots map[string]string;calls int}
func(g *cursorServiceGatewayFixture)Execute(_ context.Context,in orchestration.GatewayRequest,_ time.Duration)(orchestration.GatewayResponse,error){
	g.mu.Lock();defer g.mu.Unlock();g.calls++
	name:="INGESTION";severity:="ERROR";if in.Owner=="gateway"{name="GATEWAY";severity="WARNING"}
	items:=[]map[string]any{}
	for index:=0;index<3;index++ {items=append(items,map[string]any{"incident_id":fmt.Sprintf("%s-%d",name,index),"module":name,"lifecycle_state":"OPEN","severity":severity,"action_required":true,"last_seen_at":"2026-10-11T00:00:00Z"})}
	result:=map[string]any{"items":items,"partial":false,"truncated":false,"cursor":g.tokens[name],"snapshotRef":g.snapshots[name],"itemsDigest":strings.Repeat("a",64),"expiresAt":time.Now().Add(time.Hour),"snapshotExpiresAt":time.Now().Add(time.Hour),"priorityProfile":"ouf.incident-priority.v1","cursorProfile":"ouf.incident-owner-cursor.v1","consultationRecorded":false}
	body,_:=json.Marshal(result);return orchestration.GatewayResponse{Status:200,Body:body},nil
}

func TestOperationalControlCursorReadbacksUseFreshAdmissionWithoutResettingWindow(t *testing.T){
	ctx,store,manifest:=controlStore(t);tenant:="control-cursor-"+uuid.NewString()
	gateway:=&cursorServiceGatewayFixture{tokens:map[string]string{"INGESTION":uuid.NewString(),"GATEWAY":uuid.NewString()},snapshots:map[string]string{"INGESTION":uuid.NewString(),"GATEWAY":uuid.NewString()}}
	service:=&orchestration.Service{RequireDelegation:true,Auth:controlPolicyFixture{},Admission:store,Gateway:gateway,Audit:store,FingerprintKey:make([]byte,32)}
	self:=controlSelfFixture{};aggregate:=&operational.Aggregator{Caller:service,Self:self,ManifestChecksum:manifest}
	composer:=operational.CursorComposer{Aggregator:aggregate,Store:store}
	in:=operational.CursorReadRequest{Identity:orchestration.Identity{ServicePrincipalID:"workload",PrincipalID:"operator",TenantID:tenant,ActorType:"HUMAN",Delegation:"e30."+strings.Repeat("a",43)},Arguments:json.RawMessage(`{"limit":1}`),AttemptID:uuid.NewString(),CorrelationID:uuid.NewString()}
	first,err:=composer.Start(ctx,in,"policy:1");if err!=nil {t.Fatal(err)}
	var page struct{Cursor string `json:"cursor"`;Next string `json:"nextCursor"`;Items []map[string]any `json:"items"`}
	if json.Unmarshal(first,&page)!=nil || len(page.Items)!=1 || page.Next=="" {t.Fatal(string(first))}
	replay,err:=composer.Page(ctx,in,"policy:1",uuid.MustParse(page.Cursor));if err!=nil || string(replay)!=string(first){t.Fatalf("metadata replay substituted for fresh owner read: %v",err)}
	// Two different committed positions read the same owner wire cursors; the
	// semantic guard must distinguish actual progress, not hide it as retries.
	for index:=0;index<2;index++ {body,err:=composer.Page(ctx,in,"policy:1",uuid.MustParse(page.Next));if err!=nil {t.Fatal(index,err)};if json.Unmarshal(body,&page)!=nil || len(page.Items)!=1 {t.Fatal(string(body))}}
	var windows int;var consumed,reserved int64
	if err=store.Pool().QueryRow(ctx,`select count(*),coalesce(sum(consumed_tool_calls),0),coalesce(sum(reserved_tool_calls),0) from ouf_mcp.budget_window where tenant_id=$1 and policy_ref=$2`,tenant,manifest+":"+orchestration.OperationalControlBudgetProfile).Scan(&windows,&consumed,&reserved);err!=nil {t.Fatal(err)}
	if windows!=1 || consumed!=8 || reserved!=0 || gateway.calls!=8 {t.Fatal("fresh page read reset or bypassed persistent accounting",windows,consumed,reserved,gateway.calls)}
}
