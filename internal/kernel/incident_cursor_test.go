package kernel

import (
 "context"
 "encoding/json"
 "io"
 "log/slog"
 "net/http"
 "net/http/httptest"
 "sync"
 "strings"
 "testing"
 "time"

 "github.com/GioNob/ouf-mcp-server/internal/orchestration"
 "github.com/GioNob/ouf-mcp-server/internal/manifest"
 "github.com/modelcontextprotocol/go-sdk/mcp"
)
type cursorKernelAuth struct{}
func (cursorKernelAuth) Authorize(_ context.Context,in orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision,error) {
 return orchestration.AuthorizationDecision{Allowed:true,DecisionRef:"cursor:1:"+in.CapabilityID,PermittedDetailLevel:in.Resource.Attributes["detailLevel"],ResourceScope:map[string]string{"resourceType":in.Resource.ResourceType,"tenantId":in.Identity.TenantID}},nil
}
type cursorKernelGateway struct {mu sync.Mutex; arguments []json.RawMessage}
func (g *cursorKernelGateway) Execute(_ context.Context,in orchestration.GatewayRequest,_ time.Duration) (orchestration.GatewayResponse,error) {
 g.mu.Lock();defer g.mu.Unlock()
 g.arguments=append(g.arguments,append(json.RawMessage{},in.Arguments...))
 return orchestration.GatewayResponse{Status:200,Body:[]byte(`{"partial":false,"consultationRecorded":false}`)},nil
}
func TestIncidentCursorOfficialMCPClientPreservesClosedOptinArguments(t *testing.T) {
 gateway:=&cursorKernelGateway{}
 admission:=&statusAdmission{}
 service:=&orchestration.Service{RequireDelegation:true,Auth:cursorKernelAuth{},Admission:admission,Gateway:gateway,FingerprintKey:make([]byte,32)}
 handler,err:=NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard,nil)),service)
 if err!=nil {t.Fatal(err)}
 server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request) {
 for k,v:=range map[string]string{
 "X-OUF-Gateway-Verified":"true","X-OUF-Service-Principal":"ouf-chatgpt","X-OUF-Principal-ID":"operator","X-OUF-Tenant-ID":"tenant",
 "X-OUF-Actor-Type":"HUMAN","X-OUF-Authentication-Context-Ref":"1","X-OUF-Token-Issuer":"issuer","X-OUF-Token-Audience":"gateway",
 "X-OUF-Granted-Scopes":"mcp.connect operations.incident.read operations.status.read","X-OUF-Delegation":"fixture-delegation",
 } {r.Header.Set(k,v)}
 handler.ServeHTTP(w,r)
 }))
 defer server.Close()
 client:=mcp.NewClient(&mcp.Implementation{Name:"cursor-native-client",Version:"1"},nil)
 session,err:=client.Connect(context.Background(),&mcp.StreamableClientTransport{Endpoint:server.URL},nil)
 if err!=nil {t.Fatal(err)}
 defer session.Close()
 token:="11111111-1111-4111-8111-111111111111"
 cases:=[]struct {name string; args map[string]any; allowed bool}{
 {"ouf.operations.incidents",map[string]any{},true},
 {"ouf.operations.incidents",map[string]any{"snapshot":true,"limit":1,"state":"OPEN","sourceId":"source-a"},true},
 {"ouf.operations.incidents",map[string]any{"cursor":token},true},
 {"ouf.operations.incidents",map[string]any{"snapshot":false},false},
 {"ouf.operations.incidents",map[string]any{"snapshot":nil},false},
 {"ouf.operations.incidents",map[string]any{"cursor":token,"limit":1},false},
 {"ouf.operations.incidents",map[string]any{"cursor":token,"snapshot":true},false},
 {"ouf.operations.incidents",map[string]any{"cursor":"00000000-0000-0000-0000-000000000000"},false},
 {"ouf.operations.incidents",map[string]any{"cursor":"11111111-AAAA-4111-8111-111111111111"},false},
 {"ouf.operations.summary",map[string]any{"snapshot":true},false},
 {"ouf.operations.summary",map[string]any{"cursor":token},false},
 }
 for _,tc:=range cases {
 before:=admission.calls.Load()
 result,err:=session.CallTool(context.Background(),&mcp.CallToolParams{Name:tc.name,Arguments:tc.args})
 if err!=nil {t.Fatal(err)}
 if result.IsError==tc.allowed {t.Fatalf("unexpected schema result for %v: %+v",tc.args,result)}
 if !tc.allowed {
 if admission.calls.Load()!=before {t.Fatalf("invalid args admitted: %v",tc.args)}
 continue
 }
 gateway.mu.Lock()
 got:=gateway.arguments[len(gateway.arguments)-1]
 gateway.mu.Unlock()
 expected,_:=json.Marshal(tc.args)
 if string(got)!=string(expected) {t.Fatalf("MCP typed decoder changed arguments: got=%s want=%s",got,expected)}
 if admission.calls.Load()!=before+1 {t.Fatal("root admission skipped or multiplied")}
 }
}

type cursorReplayAdmission struct {statusAdmission}
func (a *cursorReplayAdmission) Reserve(_ context.Context,in orchestration.AdmissionRequest) (orchestration.AdmissionDecision,error) {
 a.calls.Add(1)
 return orchestration.AdmissionDecision{AttemptID:in.AttemptID,Replay:true},nil
}
func TestIncidentCursorReadReplayMetadataCannotBecomeSuccessfulEmptyPage(t *testing.T) {
 gateway:=&cursorKernelGateway{}
 admission:=&cursorReplayAdmission{}
 service:=&orchestration.Service{RequireDelegation:true,Auth:cursorKernelAuth{},Admission:admission,Gateway:gateway,FingerprintKey:make([]byte,32)}
 ctx:=context.WithValue(context.Background(),identityKey{},requestIdentity{Identity:orchestration.Identity{
 ServicePrincipalID:"workload",PrincipalID:"operator",TenantID:"tenant",ActorType:"HUMAN",Delegation:"fixture-delegation",
 }})
 cap:=manifest.Capability{CapabilityID:"ouf.operations.incidents",Owner:"mcp",OperationClass:"READ",GatewayBindingRef:"capability://ouf.operations.incidents"}
 result,_,err:=invoke(ctx,cap,strings.Repeat("a",64),[]byte(`{"cursor":"11111111-1111-4111-8111-111111111111"}`),service,100,512<<10)
 if err!=nil || !result.IsError || len(result.Content)!=1 || len(gateway.arguments)!=0 || admission.calls.Load()!=1 {t.Fatal("admission metadata became successful page",err,result)}
 var problem unavailableResult
 text:=result.Content[0].(*mcp.TextContent).Text
 if json.Unmarshal([]byte(text),&problem)!=nil || problem.Code!="OPERATIONAL_READ_REQUIRES_FRESH_AUTHORIZED_ATTEMPT" || !problem.Retryable {t.Fatal(text)}
}
