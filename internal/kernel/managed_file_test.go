package kernel

import (
    "context"
    "encoding/json"
    "io"
    "log/slog"
    "net/http"
    "net/http/httptest"
    "net/url"
    "testing"
    "time"

    "github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
    "github.com/GioNob/ouf-mcp-server/internal/authorization"
    "github.com/GioNob/ouf-mcp-server/internal/orchestration"
    "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestManagedFileProfileUsesDelegatedGatewayWithoutRawAttachment(t *testing.T) {
    const capID = "ouf.managed-source.file.profile"
    const asset = "00000000-0000-4000-8000-000000000001"
    now := time.Now().UTC()
    cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{
        BundleID:"managed", BundleVersion:1, ActivatedAt:now,
        Bundle:authorization.PolicyBundle{BundleID:"managed",Version:1,PublishedAt:now,
            Capabilities:[]authorization.CapabilityDescriptor{{CapabilityID:capID,Operation:"COMMAND",RequiredScope:capID,AllowedActors:[]string{"HUMAN"}}},
            Grants:[]authorization.Grant{{GrantID:"profile",CapabilityID:capID,TenantID:"tenant-a",SubjectID:"user-a",ValidFrom:now.Add(-time.Minute),ValidUntil:now.Add(time.Hour)}},
        },
    }})
    if err:=cache.Refresh(context.Background());err!=nil{t.Fatal(err)}
    calls:=0
    gateway:=httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
        calls++
        if r.URL.Path!="/internal/capabilities/v1/execute/managed.file/profile" ||
            r.Header.Get("Authorization")!="Bearer workload" || r.Header.Get("X-OUF-Delegation")!="human-proof" {
            t.Error("managed-file profile bypassed governed transport")
        }
        var in orchestration.GatewayRequest
        if err:=json.NewDecoder(r.Body).Decode(&in);err!=nil{t.Fatal(err)}
        var args map[string]any
        if err:=json.Unmarshal(in.Arguments,&args);err!=nil{t.Fatal(err)}
        if len(args)!=1 || args["assetId"]!=asset || in.Owner!="onboarding" || in.OperationClass!="COMMAND" || in.Identity.Delegation!="" {
            t.Error("unsafe managed-file arguments or identity")
        }
        w.Header().Set("Content-Type","application/json")
        io.WriteString(w,`{"jobId":"00000000-0000-4000-8000-000000000002","status":"QUEUED"}`)
    }))
    defer gateway.Close()
    target,_:=url.Parse(gateway.URL+"/internal/capabilities/v1/execute")
    service:=&orchestration.Service{RequireDelegation:true,Auth:cache,Admission:&statusAdmission{},Gateway:&httpclient.GatewayClient{Endpoint:target,Client:gateway.Client(),TokenSource:httpclient.StaticTokenSource("workload")},FingerprintKey:make([]byte,32)}
    handler,err:=NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard,nil)),service)
    if err!=nil{t.Fatal(err)}
    caller:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
        for k,v:=range map[string]string{"X-OUF-Delegation":"human-proof","X-OUF-Gateway-Verified":"true","X-OUF-Service-Principal":"ouf-chatgpt","X-OUF-Principal-ID":"user-a","X-OUF-Tenant-ID":"tenant-a","X-OUF-Actor-Type":"HUMAN","X-OUF-Authentication-Context-Ref":"1","X-OUF-Token-Issuer":"issuer","X-OUF-Token-Audience":"gateway","X-OUF-Granted-Scopes":capID} {r.Header.Set(k,v)}
        handler.ServeHTTP(w,r)
    }))
    defer caller.Close()
    client:=mcp.NewClient(&mcp.Implementation{Name:"managed-file-test",Version:"1"},nil)
    session,err:=client.Connect(context.Background(),&mcp.StreamableClientTransport{Endpoint:caller.URL},nil)
    if err!=nil{t.Fatal(err)}
    defer session.Close()
    result,err:=session.CallTool(context.Background(),&mcp.CallToolParams{Name:"source.file.profile",Arguments:map[string]any{"assetId":asset}})
    if err!=nil || result.IsError || calls!=1{t.Fatalf("profile failed: result=%+v err=%v calls=%d",result,err,calls)}
    bad,err:=session.CallTool(context.Background(),&mcp.CallToolParams{Name:"source.file.profile",Arguments:map[string]any{"assetId":asset,"url":"https://attacker.invalid/file.csv"}})
    if err!=nil{t.Fatal(err)}
    if !bad.IsError || calls!=1{t.Fatal("tool accepted arbitrary file URL")}
}
