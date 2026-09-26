package httpclient

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func TestManagedUploadStreamsExactBytesWithDelegationAndNoURL(t *testing.T) {
	const csv="\xef\xbb\xbfcinema,indirizzo\r\nA,Trieste\r\n"
	const digest="sha256:0616660620b44895c9191c5c71d003516fcb277c18dfba2b867e3ca1d9d00a5a"
	called:=0
	server:=httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){
		called++
		body,err:=io.ReadAll(r.Body)
		if err!=nil||string(body)!=csv||r.URL.Path!="/internal/capabilities/v1/execute/managed.file/upload" || r.ContentLength!=int64(len(csv)) ||
			r.Header.Get("Content-Type")!="text/csv" || r.Header.Get("X-Content-SHA256")!=digest || r.Header.Get("X-OUF-File-ID")!="file_123" ||
			r.Header.Get("X-OUF-Delegation")!="signed-human" || r.Header.Get("Authorization")!="Bearer workload" ||
			r.Header.Get("Idempotency-Key")!="upload:file_123" || bytes.Contains(body,[]byte("download_url")) {
			t.Error("managed upload left governed transport or changed bytes")
		}
		w.Header().Set("Content-Type","application/json")
		io.WriteString(w,`{"assetId":"00000000-0000-4000-8000-000000000001","status":"STAGED"}`)
	}))
	defer server.Close()
	endpoint,_:=url.Parse(server.URL+"/internal/capabilities/v1/execute")
	client:=&GatewayClient{Endpoint:endpoint,Client:server.Client(),TokenSource:StaticTokenSource("workload")}
	in:=orchestration.GatewayRequest{CapabilityID:"ouf.managed-source.file.upload",Identity:orchestration.Identity{Delegation:"signed-human"},
		IdempotencyKey:"upload:file_123",MaxResultBytes:4096,
		Upload:&orchestration.UploadStream{Reader:bytes.NewBufferString(csv),Size:int64(len(csv)),SHA256:digest,FileID:"file_123"}}
	response,err:=client.Execute(context.Background(),in,30*time.Second)
	if err!=nil||response.Status!=201&&response.Status!=200||called!=1 { t.Fatalf("governed upload failed: status=%d calls=%d err=%v",response.Status,called,err) }
	in.Upload=nil
	if _,err:=client.Execute(context.Background(),in,time.Second);err==nil||called!=1 {t.Fatal("upload without verified stream reached Gateway")}
}
