package kernel

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/hostfiles"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestHostFileToolIsOptInAndAdvertisesFileParameter(t *testing.T) {
	fetcher, err := hostfiles.New([]string{"https://files.example.org"})
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewGovernedHTTPHandlerWithHostFiles(slog.New(slog.NewTextHandler(io.Discard, nil)), &orchestration.Service{}, fetcher)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-OUF-Gateway-Verified", "true")
		h.ServeHTTP(w, r)
	}))
	defer server.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "upload-discovery-test", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	list, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range list.Tools {
		if tool.Name == "source.file.upload" {
			files, ok := tool.Meta["openai/fileParams"].([]any)
			if !ok || len(files) != 1 || files[0] != "file" {
				t.Fatalf("missing host fileParams: %#v", tool.Meta)
			}
			return
		}
	}
	t.Fatal("host-enabled upload tool not discovered")
}
