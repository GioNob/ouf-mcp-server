package kernel

import (
	"bytes"
	"context"
	"encoding/json"
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
	h, err := NewGovernedHTTPHandlerWithHostFiles(slog.New(slog.NewTextHandler(io.Discard, nil)), &orchestration.Service{}, hostfiles.New())
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

func TestHostOriginProbeReportsOnlyOriginWithoutFetchingOrUploading(t *testing.T) {
	h, err := NewGovernedHTTPHandlerWithHostOriginProbe(slog.New(slog.NewTextHandler(io.Discard, nil)), &orchestration.Service{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-OUF-Gateway-Verified", "true")
		r.Header.Set("X-OUF-Delegation", "human-proof")
		r.Header.Set("X-OUF-Actor-Type", "HUMAN")
		h.ServeHTTP(w, r)
	}))
	defer server.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "host-origin-test", Version: "1"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	list, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	probeFound := false
	for _, tool := range list.Tools {
		if tool.Name == "source.file.upload" {
			t.Fatal("upload action must not be advertised in probe mode")
		}
		if tool.Name == "source.file.attachment_origin_probe" {
			probeFound = true
			if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
				t.Fatal("origin probe must be read-only")
			}
			files, ok := tool.Meta["openai/fileParams"].([]any)
			if !ok || len(files) != 1 || files[0] != "file" {
				t.Fatalf("probe missing host file parameter: %#v", tool.Meta)
			}
		}
	}
	if !probeFound {
		t.Fatal("origin probe not advertised")
	}
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "source.file.attachment_origin_probe", Arguments: map[string]any{
		"file": map[string]any{"file_id": "file_attached", "download_url": "https://files.example.org/private?token=sensitive", "mime_type": "text/csv"},
	}})
	if err != nil || result.IsError || len(result.Content) != 1 {
		t.Fatalf("invalid probe result: %+v %v", result, err)
	}
	encoded, _ := json.Marshal(result.Content)
	if !bytes.Contains(encoded, []byte("file_attached")) || !bytes.Contains(encoded, []byte("https://files.example.org")) || bytes.Contains(encoded, []byte("sensitive")) || bytes.Contains(encoded, []byte("/private")) {
		t.Fatalf("probe did not isolate origin: %s", encoded)
	}
}

func TestUploadResultProjectsOnlySafeAssetIdentity(t *testing.T) {
	const asset = `00000000-0000-4000-8000-000000000001`
	filtered, ok := safeUploadAsset([]byte(`{"assetId":"` + asset + `","status":"STAGED","staging_ref":"object://private","download_url":"https://secret"}`))
	if !ok || bytes.Contains(filtered, []byte("staging_ref")) || bytes.Contains(filtered, []byte("download_url")) {
		t.Fatal("upload result leaked owner fields")
	}
	for _, bad := range []string{`{"assetId":"other","status":"STAGED"}`, `{"assetId":"` + asset + `","status":"ACTIVE"}`} {
		if _, ok := safeUploadAsset([]byte(bad)); ok {
			t.Fatal("invalid upload result accepted")
		}
	}
}
