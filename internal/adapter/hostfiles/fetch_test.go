package hostfiles

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestHostAttachmentSpoolsExactBytesAndRejectsUnapprovedDestinations(t *testing.T) {
	const csv = "\xef\xbb\xbfcinema,indirizzo\r\nA,Trieste\r\n"
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/file", http.StatusFound)
			return
		}
		if r.URL.Path == "/large" {
			io.WriteString(w, strings.Repeat("x", int(MaxCSVBytes)+1))
			return
		}
		io.WriteString(w, csv)
	}))
	defer server.Close()
	f, err := New([]string{server.URL})
	if err != nil {
		t.Fatal(err)
	}
	f.client = server.Client()
	f.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	input := Input{FileID: "file_123", DownloadURL: server.URL + "/file?temporary=signed", MimeType: "text/csv"}
	staged, err := f.Fetch(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(staged.File)
	if err != nil || string(content) != csv || staged.Size != int64(len(csv)) || staged.SHA256 != "sha256:0616660620b44895c9191c5c71d003516fcb277c18dfba2b867e3ca1d9d00a5a" {
		t.Fatalf("bytes/hash changed or incomplete: %v", err)
	}
	path := staged.File.Name()
	if err := staged.CloseAndRemove(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("spool file persisted")
	}
	for _, bad := range []Input{
		{FileID: "file_123", DownloadURL: "https://other.invalid/file"},
		{FileID: "file_123", DownloadURL: "http://" + strings.TrimPrefix(server.URL, "https://") + "/file"},
		{FileID: "file_123", DownloadURL: server.URL + ".attacker.invalid/file"},
		{FileID: "file_123", DownloadURL: server.URL + "/redirect"},
		{FileID: "forged", DownloadURL: server.URL + "/file"},
		{FileID: "file_123", DownloadURL: server.URL + "/large"},
	} {
		if result, err := f.Fetch(context.Background(), bad); err == nil {
			result.CloseAndRemove()
			t.Fatal("untrusted or oversized host file accepted")
		}
	}
	if calls != 3 {
		t.Fatalf("unexpected network calls: %d", calls)
	}
	if _, err := New([]string{"https://*.example.org"}); err == nil {
		t.Fatal("wildcard origin accepted")
	}
}
