package hostfiles

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestHostAttachmentSpoolsExactBytesAndRejectsInvalidDescriptors(t *testing.T) {
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
	f := New()
	client := server.Client()
	transport := client.Transport.(*http.Transport).Clone()
	// The test dialer deliberately maps an example hostname to the local TLS server.
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	client.Transport = transport
	f.client = client
	f.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	const attachmentHost = "https://files.example.org"
	input := Input{FileID: "file_123", DownloadURL: attachmentHost + "/file?temporary=signed", MimeType: "text/csv"}
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
		{FileID: "file_123", DownloadURL: "http://files.example.org/file"},
		{FileID: "file_123", DownloadURL: attachmentHost + "/file#fragment"},
		{FileID: "file_123", DownloadURL: "https://user@files.example.org/file"},
		{FileID: "file_123", DownloadURL: attachmentHost + "/redirect"},
		{FileID: "forged", DownloadURL: attachmentHost + "/file"},
		{FileID: "file_123", DownloadURL: attachmentHost + "/large"},
	} {
		if result, err := f.Fetch(context.Background(), bad); err == nil {
			result.CloseAndRemove()
			t.Fatal("untrusted or oversized host file accepted")
		}
	}
	if calls != 3 {
		t.Fatalf("unexpected network calls: %d", calls)
	}
}

func TestPublicOnlyDialRejectsInternalAndReservedDestinations(t *testing.T) {
	transport, ok := New().client.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil || transport.DialContext == nil ||
		(transport.TLSClientConfig != nil && transport.TLSClientConfig.InsecureSkipVerify) {
		t.Fatal("host file client has no public-only direct transport")
	}
	for _, address := range []string{"127.0.0.1:443", "10.0.0.5:443", "169.254.169.254:443", "[::1]:443", "192.0.2.4:443", "198.18.0.1:443", "8.8.8.8:80"} {
		if conn, err := dialPublicHTTPS(context.Background(), "tcp", address); err == nil {
			conn.Close()
			t.Fatalf("private or reserved destination accepted: %s", address)
		}
	}
	for _, address := range []string{"8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"} {
		if ip := net.ParseIP(address); !publicIP(ip) {
			t.Fatalf("public IP rejected: %s", address)
		}
	}
}

func TestHostOriginProbeValidatesDescriptorWithoutFetching(t *testing.T) {
	origin, err := DescriptorOrigin(Input{FileID: "file_abc", DownloadURL: "https://files.example.org/private?token=secret", MimeType: "text/csv"})
	if err != nil || origin != "https://files.example.org" {
		t.Fatalf("unexpected origin: %q %v", origin, err)
	}
	for _, input := range []Input{
		{FileID: "file_abc", DownloadURL: "http://files.example.org/private"},
		{FileID: "file_abc", DownloadURL: "https://user:secret@files.example.org/private"},
		{FileID: "file_abc", DownloadURL: "https://files.example.org/private#fragment"},
		{FileID: "forged", DownloadURL: "https://files.example.org/private"},
	} {
		if _, err := DescriptorOrigin(input); err == nil {
			t.Fatal("invalid probe descriptor accepted")
		}
	}
}
