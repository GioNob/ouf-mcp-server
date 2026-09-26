// Package hostfiles obtains a bounded host-authorized attachment for the
// governed Gateway upload. It never accepts a model-chosen fetch destination.
package hostfiles

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"regexp"
	"time"
)

const MaxCSVBytes int64 = 10 * 1024 * 1024

var fileID = regexp.MustCompile(`^file_[A-Za-z0-9_-]{1,128}$`)

type Input struct {
	DownloadURL string `json:"download_url"`
	FileID      string `json:"file_id"`
	MimeType    string `json:"mime_type,omitempty"`
	FileName    string `json:"file_name,omitempty"`
}

type Fetcher struct {
	client *http.Client
}

type Staged struct {
	File   *os.File
	Size   int64
	SHA256 string
}

// DescriptorOrigin validates only the host-provided descriptor's shape. It
// never fetches the URL or treats its origin as approved for future requests.
func DescriptorOrigin(input Input) (string, error) {
	u, err := url.Parse(input.DownloadURL)
	if err != nil || !fileID.MatchString(input.FileID) || u.Scheme != "https" || u.User != nil || u.Host == "" || u.Hostname() == "" || (u.Port() != "" && u.Port() != "443") || u.Fragment != "" || u.Opaque != "" || (input.MimeType != "" && input.MimeType != "text/csv") {
		return "", errors.New("invalid host file descriptor")
	}
	return u.Scheme + "://" + u.Host, nil
}

// New accepts the host-resolved file URL without pinning its temporary host.
// The dedicated transport resolves and pins only public IP addresses before
// connecting; HTTPS validates the original hostname and redirects are denied.
func New() *Fetcher {
	return &Fetcher{client: &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			Proxy:               nil,
			DialContext:         dialPublicHTTPS,
			TLSHandshakeTimeout: 5 * time.Second,
			DisableKeepAlives:   true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

var blockedPublicRanges = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("fec0::/10"),
}

func publicIP(raw net.IP) bool {
	ip, ok := netip.AddrFromSlice(raw)
	if !ok {
		return false
	}
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return false
	}
	for _, prefix := range blockedPublicRanges {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}

func dialPublicHTTPS(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port != "443" || network != "tcp" {
		return nil, errors.New("public HTTPS port required")
	}
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil || len(addresses) == 0 {
		return nil, errors.New("file host unavailable")
	}
	for _, address := range addresses {
		if !publicIP(address.IP) {
			return nil, errors.New("file host resolves to an unapproved network")
		}
	}
	dialer := net.Dialer{Timeout: 5 * time.Second}
	for _, address := range addresses {
		connection, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(address.IP.String(), port))
		if dialErr == nil {
			return connection, nil
		}
	}
	return nil, errors.New("file host unavailable")
}

// Fetch downloads the exact host file to a private bounded spool. The URL is
// never returned, included in an error, or written to logs. The caller must
// CloseAndRemove on every success path, including Gateway rejection.
func (f *Fetcher) Fetch(ctx context.Context, input Input) (_ *Staged, err error) {
	_, validationErr := DescriptorOrigin(input)
	if validationErr != nil {
		return nil, validationErr
	}
	u, _ := url.Parse(input.DownloadURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, errors.New("invalid host file descriptor")
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, errors.New("host attachment unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ContentLength > MaxCSVBytes {
		return nil, errors.New("host attachment rejected or exceeds limit")
	}
	spool, err := os.CreateTemp("", "ouf-host-attachment-*.csv")
	if err != nil {
		return nil, errors.New("host attachment staging unavailable")
	}
	defer func() {
		if err != nil {
			spool.Close()
			os.Remove(spool.Name())
		}
	}()
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(spool, hash), io.LimitReader(resp.Body, MaxCSVBytes+1))
	if copyErr != nil || size == 0 || size > MaxCSVBytes {
		err = errors.New("host attachment read failed or exceeds limit")
		return nil, err
	}
	if _, err = spool.Seek(0, io.SeekStart); err != nil {
		err = errors.New("host attachment staging unavailable")
		return nil, err
	}
	return &Staged{File: spool, Size: size, SHA256: "sha256:" + hex.EncodeToString(hash.Sum(nil))}, nil
}

func (s *Staged) CloseAndRemove() error {
	if s == nil || s.File == nil {
		return nil
	}
	name := s.File.Name()
	err := s.File.Close()
	if removeErr := os.Remove(name); err == nil && !os.IsNotExist(removeErr) {
		err = removeErr
	}
	return err
}
