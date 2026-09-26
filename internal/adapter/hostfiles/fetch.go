// Package hostfiles obtains a bounded host-authorized attachment for the
// governed Gateway upload. It never accepts a model-chosen fetch destination.
package hostfiles

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
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
	origins map[string]struct{}
	client  *http.Client
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
	if err != nil || !fileID.MatchString(input.FileID) || u.Scheme != "https" || u.User != nil || u.Host == "" || u.Fragment != "" || u.Opaque != "" || (input.MimeType != "" && input.MimeType != "text/csv") {
		return "", errors.New("invalid host file descriptor")
	}
	return u.Scheme + "://" + u.Host, nil
}

// New requires exact, installation-approved HTTPS origins. No wildcard,
// suffix match, userinfo, private URL override, or following redirects.
func New(origins []string) (*Fetcher, error) {
	if len(origins) == 0 {
		return nil, errors.New("host attachment origin is not configured")
	}
	allowed := make(map[string]struct{}, len(origins))
	for _, raw := range origins {
		u, err := url.Parse(raw)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || strings.Contains(u.Host, "*") || u.String() != raw {
			return nil, errors.New("invalid host attachment origin")
		}
		allowed[u.Scheme+"://"+u.Host] = struct{}{}
	}
	return &Fetcher{origins: allowed, client: &http.Client{
		Timeout:       15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

// Fetch downloads the exact host file to a private bounded spool. The URL is
// never returned, included in an error, or written to logs. The caller must
// CloseAndRemove on every success path, including Gateway rejection.
func (f *Fetcher) Fetch(ctx context.Context, input Input) (_ *Staged, err error) {
	origin, validationErr := DescriptorOrigin(input)
	if validationErr != nil {
		return nil, validationErr
	}
	if _, ok := f.origins[origin]; !ok {
		return nil, errors.New("host attachment origin denied")
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
