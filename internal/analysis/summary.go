package analysis

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

const DiscoveryDecisionRequired = "DISCOVERY_DECISION_REQUIRED"

const SummaryContract = "ouf.analysis.summary.v2"

var ErrProfile = errors.New("ANALYSIS_PROFILE_CHANGED")
var ErrCursor = errors.New("ANALYSIS_CURSOR_INVALID")
var ErrExpired = errors.New("ANALYSIS_CURSOR_EXPIRED")

type Limits struct {
	MaxColumnReformulations int
	PreviewPageSize         int
	SummaryPageBytes        int
	SummaryCursorSeconds    int
}

func SummaryLimits(m *manifest.Snapshot) (Limits, error) {
	c, ok := Find(m, "source.analysis.read")
	var l Limits
	if !ok || json.Unmarshal(c.OperationalLimits, &l) != nil || l.MaxColumnReformulations < 0 || l.MaxColumnReformulations > MaxColumnReformulations ||
		l.PreviewPageSize < 1 || l.PreviewPageSize > 128 ||
		l.SummaryPageBytes < 4096 || l.SummaryPageBytes > 1<<20 || l.SummaryCursorSeconds < 1 || l.SummaryCursorSeconds > 86400 {
		return Limits{}, ErrInvalid
	}
	return l, nil
}

func previewPage(body []byte, asset, profile string, pin Profile, offset, limit int) (Profile, []string, error) {
	page, columns, err := verifiedPreviewPage(body, asset, profile, pin, offset, limit)
	if err != nil {
		return Profile{}, nil, err
	}
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}
	return page, names, nil
}

type previewColumn struct {
	Name string
	Raw  json.RawMessage
}

func verifiedPreviewPage(body []byte, asset, profile string, pin Profile, offset, limit int) (Profile, []previewColumn, error) {
	var page struct {
		Profile
		View               string
		Offset, NextOffset *int
		HasMore            *bool
		Columns            []json.RawMessage
	}
	if !ValidEvidenceJSON(body) || json.Unmarshal(body, &page) != nil || !page.Profile.Valid() || !CanonicalUUID(page.AssetID) || !CanonicalUUID(page.ProfileID) || page.SchemaHash != strings.ToLower(page.SchemaHash) || page.AssetID != asset || page.ProfileID != profile ||
		page.View != "COLUMNS" || page.Offset == nil || *page.Offset != offset || (offset > 0 && page.Profile != pin) ||
		len(page.Columns) == 0 || len(page.Columns) > limit {
		return Profile{}, nil, ErrProfile
	}
	end := offset + len(page.Columns)
	if end > page.ColumnCount || page.HasMore == nil || *page.HasMore != (end < page.ColumnCount) ||
		(*page.HasMore && (page.NextOffset == nil || *page.NextOffset != end)) || (page.NextOffset != nil && *page.NextOffset != end) {
		return Profile{}, nil, ErrProfile
	}
	columns := make([]previewColumn, len(page.Columns))
	for i, raw := range page.Columns {
		var col struct {
			Ordinal *int
			Name    json.RawMessage
		}
		if json.Unmarshal(raw, &col) != nil || (col.Ordinal != nil && *col.Ordinal != offset+i) {
			return Profile{}, nil, ErrProfile
		}
		var name string
		if json.Unmarshal(col.Name, &name) == nil && UsableColumnName(name) {
			columns[i].Name = name
		}
		columns[i].Raw = raw
	}
	return page.Profile, columns, nil
}

func UsableColumnName(name string) bool {
	return strings.TrimSpace(name) != "" && strings.IndexByte(name, 0) < 0 && utf8.RuneCountInString(name) <= 2000
}

type Counts struct {
	Total   int `json:"total"`
	Matched int `json:"matchedPublished"`
	Ready   int `json:"candidatesReady"`
	Zero    int `json:"noCandidates"`
	Blocked int `json:"blocked"`
	Pending int `json:"pending"`
}

func (c Counts) State() string {
	switch {
	case c.Blocked == c.Total:
		return "BLOCKED"
	case c.Pending > 0:
		return "RUNNING"
	case c.Blocked > 0:
		return "PARTIAL"
	default:
		return "COMPLETE"
	}
}

type PublishedMatch struct {
	SemanticID       string   `json:"semanticId"`
	SemanticVersion  string   `json:"semanticVersion"`
	RevisionID       string   `json:"revisionId"`
	PublicationSetID string   `json:"publicationSetId"`
	Score            int      `json:"score"`
	Reasons          []string `json:"reasons"`
}

// Ordinal and changedSeq are supplied by the immutable version key.
type Outcome struct {
	NotBefore     time.Time       `json:"-"`
	PolicyVersion *int64          `json:"policyVersion,omitempty"`
	Match         *PublishedMatch `json:"match,omitempty"`
	State         string          `json:"state"`
	RequestID     string          `json:"requestId,omitempty"`
	Reason        string          `json:"reasonCode,omitempty"`
	Count         *int            `json:"candidateCount,omitempty"`
	ObservedAt    string          `json:"observedAt,omitempty"`
}

func CanonicalUUID(s string) bool {
	id, err := uuid.Parse(s)
	return err == nil && id.String() == s
}

func sameDiscoveryRequest(request, other string) bool {
	if len(other) != 36 {
		return false
	}
	id, err := uuid.Parse(request)
	if err != nil {
		return false
	}
	otherID, err := uuid.Parse(other)
	return err == nil && id == otherID
}

func discoveryEnvelope(request string, raw []byte) (map[string]json.RawMessage, bool) {
	var fields map[string]json.RawMessage
	if !CanonicalUUID(request) || !ValidEvidenceJSON(raw) || json.Unmarshal(raw, &fields) != nil || fields == nil {
		return nil, false
	}
	for key, value := range fields {
		for _, field := range []string{"requestId", "state", "recoveryState", "candidateCount", "completedCandidateCount", "candidates"} {
			if strings.EqualFold(key, field) && (key != field || string(value) == "null") {
				return nil, false
			}
		}
	}
	if rawID, ok := fields["requestId"]; ok {
		var ownerID string
		if json.Unmarshal(rawID, &ownerID) != nil || !sameDiscoveryRequest(request, ownerID) {
			return nil, false
		}
	}
	return fields, true
}

// EmptyCandidatePage describes a live owner list, independently of completion evidence.
func EmptyCandidatePage(request string, raw []byte) bool {
	fields, ok := discoveryEnvelope(request, raw)
	var state string
	var items []json.RawMessage
	if !ok || json.Unmarshal(fields["state"], &state) != nil || state != "SUCCEEDED" ||
		json.Unmarshal(fields["candidates"], &items) != nil || items == nil || len(items) != 0 {
		return false
	}
	if count, present := fields["candidateCount"]; present {
		var n int
		return json.Unmarshal(count, &n) == nil && n == 0
	}
	return true
}

func CompletedCount(request string, raw []byte) *int {
	fields, ok := discoveryEnvelope(request, raw)
	var state string
	var count int
	if !ok || json.Unmarshal(fields["state"], &state) != nil || state != "SUCCEEDED" ||
		json.Unmarshal(fields["completedCandidateCount"], &count) != nil || count < 0 {
		return nil
	}
	var live int
	if v, exists := fields["candidateCount"]; exists && (json.Unmarshal(v, &live) != nil || live < 0 || live > count) {
		return nil
	}
	return &count
}

// DiscoveryContinuation freezes a relative owner delay at observation time.
func DiscoveryContinuation(request string, raw []byte, now, floor time.Time, next string) (Outcome, string) {
	var obs map[string]any
	if json.Unmarshal(raw, &obs) == nil {
		if due, ok := orchestration.OwnerDue(obs, now, floor, ""); ok {
			pending := Outcome{State: "PENDING", RequestID: request}
			if due.After(now) {
				pending.NotBefore = due
			}
			return pending, next
		}
	}
	return Outcome{State: "BLOCKED", RequestID: request, Reason: "RETRY_WINDOW_UNAVAILABLE"}, ""
}

// DiscoveryEvidence consumes an authorized owner read of exactly requestID.
// A live count alone still needs the candidate list, including a live zero.
func DiscoveryEvidence(tool, requestID string, raw []byte, now time.Time) (Outcome, string) {
	return DiscoveryEvidenceWithCompletion(tool, requestID, raw, nil, now, time.Time{})
}

// Completion is immutable owner evidence from an earlier read of this same job.
func DiscoveryEvidenceWithCompletion(tool, requestID string, raw []byte, completion *int, now, floor time.Time) (Outcome, string) {
	unavailable := Outcome{State: "BLOCKED", RequestID: requestID, Reason: "DISCOVERY_EVIDENCE_UNAVAILABLE"}
	var r struct {
		RequestID                               string
		State, RecoveryState                    string
		CompletedCandidateCount, CandidateCount *int
		Candidates                              []struct {
			ID          string     `json:"candidate_id"`
			CandidateID string     `json:"candidateId"`
			RequestID   string     `json:"requestId"`
			ExpiresAt   *time.Time `json:"expiresAt"`
		}
	}
	if _, ok := discoveryEnvelope(requestID, raw); !ok || json.Unmarshal(raw, &r) != nil {
		return unavailable, ""
	}
	switch r.State {
	case "CANCELLED":
		return Outcome{State: "BLOCKED", RequestID: requestID, Reason: "DISCOVERY_CANCELLED"}, ""
	case "FAILED", "EXHAUSTED":
		return Outcome{State: "BLOCKED", RequestID: requestID, Reason: "DISCOVERY_FAILED"}, ""
	case "PENDING", "RUNNING", "RETRY_WAIT":
		if r.RecoveryState == "BLOCKED" || r.RecoveryState == "PAUSED_EXHAUSTED" || r.RecoveryState == "CANCELLED" {
			return Outcome{State: "BLOCKED", RequestID: requestID, Reason: "DISCOVERY_STOPPED"}, ""
		}
		return DiscoveryContinuation(requestID, raw, now, floor, "semantic.discovery.status")
	case "SUCCEEDED":
		if r.CompletedCandidateCount == nil {
			r.CompletedCandidateCount = completion
		}
		if (r.CompletedCandidateCount != nil && *r.CompletedCandidateCount < 0) || (r.CandidateCount != nil && *r.CandidateCount < 0) {
			return unavailable, ""
		}
	default:
		return unavailable, ""
	}
	evidence := func(state string, n int) Outcome {
		return Outcome{State: state, RequestID: requestID, Count: &n, ObservedAt: now.UTC().Format(time.RFC3339Nano)}
	}
	if tool == "semantic.discovery.candidates" && EmptyCandidatePage(requestID, raw) {
		return Outcome{State: "BLOCKED", RequestID: requestID, Reason: DiscoveryDecisionRequired}, "semantic.discovery.candidates"
	}
	if r.CompletedCandidateCount != nil && *r.CompletedCandidateCount == 0 {
		if tool == "semantic.discovery.status" && len(r.Candidates) == 0 && (r.CandidateCount == nil || *r.CandidateCount == 0) {
			return Outcome{State: "BLOCKED", RequestID: requestID, Reason: DiscoveryDecisionRequired}, "semantic.discovery.candidates"
		}
		return unavailable, ""
	}
	if tool == "semantic.discovery.status" {
		return DiscoveryContinuation(requestID, raw, now, floor, "semantic.discovery.candidates")
	}
	if tool != "semantic.discovery.candidates" || len(r.Candidates) == 0 ||
		r.CompletedCandidateCount == nil || *r.CompletedCandidateCount != len(r.Candidates) ||
		(r.CandidateCount != nil && *r.CandidateCount != len(r.Candidates)) {
		return unavailable, ""
	}
	seen := map[string]bool{}
	for _, candidate := range r.Candidates {
		id := candidate.ID
		if id == "" {
			id = candidate.CandidateID
		}
		if !CanonicalUUID(id) || seen[id] || (candidate.RequestID != "" && !sameDiscoveryRequest(requestID, candidate.RequestID)) ||
			(candidate.ExpiresAt != nil && !candidate.ExpiresAt.After(now)) {
			return unavailable, ""
		}
		seen[id] = true
	}
	return evidence("CANDIDATES_READY", len(seen)), ""
}

type Position struct {
	Seq         int64
	Last, Limit int
	Expires     int64
	Snapshot    uuid.UUID
}

// Domain-separated MAC over fixed-width fields. Compare the authenticated
// identity binding separately so another tenant sees the ordinary 404.
func (p Position) Token(key []byte, scope Scope, id string) string {
	b := make([]byte, 68)
	binding := sha256.Sum256([]byte(Digest([]any{SummaryContract, scope, id})))
	copy(b, binding[:])
	binary.BigEndian.PutUint64(b[32:], uint64(p.Seq))
	binary.BigEndian.PutUint16(b[40:], uint16(p.Last))
	binary.BigEndian.PutUint16(b[42:], uint16(p.Limit))
	binary.BigEndian.PutUint64(b[44:], uint64(p.Expires))
	copy(b[52:], p.Snapshot[:])
	mac := hmac.New(sha256.New, key)
	mac.Write(b)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(b))
}

func OpenPosition(token string, key []byte, scope Scope, id string, now time.Time) (Position, error) {
	b, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(b) != 100 || base64.RawURLEncoding.EncodeToString(b) != token {
		return Position{}, ErrCursor
	}
	mac := hmac.New(sha256.New, key)
	mac.Write(b[:68])
	if !hmac.Equal(b[68:], mac.Sum(nil)) {
		return Position{}, ErrCursor
	}
	binding := sha256.Sum256([]byte(Digest([]any{SummaryContract, scope, id})))
	if !hmac.Equal(b[:32], binding[:]) {
		return Position{}, ErrScope
	}
	p := Position{int64(binary.BigEndian.Uint64(b[32:])), int(binary.BigEndian.Uint16(b[40:])), int(binary.BigEndian.Uint16(b[42:])), int64(binary.BigEndian.Uint64(b[44:])), uuid.UUID(b[52:68])}
	if now.Unix() >= p.Expires {
		return Position{}, ErrExpired
	}
	return p, nil
}

type Hint struct {
	DecisionRequired                                            bool   `json:",omitempty"`
	OwnerReason                                                 string `json:",omitempty"`
	NotBefore                                                   string `json:",omitempty"`
	Tool, TaskID, RequestID, AssetID, ProfileID, Header, Reason string
	Ordinal                                                     int
	Revision                                                    int64
}

func (h Hint) Timing() map[string]any {
	if h.NotBefore == "" {
		return nil
	}
	return map[string]any{"notBefore": h.NotBefore, "ownerDueReason": h.OwnerReason}
}

type Summary struct {
	Epoch               string
	Visibility          string
	Seq                 int64
	Counts              Counts
	Items               []json.RawMessage
	Hint                Hint
	Onboarding          bool
	Stopped             bool
	ReadRows, ReadBytes int
}

func (s Summary) State() string {
	if s.Stopped {
		return "BLOCKED"
	}
	if s.Hint.DecisionRequired && s.Hint.Tool == "semantic.discovery.candidates" {
		return "RUNNING"
	}
	return s.Counts.State()
}

type PreviewStore interface {
	PreviewProgress(context.Context, Scope, string, string, ...string) (Profile, []string, error)
	SavePreviewPage(context.Context, Scope, string, string, Profile, int, []string, ...string) error
}

type SummaryStore interface {
	ReadSummary(context.Context, Scope, string, string, Position, bool) (Summary, error)
	AuditSummary(context.Context, Scope, string, string, int64, int, int, uuid.UUID, *Hint) error
	RecordDiscovery(context.Context, Scope, string, string, string, string, []byte, orchestration.OwnerObservation) (orchestration.OwnerObservation, error)
}

func (c Coordinator) SummaryAccess(ctx context.Context, i orchestration.Identity) (string, string, error) {
	cap, ok := Find(c.Manifest, "source.file.preview")
	if !ok || cap.PublicationState != "ACTIVE" {
		return "", "", ErrUnavailable
	}
	decision, err := c.authorization(ctx, i, cap)
	if err != nil {
		return "", "", err
	}
	checksum, err := c.Manifest.Checksum()
	return checksum, decision, err
}

// Check evidence permissions again before releasing either live counts or
// retained job references from an older snapshot.
func (c Coordinator) EvidenceAccess(ctx context.Context, i orchestration.Identity, s Summary) error {
	if err := c.CheckEpoch(ctx, i, s.Epoch); err != nil {
		return err
	}
	if err := c.CheckPublishedScope(ctx, i, s.Visibility); err != nil {
		return err
	}
	status := s.Counts.Zero > 0 || s.Hint.RequestID != ""
	for _, raw := range s.Items {
		var item struct{ RequestID string }
		if err := json.Unmarshal(raw, &item); err != nil {
			return ErrProfile
		}
		status = status || item.RequestID != ""
	}
	for _, read := range []struct {
		tool   string
		needed bool
	}{
		{"semantic.search", s.Counts.Matched > 0},
		{"semantic.discovery.status", status},
		{"semantic.discovery.candidates", s.Counts.Ready > 0 || s.Hint.Tool == "semantic.discovery.candidates"},
	} {
		if read.needed {
			cap, ok := Find(c.Manifest, read.tool)
			if !ok || cap.PublicationState != "ACTIVE" {
				return ErrUnavailable
			}
			if err := c.authorize(ctx, i, cap); err != nil {
				return err
			}
		}
	}
	return nil
}
