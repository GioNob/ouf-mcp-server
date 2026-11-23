package analysis

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSummaryDiscoveryEvidence(t *testing.T) {
	const job = "11111111-1111-4111-8111-111111111111"
	const candidate = "22222222-2222-4222-8222-222222222222"
	now := time.Unix(1893492000, 0).UTC()
	for _, test := range []struct{ name, tool, raw, state, reason, next string }{
		{"receipt", "status", `{"state":"PENDING","candidateCount":0}`, "PENDING", "", "semantic.discovery.status"},
		{"positive status", "status", `{"state":"SUCCEEDED","candidateCount":1}`, "PENDING", "", "semantic.discovery.candidates"},
		{"attested zero", "status", `{"state":"SUCCEEDED","completedCandidateCount":0}`, "NO_CANDIDATES", "", ""},
		{"missing count", "status", `{"state":"SUCCEEDED","candidateCount":0}`, "BLOCKED", "DISCOVERY_EVIDENCE_UNAVAILABLE", ""},
		{"expired list", "candidates", `{"state":"SUCCEEDED","completedCandidateCount":1,"candidates":[]}`, "BLOCKED", "DISCOVERY_EVIDENCE_UNAVAILABLE", ""},
		{"old list without attestation", "candidates", `{"state":"SUCCEEDED","candidates":[{"candidate_id":"` + candidate + `"}]}`, "BLOCKED", "DISCOVERY_EVIDENCE_UNAVAILABLE", ""},
		{"positive list", "candidates", `{"state":"SUCCEEDED","completedCandidateCount":1,"candidates":[{"candidate_id":"` + candidate + `"}]}`, "CANDIDATES_READY", "", ""},
		{"wrong job", "status", `{"requestId":"` + candidate + `","state":"SUCCEEDED","completedCandidateCount":0}`, "BLOCKED", "DISCOVERY_EVIDENCE_UNAVAILABLE", ""},
		{"duplicate count", "status", `{"state":"SUCCEEDED","completedCandidateCount":2,"completedCandidateCount":0}`, "BLOCKED", "DISCOVERY_EVIDENCE_UNAVAILABLE", ""},
		{"malformed candidate", "candidates", `{"state":"SUCCEEDED","candidates":[{}]}`, "BLOCKED", "DISCOVERY_EVIDENCE_UNAVAILABLE", ""},
		{"expired candidate", "candidates", `{"state":"SUCCEEDED","candidates":[{"candidateId":"` + candidate + `","expiresAt":"2020-01-01T00:00:00Z"}]}`, "BLOCKED", "DISCOVERY_EVIDENCE_UNAVAILABLE", ""},
		{"legacy array", "candidates", `[]`, "BLOCKED", "DISCOVERY_EVIDENCE_UNAVAILABLE", ""},
		{"failed", "status", `{"state":"FAILED","completedCandidateCount":0}`, "BLOCKED", "DISCOVERY_FAILED", ""},
		{"cancelled", "status", `{"state":"CANCELLED"}`, "BLOCKED", "DISCOVERY_CANCELLED", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			o, next := DiscoveryEvidence("semantic.discovery."+test.tool, job, []byte(test.raw), now)
			if o.State != test.state || o.Reason != test.reason || next != test.next || o.RequestID != job {
				t.Fatal(o, next)
			}
			verified := o.State == "NO_CANDIDATES" || o.State == "CANDIDATES_READY"
			if verified != (o.Count != nil) || verified != (o.ObservedAt != "") {
				t.Fatal(o)
			}
			if verified && (*o.Count == 0) != (o.State == "NO_CANDIDATES") {
				t.Fatal(o)
			}
		})
	}
}

func TestSummaryPreviewContinuity(t *testing.T) {
	_, p, _ := fixture(t, 3)
	raw, _ := json.Marshal(p)
	var page map[string]any
	json.Unmarshal(raw, &page)
	page["view"], page["offset"], page["hasMore"], page["nextOffset"] = "COLUMNS", 0, true, 2
	page["columns"] = []any{map[string]any{"ordinal": 0, "name": "duplicata"}, map[string]any{"ordinal": 1, "name": " "}}
	parse := func(offset int, pin Profile) ([]string, error) {
		raw, _ := json.Marshal(page)
		_, names, err := previewPage(raw, p.AssetID, p.ProfileID, pin, offset, 2)
		return names, err
	}
	if names, err := parse(0, Profile{}); err != nil || len(names) != 2 || names[1] != "" {
		t.Fatal(names, err)
	}
	if _, err := parse(2, p); !errors.Is(err, ErrProfile) {
		t.Fatal("repeated page", err)
	}
	page["offset"], page["hasMore"] = 2, false
	delete(page, "nextOffset")
	page["columns"] = []any{map[string]any{"name": "duplicata", "ordinal": 2}}
	if names, err := parse(2, p); err != nil || names[0] != "duplicata" {
		t.Fatal(names, err)
	}
	page["profileVersion"] = 2
	if _, err := parse(2, p); !errors.Is(err, ErrProfile) {
		t.Fatal("changed profile", err)
	}
	page["profileVersion"], page["columns"] = 1, []any{}
	if _, err := parse(2, p); !errors.Is(err, ErrProfile) {
		t.Fatal("missing page", err)
	}
}

func TestSummaryAuthenticatedCursor(t *testing.T) {
	scope := Scope{"fixture", "analyst", "tenant"}
	id := strings.Repeat("a", 64)
	key := []byte(strings.Repeat("k", 32))
	now := time.Unix(100, 0)
	p := Position{Seq: 4, Last: 31, Limit: 32, Expires: 101, Snapshot: uuid.New()}
	token := p.Token(key, scope, id)
	if got, err := OpenPosition(token, key, scope, id, now); err != nil || got != p {
		t.Fatal(got, err)
	}
	if _, err := OpenPosition(token, key, scope, id, now.Add(time.Second)); !errors.Is(err, ErrExpired) {
		t.Fatal(err)
	}
	tampered := token[:10] + "A" + token[11:]
	if tampered == token {
		tampered = token[:10] + "B" + token[11:]
	}
	if _, err := OpenPosition(tampered, key, scope, id, now); !errors.Is(err, ErrCursor) {
		t.Fatal(err)
	}
	scope.Tenant = "other"
	if _, err := OpenPosition(token, key, scope, id, now); !errors.Is(err, ErrScope) {
		t.Fatal(err)
	}
}
