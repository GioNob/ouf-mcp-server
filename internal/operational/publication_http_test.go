package operational

import (
	"encoding/json"
	"github.com/google/uuid"
	"net/http"
	"strings"
	"testing"
	"time"
)

func publicationHTTPFixture(limit int) (*ownerAPI, *composerCallerFixture, *composerStoreFixture, *publicationStoreFixture, *aggregateAuthorizationFixture, map[string]string) {
	p, _, caller, store, book, tokens := publicationComposerFixture(limit)
	auth := &aggregateAuthorizationFixture{}
	h := NewOwnerAPI(p.Cursor.Aggregator.Self, p.Cursor.Aggregator).WithAuthorization(auth).WithOperationalDelegation("mcp-control").WithIncidentCursors(store).WithPublicationBookmarks(book)
	return h, caller, store, book, auth, tokens
}
func publicationHTTPRead(t *testing.T, h *ownerAPI, body string) map[string]any {
	t.Helper()
	w := cursorHTTPRead(h, cursorHTTPRequest(body))
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("read metadata cached")
	}
	var result map[string]any
	if json.Unmarshal(w.Body.Bytes(), &result) != nil {
		t.Fatal("invalid JSON")
	}
	if result["consultationRecorded"] != false {
		t.Fatal("implicit receipt")
	}
	return result
}
func TestPublicationHTTPCompleteChainReopenAndResumeAfterNewLogin(t *testing.T) {
	h, caller, store, book, auth, tokens := publicationHTTPFixture(1)
	page := publicationHTTPRead(t, h, `{"snapshot":true,"coverage":true,"sourceId":"source-a","limit":1}`)
	if page["publicationBookmark"] != nil || book.writes != 0 {
		t.Fatal("incomplete chain finalized")
	}
	h = NewOwnerAPI(h.self, h.aggregator).WithAuthorization(auth).WithOperationalDelegation("mcp-control").WithIncidentCursors(store).WithPublicationBookmarks(book)
	for page["nextCursor"] != nil {
		page = publicationHTTPRead(t, h, `{"coverage":true,"cursor":"`+page["nextCursor"].(string)+`"}`)
	}
	root := page["publicationBookmark"].(string)
	if book.writes != 1 || page["coverage"].(map[string]any)["mcpSelfHistoryCovered"] != false {
		t.Fatal("unproven coverage")
	}
	for id, node := range store.nodes {
		node.ExpiresAt = time.Now().Add(-time.Minute)
		store.nodes[id] = node
	}
	caller.pages = map[string][]cursorOwnerPage{"GATEWAY": {composerPage("GATEWAY", "WARNING", 100, 0, false)}, "INGESTION": {composerPage("INGESTION", "ERROR", 100, 1, false)}}
	successor := map[string]string{"GATEWAY": tokens["GATEWAY"], "INGESTION": uuid.NewString()}
	caller.transform = func(name string, page *cursorOwnerPage) {
		page.BookmarkProfile = "ouf.owner-publication-bookmark.v1"
		page.PublicationBookmark = successor[name]
		page.Coverage.ParentBookmark = tokens[name]
		idle := successor[name] == tokens[name]
		page.Coverage.NoPublicationProgress = &idle
	}
	r := cursorHTTPRequest(`{"resumeFrom":"` + root + `","limit":100}`)
	r.Header.Set("X-OUF-Authentication-Context-Ref", "new-login")
	w := cursorHTTPRead(h, r)
	var resumed map[string]any
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &resumed) != nil {
		t.Fatalf("resume: %d %s", w.Code, w.Body.String())
	}
	if resumed["publicationBookmark"] == root || len(resumed["items"].([]any)) != 1 || resumed["consultationRecorded"] != false || book.writes != 2 {
		t.Fatal("resume lost publication or invented receipt")
	}
	for _, call := range caller.calls[len(caller.calls)-2:] {
		var query map[string]any
		_ = json.Unmarshal(call.Arguments, &query)
		name := "INGESTION"
		if call.Owner == "gateway" {
			name = "GATEWAY"
		}
		if query["resumeFrom"] != tokens[name] || call.ReadPositionRef != root {
			t.Fatal("wrong owner boundary")
		}
		if call.Owner == "ingestion" && call.Resource.Attributes["sourceRef"] != "source-a" {
			t.Fatal("inherited SDK scope lost")
		}
	}
}
func TestPublicationHTTPCurrentAuthorizationAndFailureNeverAdvance(t *testing.T) {
	h, caller, store, book, auth, _ := publicationHTTPFixture(1)
	page := publicationHTTPRead(t, h, `{"snapshot":true,"coverage":true,"limit":1}`)
	raw := `{"coverage":true,"cursor":"` + page["nextCursor"].(string) + `"}`
	commits, calls := store.commits, len(caller.calls)
	auth.failure = "denied"
	w := cursorHTTPRead(h, cursorHTTPRequest(raw))
	if w.Code != 403 || len(caller.calls) != calls || store.commits != commits {
		t.Fatal("revocation advanced")
	}
	auth.failure = ""
	r := cursorHTTPRequest(raw)
	r.Header.Set("X-OUF-Principal-ID", "other")
	w = cursorHTTPRead(h, r)
	if w.Code != 404 || len(caller.calls) != calls {
		t.Fatal("principal replay reached owners")
	}
	caller.fail = "INGESTION"
	w = cursorHTTPRead(h, cursorHTTPRequest(raw))
	if w.Code != 503 || store.commits != commits || book.writes != 0 {
		t.Fatal("outage advanced")
	}
	caller.fail = ""
	publicationHTTPRead(t, h, raw)
	h, caller, _, book, _, _ = publicationHTTPFixture(100)
	page = publicationHTTPRead(t, h, `{"snapshot":true,"coverage":true}`)
	token := uuid.MustParse(page["publicationBookmark"].(string))
	expired := book.records[token]
	expired.ExpiresAt = time.Now().Add(-time.Minute)
	book.records[token] = expired
	calls = len(caller.calls)
	w = cursorHTTPRead(h, cursorHTTPRequest(`{"resumeFrom":"`+token.String()+`"}`))
	if w.Code != 410 || len(caller.calls) != calls {
		t.Fatal("expired history silently restarted")
	}
}
func TestPublicationHTTPClosedQueriesAndConfigurationBeforeOwnerIO(t *testing.T) {
	token := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	invalid := []string{
		`{"coverage":true}`, `{"coverage":null,"snapshot":true}`, `{"coverage":false,"snapshot":true}`,
		`{"coverage":true,"snapshot":true,"since":"2026-10-11T00:00:00Z"}`,
		`{"coverage":true,"snapshot":true,"coverage":true}`, `{"coverage":true,"snapshot":true,"sourceId":null}`,
		`{"resumeFrom":"` + token + `","sourceId":"changed"}`, `{"resumeFrom":"` + token + `","coverage":true}`,
		`{"coverage":true,"cursor":"` + token + `","limit":1}`, `{"resumeFrom":null}`,
		`{"resumeFrom":"` + strings.ToUpper(token) + `"}`, `{"resumeFrom":"` + token + `","limit":101}`,
		`{"coverage":true,"snapshot":true,"secret":"x"}`, `{"coverage":true,"snapshot":true} {}`,
	}
	for _, raw := range invalid {
		h, caller, store, book, _, _ := publicationHTTPFixture(100)
		w := cursorHTTPRead(h, cursorHTTPRequest(raw))
		if w.Code != 400 || len(caller.calls) != 0 || store.commits != 0 || book.writes != 0 {
			t.Fatalf("invalid I/O: %s %d", raw, w.Code)
		}
	}
	for _, mode := range []string{"no-store", "no-delegation", "no-policy"} {
		h, caller, store, _, _, _ := publicationHTTPFixture(100)
		r := cursorHTTPRequest(`{"coverage":true,"snapshot":true}`)
		want := http.StatusForbidden
		switch mode {
		case "no-store":
			h.publicationStore = nil
			want = http.StatusServiceUnavailable
		case "no-delegation":
			r.Header.Del("X-OUF-Delegation")
			r.Header.Del("X-OUF-Delegation-Verified")
		case "no-policy":
			r.Header.Del("X-OUF-Authorization-Decision-Ref")
		}
		w := cursorHTTPRead(h, r)
		if w.Code != want || len(caller.calls) != 0 || store.commits != 0 {
			t.Fatalf("%s: %d", mode, w.Code)
		}
	}
}

