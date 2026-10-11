package operational

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func cursorHTTPRequest(body string) *http.Request {
	r := aggregateOwnerRequest("incidents", body)
	r.Header.Set("X-OUF-Actor-Type", "HUMAN")
	r.Header.Set("X-OUF-Service-Principal", "mcp-control")
	r.Header.Set("X-OUF-Capability-ID", "ouf.operations.incidents")
	r.Header.Set("X-OUF-Delegation-Verified", "mcp-operational-aggregate-v1")
	r.Header.Set("X-OUF-Delegation", "payload."+strings.Repeat("a", 43))
	r.Header.Set("X-Tool-Attempt-ID", uuid.NewString())
	r.Header.Set("X-Correlation-ID", "consultation")
	return r
}
func cursorHTTPFixture() (*ownerAPI, *composerCallerFixture, *composerStoreFixture, *aggregateAuthorizationFixture) {
	c, _, caller, store := composerFixture(1)
	auth := &aggregateAuthorizationFixture{}
	h := NewOwnerAPI(c.Aggregator.Self, c.Aggregator).WithAuthorization(auth).WithOperationalDelegation("mcp-control").WithIncidentCursors(store)
	return h, caller, store, auth
}
func cursorHTTPRead(h *ownerAPI, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestIncidentCursorHTTPStablePagesFreshRootAndNoConsultation(t *testing.T) {
	h, caller, store, auth := cursorHTTPFixture()
	w := cursorHTTPRead(h, cursorHTTPRequest(`{"snapshot":true,"limit":1,"sourceId":"source-a"}`))
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var page composedPage
	if json.Unmarshal(w.Body.Bytes(), &page) != nil || page.Next == "" || page.Consulted || len(page.Items) != 1 || page.Items[0]["severity"] != "CRITICAL" {
		t.Fatal("invalid first page")
	}
	first := w.Body.String()
	h = NewOwnerAPI(h.self, h.aggregator).WithAuthorization(auth).WithOperationalDelegation("mcp-control").WithIncidentCursors(store)
	replay := cursorHTTPRead(h, cursorHTTPRequest(`{"cursor":"`+page.Cursor+`"}`))
	if replay.Code != 200 || replay.Body.String() != first {
		t.Fatalf("unstable replay: %d %s", replay.Code, replay.Body.String())
	}
	commits := store.commits
	calls := len(caller.calls)
	auth.failure = "denied"
	denied := cursorHTTPRead(h, cursorHTTPRequest(`{"cursor":"`+page.Next+`"}`))
	if denied.Code != 403 || store.commits != commits || len(caller.calls) != calls {
		t.Fatal("revoked root read advanced or contacted producer")
	}
	auth.failure = ""
	other := cursorHTTPRequest(`{"cursor":"` + page.Next + `"}`)
	other.Header.Set("X-OUF-Principal-ID", "other")
	w = cursorHTTPRead(h, other)
	if w.Code != 404 || store.commits != commits || len(caller.calls) != calls {
		t.Fatal("cross-principal cursor disclosed")
	}
	w = cursorHTTPRead(h, cursorHTTPRequest(`{"cursor":"`+page.Next+`"}`))
	if w.Code != 200 || store.commits != commits+1 {
		t.Fatalf("valid continuation denied: %d %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "payload.") || strings.Contains(w.Body.String(), "consultationRecorded\":true") {
		t.Fatal("delegation or false consultation leaked")
	}
}
func TestIncidentCursorHTTPOwnerFailureOrMutationDoesNotAdvance(t *testing.T) {
	for _, mode := range []string{"unavailable", "changed"} {
		t.Run(mode, func(t *testing.T) {
			h, caller, store, _ := cursorHTTPFixture()
			w := cursorHTTPRead(h, cursorHTTPRequest(`{"snapshot":true,"limit":1}`))
			var page composedPage
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &page) != nil {
				t.Fatal("snapshot failed")
			}
			commits := store.commits
			if mode == "unavailable" {
				caller.fail = "INGESTION"
			} else {
				caller.transform = func(name string, p *cursorOwnerPage) {
					if name == "INGESTION" {
						p.Items[0]["impact_summary"] = "changed"
					}
				}
			}
			w = cursorHTTPRead(h, cursorHTTPRequest(`{"cursor":"`+page.Next+`"}`))
			expected := 503
			if mode == "changed" {
				expected = 409
			}
			if w.Code != expected || store.commits != commits {
				t.Fatal("failed owner read advanced page")
			}
			caller.fail = ""
			caller.transform = nil
			w = cursorHTTPRead(h, cursorHTTPRequest(`{"cursor":"`+page.Next+`"}`))
			if w.Code != 200 {
				t.Fatalf("safe recovery failed: %d %s", w.Code, w.Body.String())
			}
		})
	}
}
func TestIncidentCursorHTTPRejectsMixedAndUntrustedBeforeIO(t *testing.T) {
	invalid := []string{
		`{"snapshot":false}`, `{"snapshot":null}`, `{"snapshot":true,"snapshot":true}`,
		`{"cursor":"00000000-0000-0000-0000-000000000000"}`,
		`{"cursor":"` + uuid.NewString() + `","limit":1}`,
		`{"snapshot":true,"cursor":"` + uuid.NewString() + `"}`,
		`{"snapshot":true,"limit":101}`, `{"snapshot":true,"state":"INVALID"}`,
		`{"snapshot":true,"secret":"anything"}`, `{"snapshot":true} {}`,
		`{"cursor":null}`, `{"cursor":"` + strings.ToUpper(uuid.NewString()) + `"}`,
	}
	for _, body := range invalid {
		h, caller, store, _ := cursorHTTPFixture()
		w := cursorHTTPRead(h, cursorHTTPRequest(body))
		if w.Code != 400 || len(caller.calls) != 0 || store.commits != 0 {
			t.Fatalf("invalid query caused I/O: %s %d", body, w.Code)
		}
	}
	for _, mode := range []string{"no-delegation", "no-store", "missing-current-reference"} {
		h, caller, store, _ := cursorHTTPFixture()
		r := cursorHTTPRequest(`{"snapshot":true}`)
		want := 403
		switch mode {
		case "no-delegation":
			r.Header.Del("X-OUF-Delegation")
			r.Header.Del("X-OUF-Delegation-Verified")
		case "no-store":
			h.cursorStore = nil
			want = 503
		case "missing-current-reference":
			r.Header.Del("X-OUF-Authorization-Decision-Ref")
		}
		w := cursorHTTPRead(h, r)
		if w.Code != want || len(caller.calls) != 0 || store.commits != 0 {
			t.Fatalf("%s admitted untrusted I/O", mode)
		}
	}
}
