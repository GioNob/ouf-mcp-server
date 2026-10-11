package operational

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

type composerStoreFixture struct {
	mu       sync.Mutex
	nodes    map[uuid.UUID]CursorNode
	bindings map[uuid.UUID]CursorBinding
	commits  int
}

func (s *composerStoreFixture) CreateIncidentCursor(_ context.Context, binding CursorBinding, frame CursorFrame) (CursorNode, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nodes == nil {
		s.nodes = map[uuid.UUID]CursorNode{}
		s.bindings = map[uuid.UUID]CursorBinding{}
	}
	node := CursorNode{Token: uuid.New(), Session: uuid.New(), Frame: frame, ExpiresAt: time.Now().UTC().Add(30 * time.Minute)}
	s.nodes[node.Token] = node
	s.bindings[node.Token] = binding
	return node, nil
}
func (s *composerStoreFixture) LoadIncidentCursor(_ context.Context, token uuid.UUID, binding CursorBinding) (CursorNode, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bindings[token] != binding {
		return CursorNode{}, ErrCursorNotFound
	}
	return s.nodes[token], nil
}
func (s *composerStoreFixture) CommitIncidentCursorPage(_ context.Context, token uuid.UUID, binding CursorBinding, digest string, next *CursorFrame) (*uuid.UUID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bindings[token] != binding {
		return nil, ErrCursorNotFound
	}
	node := s.nodes[token]
	if node.PageDigest != "" {
		if node.PageDigest != digest || (node.NextToken == nil) != (next == nil) {
			return nil, ErrCursorConflict
		}
		return node.NextToken, nil
	}
	if next != nil {
		id := uuid.New()
		node.NextToken = &id
		s.nodes[id] = CursorNode{Token: id, Session: node.Session, Frame: *next, ExpiresAt: node.ExpiresAt}
		s.bindings[id] = binding
	}
	node.PageDigest = digest
	s.nodes[token] = node
	s.commits++
	return node.NextToken, nil
}

type composerCallerFixture struct {
	mu        sync.Mutex
	pages     map[string][]cursorOwnerPage
	calls     []orchestration.Invocation
	fail      string
	transform func(string, *cursorOwnerPage)
}

func (f *composerCallerFixture) Call(_ context.Context, in orchestration.Invocation) (orchestration.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, in)
	name := "INGESTION"
	if in.Owner == "gateway" {
		name = "GATEWAY"
	}
	if f.fail == name {
		return orchestration.Result{}, errors.New("owner unavailable")
	}
	var query map[string]json.RawMessage
	_ = json.Unmarshal(in.Arguments, &query)
	page := f.pages[name][0]
	if raw, ok := query["cursor"]; ok {
		var token string
		_ = json.Unmarshal(raw, &token)
		found := false
		for _, p := range f.pages[name] {
			if p.Cursor == token {
				page = p
				found = true
				break
			}
		}
		if !found {
			return orchestration.Result{}, errors.New("unknown owner cursor")
		}
	}
	encoded, _ := json.Marshal(page)
	_ = json.Unmarshal(encoded, &page)
	if f.transform != nil {
		f.transform(name, &page)
	}
	body, _ := json.Marshal(page)
	return orchestration.Result{Body: body}, nil
}

func composerPage(name, severity string, start, count int, next bool) cursorOwnerPage {
	partial := false
	p := cursorOwnerPage{Items: []map[string]any{}, Partial: &partial, Cursor: uuid.NewString(), SnapshotRef: uuid.NewString(), ExpiresAt: time.Now().Add(time.Hour), SnapshotExpiresAt: time.Now().Add(time.Hour), ItemsDigest: strings.Repeat("a", 64), PriorityProfile: "ouf.incident-priority.v1", CursorProfile: "ouf.incident-owner-cursor.v1"}
	for index := start; index < start+count; index++ {
		p.Items = append(p.Items, map[string]any{"incident_id": fmt.Sprintf("%s-%04d", name, index), "module": name, "severity": severity, "lifecycle_state": "OPEN", "action_required": true, "last_seen_at": "2026-10-11T00:00:00Z", "visibility_class": "TENANT_OPERATIONAL"})
	}
	p.Truncated = next
	return p
}
func composerFixture(limit int) (CursorComposer, aggregateRequest, *composerCallerFixture, *composerStoreFixture) {
	caller := &composerCallerFixture{pages: map[string][]cursorOwnerPage{"GATEWAY": {composerPage("GATEWAY", "WARNING", 0, 2, false)}, "INGESTION": {composerPage("INGESTION", "CRITICAL", 0, 2, false)}}}
	store := &composerStoreFixture{}
	self := aggregateSelfFixture{body: []byte(`{"module":"MCP","status":"HEALTHY","actionRequired":false,"partial":false}`)}
	a := &Aggregator{Caller: caller, Self: self, ManifestChecksum: strings.Repeat("b", 64)}
	in := aggregateRequest{Identity: orchestration.Identity{PrincipalID: "operator", TenantID: "tenant", ActorType: "HUMAN"}, Arguments: json.RawMessage(fmt.Sprintf(`{"limit":%d,"sourceId":"source-a"}`, limit)), CorrelationID: "conversation", AttemptID: uuid.NewString()}
	return CursorComposer{a, store}, in, caller, store
}

type composedPage struct {
	Items     []map[string]any `json:"items"`
	Cursor    string           `json:"cursor"`
	Next      string           `json:"nextCursor"`
	Consulted bool             `json:"consultationRecorded"`
	Coverage  struct {
		Calls int  `json:"ownerCalls"`
		Scan  bool `json:"scanPending"`
	} `json:"coverage"`
}

func composed(t *testing.T, body []byte, err error) composedPage {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	var page composedPage
	if json.Unmarshal(body, &page) != nil {
		t.Fatal(string(body))
	}
	return page
}

func TestIncidentCursorComposerPriorityReplayAndIndependentRestart(t *testing.T) {
	c, in, caller, store := composerFixture(1)
	body, err := c.Start(context.Background(), in, "policy:1")
	page := composed(t, body, err)
	if page.Items[0]["severity"] != "CRITICAL" || page.Next == "" || page.Consulted {
		t.Fatal(string(body))
	}
	firstToken := uuid.MustParse(page.Cursor)
	// Fresh wrapper simulates process recreation; metadata comes from its store.
	c = CursorComposer{Aggregator: c.Aggregator, Store: store}
	replay, replayErr := c.Page(context.Background(), in, "policy:1", firstToken)
	if replayErr != nil || string(replay) != string(body) {
		t.Fatalf("unstable replay: %v", replayErr)
	}
	all := append([]map[string]any{}, page.Items...)
	for page.Next != "" {
		body, err = c.Page(context.Background(), in, "policy:1", uuid.MustParse(page.Next))
		page = composed(t, body, err)
		all = append(all, page.Items...)
	}
	if len(all) != 4 || !sort.SliceIsSorted(all, func(i, j int) bool { return incidentBefore(all[i], all[j]) }) {
		t.Fatal(all)
	}
	for _, call := range caller.calls {
		if call.Owner == "ingestion" && call.Resource.Attributes["sourceRef"] != "source-a" {
			t.Fatal("lost saved source grant")
		}
		if call.Owner == "gateway" && call.Resource.Attributes["incidentScope"] != "PLATFORM" {
			t.Fatal("gateway scope widened")
		}
	}
}

func TestIncidentCursorComposerOwnerFailureNeverAdvances(t *testing.T) {
	c, in, caller, store := composerFixture(1)
	body, err := c.Start(context.Background(), in, "policy:1")
	page := composed(t, body, err)
	before := store.commits
	caller.fail = "INGESTION"
	if _, err = c.Page(context.Background(), in, "policy:1", uuid.MustParse(page.Next)); !errors.Is(err, ErrCursorOwnerUnavailable) || store.commits != before {
		t.Fatal("unavailable owner advanced page")
	}
	caller.fail = ""
	_, err = c.Page(context.Background(), in, "policy:1", uuid.MustParse(page.Next))
	if err != nil {
		t.Fatal(err)
	}
}

func TestIncidentCursorComposerRedactionAndPayloadChangesInvalidate(t *testing.T) {
	for _, mode := range []string{"redacted", "payload", "snapshot", "owner-digest", "priority", "unknown-private-field"} {
		t.Run(mode, func(t *testing.T) {
			c, in, caller, store := composerFixture(1)
			body, err := c.Start(context.Background(), in, "policy:1")
			page := composed(t, body, err)
			before := store.commits
			caller.transform = func(name string, p *cursorOwnerPage) {
				if name != "GATEWAY" {
					return
				}
				switch mode {
				case "redacted":
					value := true
					p.Partial = &value
				case "payload":
					p.Items[0]["impact_summary"] = "changed despite unchanged reported digest"
				case "snapshot":
					p.SnapshotRef = uuid.NewString()
				case "owner-digest":
					p.ItemsDigest = strings.Repeat("c", 64)
				case "priority":
					p.Items[0]["severity"] = "INFO"
				case "unknown-private-field":
					p.Items[0]["future_private_secret"] = "hidden"
				}
			}
			if _, err = c.Page(context.Background(), in, "policy:1", uuid.MustParse(page.Next)); err == nil || store.commits != before {
				t.Fatal("changed owner visibility or payload accepted")
			}
		})
	}
}

func TestIncidentCursorComposerBindingAndPolicyCannotBeReused(t *testing.T) {
	c, in, caller, _ := composerFixture(1)
	body, err := c.Start(context.Background(), in, "policy:1")
	page := composed(t, body, err)
	count := len(caller.calls)
	for _, mode := range []string{"principal", "tenant", "scope", "policy"} {
		other := in
		other.Identity = in.Identity
		policy := "policy:1"
		switch mode {
		case "principal":
			other.Identity.PrincipalID = "other"
		case "tenant":
			other.Identity.TenantID = "other"
		case "scope":
			other.Identity.Scopes = []string{"new-scope"}
		case "policy":
			policy = "policy:2"
		}
		if _, err = c.Page(context.Background(), other, policy, uuid.MustParse(page.Next)); !errors.Is(err, ErrCursorNotFound) {
			t.Fatal(mode, err)
		}
	}
	if len(caller.calls) != count {
		t.Fatal("invalid binding queried owners")
	}
}

func TestIncidentCursorComposerBoundedRefillKeepsGlobalOrder(t *testing.T) {
	c, in, caller, _ := composerFixture(70)
	for _, name := range []string{"GATEWAY", "INGESTION"} {
		first := composerPage(name, "ERROR", 0, 100, true)
		second := composerPage(name, "ERROR", 100, 100, true)
		third := composerPage(name, "ERROR", 200, 20, false)
		second.SnapshotRef = first.SnapshotRef
		third.SnapshotRef = first.SnapshotRef
		first.NextCursor = second.Cursor
		second.NextCursor = third.Cursor
		caller.pages[name] = []cursorOwnerPage{first, second, third}
	}
	body, err := c.Start(context.Background(), in, "policy:1")
	page := composed(t, body, err)
	all := []map[string]any{}
	seen := map[string]bool{}
	for index := 0; index < 20; index++ {
		if page.Coverage.Calls > 3 {
			t.Fatal("owner RPC bound exceeded")
		}
		for _, item := range page.Items {
			id := stringValue(item["incident_id"])
			if seen[id] {
				t.Fatal("duplicate", id)
			}
			seen[id] = true
		}
		all = append(all, page.Items...)
		if page.Next == "" {
			break
		}
		body, err = c.Page(context.Background(), in, "policy:1", uuid.MustParse(page.Next))
		page = composed(t, body, err)
	}
	if len(all) != 440 || !sort.SliceIsSorted(all, func(i, j int) bool { return incidentBefore(all[i], all[j]) }) {
		t.Fatal("lost or reordered incidents", len(all))
	}
}

func TestIncidentCursorComposerRejectsUnknownFilterWithoutIO(t *testing.T) {
	c, in, caller, store := composerFixture(1)
	in.Arguments = json.RawMessage(`{"cursor":"forged"}`)
	if _, err := c.Start(context.Background(), in, "policy:1"); !errors.Is(err, ErrCursorConflict) || len(caller.calls) != 0 || store.commits != 0 {
		t.Fatal("invalid filter accessed state")
	}
}

func TestIncidentCursorComposerConcurrentReplayOneContinuation(t *testing.T) {
	c, in, _, _ := composerFixture(1)
	body, err := c.Start(context.Background(), in, "policy:1")
	page := composed(t, body, err)
	var wg sync.WaitGroup
	results := make(chan string, 12)
	failures := make(chan error, 12)
	for index := 0; index < 12; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			raw, err := c.Page(context.Background(), in, "policy:1", uuid.MustParse(page.Cursor))
			if err != nil {
				failures <- err
				return
			}
			results <- string(raw)
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	for result := range results {
		if result != string(body) {
			t.Fatal("different replay continuation")
		}
	}
}

func TestIncidentCursorComposerCompletedOwnerStillReauthorized(t *testing.T) {
	c, in, caller, store := composerFixture(1)
	caller.pages["INGESTION"][0].Items = []map[string]any{}
	body, err := c.Start(context.Background(), in, "policy:1")
	page := composed(t, body, err)
	before := store.commits
	caller.fail = "INGESTION"
	if _, err = c.Page(context.Background(), in, "policy:1", uuid.MustParse(page.Next)); !errors.Is(err, ErrCursorOwnerUnavailable) || store.commits != before {
		t.Fatal("completed owner bypassed reauthorization")
	}
}

func TestIncidentCursorComposerPartialSnapshotDoesNotCreateCursor(t *testing.T) {
	c, in, caller, store := composerFixture(1)
	value := true
	caller.pages["GATEWAY"][0].Partial = &value
	if _, err := c.Start(context.Background(), in, "policy:1"); !errors.Is(err, ErrCursorOwnerUnavailable) || len(store.nodes) != 0 {
		t.Fatal("partial owner was saved as complete")
	}
}

func TestIncidentCursorComposerSelfChangeFencesSnapshot(t *testing.T) {
	c, in, _, store := composerFixture(1)
	body, err := c.Start(context.Background(), in, "policy:1")
	page := composed(t, body, err)
	before := store.commits
	c.Aggregator.Self = aggregateSelfFixture{body: []byte(`{"module":"MCP","status":"DEGRADED","actionRequired":true,"partial":false}`)}
	if _, err = c.Page(context.Background(), in, "policy:1", uuid.MustParse(page.Next)); !errors.Is(err, ErrCursorConflict) || store.commits != before {
		t.Fatal("self state change ignored")
	}
}
