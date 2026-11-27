package analysis

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/google/uuid"
)

func fixture(t *testing.T, n int) (Scope, Profile, *manifest.Snapshot) {
	t.Helper()
	m, e := manifest.Load()
	if e != nil {
		t.Fatal(e)
	}
	return Scope{"mcp", "actor", "tenant"}, Profile{uuid.NewString(), uuid.NewString(), 1, "sha256:" + strings.Repeat("a", 64), n}, m
}
func TestReadRetryTimeUsesLatestBound(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		header, nextAttempt, notBefore, want int
	}{
		{1, 2, 3, 5}, {90, 72, 60, 90}, {15, 72, 60, 72}, {15, 60, 72, 72},
	} {
		at := func(seconds int) time.Time { return now.Add(time.Duration(seconds) * time.Second) }
		body, _ := json.Marshal(map[string]time.Time{"nextAttemptAt": at(tc.nextAttempt), "notBefore": at(tc.notBefore)})
		got, valid := readRetryTime(now, 5*time.Second, fmt.Sprint(tc.header), body)
		if !valid || !got.Equal(at(tc.want)) {
			t.Fatal(tc, got, valid)
		}
	}
}

func TestReadRetryTimeEachOwnerBound(t *testing.T) {
	now := time.Unix(1800000000, 0).UTC()
	due := now.Add(72 * time.Second)
	for _, tc := range []struct {
		field string
		value any
		want  time.Time
	}{
		{"nextAttemptAt", due, due}, {"notBefore", due, due}, {"next_retry_at", due, due}, {"retryAt", due, due},
		{"pollAfterSeconds", 72, due}, {"retryAfterSeconds", 72, due}, {"Retry-After", "72", due},
		{"RetryAfter", "72", due}, {"recoveryState", "RETRY_WAIT", now.Add(5 * time.Second)},
	} {
		for _, nested := range []bool{false, true} {
			fields := map[string]any{tc.field: tc.value}
			if nested {
				fields = map[string]any{"result": fields}
			}
			body, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			got, valid := readRetryTime(now, time.Second, "", body)
			if !valid || !got.Equal(tc.want) {
				t.Fatalf("%s nested=%t: got %v, valid=%t", tc.field, nested, got, valid)
			}
		}
	}
}

func TestPlanActualWorkForWideProfiles(t *testing.T) {
	for _, n := range []int{20, 50, 100, 200, 300, 400, 1024} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s, p, m := fixture(t, n)
			all := []Task{}
			for start := 0; start < n; start += 128 {
				work := []Work{}
				for i := start; i < min(start+128, n); i++ {
					args, _ := json.Marshal(map[string]any{"q": fmt.Sprintf("property-%d", i), "type": "PROPERTY"})
					work = append(work, Work{Tool: "semantic.search", Arguments: args, Columns: []int{i}})
				}
				plan, e := Build(s, p, m, 0, work)
				if e != nil {
					t.Fatal(e)
				}
				all = append(all, plan.Tasks...)
			}
			budget, covered, complete := Calculate(all)
			if budget.PlannedCalls != int64(n) || budget.MaxUsefulCallTimeMS != int64(n)*3000 || covered != n || complete != 0 {
				t.Fatal(budget, covered, complete)
			}
		})
	}
}
func TestPlanDeduplicatesSharedWorkAndKeepsDiscoveryStable(t *testing.T) {
	s, p, m := fixture(t, 400)
	work := []Work{{Tool: "semantic.search", Arguments: json.RawMessage(`{"q":"tipologia","type":"PROPERTY"}`), Columns: []int{2}}, {Tool: "semantic.search", Arguments: json.RawMessage(`{"type":"PROPERTY","q":"tipologia"}`), Columns: []int{3, 2}}}
	a, e := Build(s, p, m, 0, work)
	if e != nil || len(a.Tasks) != 1 || len(a.Tasks[0].Columns) != 2 {
		t.Fatal(a, e)
	}
	work = []Work{{Tool: "semantic.discovery.request", Arguments: json.RawMessage(`{"requestedArtifactType":"PROPERTY","intent":"Proprietà che indica la tipologia del teatro","preferredLanguages":["it"]}`), Columns: []int{2}}}
	a, e = Build(s, p, m, 0, work)
	if e != nil {
		t.Fatal(e)
	}
	b, e := Build(s, p, m, 12, work)
	if e != nil || a.ID != b.ID || a.Tasks[0].ID != b.Tasks[0].ID || string(a.Tasks[0].Arguments) != string(b.Tasks[0].Arguments) {
		t.Fatal("revision reset owner key", e)
	}
	var args map[string]any
	json.Unmarshal(a.Tasks[0].Arguments, &args)
	if !strings.HasPrefix(args["idempotencyKey"].(string), "analysis:") {
		t.Fatal(args)
	}
}
func TestPlanRejectsForgedScopeAndUnsupportedWork(t *testing.T) {
	s, p, m := fixture(t, 20)
	base := Work{Tool: "semantic.search", Arguments: json.RawMessage(`{"q":"name"}`), Columns: []int{0}}
	for _, w := range []Work{{Tool: "semantic.search", Arguments: json.RawMessage(`{"q":"name","url":"https://example.org"}`), Columns: []int{0}}, {Tool: "semantic.search", Arguments: base.Arguments, Columns: []int{20}}, {Tool: "semantic.propose", Arguments: json.RawMessage(`{}`), Columns: []int{0}}, {Tool: "semantic.search", Arguments: base.Arguments, Columns: []int{0}, Observation: 1}, {Tool: "semantic.discovery.request", Arguments: json.RawMessage(`{"idempotencyKey":"attacker-reset-key","intent":"name"}`), Columns: []int{0}}} {
		if _, e := Build(s, p, m, 0, []Work{w}); e == nil {
			t.Fatal("accepted", w)
		}
	}
	p.ColumnCount = 1025
	if _, e := Build(s, p, m, 0, []Work{base}); e == nil {
		t.Fatal("accepted 1025")
	}
}

func TestBuildPreservesDiscoveryTaskIdentity(t *testing.T) {
	scope, profile, manifest := fixture(t, 2)
	const canonical = `{"requestId":"abcdef01-2345-4abc-8def-0123456789ab"}`
	const upper = `{"requestId":"ABCDEF01-2345-4ABC-8DEF-0123456789AB"}`
	a := Work{Tool: "semantic.discovery.status", Arguments: json.RawMessage(canonical), Columns: []int{0}, Observation: 1}
	b := Work{Tool: a.Tool, Arguments: json.RawMessage(upper), Columns: []int{1}, Observation: a.Observation}
	plan, err := Build(scope, profile, manifest, 0, []Work{a, b})
	if err != nil || len(plan.Tasks) != 2 {
		t.Fatal(plan.Tasks, err)
	}
	for i, work := range []Work{a, b} {
		task := plan.Tasks[i]
		if task.ID != Digest([]any{work.Tool, work.Arguments, work.Observation}) ||
			string(task.Arguments) != string(work.Arguments) || len(task.Columns) != 1 || task.Columns[0] != work.Columns[0] {
			t.Fatal("changed the retained task identity", task)
		}
	}
	b.Arguments = json.RawMessage(`{"requestId":"invalid"}`)
	if _, err := Build(scope, profile, manifest, 0, []Work{b}); err != ErrInvalid {
		t.Fatal("non-UUID discovery request was accepted", err)
	}
	const candidateArgs = `{"includeState":true,"requestId":"ABCDEF01-2345-4ABC-8DEF-0123456789AB"}`
	candidates := Work{Tool: "semantic.discovery.candidates", Arguments: json.RawMessage(candidateArgs), Columns: []int{0}}
	plan, err = Build(scope, profile, manifest, 0, []Work{candidates})
	if err != nil || len(plan.Tasks) != 1 || string(plan.Tasks[0].Arguments) != candidateArgs {
		t.Fatal("changed the candidate arguments", plan.Tasks, err)
	}
}
