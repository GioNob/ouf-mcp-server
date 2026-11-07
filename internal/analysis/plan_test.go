package analysis

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

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
