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
	return Scope{"mcp", "actor", "tenant"}, Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), Version: 1, SchemaHash: "sha256:" + strings.Repeat("a", 64), ColumnCount: n}, m
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

func TestLegacyCSVIdentityAndUnitIsolation(t *testing.T) {
	const historical = `{"assetId":"00000000-0000-4000-8000-000000000001","profileId":"00000000-0000-4000-8000-000000000002","profileVersion":1,"schemaHash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","columnCount":2}`
	const id = "d58d34118fa64f9b70619017fc9446e1ff25af254627b6c54c8c1d90f2b432b4"
	const taskID = "0d22f67b20e28210e7cad6c9dcb8dfb14f0aec4a494cc2112aa87e5723f57da6"
	scope, _, snapshot := fixture(t, 2)
	var p Profile
	if err := json.Unmarshal([]byte(historical), &p); err != nil {
		t.Fatal(err)
	}
	plan, err := Build(scope, p, snapshot, 7, []Work{{Tool: "semantic.search", Arguments: json.RawMessage(`{"q":"name"}`), Columns: []int{0}}})
	if err != nil || plan.ID != id || plan.ExpectedRevision != 7 || plan.Tasks[0].ID != taskID {
		t.Fatal("historical IDs or revision changed", plan, err)
	}
	if encoded, err := json.Marshal(p); err != nil || string(encoded) != historical {
		t.Fatal("historical profile encoding changed", string(encoded), err)
	}
	p.UnitRef = "table:0"
	if AnalysisID(scope, p) != id {
		t.Fatal("second budget for the singleton CSV table")
	}
	p.UnitRef = "sheet:0"
	first := AnalysisID(scope, p)
	p.UnitRef = "sheet:1"
	if first == id || first == AnalysisID(scope, p) {
		t.Fatal("distinct selections share a plan")
	}
}

func TestUnitPageChecksOwnerSelectionAndBounds(t *testing.T) {
	const asset = "00000000-0000-4000-8000-000000000001"
	const profile = "00000000-0000-4000-8000-000000000002"
	hash := "sha256:" + strings.Repeat("e", 64)
	for unit, kind := range map[string]string{"layer:2": "LAYER", "sheet:1": "SHEET", "table:0": "TABLE"} {
		page := map[string]any{
			"schema": "ouf.source-unit-profile.v1", "profileId": profile, "profileVersion": 1, "schemaHash": hash,
			"sourceKind": "FILE", "origin": map[string]string{"assetId": asset}, "unitKind": kind, "unitRef": unit,
			"columnCount": 1024, "columns": []any{map[string]string{"name": "id"}}, "hasMore": false, "nextOffset": nil,
		}
		body, _ := json.Marshal(page)
		p, err := UnitPage(body, asset, profile, unit, 1023, 128)
		if err != nil || p.Reference() != (UnitProfileRef{profile, 1, unit, hash}) {
			t.Fatal("last page of wide unit", kind, p, err)
		}
		for k, invalid := range map[string]any{"origin": map[string]string{"assetId": profile}, "unitRef": "other", "profileId": asset, "hasMore": true, "columnCount": 1025, "profileVersion": 0, "schemaHash": "invalid", "columns": []any{}} {
			saved := page[k]
			page[k] = invalid
			bad, _ := json.Marshal(page)
			if _, err := UnitPage(bad, asset, profile, unit, 1023, 128); err == nil {
				t.Fatalf("%s accepted invalid %s", kind, k)
			}
			page[k] = saved
		}
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
