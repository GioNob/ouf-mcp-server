package analysis

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/analysis/unicode151"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func TestPublishedIntentIdentity(t *testing.T) { // MCP-50T-02
	for input, want := range map[string]string{
		"\u0085Straße\u00a0\tÉ\u200a": "strasse é", "ςΣ": "σσ", "İ": "i\u0307", "\ufb03": "ffi",
		"\u1100\u1161\u11a8": "각", "A\u0315\u0300": "à\u0315", "x\u200by": "x\u200by",
	} {
		if got := unicode151.Normalize(input); got != want {
			t.Fatalf("%q: got %q want %q", input, got, want)
		}
	}
	base := Intent{Text: "  CAFÉ  Straße ", Type: "PROPERTY", Languages: []string{"IT", "en"}, Context: map[string]string{"domain": "A<\u2028&"}}
	if base.identify() != nil {
		t.Fatal(base)
	}
	// Computed independently with canonical UTF-8 JSON and SHA-256.
	if base.ID != "4f455963438029cb0cc8b97cb6e932d1e255eeabad6f99d65364b19a979167c0" {
		t.Fatal(base.ID)
	}
	for n, i := range []Intent{
		{Text: "cafe\u0301\u00a0STRASSE", Type: "PROPERTY", Languages: []string{"it", "EN"}, Context: base.Context},
		{Text: "cafe strasse", Type: base.Type, Languages: base.Languages, Context: base.Context},
		{Text: base.Text, Type: base.Type, Languages: []string{"en", "it"}, Context: base.Context},
		{Text: base.Text, Type: base.Type, Languages: base.Languages, Context: map[string]string{"domain": "a<\u2028&"}},
		{Text: base.Text, Type: "CLASS", Languages: base.Languages, Context: base.Context},
	} {
		if i.identify() != nil || (i.ID == base.ID) != (n == 0) {
			t.Fatal(n, i)
		}
		if n > 0 && ResidualTask("plan", base, []int{1}, 100, false).OwnerKey == ResidualTask("plan", i, []int{2}, 100, false).OwnerKey {
			t.Fatal("owner key collision")
		}
	}
}

func TestPublishedPackingAndCrossPageOrdinals(t *testing.T) { // MCP-50T-03
	for _, width := range []int{18, 1000, 1024} {
		scope, p, m := fixture(t, width)
		names := make([]string, width)
		for n := range names {
			names[n] = fmt.Sprintf("fixture-%d", n)
		}
		names[0], names[width-1] = "Straße", " STRASSE "
		for n := range m.Capabilities {
			if m.Capabilities[n].ToolName == BatchTool {
				m.Capabilities[n].OperationalLimits = json.RawMessage(`{"limitPerIntent":3,"batchMaxIntents":7,"batchMaxRequestBytes":1200,"batchMaxResponseBytes":10000,"preferredLanguages":["it"]}`)
			}
		}
		plan, err := BuildPublished(scope, p, m, names, nil, "test", 250)
		if err != nil {
			t.Fatal(err)
		}
		seen := make(map[int]bool)
		count := 0
		for _, task := range plan.Tasks {
			var b Batch
			if json.Unmarshal(task.Arguments, &b) != nil || len(task.Arguments)+250 > 1200 || len(b.Intents) > 7 || task.State != "READY" {
				t.Fatal(task)
			}
			count += len(b.Intents)
			for _, i := range b.Intents {
				cols := task.Groups[i.ID]
				if i.Text == "Straße" && !slices.Equal(cols, []int{0, width - 1}) {
					t.Fatal(cols)
				}
				for _, n := range cols {
					if seen[n] {
						t.Fatal("duplicate", n)
					}
					seen[n] = true
				}
			}
		}
		if len(seen) != width || count != width-1 {
			t.Fatal(width, len(seen), count)
		}
	}
}

func TestPublishedWholeBatchErrors(t *testing.T) { // MCP-50T-04
	scope, p, m := fixture(t, 2)
	plan, err := BuildPublished(scope, p, m, []string{"x", "y"}, nil, "fixture", 0)
	if err != nil {
		t.Fatal(err)
	}
	task := plan.Tasks[0]
	var b Batch
	if err = json.Unmarshal(task.Arguments, &b); err != nil {
		t.Fatal(err)
	}
	none := func(id string) string {
		return fmt.Sprintf(`{"intentId":%q,"state":"NONE","hasMore":false,"hits":[]}`, id)
	}
	a, z := none(b.Intents[0].ID), none(b.Intents[1].ID)
	for n, rows := range []string{a + "," + z, z + "," + a, a, a + "," + a, a + "," + strings.Replace(z, "NONE", "MATCH", 1), a + "," + strings.Replace(z, "false", "null", 1), a + "," + strings.Replace(z, `"hits":[]`, `"hits":[],"hits":[]`, 1)} {
		body := []byte(`{"contract":"ouf.semantic.published-batch.v1","results":[` + rows + "]}")
		if _, ok := AcceptedBatch(task, body); ok != (n == 0) {
			t.Fatal(n)
		}
	}
	for _, test := range []struct {
		status   int
		code     string
		expected string
		children int
		stop     bool
	}{
		{413, "SEM_BATCH_REQUEST_TOO_LARGE", "PUBLISHED_BATCH_SPLIT", 2, false},
		{422, "SEM_BATCH_RESULT_TOO_LARGE", "PUBLISHED_BATCH_SPLIT", 2, false},
		{403, "SEM_READ_RECEIPT_OR_POLICY_DENIED", "OWNER_AUTHORIZATION_DENIED", 0, true},
		{404, "ROUTE_NOT_FOUND", "PUBLISHED_BATCH_UNAVAILABLE", 2, false},
		{404, "OTHER", "OWNER_AUTHORIZATION_DENIED", 0, false},
		{503, "UNCLASSIFIED", "OWNER_REQUEST_REJECTED", 0, false},
	} {
		r := orchestration.Result{Problem: &orchestration.Problem{Status: test.status, Code: test.code}}
		f := (Coordinator{}).batchFinish(task, plan.ID, r, nil, Finish{State: "HELD", Code: "OWNER_AUTHORIZATION_DENIED"})
		if f.Code != test.expected || len(f.Children) != test.children || f.StopScope != test.stop {
			t.Fatal(test, f)
		}
		if test.status == 413 || test.status == 422 {
			child := f.Children[0]
			if child.Tool != BatchTool || len(SplitBatch(child, false)) != 0 {
				t.Fatal(child)
			}
			for n := 3; n > 1; n-- {
				next := SplitBatch(child, true)
				if len(next) != 1 {
					t.Fatal(n)
				}
				child = next[0]
			}
			if len(SplitBatch(child, true)) != 0 {
				t.Fatal("unbounded split")
			}
		}
	}
}

func TestPublishedScopeAndUnavailableDatatype(t *testing.T) { // MCP-50T-02, MCP-50T-07
	scope, profile, m := fixture(t, 2)
	i := orchestration.Identity{ServicePrincipalID: scope.Service, PrincipalID: scope.Principal, TenantID: scope.Tenant, Scopes: []string{"b", "a"}}
	d := orchestration.AuthorizationDecision{BundleID: "policy", BundleVersion: 1, ResourceScope: map[string]string{"visibility": "a"}}
	first := visibilityDigest(i, d)
	i.Scopes = []string{"a", "b"}
	if first != visibilityDigest(i, d) {
		t.Fatal("scope order changed visibility")
	}
	d.BundleVersion++
	if first == visibilityDigest(i, d) {
		t.Fatal("policy change reused evidence")
	}
	d.BundleVersion--
	d.ResourceScope["visibility"] = "b"
	if first == visibilityDigest(i, d) {
		t.Fatal("visibility change reused evidence")
	}
	work := []Work{
		{Tool: "semantic.discovery.request", Columns: []int{0}, Arguments: json.RawMessage(`{"intent":"fixture","preferredLanguages":["it"],"requestedArtifactType":"DATATYPE"}`)},
		{Tool: "semantic.discovery.request", Columns: []int{1}, Arguments: json.RawMessage(`{"intent":"fixture","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`)},
	}
	p, err := buildWithoutBatch(scope, profile, m, 0, work)
	if err != nil || len(p.Tasks) != 2 {
		t.Fatal(p, err)
	}
	if p.Tasks[0].State != "HELD" || p.Tasks[0].Code != "DISCOVERY_TYPE_UNSUPPORTED" || p.Tasks[1].State != "READY" || p.Tasks[1].Compatibility != "PUBLISHED_BATCH_UNAVAILABLE" {
		t.Fatal(p.Tasks)
	}
}

func TestPublishedPackingCountLimit(t *testing.T) { // MCP-50T-03
	scope, profile, m := fixture(t, 130)
	headers := make([]string, profile.ColumnCount)
	for n := range headers {
		headers[n] = fmt.Sprint(n)
	}
	for index := range m.Capabilities {
		if m.Capabilities[index].ToolName == BatchTool {
			m.Capabilities[index].OperationalLimits = json.RawMessage(`{"batchMaxIntents":64,"batchMaxRequestBytes":65536,"batchMaxResponseBytes":262144,"limitPerIntent":1,"preferredLanguages":["it"]}`)
		}
	}
	p, err := BuildPublished(scope, profile, m, headers, nil, "fixture", 0)
	if err != nil || len(p.Tasks) != 3 {
		t.Fatal(err, len(p.Tasks))
	}
	for n, want := range []int{64, 64, 2} {
		if len(p.Tasks[n].Groups) != want {
			t.Fatal(n, len(p.Tasks[n].Groups))
		}
	}
}

// Different client inputs can materialize identical tasks. The seed hash
// verifies a candidate input but cannot supply a missing original.
func TestPublishedTasksLoseOriginalOverrideGrouping(t *testing.T) {
	scope, p, m := fixture(t, 2)
	args := json.RawMessage(`{"intent":"name","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`)
	grouped := []Work{{Tool: "semantic.discovery.request", Arguments: args, Columns: []int{0, 1}}}
	separate := []Work{{Tool: grouped[0].Tool, Arguments: args, Columns: []int{0}}, {Tool: grouped[0].Tool, Arguments: args, Columns: []int{1}}}
	build := func(work []Work) Plan {
		plan, err := BuildPublished(scope, p, m, []string{"name", "name"}, work, "test", 0)
		if err != nil {
			t.Fatal(err)
		}
		return plan
	}
	a, b := build(grouped), build(separate)
	if a.Seed == b.Seed || Digest(a.Tasks) != Digest(b.Tasks) {
		t.Fatal("expected different seeds and identical stored tasks")
	}
}
