package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Measure logical tool round trips with an open interactive window. All
// plan, claim, receipt, version and hint operations still use PostgreSQL.
type publishedPETStore struct{ *postgres.Store }

func (s publishedPETStore) Reserve(_ context.Context, r orchestration.AdmissionRequest) (orchestration.AdmissionDecision, error) {
	if _, _, err := orchestration.AdmissionWindowBudget(r); err != nil {
		return orchestration.AdmissionDecision{}, err
	}
	return orchestration.AdmissionDecision{AttemptID: r.AttemptID}, nil
}
func (publishedPETStore) Dispatch(context.Context, uuid.UUID, string, time.Duration, int64) error {
	return nil
}
func (publishedPETStore) Reconcile(context.Context, uuid.UUID, orchestration.Cost, orchestration.AttemptOutcome) error {
	return nil
}

type publishedPETOwner struct {
	mu      sync.Mutex
	columns []string
	store   *postgres.Store
	plan    string
	batches int
	jobs    map[string]string
	t1      bool
}

func (o *publishedPETOwner) Execute(ctx context.Context, r orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var args struct {
		AssetID, ProfileID, RequestID, Intent, IdempotencyKey string
		Offset, Limit                                         int
	}
	if err := json.Unmarshal(r.Arguments, &args); err != nil {
		return orchestration.GatewayResponse{}, err
	}
	var body any
	switch r.CapabilityID {
	case "ouf.managed-source.preview":
		end := min(args.Offset+args.Limit, len(o.columns))
		var columns []nextstep.Object
		for n := args.Offset; n < end; n++ {
			columns = append(columns, nextstep.Object{"ordinal": n, "name": o.columns[n]})
		}
		body = nextstep.Object{"assetId": args.AssetID, "profileId": args.ProfileID, "profileVersion": 1, "schemaHash": "sha256:" + strings.Repeat("a", 64),
			"view": "COLUMNS", "format": "CSV", "workflowState": "PROFILE_COLUMNS", "columnCount": len(o.columns), "columns": columns, "offset": args.Offset, "nextOffset": end, "hasMore": end < len(o.columns)}
	case "ouf.semantic.search.batch":
		o.batches++
		var b analysis.Batch
		if err := json.Unmarshal(r.Arguments, &b); err != nil {
			return orchestration.GatewayResponse{}, err
		}
		// Synthetic Italian exact labels for the three published properties.
		catalog := map[string]string{"id_redazionale": "POIID", "nome": "POIofficialName", "indirizzo": "fullAddress"}
		var results []analysis.BatchResult
		for _, i := range b.Intents {
			res := analysis.BatchResult{ID: i.ID, State: "NONE", Hits: []analysis.PublishedMatch{}}
			if term := catalog[i.Text]; term != "" {
				res.State = "MATCH"
				res.Hits = []analysis.PublishedMatch{{SemanticID: "urn:fixture:" + term, SemanticVersion: "1.0.0", RevisionID: uuid.NewString(),
					PublicationSetID: "55555555-5555-4555-8555-555555555555", Score: 100, Reasons: []string{"EXACT_LABEL", "PREFERRED_LANGUAGE"}}}
			} else if i.Text == "tipologia" {
				res.State = "AMBIGUOUS"
				res.Hits = []analysis.PublishedMatch{{SemanticID: "urn:fixture:weak", SemanticVersion: "1.0.0", RevisionID: uuid.NewString(),
					PublicationSetID: "55555555-5555-4555-8555-555555555555", Score: 60, Reasons: []string{"LABEL_PREFIX", "PREFERRED_LANGUAGE"}}}
			}
			results = append(results, res)
		}
		body = nextstep.Object{"contract": analysis.BatchContract, "results": results}
	case "ouf.semantic.discovery":
		if _, sent := o.jobs[args.Intent]; sent || args.IdempotencyKey != r.IdempotencyKey {
			return orchestration.GatewayResponse{}, fmt.Errorf("duplicate or changed owner key for %q", args.Intent)
		}
		if o.t1 {
			if args.Intent == "id_redazionale" || args.Intent == "nome" || args.Intent == "indirizzo" || args.Intent == "datatype-fixture" {
				return orchestration.GatewayResponse{}, fmt.Errorf("unexpected discovery for %q", args.Intent)
			}
			var count int
			err := o.store.Pool().QueryRow(ctx, `select count(*) from ouf_mcp.profile_analysis_task where analysis_id=$1 and task->'arguments'->>'idempotencyKey'=$2 and task->>'state'='RUNNING'`, o.plan, r.IdempotencyKey).Scan(&count)
			if err != nil || count != 1 {
				return orchestration.GatewayResponse{}, fmt.Errorf("key absent before owner dispatch: %d %v", count, err)
			}
		}
		ref := uuid.NewString()
		o.jobs[args.Intent] = ref
		body = nextstep.Object{"requestId": ref, "state": "PENDING"}
	case "ouf.semantic.discovery.status", "ouf.semantic.discovery.candidates":
		return (summaryOwner{args.RequestID}).Execute(ctx, r, 0)
	default:
		return orchestration.GatewayResponse{}, fmt.Errorf("unexpected capability %s", r.CapabilityID)
	}
	raw, err := json.Marshal(body)
	return orchestration.GatewayResponse{Status: 200, Body: raw}, err
}

func TestPublishedNextOnlyRoundTrips(t *testing.T) { // MCP-50T-05, MCP-50T-06
	dsn := os.Getenv("MCP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("MCP_TEST_DATABASE_URL required")
	}
	t.Setenv("OUF_ANALYSIS_SUMMARY_PAGE_BYTES", "65536")
	ctx := context.Background()
	db, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = postgres.Migrate(ctx, db.Pool()); err != nil {
		t.Fatal(err)
	}
	var fixture railFixture
	raw, err := os.ReadFile("../nextstep/testdata/e2e-20261123.json")
	if err != nil || json.Unmarshal(raw, &fixture) != nil || len(fixture.ProfileColumns) != 18 {
		t.Fatal("missing 23/11 fixture", err)
	}
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	checksum, err := m.Checksum()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := m.CanonicalPayload()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.EnsureManifest(ctx, checksum, m.Version, "mcp-manifest-v1", payload); err != nil {
		t.Fatal(err)
	}
	before, after := 0, 0
	for _, tc := range []struct {
		width int
		t1    bool
	}{{18, false}, {18, true}, {1000, true}} {
		p := analysis.Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), Version: 1, SchemaHash: "sha256:" + strings.Repeat("a", 64), ColumnCount: tc.width}
		scope := analysis.Scope{Service: "ouf-chatgpt", Principal: "user-a", Tenant: "tenant-a"}
		names := make([]string, tc.width)
		var work []analysis.Work
		for n := range names {
			names[n] = fixture.ProfileColumns[n%18]
		}
		if tc.width == 1000 {
			names[0], names[999] = "Straße", " STRASSE "
			names[1] = "datatype-fixture"
			for n := 0; n < 18; n++ {
				typ := "PROPERTY"
				if names[n] == "datatype-fixture" {
					typ = "DATATYPE"
				}
				args, _ := json.Marshal(map[string]any{"intent": names[n], "preferredLanguages": []string{"it"}, "requestedArtifactType": typ})
				work = append(work, analysis.Work{Tool: "semantic.discovery.request", Arguments: args, Columns: []int{n}})
			}
		}
		owner := &publishedPETOwner{columns: names, store: db, plan: analysis.AnalysisID(scope, p), t1: tc.t1, jobs: map[string]string{}}
		service := &orchestration.Service{Auth: analysisSDKAuth{}, Admission: publishedPETStore{db}, Gateway: owner, RequireDelegation: true, FingerprintKey: make([]byte, 32)}
		session := revisionSession(t, service, "ouf.managed-source.preview ouf.semantic.search ouf.semantic.discovery ouf.semantic.discovery.status ouf.semantic.discovery.candidates")
		name := "source.analysis.plan"
		args := nextstep.Object{"assetId": p.AssetID, "profileId": p.ProfileID, "expectedRevision": 0, "work": work}
		if tc.width == 18 {
			preview, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "source.file.preview", Arguments: nextstep.Object{
				"assetId": p.AssetID, "profileId": p.ProfileID, "view": "COLUMNS", "offset": 0, "limit": 18,
			}})
			if err != nil || preview.IsError {
				t.Fatal(preview, err)
			}
			next := checkNext(t, nextContract(t), preview)
			if next["action"] != "CALL_TOOL" || next["tool"] != name {
				t.Fatal("preview did not generate a plan", next)
			}
			args = next["arguments"].(nextstep.Object)
			raw, err := json.Marshal(args["work"])
			if err != nil || json.Unmarshal(raw, &work) != nil || len(work) != 18 {
				t.Fatal("incomplete generated plan", err)
			}
		}
		if !tc.t1 {
			legacy, err := analysis.Build(scope, p, m, 0, work)
			if err != nil {
				t.Fatal(err)
			}
			legacy.Headers = names
			if _, err = db.PlanAnalysis(ctx, legacy); err != nil {
				t.Fatal(err)
			}
		}
		calls, summaryPages, delivered, matched, ready, blocked := 0, 0, 0, 0, 0, 0
		for calls < 200 {
			res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
			calls++
			if err != nil || res.IsError {
				t.Fatalf("%s %v %v", name, err, res)
			}
			data := ownerObject(res)
			if rows, ok := data["items"].([]any); ok && name == "source.analysis.read" && args["view"] == "SUMMARY" {
				summaryPages++
				for _, v := range rows {
					row := v.(nextstep.Object)
					if fmt.Sprint(row["ordinal"]) != fmt.Sprint(delivered) {
						t.Fatal("ordinal gap", row)
					}
					delivered++
					switch row["state"] {
					case "MATCHED_PUBLISHED":
						matched++
						if pin, ok := row["match"].(nextstep.Object); !ok || pin["semanticVersion"] != "1.0.0" || pin["publicationSetId"] != "55555555-5555-4555-8555-555555555555" {
							t.Fatal("pin missing")
						}
					case "CANDIDATES_READY":
						ready++
					case "BLOCKED":
						blocked++
						if row["reasonCode"] != "DISCOVERY_TYPE_UNSUPPORTED" {
							t.Fatal(row)
						}
					default:
						t.Fatal("unexpected outcome", row)
					}
				}
			}
			if delivered == tc.width {
				break
			}
			next := data["next"].(nextstep.Object)
			if next["action"] != "CALL_TOOL" {
				t.Fatal("rail stopped", data)
			}
			name = next["tool"].(string)
			args = next["arguments"].(nextstep.Object)
		}
		if delivered != tc.width || matched+ready+blocked != tc.width {
			t.Fatal(tc, delivered)
		}
		owner.mu.Lock()
		jobs, batches := len(owner.jobs), owner.batches
		owner.mu.Unlock()
		if tc.width == 1000 {
			if jobs != 16 || batches != 1 || blocked != 1 || matched != 166 {
				t.Fatalf("1000 columns: batches=%d jobs=%d blocked=%d matched=%d", batches, jobs, blocked, matched)
			}
			// One plan, one published batch, then step/status/candidates per residual job.
			if summaryPages != 32 || calls != 1+batches+3*jobs+summaryPages {
				t.Fatalf("1000 columns: calls=%d batches=%d jobs=%d summaryPages=%d", calls, batches, jobs, summaryPages)
			}
			t.Logf("MCP-50T-06 chatbot calls from plan through SUMMARY: %d (%d summary pages)", calls, summaryPages)
		} else if tc.t1 {
			if _, ok := owner.jobs["ente gestore"]; !ok {
				t.Fatal("residual discovery lost the preview wording", owner.jobs)
			}
			after = calls
			if matched != 3 || ready != 15 || batches != 1 || jobs != 15 {
				t.Fatal(tc, matched, ready)
			}
		} else {
			before = calls
		}
	}
	t.Logf("MCP-50T-05 chatbot calls from plan through SUMMARY: before=%d after=%d; excluding common plan and SUMMARY: %d -> %d", before, after, before-2, after-2)
	if before != 74 || after != 48 {
		t.Fatal("unexpected next-only call counts", before, after)
	}
}
