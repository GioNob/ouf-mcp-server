//go:build batchload

package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/sync/errgroup"
)

const loadPageWidth = 8

type loadConfig struct {
	Profile     string         `json:"profile"`
	Processes   int            `json:"processes"`
	Users       int            `json:"users"`
	Columns     int            `json:"columns"`
	Connections int32          `json:"connections"`
	WindowSec   int            `json:"quota_window_seconds"`
	Gateway     map[string]int `json:"gateway_quota_per_route"`
	Semantic    map[string]int `json:"semantic_quota_per_route"`
}

func readLoadConfig(t *testing.T) loadConfig {
	t.Helper()
	c := loadConfig{Profile: os.Getenv("OUF_BATCH_LOAD_PROFILE"), Connections: 5, WindowSec: 60,
		Gateway: map[string]int{}, Semantic: map[string]int{}}
	var quota int
	switch c.Profile {
	case "", "small":
		c.Profile, c.Processes, c.Users, c.Columns, quota = "small", 30, 5, 100, 4
	case "full":
		c.Processes, c.Users, c.Columns, quota = 3000, 100, 1000, 200
	default:
		t.Fatal("OUF_BATCH_LOAD_PROFILE must be small or full")
	}
	for _, route := range loadRoutes {
		c.Gateway[route] = quota
	}
	if c.Profile == "small" {
		c.Semantic[loadCandidates] = 2
	}
	if raw := os.Getenv("OUF_BATCH_LOAD_QUOTAS"); raw != "" {
		var overrides struct {
			WindowSec   *int           `json:"window_seconds"`
			Connections *int32         `json:"connections"`
			Gateway     map[string]int `json:"gateway"`
			Semantic    map[string]int `json:"semantic"`
		}
		d := json.NewDecoder(strings.NewReader(raw))
		d.DisallowUnknownFields()
		if err := d.Decode(&overrides); err != nil || !json.Valid([]byte(raw)) {
			t.Fatal("invalid OUF_BATCH_LOAD_QUOTAS JSON")
		}
		if overrides.WindowSec != nil {
			c.WindowSec = *overrides.WindowSec
		}
		if overrides.Connections != nil {
			c.Connections = *overrides.Connections
		}
		for _, entry := range []struct{ target, source map[string]int }{{c.Gateway, overrides.Gateway}, {c.Semantic, overrides.Semantic}} {
			for route, n := range entry.source {
				if _, known := c.Gateway[route]; !known || n < 0 {
					t.Fatal("unknown route or negative limit in OUF_BATCH_LOAD_QUOTAS")
				}
				entry.target[route] = n
			}
		}
	}
	if c.Connections < 1 || c.Connections > 100 || c.WindowSec < 1 || c.WindowSec > 3600 {
		t.Fatal("invalid database pool size or quota window")
	}
	return c
}

func TestBatchLoadQualification(t *testing.T) {
	config := readLoadConfig(t)
	dsn := os.Getenv("OUF_BATCH_LOAD_DATABASE_URL")
	if dsn == "" && os.Getenv("MCP_TEST_DATABASE_URL") == "" {
		t.Fatal("set OUF_BATCH_LOAD_DATABASE_URL or MCP_TEST_DATABASE_URL")
	}
	dir, err := newLoadReportDir(os.Getenv("OUF_BATCH_LOAD_REPORT_DIR"))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	report := loadReport{Contract: "D-103 §8.3", Run: uuid.NewString(), Config: config,
		Plans: make(map[string]json.RawMessage), Recovery: make(map[string]any)}
	metrics := loadMeasurements{}
	owner := newLoadGateway(config, time.Now)
	t.Cleanup(func() {
		report.Passed, report.ElapsedMS = !t.Failed(), time.Since(started).Milliseconds()
		report.Operations, report.Routes = metrics.summarize(), owner.stats()
		depth := make([]int64, 0, len(report.Queue))
		for _, q := range report.Queue {
			depth = append(depth, q.Pending+q.Running)
		}
		report.QueueDepth = loadQuantiles(depth)
		if err := report.write(dir); err != nil {
			t.Errorf("write qualification reports: %v", err)
		}
		t.Logf("qualification reports: %s", dir)
	})
	if dsn == "" {
		dsn = isolatedSummaryTestDSN(t)
	}
	base, manifest := summaryTestStore(t, dsn)
	cfg := base.Pool().Config()
	cfg.MaxConns = config.Connections
	cfg.ConnConfig.Tracer = loadTracer{}
	dial := cfg.ConnConfig.DialFunc
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &loadWire{Conn: conn}, nil
	}
	store, err := postgres.BatchQualificationStore(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if err := store.Pool().QueryRow(t.Context(), "show server_version").Scan(&report.PostgreSQL); err != nil {
		t.Fatal(err)
	}
	service := &orchestration.Service{RequireDelegation: true, Auth: &batchProfileAuth{}, Admission: store,
		Gateway: owner, FingerprintKey: make([]byte, 32)}
	worker := analysis.BatchWorker{Store: store, Service: service, Manifest: manifest}
	sum, err := manifest.Checksum()
	if err != nil {
		t.Fatal(err)
	}
	tenant := "batch-load-" + report.Run
	users := make([]loadUser, config.Users)
	for n := range users {
		users[n].identity = loadIdentity(tenant, fmt.Sprintf("user-%03d", n))
		users[n].client = newLoadClient(t, service, users[n].identity, &metrics)
	}
	var firstID, lastID string
	lastStart := ((config.Columns - 1) / loadPageWidth) * loadPageWidth
	for n := range config.Processes {
		u := &users[n%len(users)]
		id := seedLoadStart(t, store, u.identity, sum, config.Columns, false)
		start := 0
		if n == 0 {
			firstID = id
		}
		if n == config.Processes-1 {
			lastID, start = id, lastStart
		}
		seedLoadOffer(t, store, id, start, min(loadPageWidth, config.Columns-start))
		if stage := []string{"", analysis.BatchRequest, analysis.BatchStatus, analysis.BatchCandidates}[n%4]; stage != "" {
			ordinal := config.Columns - 1
			if start != 0 {
				ordinal = 0
			}
			seedLoadWork(t, store, owner.owner, id, ordinal, stage)
		}
		u.jobs = append(u.jobs, id)
	}
	// Run the plans on the populated data with the server's normal planner settings.
	loadExplainPages(t, store, &report, firstID, lastID, lastStart, config.Columns)
	q, err := readLoadQueue(t.Context(), store, tenant, "before", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	report.Queue = append(report.Queue, q)
	var processed atomic.Int64
	midpoint := make(chan loadQueueSample, 1)
	group, ctx := errgroup.WithContext(t.Context())
	start := make(chan struct{})
	// The runtime has one active lease per tenant. Leave the SDK users concurrent.
	workerSlots := make(chan struct{}, 1)
	for _, u := range users {
		group.Go(func() error {
			<-start
			for _, id := range u.jobs {
				if err := u.exercise(ctx, id); err != nil {
					return err
				}
				if processed.Add(1) == int64(config.Processes/2) {
					q, err := readLoadQueue(ctx, store, tenant, "during", time.Now())
					if err != nil {
						return err
					}
					midpoint <- q
				}
				select {
				case workerSlots <- struct{}{}:
				case <-ctx.Done():
					return ctx.Err()
				}
				sample, started := &loadSample{}, time.Now()
				workerCtx := context.WithValue(ctx, loadSampleKey{}, sample)
				_, err := worker.RunOnce(workerCtx, id, u.identity, started.Add(time.Hour))
				<-workerSlots
				metrics.record("WORKER_RUN_ONCE", sample, started)
				if err != nil {
					return fmt.Errorf("run batch worker: %w", err)
				}
			}
			return nil
		})
	}
	close(start)
	workloadErr := group.Wait()
	select {
	case q := <-midpoint:
		report.Queue = append(report.Queue, q)
	default:
	}
	if workloadErr != nil {
		t.Fatal(workloadErr)
	}
	q, err = readLoadQueue(t.Context(), store, tenant, "after", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	report.Queue = append(report.Queue, q)
	measurements := metrics.summarize()
	for _, name := range []string{"STATUS", "BATCH", "RECEIPT_COMMIT", "RECEIPT_REPLAY", "WORKER_RUN_ONCE"} {
		p := measurements[name]
		if p["db_rows"].Samples != config.Processes || p["db_rows"].P50 == 0 || p["db_received_bytes"].P50 == 0 || p["db_sent_bytes"].P50 == 0 {
			t.Fatalf("incomplete database measurements for %s: %+v", name, p)
		}
	}
	for route, stats := range owner.stats() {
		if stats.Forwarded == 0 {
			t.Errorf("no worker dispatch to %s during load", route)
		}
	}
	t.Run("recovery", func(t *testing.T) { verifyLoadRecovery(t, store, worker, &report, &metrics, sum) })
}

func loadIdentity(tenant, user string) orchestration.Identity {
	return orchestration.Identity{ServicePrincipalID: "ouf-chatgpt", PrincipalID: user, TenantID: tenant,
		ActorType: "HUMAN", Delegation: "human-proof", Issuer: "issuer", Audience: "gateway", AuthenticationContextRef: "1",
		Scopes: strings.Fields(decisionScopes), Claims: &orchestration.IdentityClaims{ExternalRoleRefs: []string{}, Acr: "1"}}
}

func seedLoadStart(t *testing.T, db *postgres.Store, identity orchestration.Identity, sum string, width int, residual bool) string {
	t.Helper()
	scope := analysis.IdentityScope(identity)
	pin := analysis.Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), SchemaHash: "sha256:" + strings.Repeat("a", 64), Version: 1, ColumnCount: width}
	id := analysis.BatchAnalysisID(scope, pin, "")
	for offset := 0; offset < width; offset += 128 {
		cols := make([]analysis.BatchPreviewColumn, min(128, width-offset))
		for n := range cols {
			name := fmt.Sprintf("published_%d", offset+n)
			if residual {
				name = fmt.Sprintf("residual_%d", offset+n)
			}
			cols[n] = analysis.BatchPreviewColumn{Name: name, Intent: analysis.ColumnIntent(name),
				BatchColumnMetadata: analysis.BatchColumnMetadata{DataType: "string", Examples: []json.RawMessage{}}}
		}
		if err := db.SaveBatchPreviewPage(t.Context(), scope, id, sum, "", pin, offset, cols); err != nil {
			t.Fatal(err)
		}
	}
	start := analysis.BatchStart{ID: id, Profile: pin, Authority: analysis.BatchAuthorityBinding(identity)}
	if _, err := db.SaveBatchStart(t.Context(), scope, sum, uuid.NewString(), analysis.Digest(start), start); err != nil {
		t.Fatal(err)
	}
	return id
}

func seedLoadOffer(t *testing.T, db *postgres.Store, id string, first, count int) {
	t.Helper()
	tag, err := db.Pool().Exec(t.Context(), `with evidence as materialized (
		select ordinal,gen_random_uuid() request from generate_series($2::int,$2::int+$3::int-1) ordinal
	), col as (
		update ouf_mcp.analysis_column c set request_id=e.request,
			outcome=jsonb_build_object('state','CANDIDATES_READY','requestId',e.request,'observedAt',statement_timestamp(),'candidateCount',1)
		from evidence e where c.analysis_id=$1 and c.ordinal=e.ordinal returning c.ordinal,c.outcome
	), versions as (
		update ouf_mcp.analysis_column_version v set outcome=col.outcome
		from col where v.analysis_id=$1 and v.ordinal=col.ordinal and v.seq=1
	), ready as (
		update ouf_mcp.analysis_batch_column b set state='READY_FOR_DECISION',due_at=null,work_stage='CANDIDATES',
			evidence_version='fixture',candidates=jsonb_build_array(jsonb_build_object('candidateId',gen_random_uuid(),
			'label','Indirizzo','term','urn:fixture:address','score',0.9))
		from col where b.analysis_id=$1 and b.ordinal=col.ordinal returning b.ordinal
	) update ouf_mcp.analysis_counts set ready=$3,pending=total-$3 where analysis_id=$1 and seq=1
		and (select count(*) from ready)=$3`, id, first, count)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatal("the seeded offer and its counters disagree")
	}
}

func seedLoadWork(t *testing.T, db *postgres.Store, owner *batchWorkerOwner, id string, ordinal int, stage string) {
	t.Helper()
	var request *string
	var completed *int
	const intent = "residual 1"
	if stage == analysis.BatchStatus || stage == analysis.BatchCandidates {
		key := uuid.NewString()
		request = &key
		owner.intents[key] = intent
	}
	if stage == analysis.BatchCandidates {
		n := 1
		completed = &n
	}
	tag, err := db.Pool().Exec(t.Context(), `with col as (
		update ouf_mcp.analysis_column set request_id=$4::uuid,
			outcome=jsonb_strip_nulls(jsonb_build_object('state','PENDING','requestId',$4::uuid))
		where analysis_id=$1 and ordinal=$2 returning outcome
	), versions as (
		update ouf_mcp.analysis_column_version v set outcome=col.outcome
		from col where v.analysis_id=$1 and v.ordinal=$2 and v.seq=1
	) update ouf_mcp.analysis_batch_column set work_stage=$3,intent=$5,completed_count=$6,
		due_at=statement_timestamp()-interval '1 minute' where analysis_id=$1 and ordinal=$2 and state='PENDING'`,
		id, ordinal, stage, request, intent, completed)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("seed worker stage %s: %v", stage, err)
	}
}

type loadUser struct {
	identity orchestration.Identity
	client   *loadClient
	jobs     []string
}

type loadClient struct {
	session *mcp.ClientSession
	metrics *loadMeasurements
	active  atomic.Pointer[loadSample]
}

func newLoadClient(t *testing.T, service *orchestration.Service, i orchestration.Identity, metrics *loadMeasurements) *loadClient {
	t.Helper()
	c := &loadClient{metrics: metrics}
	handler := revisionHandlerForScope(t, service, analysis.IdentityScope(i), decisionScopes)
	wrapper := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sample := c.active.Load(); sample != nil {
			r = r.WithContext(context.WithValue(r.Context(), loadSampleKey{}, sample))
		}
		handler.ServeHTTP(w, r)
	})
	c.session = connectRevisionSession(t, "http://fixture.invalid", &http.Client{Transport: inProcessMCPTransport{wrapper}})
	return c
}

func (c *loadClient) call(ctx context.Context, metric, tool string, args nextstep.Object) (nextstep.Object, error) {
	ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	sample := &loadSample{}
	c.active.Store(sample)
	start := time.Now()
	res, err := c.session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
	c.active.Store(nil)
	c.metrics.record(metric, sample, start)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", metric, err)
	}
	body, ok := res.StructuredContent.(map[string]any)
	if !ok || res.IsError {
		return nil, fmt.Errorf("%s: failed response %v", metric, res.StructuredContent)
	}
	return body, nil
}

func (u loadUser) exercise(ctx context.Context, id string) error {
	args := nextstep.Object{"analysisId": id, "view": "STATUS"}
	status, err := u.client.call(ctx, "STATUS", "source.analysis.read", args)
	if err != nil {
		return err
	}
	if status["analysisId"] != id || status["counts"] == nil {
		return fmt.Errorf("missing STATUS projection for %s", id)
	}
	args["view"] = "BATCH"
	offer, err := u.client.call(ctx, "BATCH", "source.analysis.read", args)
	if err != nil {
		return err
	}
	items, ok := offer["items"].([]any)
	if !ok || len(items) == 0 || len(items) > loadPageWidth {
		return fmt.Errorf("invalid offer for %s", id)
	}
	decisions := make([]nextstep.Object, 0, len(items))
	for _, item := range items {
		column, ok := item.(map[string]any)
		if !ok || column["ordinal"] == nil {
			return fmt.Errorf("offer without an ordinal for %s", id)
		}
		decisions = append(decisions, nextstep.Object{"ordinal": column["ordinal"], "action": "EXCLUDE", "reason": "Fuori perimetro della qualifica"})
	}
	command := batchDecisionCommand(offer, decisions...)
	result, err := u.client.call(ctx, "RECEIPT_COMMIT", "source.analysis.decide", command)
	if err != nil {
		return err
	}
	rows := decisionStatuses(result)
	if len(rows) != len(decisions) {
		return fmt.Errorf("incomplete receipt for %s", id)
	}
	for _, row := range rows {
		if row.Status != "RECORDED" {
			return fmt.Errorf("decision was rejected for %s: %+v", id, row)
		}
	}
	replay, err := u.client.call(ctx, "RECEIPT_REPLAY", "source.analysis.decide", command)
	if err == nil && !reflect.DeepEqual(result, replay) {
		return fmt.Errorf("receipt changed on replay for %s", id)
	}
	return err
}
