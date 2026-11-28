//go:build batchload

package kernel

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

const (
	loadSearch     = "ouf.semantic.search.batch"
	loadDiscovery  = "ouf.semantic.discovery"
	loadStatus     = "ouf.semantic.discovery.status"
	loadCandidates = "ouf.semantic.discovery.candidates"
)

var loadRoutes = []string{loadSearch, loadDiscovery, loadStatus, loadCandidates}

type loadRouteStats struct {
	Binding         string          `json:"gateway_binding,omitempty"`
	Calls           int             `json:"calls"`
	Forwarded       int             `json:"forwarded"`
	Gateway429      int             `json:"gateway_429"`
	Semantic429     int             `json:"semantic_429"`
	GatewayQuota    int             `json:"gateway_quota"`
	SemanticQuota   int             `json:"semantic_quota"`
	GatewayWindows  loadPercentiles `json:"gateway_429_per_active_window"`
	SemanticWindows loadPercentiles `json:"semantic_429_per_active_window"`
}

type loadQuotaWindow struct {
	forwarded, accepted, gateway429, semantic429 int
}

type loadGateway struct {
	mu               sync.Mutex
	config           loadConfig
	now              func() time.Time
	owner            *batchWorkerOwner
	routes           map[string]loadRouteStats
	windows          map[string]map[time.Time]loadQuotaWindow
	outage           bool // Only the discovery command route is unavailable.
	outageRetryAfter string
}

func newLoadGateway(config loadConfig, now func() time.Time) *loadGateway {
	return &loadGateway{config: config, now: now,
		owner:  &batchWorkerOwner{requests: map[string]string{}, intents: map[string]string{}},
		routes: make(map[string]loadRouteStats), windows: make(map[string]map[time.Time]loadQuotaWindow)}
}

func (g *loadGateway) Execute(ctx context.Context, r orchestration.GatewayRequest, timeout time.Duration) (orchestration.GatewayResponse, error) {
	if err := ctx.Err(); err != nil {
		return orchestration.GatewayResponse{}, err
	}
	code, outage, retry, err := g.admit(r)
	if err != nil {
		return orchestration.GatewayResponse{}, err
	}
	if code != "" {
		return orchestration.GatewayResponse{Status: http.StatusTooManyRequests, Problem: &orchestration.Problem{
			Status: http.StatusTooManyRequests, Code: code, RetryAfter: strconv.Itoa(retry),
		}}, nil
	}
	if outage {
		return orchestration.GatewayResponse{Status: http.StatusServiceUnavailable, Problem: &orchestration.Problem{
			Status: http.StatusServiceUnavailable, Code: "OWNER_UNAVAILABLE", RetryAfter: g.outageRetryAfter,
		}}, nil
	}
	return g.owner.Execute(ctx, r, timeout)
}

func (g *loadGateway) admit(r orchestration.GatewayRequest) (code string, outage bool, retry int, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	route := r.CapabilityID
	limit, known := g.config.Gateway[route]
	if !known {
		return "", false, 0, fmt.Errorf("unexpected route %s", route)
	}
	now, size := g.now(), time.Duration(g.config.WindowSec)*time.Second
	begin := now.Truncate(size)
	if g.windows[route] == nil {
		g.windows[route] = make(map[time.Time]loadQuotaWindow)
	}
	window, stats := g.windows[route][begin], g.routes[route]
	stats.Calls++
	stats.Binding = r.GatewayBindingRef
	semantic := g.config.Semantic[route]
	switch {
	case limit > 0 && window.forwarded >= limit:
		stats.Gateway429++
		window.gateway429++
		code = "GATEWAY_RATE_LIMITED"
	case semantic > 0 && window.accepted >= semantic:
		stats.Forwarded++
		window.forwarded++
		stats.Semantic429++
		window.semantic429++
		code = "SEM_DISCOVERY_RATE_LIMITED"
		switch route {
		case loadSearch:
			code = "SEM_BATCH_RATE_LIMITED"
		case loadCandidates:
			code = "SEM_CANDIDATES_RATE_LIMITED"
		}
	default:
		stats.Forwarded++
		window.forwarded++
		window.accepted++
	}
	g.windows[route][begin], g.routes[route] = window, stats
	return code, g.outage && route == loadDiscovery, int((begin.Add(size).Sub(now) + time.Second - 1) / time.Second), nil
}

func (g *loadGateway) stats() map[string]loadRouteStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	result := make(map[string]loadRouteStats, len(loadRoutes))
	for _, route := range loadRoutes {
		stats := g.routes[route]
		stats.GatewayQuota, stats.SemanticQuota = g.config.Gateway[route], g.config.Semantic[route]
		var gateway, semantic []int64
		for _, window := range g.windows[route] {
			gateway = append(gateway, int64(window.gateway429))
			semantic = append(semantic, int64(window.semantic429))
		}
		stats.GatewayWindows, stats.SemanticWindows = loadQuantiles(gateway), loadQuantiles(semantic)
		result[route] = stats
	}
	return result
}

type loadRecoveryAuth struct {
	batchProfileAuth
	revoked atomic.Bool
}

func (a *loadRecoveryAuth) Authorize(ctx context.Context, r orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	if a.revoked.Load() && r.CapabilityID == loadDiscovery {
		return orchestration.AuthorizationDecision{DecisionCode: "NO_APPLICABLE_GRANT"}, nil
	}
	return a.batchProfileAuth.Authorize(ctx, r)
}

type loadCheckpoint struct {
	Stage    string     `json:"stage"`
	State    string     `json:"state"`
	Attempts int        `json:"attempts"`
	Started  *time.Time `json:"started,omitempty"`
	Due      *time.Time `json:"due,omitempty"`
}

func loadCheckpointFor(t *testing.T, db *postgres.Store, id string) loadCheckpoint {
	t.Helper()
	var value loadCheckpoint
	err := db.Pool().QueryRow(t.Context(), `select work_stage,state,retry_attempts,retry_started_at,due_at
		from ouf_mcp.analysis_batch_column where analysis_id=$1 and ordinal=0`, id).Scan(&value.Stage, &value.State, &value.Attempts, &value.Started, &value.Due)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func sameLoadCheckpoint(a, b loadCheckpoint) bool {
	return a.Attempts == b.Attempts && a.State == b.State && a.Stage == b.Stage &&
		a.Started != nil && b.Started != nil && a.Started.Equal(*b.Started) &&
		a.Due != nil && b.Due != nil && a.Due.Equal(*b.Due)
}

func verifyLoadRecovery(t *testing.T, db *postgres.Store, worker analysis.BatchWorker, report *loadReport, metrics *loadMeasurements, sum string) {
	t.Helper()
	const outageDuration = 3 * time.Hour
	var virtual time.Time
	if err := db.Pool().QueryRow(t.Context(), `select greatest(clock_timestamp(),
		(select max(due_at) from ouf_mcp.analysis_batch_route_pause),
		(select max(due_at) from ouf_mcp.analysis_batch_column where state='RUNNING' and fence is not null))
		+interval '2 seconds'`).Scan(&virtual); err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return virtual }
	owner := newLoadGateway(report.Config, clock)
	defer func() { report.Recovery["routes"] = owner.stats() }()
	auth := &loadRecoveryAuth{}
	service := *worker.Service
	service.Gateway, service.Auth = owner, auth
	worker.Service, worker.Now = &service, clock
	initialStore := db
	restart := func() {
		t.Helper()
		reopened, err := postgres.BatchQualificationStore(t.Context(), db.Pool().Config())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(reopened.Close)
		if db != initialStore {
			db.Close()
		}
		restarted := *worker.Service
		restarted.Admission = reopened
		db, worker.Store, worker.Service = reopened, reopened, &restarted
	}
	identity := loadIdentity("batch-load-recovery-"+report.Run, "recovery-user")
	step := func(id string, want bool) {
		t.Helper()
		worked, err := worker.RunOnce(t.Context(), id, identity, clock().Add(time.Hour))
		if err != nil || worked != want {
			t.Fatalf("worker step at %v: worked=%v, want %v, err=%v", clock(), worked, want, err)
		}
	}
	advanceWindow := func() {
		size := time.Duration(report.Config.WindowSec) * time.Second
		virtual = virtual.Truncate(size).Add(size + time.Microsecond)
	}
	control := func(id string) {
		t.Helper()
		client := newLoadClient(t, worker.Service, identity, metrics)
		body, err := client.call(t.Context(), "STATUS_DURING_OUTAGE", "source.analysis.read", nextstep.Object{"analysisId": id, "view": "STATUS"})
		if err != nil || body["analysisId"] != id || body["counts"] == nil {
			t.Fatalf("control STATUS unavailable: %v (%v)", body, err)
		}
	}
	id := seedLoadStart(t, db, identity, sum, 1, true)
	step(id, true) // Search published terms, then schedule discovery.
	owner.outage = true
	outageStart := clock()
	// Advance the worker clock across a real Retry-After. The independent
	// PostgreSQL admission window keeps using its actual transaction clock.
	owner.outageRetryAfter = outageStart.Add(outageDuration).UTC().Truncate(time.Second).Add(time.Second).Format(http.TimeFormat)
	step(id, true)
	first := loadCheckpointFor(t, db, id)
	if first.Attempts != 1 || first.Started == nil || first.Due == nil || first.Stage != analysis.BatchRequest ||
		first.Due.Before(outageStart.Add(outageDuration)) {
		t.Fatalf("initial outage did not persist a retry checkpoint: %+v", first)
	}
	report.Recovery["outage_simulated_seconds"] = int(outageDuration.Seconds())
	report.Recovery["first_checkpoint"] = first
	control(id)
	calls := owner.stats()[loadDiscovery].Calls
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if worked, _ := worker.RunOnce(cancelled, id, identity, clock().Add(time.Hour)); worked ||
		!sameLoadCheckpoint(first, loadCheckpointFor(t, db, id)) || owner.stats()[loadDiscovery].Calls != calls {
		t.Fatal("cancelling the worker while paused changed its checkpoint or dispatched work")
	}
	report.Recovery["cancelled_worker_preserved_checkpoint"] = true
	restart()
	if !sameLoadCheckpoint(first, loadCheckpointFor(t, db, id)) {
		t.Fatal("worker restart changed the retry clock or consumed attempts")
	}
	virtual = first.Due.Add(-time.Microsecond)
	step(id, false)
	if owner.stats()[loadDiscovery].Calls != calls {
		t.Fatal("worker made an owner call before the persisted due time")
	}
	checkpoint := loadCheckpointFor(t, db, id)
	if !sameLoadCheckpoint(first, checkpoint) || len(owner.owner.requests) != 0 {
		t.Fatal("retry budget or owner effects changed while paused")
	}
	control(id)
	report.Recovery["before_resume_checkpoint"] = checkpoint
	report.Recovery["restart_preserved_checkpoint"] = true
	report.Recovery["control_during_outage"] = true
	owner.outage = false
	owner.outageRetryAfter = ""
	virtual = maxTimeForLoad(clock(), *checkpoint.Due)
	advanceWindow()
	beforeResume := owner.stats()
	step(id, true)
	advanceWindow()
	step(id, true)
	step(id, false)
	read, err := db.ReadBatch(t.Context(), analysis.IdentityScope(identity), id, sum, nil)
	if err != nil || read.Counts.Pending != 0 || read.Counts.Zero != 1 || len(owner.owner.requests) != 1 {
		t.Fatalf("outage recovery did not complete one logical discovery: %+v (%v)", read.Counts, err)
	}
	for route, stats := range owner.stats() {
		if stats.Gateway429 != beforeResume[route].Gateway429 || stats.Semantic429 != beforeResume[route].Semantic429 {
			t.Fatalf("recovery exceeded quota on %s", route)
		}
	}
	report.Recovery["resumed_within_quota"] = true

	advanceWindow()
	identity = loadIdentity(identity.TenantID, "lost-reply-user")
	lostID := seedLoadStart(t, db, identity, sum, 1, true)
	effectsBefore, commandsBefore, readsBefore := len(owner.owner.requests), owner.owner.commandCalls, owner.owner.candidateCalls
	step(lostID, true)
	owner.owner.loseResponse = true
	step(lostID, true)
	if len(owner.owner.requests)-effectsBefore != 1 {
		t.Fatal("owner did not commit discovery before losing its response")
	}
	lost := loadCheckpointFor(t, db, lostID)
	if lost.Due == nil || lost.Attempts != 1 || lost.Started == nil {
		t.Fatalf("lost response did not persist retry state: %+v", lost)
	}
	restart()
	if !sameLoadCheckpoint(lost, loadCheckpointFor(t, db, lostID)) {
		t.Fatal("restarting after the lost reply changed the persisted checkpoint")
	}
	virtual = maxTimeForLoad(clock(), *lost.Due)
	advanceWindow()
	step(lostID, true)
	advanceWindow()
	step(lostID, true)
	step(lostID, false)
	read, err = db.ReadBatch(t.Context(), analysis.IdentityScope(identity), lostID, sum, nil)
	effects, commands, reads := len(owner.owner.requests)-effectsBefore, owner.owner.commandCalls-commandsBefore, owner.owner.candidateCalls-readsBefore
	if err != nil || read.Counts.Pending != 0 || read.Counts.Zero != 1 || effects != 1 || commands != 2 || reads != 1 {
		t.Fatalf("lost reply reconciliation: counts=%+v effects=%d commands=%d reads=%d err=%v", read.Counts, effects, commands, reads, err)
	}
	report.Recovery["restart_after_lost_reply_preserved_checkpoint"] = true
	report.Recovery["discovery_effects_after_lost_reply"] = effects
	report.Recovery["discovery_calls_in_lost_reply_case"] = commands
	report.Recovery["candidate_reads_in_lost_reply_case"] = reads

	advanceWindow()
	identity = loadIdentity(identity.TenantID, "revoked-user")
	revoked := seedLoadStart(t, db, identity, sum, 1, true)
	step(revoked, true)
	owner.outage = true
	step(revoked, true)
	paused := loadCheckpointFor(t, db, revoked)
	if paused.Due == nil || paused.Attempts != 1 {
		t.Fatalf("revocation specimen was not paused: %+v", paused)
	}
	owner.outage = false
	auth.revoked.Store(true)
	virtual = maxTimeForLoad(clock(), *paused.Due)
	advanceWindow()
	calls = owner.stats()[loadDiscovery].Calls
	step(revoked, true)
	step(revoked, false)
	var reason string
	if err := db.Pool().QueryRow(t.Context(), `select reason_code from ouf_mcp.analysis_batch_hold
		where analysis_id=$1 and ordinal=0 and work_stage='REQUEST'`, revoked).Scan(&reason); err != nil ||
		reason != "AUTHORIZATION_DENIED" || owner.stats()[loadDiscovery].Calls != calls {
		t.Fatalf("revoked work was not blocked without owner dispatch: %s (%v)", reason, err)
	}
	report.Recovery["revocation_held_without_dispatch"] = true
	control(revoked)

	advanceWindow()
	auth.revoked.Store(false)
	identity = loadIdentity(identity.TenantID, "exhausted-user")
	exhausted := seedLoadStart(t, db, identity, sum, 1, true)
	step(exhausted, true)
	owner.outage = true
	step(exhausted, true)
	initial := loadCheckpointFor(t, db, exhausted)
	if initial.Started == nil || initial.Attempts != 1 {
		t.Fatalf("expiration specimen did not start a retry budget: %+v", initial)
	}
	virtual = initial.Started.Add(24*time.Hour + time.Microsecond)
	calls = owner.stats()[loadDiscovery].Calls
	step(exhausted, true)
	step(exhausted, false)
	var spent int
	if err := db.Pool().QueryRow(t.Context(), `select reason_code,retry_attempts from ouf_mcp.analysis_batch_hold
		where analysis_id=$1 and ordinal=0 and work_stage='REQUEST'`, exhausted).Scan(&reason, &spent); err != nil ||
		reason != "RETRY_BUDGET_EXHAUSTED" || spent != initial.Attempts || owner.stats()[loadDiscovery].Calls != calls {
		t.Fatalf("expired retry budget dispatched or lost its history: %s, %d attempts (%v)", reason, spent, err)
	}
	report.Recovery["24h_budget_expired_without_dispatch"] = true
}

func maxTimeForLoad(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
