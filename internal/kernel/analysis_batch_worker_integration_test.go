package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type batchWorkerOwner struct {
	mu                sync.Mutex
	residuals         int
	throttle          bool
	publishedThrottle bool
	publishedDeadline time.Time
	loseResponse      bool
	rejectZero        bool
	statusNotBefore   time.Time
	publishedCalls    int
	commandCalls      int
	statusCalls       int
	candidateCalls    int
	requests          map[string]string
	intents           map[string]string
	entered           chan struct{}
	release           chan struct{}
}

func (o *batchWorkerOwner) Execute(ctx context.Context, in orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var body any
	switch in.CapabilityID {
	case "ouf.semantic.search.batch":
		o.publishedCalls++
		if o.publishedThrottle {
			o.publishedThrottle = false
			problem := &orchestration.Problem{Status: http.StatusTooManyRequests, Code: "SEM_BATCH_RATE_LIMITED", RetryAfter: "90"}
			return orchestration.GatewayResponse{Status: problem.Status, Problem: problem}, nil
		}
		if o.entered != nil && o.publishedCalls == 1 {
			o.publishedDeadline, _ = ctx.Deadline()
			close(o.entered)
			select {
			case <-o.release:
			case <-ctx.Done():
				return orchestration.GatewayResponse{}, ctx.Err()
			}
		}
		var query analysis.Batch
		if err := json.Unmarshal(in.Arguments, &query); err != nil {
			return orchestration.GatewayResponse{}, err
		}
		rows := make([]analysis.BatchResult, 0, len(query.Intents))
		for _, intent := range query.Intents {
			row := analysis.BatchResult{ID: intent.ID, State: "NONE", Hits: []analysis.PublishedMatch{}}
			if !strings.HasPrefix(intent.Text, "residual ") {
				row.State = "MATCH"
				row.Hits = append(row.Hits, analysis.PublishedMatch{SemanticID: "urn:published:" + intent.ID,
					SemanticVersion: "1", RevisionID: uuid.NewString(), PublicationSetID: uuid.NewString(),
					Score: 100, Reasons: []string{"EXACT_LABEL", "PREFERRED_LANGUAGE"}})
			}
			rows = append(rows, row)
		}
		body = map[string]any{"contract": analysis.BatchContract, "results": rows}
	case "ouf.semantic.discovery":
		o.commandCalls++
		var command struct{ Intent, IdempotencyKey string }
		if err := json.Unmarshal(in.Arguments, &command); err != nil {
			return orchestration.GatewayResponse{}, err
		}
		if !strings.HasPrefix(command.Intent, "residual ") || command.IdempotencyKey != in.IdempotencyKey {
			return orchestration.GatewayResponse{}, errors.New("unexpected discovery arguments")
		}
		if o.throttle {
			o.throttle = false
			problem := &orchestration.Problem{Status: http.StatusTooManyRequests, Code: "SEM_DISCOVERY_RATE_LIMITED", RetryAfter: "90"}
			return orchestration.GatewayResponse{Status: problem.Status, Problem: problem}, nil
		}
		if o.rejectZero && command.Intent == "residual 0" {
			problem := &orchestration.Problem{Status: http.StatusBadRequest, Code: "SEM_DISCOVERY_REQUEST_INVALID"}
			return orchestration.GatewayResponse{Status: problem.Status, Problem: problem}, nil
		}
		request, found := o.requests[command.IdempotencyKey]
		if !found {
			request = uuid.NewString()
			o.requests[command.IdempotencyKey], o.intents[request] = request, command.Intent
		}
		if o.loseResponse {
			o.loseResponse = false
			return orchestration.GatewayResponse{}, context.DeadlineExceeded
		}
		if o.statusNotBefore.IsZero() {
			body = map[string]any{"state": "SUCCEEDED", "requestId": request, "completedCandidateCount": o.total(request)}
		} else {
			body = map[string]any{"state": "RUNNING", "requestId": request, "nextAttemptAt": o.statusNotBefore}
		}
	case "ouf.semantic.discovery.status", "ouf.semantic.discovery.candidates":
		var read struct {
			RequestID    string
			IncludeState bool
		}
		if err := json.Unmarshal(in.Arguments, &read); err != nil {
			return orchestration.GatewayResponse{}, err
		}
		if _, found := o.intents[read.RequestID]; !found {
			return orchestration.GatewayResponse{}, errors.New("unknown owner request")
		}
		count := o.total(read.RequestID)
		observation := map[string]any{"requestId": read.RequestID, "state": "SUCCEEDED", "completedCandidateCount": count, "candidateCount": count}
		if in.CapabilityID == "ouf.semantic.discovery.status" {
			o.statusCalls++
		} else {
			o.candidateCalls++
			if !read.IncludeState {
				return orchestration.GatewayResponse{}, errors.New("owner completion required")
			}
			list := make([]map[string]any, 0, count)
			if count > 0 {
				list = append(list, map[string]any{"candidate_id": uuid.NewString(), "canonical_uri": "urn:provider:property", "provider_id": "fixture", "score": 0.8})
			}
			observation["candidates"] = list
		}
		body = observation
	default:
		return orchestration.GatewayResponse{}, fmt.Errorf("unexpected owner capability %s", in.CapabilityID)
	}
	raw, err := json.Marshal(body)
	return orchestration.GatewayResponse{Status: http.StatusOK, Body: raw}, err
}

func (o *batchWorkerOwner) total(request string) int {
	if o.intents[request] == "residual 0" {
		return 0
	}
	return 1
}

const batchWorkerScopes = "ouf.managed-source.preview ouf.semantic.search ouf.semantic.discovery ouf.semantic.discovery.status ouf.semantic.discovery.candidates"

func batchWorkerFixture(t *testing.T, width int) (*postgres.Store, string, analysis.BatchWorker, *batchWorkerOwner, orchestration.Identity, string) {
	t.Helper()
	return batchWorkerFixtureWithScopes(t, width, batchWorkerScopes)
}

func batchWorkerFixtureWithScopes(t *testing.T, width int, scopes string) (*postgres.Store, string, analysis.BatchWorker, *batchWorkerOwner, orchestration.Identity, string) {
	t.Helper()
	dsn := isolatedSummaryTestDSN(t)
	store, snapshot := summaryTestStore(t, dsn)
	owner := &batchWorkerOwner{residuals: 2, requests: map[string]string{}, intents: map[string]string{}}
	service := &orchestration.Service{RequireDelegation: true, Auth: &batchProfileAuth{}, Admission: store, Gateway: owner, FingerprintKey: make([]byte, 32)}
	worker := analysis.BatchWorker{Store: store, Service: service, Manifest: snapshot}
	i := orchestration.Identity{ServicePrincipalID: "ouf-chatgpt", PrincipalID: "worker-fixture", TenantID: "tenant", ActorType: "HUMAN", Delegation: "human-proof",
		Issuer: "issuer", Audience: "gateway", AuthenticationContextRef: "1", Scopes: strings.Fields(scopes),
		// Match the trusted HTTP claims in revisionSessionForScope.
		Claims: &orchestration.IdentityClaims{ExternalRoleRefs: []string{}, Acr: "1"}}
	scope := analysis.IdentityScope(i)
	pin := analysis.Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), SchemaHash: "sha256:" + strings.Repeat("a", 64), Version: 1, ColumnCount: width}
	id := analysis.BatchAnalysisID(scope, pin, "")
	sum, _ := snapshot.Checksum()
	for offset := 0; offset < width; offset += 128 {
		columns := make([]analysis.BatchPreviewColumn, min(128, width-offset))
		for n := range columns {
			ordinal := offset + n
			name := fmt.Sprintf("published_%d", ordinal)
			if ordinal < owner.residuals {
				name = fmt.Sprintf("residual_%d", ordinal)
			}
			columns[n] = analysis.BatchPreviewColumn{Name: name, Intent: analysis.ColumnIntent(name),
				BatchColumnMetadata: analysis.BatchColumnMetadata{DataType: "string", Examples: []json.RawMessage{}}}
		}
		if err := store.SaveBatchPreviewPage(t.Context(), scope, id, sum, "", pin, offset, columns); err != nil {
			t.Fatal(err)
		}
	}
	start := analysis.BatchStart{ID: id, Profile: pin, Authority: analysis.BatchAuthorityBinding(i)}
	if _, err := store.SaveBatchStart(t.Context(), scope, sum, uuid.NewString(), analysis.Digest(start), start); err != nil {
		t.Fatal(err)
	}
	return store, dsn, worker, owner, i, id
}

func batchWorkerStep(t *testing.T, worker analysis.BatchWorker, identity orchestration.Identity, id string) bool {
	t.Helper()
	worked, err := worker.RunOnce(t.Context(), id, identity, time.Now().Add(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return worked
}

func batchWorkerDrain(t *testing.T, worker analysis.BatchWorker, identity orchestration.Identity, id string) {
	t.Helper()
	for range 200 {
		if !batchWorkerStep(t, worker, identity, id) {
			return
		}
	}
	t.Fatal("worker did not drain the fixture")
}

func TestBatchWorkerPostgresFirstRound(t *testing.T) {
	for _, width := range []int{32, 1000} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			store, _, worker, owner, i, id := batchWorkerFixture(t, width)
			batchWorkerDrain(t, worker, i, id)
			sum, _ := worker.Manifest.Checksum()
			read, err := store.ReadBatch(t.Context(), analysis.IdentityScope(i), id, sum, func(offer analysis.BatchOffer) ([]analysis.BatchOfferItem, error) { return offer.Items, nil })
			if err != nil || read.Counts.Matched != width-2 || read.Counts.Zero != 1 || read.Counts.Ready != 1 || read.Counts.Pending != 0 || len(read.Offer.Items) != 2 {
				t.Fatalf("first round width %d: %+v %v", width, read, err)
			}
			if owner.commandCalls != 2 || len(owner.requests) != 2 || owner.candidateCalls != 2 {
				t.Fatal("expected one owner request and one evidence read per residual")
			}
			var evidence int
			if err := store.Pool().QueryRow(t.Context(), `select count(*) from ouf_mcp.analysis_column
				where analysis_id=$1 and outcome->>'state'='MATCHED_PUBLISHED' and outcome->'match'->>'publicationSetId' is not null`, id).Scan(&evidence); err != nil || evidence != width-2 {
				t.Fatal("missing published evidence", evidence, err)
			}
		})
	}
}

func TestBatchWorkerPostgresLongUnicodeHeaderGoesToDiscovery(t *testing.T) {
	store, _, worker, owner, i, id := batchWorkerFixture(t, 1)
	intent := analysis.ColumnIntent("residual_" + strings.Repeat("\u00e9", 1100))
	if _, err := store.Pool().Exec(t.Context(), `update ouf_mcp.analysis_batch_column set intent=$2 where analysis_id=$1`, id, intent); err != nil {
		t.Fatal(err)
	}
	batchWorkerDrain(t, worker, i, id)
	if owner.publishedCalls != 0 || owner.commandCalls != 1 || len(owner.intents) != 1 || owner.candidateCalls != 1 {
		t.Fatal("long header did not go straight to discovery", owner.publishedCalls, owner.commandCalls, owner.candidateCalls)
	}
	for _, submitted := range owner.intents {
		if submitted != intent {
			t.Fatal("discovery did not receive the complete column intent")
		}
	}
}

func TestBatchWorkerPostgresRetryAfterAndLostResponse(t *testing.T) {
	store, dsn, worker, owner, i, id := batchWorkerFixture(t, 32)
	owner.throttle = true
	owner.loseResponse = true
	if !batchWorkerStep(t, worker, i, id) || !batchWorkerStep(t, worker, i, id) {
		t.Fatal("missing published lookup or first request attempt")
	}
	var due time.Time
	var attempts int
	err := store.Pool().QueryRow(t.Context(), `select due_at,retry_attempts from ouf_mcp.analysis_batch_column where analysis_id=$1 and ordinal=0`, id).Scan(&due, &attempts)
	if err != nil || attempts != 1 || time.Until(due) < 85*time.Second {
		t.Fatal("Retry-After was not retained", due, attempts, err)
	}
	worker.Now = func() time.Time { return due.Add(-time.Second) }
	if batchWorkerStep(t, worker, i, id) || owner.commandCalls != 1 {
		t.Fatal("a request bypassed the shared route pause")
	}
	worker.Now = func() time.Time { return due.Add(time.Second) }
	if !batchWorkerStep(t, worker, i, id) || len(owner.requests) != 1 {
		t.Fatal("owner did not commit the request before losing the response")
	}
	var resumedDue time.Time
	err = store.Pool().QueryRow(t.Context(), `select due_at from ouf_mcp.analysis_batch_column
		where analysis_id=$1 and state='PENDING' and work_stage='REQUEST' and due_at>$2 order by due_at desc limit 1`, id, due).Scan(&resumedDue)
	if err != nil || resumedDue.Before(due.Add(5*time.Second)) {
		t.Fatal("lost response did not retain the technical wait", resumedDue, err)
	}
	reopened, err := postgres.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Close)
	restartedService := *worker.Service
	restartedService.Admission = reopened
	worker.Service, worker.Store = &restartedService, reopened
	worker.Now = func() time.Time { return resumedDue.Add(time.Second) }
	batchWorkerDrain(t, worker, i, id)
	if owner.commandCalls != 4 || len(owner.requests) != 2 || owner.candidateCalls != 2 {
		t.Fatal("a retry lost or duplicated a logical effect", owner.commandCalls, len(owner.requests), owner.candidateCalls)
	}
	var limited, useful int
	err = reopened.Pool().QueryRow(t.Context(), `select
		(select observations from ouf_mcp.analysis_batch_traffic where work_stage='REQUEST' and metric='429_SEMANTIC_CODE'),
		(select observations from ouf_mcp.analysis_batch_traffic where work_stage='CANDIDATES' and metric='GET_USEFUL')`).Scan(&limited, &useful)
	if err != nil || limited != 1 || useful != 2 {
		t.Fatal("incorrect traffic observations", limited, useful, err)
	}
}

func TestBatchWorkerPostgresIsolatesPermanentRefusals(t *testing.T) {
	store, _, worker, owner, i, id := batchWorkerFixture(t, 32)
	owner.rejectZero = true
	batchWorkerDrain(t, worker, i, id)
	sum, _ := worker.Manifest.Checksum()
	read, err := store.ReadBatch(t.Context(), analysis.IdentityScope(i), id, sum, nil)
	if err != nil || read.Counts.Blocked != 1 || read.Counts.Ready != 1 || read.Counts.Matched != 30 || read.Counts.Pending != 0 || len(owner.requests) != 1 {
		t.Fatal("permanent refusal delayed independent columns", read, err)
	}
}

func TestBatchWorkerPostgresHonorsOwnerObservationClock(t *testing.T) {
	_, _, worker, owner, i, id := batchWorkerFixture(t, 32)
	due := time.Now().UTC().Truncate(time.Microsecond).Add(time.Minute)
	owner.statusNotBefore = due
	batchWorkerDrain(t, worker, i, id)
	worker.Now = func() time.Time { return due.Add(-time.Microsecond) }
	if batchWorkerStep(t, worker, i, id) || owner.commandCalls != 2 || owner.statusCalls != 0 || owner.candidateCalls != 0 {
		t.Fatal("discovery observations bypassed the owner clock")
	}
	worker.Now = func() time.Time { return due.Add(time.Microsecond) }
	batchWorkerDrain(t, worker, i, id)
	if owner.commandCalls != 2 || owner.statusCalls != 2 || owner.candidateCalls != 2 {
		t.Fatal("the worker did not resume observing the same requests after the owner clock")
	}
}

func TestBatchWorkerPostgresExhaustionSurvivesHoursAndRestart(t *testing.T) {
	store, dsn, worker, owner, i, id := batchWorkerFixture(t, 32)
	if !batchWorkerStep(t, worker, i, id) {
		t.Fatal("missing published lookup")
	}
	_, err := store.Pool().Exec(t.Context(), `update ouf_mcp.analysis_batch_column
		set retry_attempts=127,retry_started_at=transaction_timestamp()-interval '12 hours'
		where analysis_id=$1 and ordinal=0`, id)
	if err != nil {
		t.Fatal(err)
	}
	owner.throttle = true
	if !batchWorkerStep(t, worker, i, id) || owner.commandCalls != 1 {
		t.Fatal("missing final technical attempt")
	}
	reopened, err := postgres.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Close)
	restartedService := *worker.Service
	restartedService.Admission = reopened
	worker.Store, worker.Service = reopened, &restartedService
	now := time.Now().Add(13 * time.Hour)
	worker.Now = func() time.Time { return now }
	batchWorkerDrain(t, worker, i, id)
	var attempts int
	var reason string
	err = reopened.Pool().QueryRow(t.Context(), `select retry_attempts,reason_code from ouf_mcp.analysis_batch_hold
		where analysis_id=$1 and ordinal=0 and work_stage='REQUEST'`, id).Scan(&attempts, &reason)
	if err != nil || attempts != 128 || reason != "RETRY_BUDGET_EXHAUSTED" || owner.commandCalls != 2 || len(owner.requests) != 1 {
		t.Fatal("restart reset the exhausted incident or delayed its independent neighbor", attempts, reason, owner.commandCalls, err)
	}
}

type failBatchCheckpoint struct {
	*postgres.Store
	stage   string
	claimed analysis.BatchWork
}

func (s *failBatchCheckpoint) FinishBatchWork(ctx context.Context, scope analysis.Scope, job analysis.BatchWork, outcomes []analysis.BatchWorkResult, pause, now time.Time) error {
	if job.Stage == s.stage {
		s.claimed = job
		return errors.New("interrupted immediately before the column checkpoint")
	}
	return s.Store.FinishBatchWork(ctx, scope, job, outcomes, pause, now)
}

func TestBatchWorkerPostgresPublishedRetryAfterSurvivesInterruptedCheckpoint(t *testing.T) {
	store, dsn, worker, owner, i, id := batchWorkerFixture(t, 1)
	owner.publishedThrottle = true
	crashed := &failBatchCheckpoint{Store: store, stage: analysis.BatchPublished}
	worker.Store = crashed
	before := time.Now()
	if worked, err := worker.RunOnce(t.Context(), id, i, before.Add(time.Hour)); !worked || err == nil || owner.publishedCalls != 1 || crashed.claimed.Fence == "" {
		t.Fatal("fixture did not interrupt the published checkpoint", worked, err, owner.publishedCalls)
	}

	reopened, err := postgres.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Close)
	restartedService := *worker.Service
	restartedService.Admission = reopened
	worker.Store, worker.Service = reopened, &restartedService
	now := crashed.claimed.LeaseUntil.Add(time.Second)
	worker.Now = func() time.Time { return now }
	if batchWorkerStep(t, worker, i, id) || owner.publishedCalls != 1 {
		t.Fatal("the restarted worker retried after lease expiry but before Retry-After", owner.publishedCalls)
	}
	var due time.Time
	if err := reopened.Pool().QueryRow(t.Context(), `select due_at from ouf_mcp.analysis_batch_route_pause where work_stage='PUBLISHED'`).Scan(&due); err != nil || due.Before(before.Add(90*time.Second)) {
		t.Fatal("the published route did not retain Retry-After", due, err)
	}
	now = due.Add(time.Microsecond)
	if !batchWorkerStep(t, worker, i, id) || owner.publishedCalls != 2 {
		t.Fatal("the restarted worker did not resume after Retry-After", owner.publishedCalls)
	}
}

func TestBatchWorkerPostgresCallDeadlineIncludesRetryBudget(t *testing.T) {
	store, _, worker, owner, i, id := batchWorkerFixture(t, 1)
	now := time.Now().UTC().Truncate(time.Microsecond)
	budgetEnd := now.Add(3 * time.Second)
	if _, err := store.Pool().Exec(t.Context(), `update ouf_mcp.analysis_batch_column
		set retry_attempts=1,retry_started_at=$2 where analysis_id=$1`, id, budgetEnd.Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	owner.entered = make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	if worked, err := worker.RunOnce(ctx, id, i, now.Add(time.Hour)); !worked || err != nil || owner.publishedCalls != 1 {
		t.Fatal("fixture did not dispatch and checkpoint the expiring attempt", worked, err, owner.publishedCalls)
	}
	if deadline := owner.publishedDeadline; !deadline.Equal(budgetEnd) {
		t.Fatal("the owner call did not end at the retry budget deadline", deadline, budgetEnd)
	}
	var reason string
	if err := store.Pool().QueryRow(t.Context(), `select reason_code from ouf_mcp.analysis_batch_hold where analysis_id=$1`, id).Scan(&reason); err != nil || reason != "RETRY_BUDGET_EXHAUSTED" {
		t.Fatal("the expiring attempt did not exhaust the retry budget", reason, err)
	}
}

func TestBatchWorkerPostgresOrphanReconcilesReceipt(t *testing.T) {
	store, dsn, worker, owner, i, id := batchWorkerFixture(t, 32)
	crashed := &failBatchCheckpoint{Store: store, stage: analysis.BatchRequest}
	worker.Store = crashed
	if !batchWorkerStep(t, worker, i, id) {
		t.Fatal("missing published lookup")
	}
	if worked, err := worker.RunOnce(t.Context(), id, i, time.Now().Add(time.Hour)); !worked || err == nil || owner.commandCalls != 1 {
		t.Fatal("fixture did not interrupt the checkpoint after the owner committed", worked, err, owner.commandCalls)
	}
	ordinals := []int{crashed.claimed.Columns[0].Ordinal}
	if err := store.BeginBatchDispatch(t.Context(), crashed.claimed, ordinals, time.Now()); !errors.Is(err, analysis.ErrConflict) {
		t.Fatal("the same live fence was allowed to dispatch again", err)
	}
	reopened, err := postgres.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Close)
	restartedService := *worker.Service
	restartedService.Admission = reopened
	worker.Store, worker.Service = reopened, &restartedService
	if batchWorkerStep(t, worker, i, id) {
		t.Fatal("second instance obtained work during the original tenant lease")
	}
	// Exercise the same instant in the JSON checkpoint and the binary claim.
	// A timestamp near the end of a microsecond must be due on the next claim.
	recoveryTime := crashed.claimed.LeaseUntil.Add(time.Second).Truncate(time.Microsecond).Add(999 * time.Nanosecond)
	worker.Now = func() time.Time { return recoveryTime }
	batchWorkerDrain(t, worker, i, id)
	if owner.commandCalls != 2 || owner.statusCalls != 1 || len(owner.requests) != 2 || owner.candidateCalls != 2 {
		t.Fatal("orphan did not use the original owner receipt", owner.commandCalls, owner.statusCalls, len(owner.requests), owner.candidateCalls)
	}
	stale := analysis.BatchWorkResult{Ordinal: crashed.claimed.Columns[0].Ordinal, State: "CLOSED", Outcome: analysis.Outcome{State: "BLOCKED", Reason: "TEST_STALE"}}
	if err := store.FinishBatchWork(t.Context(), analysis.IdentityScope(i), crashed.claimed, []analysis.BatchWorkResult{stale}, time.Time{}, worker.Now()); !errors.Is(err, analysis.ErrConflict) {
		t.Fatal("expired worker was allowed to checkpoint", err)
	}
}

func TestBatchWorkerPostgresConcurrentDispatch(t *testing.T) {
	_, dsn, worker, owner, i, id := batchWorkerFixture(t, 32)
	owner.entered, owner.release = make(chan struct{}), make(chan struct{})
	otherStore, err := postgres.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(otherStore.Close)
	other := worker
	otherService := *worker.Service
	otherService.Admission = otherStore
	other.Store, other.Service = otherStore, &otherService
	done := make(chan error, 1)
	go func() {
		_, err := worker.RunOnce(t.Context(), id, i, time.Now().Add(time.Hour))
		done <- err
	}()
	select {
	case <-owner.entered:
	case <-time.After(5 * time.Second):
		close(owner.release)
		t.Fatal("first worker did not reach the owner")
	}
	worked, err := other.RunOnce(t.Context(), id, i, time.Now().Add(time.Hour))
	close(owner.release)
	if worked || err != nil {
		t.Fatal("concurrent worker acquired the tenant's active lease", worked, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	batchWorkerDrain(t, other, i, id)
	if owner.publishedCalls != 1 || owner.commandCalls != 2 || len(owner.requests) != 2 {
		t.Fatalf("expected one published dispatch and one command per residual, got %d published, %d commands and %d requests", owner.publishedCalls, owner.commandCalls, len(owner.requests))
	}
}

type pausedBatchDispatch struct {
	*postgres.Store
	entered chan uuid.UUID
	release chan struct{}
}

func (s pausedBatchDispatch) Dispatch(ctx context.Context, id uuid.UUID, backend string, lease time.Duration, expected int64) error {
	s.entered <- id
	select {
	case <-s.release:
		return s.Store.Dispatch(ctx, id, backend, lease, expected)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestBatchWorkerPostgresRejectsDispatchFromSupersededLease(t *testing.T) {
	store, dsn, first, owner, identity, id := batchWorkerFixture(t, 1)
	assertSupersededBatchDispatch(t, store, dsn, first, identity, id)
	if owner.publishedCalls != 1 {
		t.Fatalf("expected one published dispatch after takeover, got %d", owner.publishedCalls)
	}
}

func assertSupersededBatchDispatch(t *testing.T, store *postgres.Store, dsn string, first analysis.BatchWorker, identity orchestration.Identity, id string) {
	t.Helper()
	otherStore, err := postgres.Open(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(otherStore.Close)
	other := first
	otherService := *other.Service
	otherService.Admission = otherStore
	other.Store, other.Service = otherStore, &otherService
	paused := pausedBatchDispatch{Store: store, entered: make(chan uuid.UUID, 1), release: make(chan struct{})}
	firstService := *first.Service
	firstService.Admission = paused
	first.Service = &firstService
	now := time.Now().UTC()
	first.Now = func() time.Time { return now }
	other.Now = func() time.Time { return now.Add(analysis.BatchLease + time.Second) }
	done := make(chan error, 1)
	go func() {
		_, err := first.RunOnce(t.Context(), id, identity, now.Add(time.Hour))
		done <- err
	}()
	var supersededAttempt uuid.UUID
	select {
	case supersededAttempt = <-paused.entered:
	case err := <-done:
		t.Fatal("worker exited before transport admission", err)
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not reach transport admission")
	}
	worked, takeoverErr := other.RunOnce(t.Context(), id, identity, now.Add(time.Hour))
	close(paused.release)
	if !worked || takeoverErr != nil {
		t.Fatal("replacement worker could not dispatch", worked, takeoverErr)
	}
	if err := <-done; !errors.Is(err, analysis.ErrConflict) {
		t.Fatal("superseded worker was allowed to advance", err)
	}
	var dispatchState string
	if err := store.Pool().QueryRow(t.Context(), `select dispatch_state from ouf_mcp.tool_attempt where attempt_id=$1`, supersededAttempt).Scan(&dispatchState); err != nil || dispatchState != "NOT_DISPATCHED" {
		t.Fatal("superseded attempt was marked dispatched", dispatchState, err)
	}
}

func TestBatchWorkerPostgresWaitReturnsExecutableRead(t *testing.T) {
	store, _, worker, owner, i, id := batchWorkerFixture(t, 32)
	scope := analysis.IdentityScope(i)
	client := revisionSessionForScope(t, worker.Service, scope, batchWorkerScopes)
	contract := nextContract(t)
	call := func(ctx context.Context, tool string, args nextstep.Object) nextstep.Object {
		t.Helper()
		result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
		if err != nil || result == nil || result.IsError {
			t.Fatalf("%s: %v %v", tool, result, err)
		}
		return assertBatchWire(t, contract, result)
	}
	status := call(t.Context(), "source.analysis.read", nextstep.Object{"analysisId": id, "view": "STATUS"})
	next := status["next"].(nextstep.Object)
	if next["action"] != "POLL" || next["reasonCode"] != nextstep.AnalysisRunning || next["tool"] != "ouf.wait" {
		t.Fatal("expected a signed wait", next)
	}
	checksum, err := worker.Manifest.Checksum()
	if err != nil {
		t.Fatal(err)
	}
	// Use the worker's real clock and stop at the first column ready for decision.
	workerCtx, stopWorker := context.WithTimeout(t.Context(), 12*time.Second)
	defer stopWorker()
	for {
		worked, err := worker.RunOnce(workerCtx, id, i, time.Now().Add(24*time.Hour))
		if err != nil {
			t.Fatal("worker step failed", err)
		}
		read, err := store.ReadBatch(workerCtx, scope, id, checksum, nil)
		if err != nil {
			t.Fatal("could not read committed batch progress", err)
		}
		if ready := read.Counts.Ready + read.Counts.Zero; ready > 0 {
			if ready != 1 || read.Counts.Pending != 1 || read.Reason != "ANALYSIS_CONTINUE" {
				t.Fatalf("expected one column ready for decision and one pending, got %+v (%s)", read.Counts, read.Reason)
			}
			break
		}
		if read.Counts.Pending == 0 {
			t.Fatalf("batch finished before any column was ready for decision: %+v (%s)", read.Counts, read.Reason)
		}
		if !worked {
			select {
			case <-workerCtx.Done():
				t.Fatalf("no column became ready for decision: %+v (%s): %v", read.Counts, read.Reason, workerCtx.Err())
			case <-time.After(250 * time.Millisecond):
			}
		}
	}
	stopWorker()
	waitCtx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	body := call(waitCtx, "ouf.wait", next["arguments"].(nextstep.Object))
	executable := body["next"].(nextstep.Object)
	if body["changed"] != true || body["analysisId"] != id || executable["action"] != "CALL_TOOL" || executable["tool"] != "source.analysis.read" {
		t.Fatal("wait did not return the ready step", body)
	}
	args, _ := executable["arguments"].(nextstep.Object)
	if args["analysisId"] != id || args["view"] != "BATCH" {
		t.Fatal("wait changed the analysis selector", args)
	}
	offer := call(t.Context(), "source.analysis.read", args)
	if items, _ := offer["items"].([]any); len(items) != 1 || owner.candidateCalls != 1 {
		t.Fatal("the wait continuation did not return the ready batch", offer)
	}
}
