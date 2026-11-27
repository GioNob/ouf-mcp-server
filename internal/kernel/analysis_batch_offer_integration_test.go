package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type batchProfileGateway struct {
	pin     analysis.Profile
	mu      sync.Mutex
	offsets []int
	paused  bool
}

type batchProfileAuth struct {
	denied atomic.Bool
}

func (a *batchProfileAuth) Authorize(_ context.Context, request orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	if a.denied.Load() && request.CapabilityID == "ouf.semantic.discovery.candidates" {
		return orchestration.AuthorizationDecision{DecisionCode: "NO_APPLICABLE_GRANT"}, nil
	}
	return orchestration.AuthorizationDecision{Allowed: true, DecisionRef: "fixture"}, nil
}

func (g *batchProfileGateway) Execute(_ context.Context, r orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	var page struct{ Offset, Limit int }
	if err := json.Unmarshal(r.Arguments, &page); err != nil {
		return orchestration.GatewayResponse{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.offsets = append(g.offsets, page.Offset)
	if page.Offset == 128 && !g.paused {
		g.paused = true
		return orchestration.GatewayResponse{Status: 503, Problem: &orchestration.Problem{Status: 503, Code: "OWNER_UNAVAILABLE"}}, nil
	}
	end := min(page.Offset+page.Limit, g.pin.ColumnCount)
	columns := make([]nextstep.Object, end-page.Offset)
	for n := range columns {
		ordinal := page.Offset + n
		columns[n] = nextstep.Object{"ordinal": ordinal, "name": "schoolAddress", "inferredType": "string"}
		if ordinal == 990 {
			columns[n]["distinctValues"] = []any{"via Roma 1", true, 42, nextstep.Object{"value": "not a scalar"}}
		}
		if ordinal == 991 {
			columns[n]["distinctValues"] = []any{"restricted"}
			columns[n]["dataAccessLabel"] = "RESTRICTED"
		}
	}
	body, err := json.Marshal(struct {
		analysis.Profile
		View       string            `json:"view"`
		Offset     int               `json:"offset"`
		NextOffset int               `json:"nextOffset"`
		HasMore    bool              `json:"hasMore"`
		Columns    []nextstep.Object `json:"columns"`
	}{g.pin, "COLUMNS", page.Offset, end, end < g.pin.ColumnCount, columns})
	return orchestration.GatewayResponse{Status: 200, Body: body}, err
}

func TestBatchPostgresResumeFromAnalysisID(t *testing.T) {
	dsn := isolatedSummaryTestDSN(t)
	db, _ := summaryTestStore(t, dsn)
	t.Setenv("OUF_ANALYSIS_PREVIEW_PAGE_SIZE", "128")
	ctx := t.Context()
	scope := analysis.Scope{Service: "ouf-chatgpt", Principal: "fixture", Tenant: "fixture"}
	profile := analysis.Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), Version: 1,
		SchemaHash: "sha256:" + strings.Repeat("a", 64), ColumnCount: 1000}
	gateway := &batchProfileGateway{pin: profile}
	auth := &batchProfileAuth{}
	service := &orchestration.Service{RequireDelegation: true, Auth: auth, Admission: db, Gateway: gateway, FingerprintKey: make([]byte, 32)}
	const scopes = "ouf.managed-source.preview ouf.semantic.discovery.status ouf.semantic.discovery.candidates"
	session := revisionSessionForScope(t, service, scope, scopes)
	contract := nextContract(t)
	call := func(session *mcp.ClientSession, tool string, args nextstep.Object, failure bool) nextstep.Object {
		t.Helper()
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
		if err != nil || result == nil {
			t.Fatalf("%s: %v", tool, err)
		}
		body := assertBatchWire(t, contract, result)
		if result.IsError != failure {
			t.Fatalf("%s returned %v", tool, body)
		}
		return body
	}
	id := analysis.BatchAnalysisID(scope, profile, "")
	start := nextstep.Object{"mode": analysis.BatchMode, "assetId": profile.AssetID, "profileId": profile.ProfileID,
		"schemaHash": profile.SchemaHash, "commandKey": uuid.NewString()}
	call(session, "source.analysis.plan", start, true)
	var retained int
	if err := db.Pool().QueryRow(ctx, `select cardinality(headers) from ouf_mcp.analysis_preview_progress where analysis_id=$1`, id).Scan(&retained); err != nil || retained != 128 {
		t.Fatal("missing verified prefix after interrupted start", retained, err)
	}
	body := call(session, "source.analysis.plan", start, false)
	if body["analysisId"] != id || body["phase"] != "RUNNING" || body["work"] != nil ||
		body["counts"].(nextstep.Object)["pending"] != json.Number("1000") {
		t.Fatal(body)
	}
	call(session, "source.analysis.plan", start, false)
	gateway.mu.Lock()
	offsets := slices.Clone(gateway.offsets)
	gateway.mu.Unlock()
	if !slices.Equal(offsets, []int{0, 128, 128, 256, 384, 512, 640, 768, 896}) {
		t.Fatal("lost preview checkpoint or fetched beyond the profile", offsets)
	}
	var columns, tasks, versions int
	err := db.Pool().QueryRow(ctx, `select
		(select count(*) from ouf_mcp.analysis_batch_column b join ouf_mcp.analysis_column c using(analysis_id,ordinal)
		 where b.analysis_id=$1 and b.state='PENDING' and b.due_at is not null and b.round=1 and b.attempt=1
		 and c.header='schoolAddress' and b.intent='school address' and c.request_id is null),
		(select count(*) from ouf_mcp.profile_analysis_task where analysis_id=$1),
		(select count(*) from ouf_mcp.analysis_column_version where analysis_id=$1)`, id).Scan(&columns, &tasks, &versions)
	if err != nil || columns != 1000 || tasks != 0 || versions != 1000 {
		t.Fatal(columns, tasks, versions, err)
	}
	resume := nextstep.Object{"analysisId": id}
	before := call(session, "source.analysis.read", resume, false)
	if before["phase"] != "RUNNING" || before["next"].(nextstep.Object)["reasonCode"] != nextstep.AnalysisRunning {
		t.Fatal(before)
	}

	// Commit the same projections that the 66b2 worker will populate.
	longLabel := strings.Repeat("<>&", 1000)
	seed := func(first, last, seq int) {
		t.Helper()
		evidence := make([]analysis.BatchCandidate, 10)
		for n := range evidence {
			evidence[n] = analysis.BatchCandidate{ID: uuid.NewString(), Label: "Indirizzo", Term: fmt.Sprintf("urn:example:address:%d", n), Score: 0.9}
		}
		evidence[0].Label = longLabel
		candidates, err := json.Marshal(evidence)
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Pool().Exec(ctx, `with evidence as (
			update ouf_mcp.analysis_column c set request_id=r.request_id,
			  outcome=jsonb_build_object('state',case when c.ordinal=$2 then 'CANDIDATES_READY' else 'NO_CANDIDATES' end,
				'requestId',r.request_id,'observedAt','2026-11-27T00:00:00Z','candidateCount',case when c.ordinal=$2 then jsonb_array_length($5::jsonb) else 0 end)
			from (select ordinal,gen_random_uuid() as request_id from generate_series($2::int,$3::int) as ordinal) r
			where c.analysis_id=$1 and c.ordinal=r.ordinal returning c.ordinal
		), updated as (
			update ouf_mcp.analysis_batch_column b set state='READY_FOR_DECISION', due_at=null,evidence_version='fixture-1',
				candidates=case when b.ordinal=$2 then $5::jsonb else '[]'::jsonb end
			from evidence e where b.analysis_id=$1 and b.ordinal=e.ordinal returning b.ordinal
		)
		insert into ouf_mcp.analysis_counts(analysis_id,seq,total,ready,zero_count,blocked,pending,onboarding)
		select $1,$4,1000,h.ready+1,h.zero_count+($3-$2),h.blocked,h.pending-(select count(*) from updated),true
		from ouf_mcp.analysis_counts h where analysis_id=$1 order by seq desc limit 1`, id, first, last, seq, candidates)
		if err != nil {
			t.Fatal(err)
		}
	}
	seed(990, 999, 2)
	session2 := revisionSessionForScope(t, service, scope, scopes)
	type readResult struct {
		result *mcp.CallToolResult
		err    error
	}
	results := make(chan readResult, 2)
	for _, client := range []*mcp.ClientSession{session, session2} {
		go func() {
			result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "source.analysis.read", Arguments: nextstep.Object{"analysisId": id, "view": "BATCH"}})
			results <- readResult{result, err}
		}()
	}
	var batch nextstep.Object
	for range 2 {
		got := <-results
		if got.err != nil || got.result == nil || got.result.IsError {
			t.Fatalf("concurrent BATCH: %v %v", got.result, got.err)
		}
		read := assertBatchWire(t, contract, got.result)
		if batch != nil && batch["batchId"] != read["batchId"] {
			t.Fatal("concurrent reads offered different batches")
		}
		batch = read
	}
	items := batch["items"].([]any)
	if len(items) != 8 || items[0].(nextstep.Object)["ordinal"] != json.Number("990") || items[7].(nextstep.Object)["ordinal"] != json.Number("997") {
		t.Fatal("unexpected bounded selection", items)
	}
	var offered int
	if err = db.Pool().QueryRow(ctx, `select count(*) from ouf_mcp.analysis_batch_column where analysis_id=$1 and offered_batch_id=$2`,
		id, batch["batchId"]).Scan(&offered); err != nil || offered != len(items) {
		t.Fatal("offer membership differs from MCP response", offered, err)
	}
	seed(0, 0, 3)
	reopened, err := postgres.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Close)
	fresh := &orchestration.Service{RequireDelegation: true, Auth: auth, Admission: reopened, Gateway: gateway, FingerprintKey: service.FingerprintKey}
	emptyChat := revisionSessionForScope(t, fresh, scope, scopes)
	resumed := call(emptyChat, "source.analysis.read", resume, false)
	if resumed["batchId"] != batch["batchId"] || resumed["expectedRevision"] != batch["expectedRevision"] {
		t.Fatal("resume did not return the offered revision", resumed)
	}
	resumedItems := resumed["items"].([]any)
	for n, entry := range resumedItems {
		original, current := maps.Clone(items[n].(nextstep.Object)), maps.Clone(entry.(nextstep.Object))
		delete(original, "candidateCursor")
		delete(current, "candidateCursor")
		if analysis.Digest(original) != analysis.Digest(current) {
			t.Fatal("resumed offer changed an item", n)
		}
	}
	status := call(emptyChat, "source.analysis.read", nextstep.Object{"analysisId": id, "view": "STATUS"}, false)
	counts, _ := status["counts"].(nextstep.Object)
	if status["analysisId"] != id || status["mode"] != analysis.BatchMode || status["phase"] != "RUNNING" ||
		status["revision"] != batch["expectedRevision"] || counts["total"] != json.Number("1000") ||
		counts["pending"] != json.Number("989") || counts["candidatesReady"] != json.Number("2") || counts["noCandidates"] != json.Number("9") ||
		status["view"] != nil || status["batchId"] != nil || status["expectedRevision"] != nil || status["items"] != nil {
		t.Fatal("unexpected STATUS projection", status)
	}
	next := status["next"].(nextstep.Object)
	alternatives, _ := next["alternatives"].([]any)
	if next["reasonCode"] != analysis.DiscoveryDecisionRequired || !slices.ContainsFunc(alternatives, func(value any) bool {
		args, _ := value.(nextstep.Object)["arguments"].(nextstep.Object)
		return args["analysisId"] == id && args["batchId"] == batch["batchId"]
	}) {
		t.Fatal("STATUS next did not reference the offered lot", next)
	}
	first := resumedItems[0].(nextstep.Object)
	if examples := first["examples"].([]any); len(examples) != 2 || examples[0] != "via Roma 1" || examples[1] != true {
		t.Fatal("invalid projection of permitted examples", examples)
	}
	if examples := resumedItems[1].(nextstep.Object)["examples"].([]any); len(examples) != 0 {
		t.Fatal("restricted examples were projected", examples)
	}
	detail := nextstep.Object{"analysisId": id, "view": "CANDIDATES", "batchId": batch["batchId"], "ordinal": 990, "cursor": first["candidateCursor"]}
	auth.denied.Store(true)
	denial := call(emptyChat, "source.analysis.read", detail, true)
	if denial["reasonCode"] != "NO_APPLICABLE_GRANT" || denial["detail"] != nil || denial["candidates"] != nil {
		t.Fatal("revocation did not stop the detail page", denial)
	}
	auth.denied.Store(false)
	var label strings.Builder
	candidates := 0
	checkCandidates := func(values []any) {
		t.Helper()
		for _, raw := range values {
			candidate := raw.(nextstep.Object)
			if candidate["term"] != fmt.Sprintf("urn:example:address:%d", candidates) {
				t.Fatal("candidate skipped or repeated", candidate)
			}
			candidates++
		}
	}
	checkCandidates(first["candidates"].([]any))
	for n := 0; detail["view"] == "CANDIDATES"; n++ {
		if n == 10 {
			t.Fatal("candidate detail continuation did not advance")
		}
		body := call(emptyChat, "source.analysis.read", detail, false)
		checkCandidates(body["candidates"].([]any))
		if part, ok := body["detail"].(nextstep.Object); ok {
			if part["field"] != "label" || part["offset"] != json.Number(fmt.Sprint(label.Len())) {
				t.Fatal(part)
			}
			label.WriteString(part["text"].(string))
		}
		detail = body["next"].(nextstep.Object)["arguments"].(nextstep.Object)
	}
	if label.String() != longLabel || candidates != 10 {
		t.Fatal("missing candidates or incomplete authorized label", candidates)
	}
	if _, err := reopened.Pool().Exec(ctx, `update ouf_mcp.analysis_counts set integrity_hold=true where analysis_id=$1 and seq=3`, id); err != nil {
		t.Fatal(err)
	}
	blocked := call(emptyChat, "source.analysis.read", resume, false)
	if blocked["phase"] != "BLOCKED" || blocked["items"] != nil {
		t.Fatal("integrity hold exposed the offer", blocked)
	}
	_, err = reopened.Pool().Exec(ctx, `with closed as (
		update ouf_mcp.analysis_batch_column set state='CLOSED',due_at=null,offered_batch_id=null where analysis_id=$1
	), retired as (
		update ouf_mcp.analysis_batch_offer set retired_at=now() where analysis_id=$1 and retired_at is null
	)
	insert into ouf_mcp.analysis_counts(analysis_id,seq,total,matched,blocked,pending,onboarding) values($1,4,1000,1000,0,0,true)`, id)
	if err != nil {
		t.Fatal(err)
	}
	complete := call(emptyChat, "source.analysis.read", resume, false)
	if next := complete["next"].(nextstep.Object); complete["phase"] != "COMPLETE" || next["reasonCode"] != "ANALYSIS_SUMMARY" {
		t.Fatal("completed profile did not offer the summary", complete)
	}
}
