package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func (s *Store) ObserveDiscovery(ctx context.Context, scope analysis.Scope, id, manifest, tool, request string, raw []byte, floor time.Time) error {
	obs := orchestration.ObserveOwner(orchestration.GatewayResponse{Status: 200, Body: raw}, nil, time.Now())
	if floor.After(obs.Due) {
		obs.Due = floor
	}
	_, err := s.RecordDiscovery(ctx, scope, id, manifest, tool, request, raw, obs)
	return err
}

func TestSummaryPostgresSnapshotAndCost(t *testing.T) {
	for _, width := range []int{18, 1000, 1024} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			ctx, s, scope, profile, m := analysisFixture(t, width)
			headers := make([]string, width)
			var groups [2][]int
			for n := range headers {
				headers[n] = fmt.Sprintf("fixture-%d", n%2)
				groups[n%2] = append(groups[n%2], n)
			}
			work := make([]analysis.Work, 2)
			for n := range work {
				args, _ := json.Marshal(map[string]any{"intent": headers[n], "preferredLanguages": []string{"it"}, "requestedArtifactType": "PROPERTY"})
				work[n] = analysis.Work{Tool: "semantic.discovery.request", Arguments: args, Columns: groups[n]}
			}
			p, err := analysis.Build(scope, profile, m, 0, work)
			if err != nil {
				t.Fatal(err)
			}
			p.Headers = headers
			if _, err = s.PlanAnalysis(ctx, p); err != nil {
				t.Fatal(err)
			}
			refs := []string{uuid.NewString(), uuid.NewString()}
			due := time.Now().UTC().Truncate(time.Microsecond).Add(72 * time.Second).Format(time.RFC3339Nano)
			for n, task := range p.Tasks {
				claimed, err := s.ClaimAnalysisTask(ctx, scope, p.ID, task.ID, p.Manifest)
				if err != nil {
					t.Fatal(err)
				}
				body, _ := json.Marshal(map[string]string{"state": "PENDING", "requestId": refs[n], "nextAttemptAt": due})
				if _, err = s.FinishAnalysisTask(ctx, scope, p.ID, task.ID, claimed.Fence, analysis.Finish{State: "SUCCEEDED", Body: body, Calls: 1}); err != nil {
					t.Fatal(err)
				}
			}
			read := func(pos analysis.Position, page bool) analysis.Summary {
				t.Helper()
				v, err := s.ReadSummary(ctx, scope, p.ID, p.Manifest, pos, page)
				if err != nil {
					t.Fatal(err)
				}
				c := v.Counts
				if c.Total != width || c.Total != c.Matched+c.Ready+c.Zero+c.Blocked+c.Pending {
					t.Fatal(c)
				}
				return v
			}
			initial := analysis.Position{Seq: math.MaxInt64, Last: -1, Limit: 32}
			status := read(initial, false)
			first := read(initial, true)
			if status.Counts.Pending != width || status.Seq != 3 || len(status.Items) != 0 || first.Seq != status.Seq {
				t.Fatal(status, first.Seq)
			}
			if status.Hint.NotBefore != due || first.Hint.NotBefore != due {
				t.Fatal("summary lost the owner clock", status.Hint, first.Hint)
			}
			if status.ReadRows != 2 || first.ReadRows != 2+min(width, 32) {
				t.Fatal("non-page rows returned", status.ReadRows, first.ReadRows)
			}
			// A later live status can lengthen the receipt's lower bound.
			// Rereading the same request with a shorter delay must preserve it.
			later := time.Now().UTC().Truncate(time.Microsecond).Add(2 * time.Minute)
			for _, bound := range []string{later.Format(time.RFC3339Nano), due} {
				body, _ := json.Marshal(map[string]string{"state": "RUNNING", "next_retry_at": bound})
				if err := s.ObserveDiscovery(ctx, scope, p.ID, p.Manifest, "semantic.discovery.status", refs[0], body, time.Time{}); err != nil {
					t.Fatal(err)
				}
			}
			first = read(initial, true)
			if got := read(initial, false).Hint.NotBefore; got != later.Format(time.RFC3339Nano) || first.Hint.NotBefore != got {
				t.Fatal("reread shortened the persisted owner wait", got, first.Hint)
			}
			t.Logf("width=%d STATUS rows=%d fieldBytes=%d first SUMMARY rows=%d fieldBytes=%d", width, status.ReadRows, status.ReadBytes, first.ReadRows, first.ReadBytes)
			pos := initial
			pos.Seq, pos.Snapshot = first.Seq, uuid.New()
			if err = s.AuditSummary(ctx, scope, p.ID, "fixture-decision", pos.Seq, 0, len(first.Items)-1, pos.Snapshot, &first.Hint); err != nil {
				t.Fatal(err)
			}

			var wg sync.WaitGroup
			errs := make(chan error, 2)
			start := make(chan struct{})
			const zero = `{"state":"SUCCEEDED","candidateCount":0,"completedCandidateCount":0}`
			for _, ref := range refs {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					errs <- s.ObserveDiscovery(ctx, scope, p.ID, p.Manifest, "semantic.discovery.status", ref, []byte(zero), time.Time{})
				}()
			}
			close(start)
			for expected := 0; expected < width; {
				v := first
				if expected > 0 {
					v = read(pos, true)
				}
				if v.Seq != first.Seq || v.Counts != first.Counts || v.Hint != first.Hint {
					t.Fatal("snapshot changed", v)
				}
				for _, raw := range v.Items {
					var item struct {
						Ordinal                      int
						State, RequestID, ChangedSeq string
					}
					if json.Unmarshal(raw, &item) != nil || item.Ordinal != expected || item.RequestID != refs[expected%2] || item.State != "PENDING" || item.ChangedSeq != fmt.Sprint(2+expected%2) {
						t.Fatal(expected, string(raw))
					}
					expected++
				}
				pos.Last = expected - 1
				read(initial, false)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatal(err)
				}
			}
			done := read(initial, false)
			if done.State() != "RUNNING" || done.Counts.Blocked != width || done.Counts.Zero != 0 || done.Seq != 5 ||
				!done.Hint.DecisionRequired || done.Hint.Tool != "semantic.discovery.candidates" {
				t.Fatal(done)
			}
			if err = s.ObserveDiscovery(ctx, scope, p.ID, p.Manifest, "semantic.discovery.status", refs[0], []byte(zero), time.Time{}); err != nil {
				t.Fatal(err)
			}
			expired := []byte(`{"state":"SUCCEEDED","candidateCount":0,"candidates":[]}`)
			if err = s.ObserveDiscovery(ctx, scope, p.ID, p.Manifest, "semantic.discovery.candidates", refs[0], expired, time.Time{}); err != nil {
				t.Fatal(err)
			}
			if got := read(initial, false); got.Seq != done.Seq || got.Counts != done.Counts || !got.Hint.DecisionRequired {
				t.Fatal("replay or expiry erased prior evidence", got)
			}
			engine := nextstep.Engine{Valid: func(string, nextstep.Object) bool { return true }, Now: func() time.Time { return later.Add(time.Second) }}
			for _, view := range []string{"SUMMARY", "STATUS"} {
				resumed := read(initial, view == "SUMMARY")
				next := engine.SummaryNext(p.ID, view, "", resumed)
				args, _ := next["arguments"].(nextstep.Object)
				if next["action"] != "CALL_TOOL" || next["tool"] != "semantic.discovery.candidates" ||
					next["reasonCode"] != analysis.DiscoveryDecisionRequired || args["requestId"] != refs[0] || args["includeState"] != true {
					t.Fatal("lost empty page skipped the decision", view, next)
				}
			}
			if width == 18 {
				// Two different ordinals share the same discovery. The empty page
				// was delivered and lost. Re-reading offers stable, per-column keys.
				for index, capability := range []string{"ouf.semantic.propose", "ouf.managed-source.onboarding.create"} {
					proposal, exclude, err := s.DecisionKeys(ctx, scope, p.ID, p.Manifest, refs[0])
					if err != nil {
						t.Fatal(err)
					}
					again, same, err := s.DecisionKeys(ctx, scope, p.ID, p.Manifest, refs[0])
					if err != nil || proposal != again || exclude != same {
						t.Fatal("choice changed on resume", err)
					}
					key, owner := proposal, "semantic"
					args := map[string]any{"commandKey": key}
					if index == 1 {
						key, owner = exclude, "onboarding"
						args = map[string]any{"assetId": profile.AssetID, "profileId": profile.ProfileID,
							"fields": []map[string]string{{"fieldName": headers[0], "extractionDecision": "EXCLUDE"}}}
					}
					raw, _ := json.Marshal(args)
					service := orchestration.Service{Auth: controlPolicyFixture{}, Gateway: &repeatabilityGateway{body: "{}"}, Admission: s, FingerprintKey: make([]byte, 32)}
					in := orchestration.Invocation{Identity: orchestration.Identity{ServicePrincipalID: scope.Service, PrincipalID: scope.Principal, TenantID: scope.Tenant},
						CapabilityID: capability, Owner: owner, OperationClass: "COMMAND", GatewayBindingRef: "capability://" + capability,
						IdempotencyKey: key, Arguments: raw, ManifestChecksum: p.Manifest, CorrelationID: uuid.NewString(), RetryThreshold: 3,
						Timeout: time.Second, Window: time.Minute, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 1, ResultBytes: 262144}}
					for replay := 0; replay < 2; replay++ {
						res, err := service.Call(ctx, in)
						if err != nil || res.Replay != (replay == 1) || read(initial, false).Counts.Zero != index+1 {
							t.Fatal("decision must close exactly one ordinal", res, err)
						}
					}
				}
			}
			wrong := scope
			wrong.Tenant = uuid.NewString()
			if _, err = s.ReadSummary(ctx, wrong, p.ID, p.Manifest, pos, true); !errors.Is(err, analysis.ErrScope) {
				t.Fatal(err)
			}
			if width == 1000 {
				var plan string
				if err = s.pool.QueryRow(ctx, "explain (analyze, buffers, format json) "+summaryPageSQL, p.ID, 479, width, 32, first.Seq).Scan(&plan); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(plan, "analysis_column_version_pkey") || strings.Contains(plan, "profile_analysis_task") {
					t.Fatal(plan)
				}
				pos.Last = 479
				middle := read(pos, true)
				t.Logf("snapshot SUMMARY 480..511 rows=%d fieldBytes=%d", middle.ReadRows, middle.ReadBytes)
			}
		})
	}
}

func TestSummaryPreviewResumeAndInheritedOutcome(t *testing.T) {
	ctx, s, scope, p, m := analysisFixture(t, 3)
	work := analysis.Work{Tool: "semantic.discovery.request", Columns: []int{0}, Arguments: json.RawMessage(`{"intent":"fixture","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`)}
	plan, err := analysis.Build(scope, p, m, 0, []analysis.Work{work})
	if err != nil {
		t.Fatal(err)
	}
	prefix := []string{"fixture", ""}
	if err = s.SavePreviewPage(ctx, scope, plan.ID, plan.Manifest, p, 0, prefix); err != nil {
		t.Fatal(err)
	}
	if err = s.SavePreviewPage(ctx, scope, plan.ID, plan.Manifest, p, 0, prefix); err != nil {
		t.Fatal(err)
	}
	if err = s.SavePreviewPage(ctx, scope, plan.ID, plan.Manifest, p, 0, []string{"other"}); !errors.Is(err, analysis.ErrProfile) {
		t.Fatal("overwrote verified prefix", err)
	}
	pos := analysis.Position{Seq: math.MaxInt64, Last: -1, Limit: 32}
	if _, err = s.ReadSummary(ctx, scope, plan.ID, plan.Manifest, pos, false); !errors.Is(err, analysis.ErrScope) {
		t.Fatal("partial preview published", err)
	}
	if err = s.SavePreviewPage(ctx, scope, plan.ID, plan.Manifest, p, 2, []string{"fixture"}); err != nil {
		t.Fatal(err)
	}
	_, plan.Headers, err = s.PreviewProgress(ctx, scope, plan.ID, plan.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.PlanAnalysis(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	task, err := s.ClaimAnalysisTask(ctx, scope, plan.ID, plan.Tasks[0].ID, plan.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	request := uuid.NewString()
	receipt, _ := json.Marshal(map[string]string{"state": "PENDING", "requestId": request})
	if _, err = s.FinishAnalysisTask(ctx, scope, plan.ID, task.ID, task.Fence, analysis.Finish{State: "SUCCEEDED", Body: receipt}); err != nil {
		t.Fatal(err)
	}
	observe := func(tool, raw string) {
		t.Helper()
		if err := s.ObserveDiscovery(ctx, scope, plan.ID, plan.Manifest, tool, request, []byte(raw), time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	observe("semantic.discovery.status", `{"state":"SUCCEEDED","candidateCount":1,"completedCandidateCount":1}`)
	observe("semantic.discovery.candidates", `{"state":"SUCCEEDED","candidates":[{"candidate_id":"11111111-1111-4111-8111-111111111111"}]}`)
	work.Columns = []int{2}
	extension, err := analysis.Build(scope, p, m, v.Revision, []analysis.Work{work})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PlanAnalysis(ctx, extension); err != nil {
		t.Fatal(err)
	}
	result, err := s.ReadSummary(ctx, scope, plan.ID, plan.Manifest, pos, true)
	if err != nil || result.State() != "PARTIAL" || result.Counts.Ready != 2 || result.Counts.Blocked != 1 {
		t.Fatal(result, err)
	}
	if !strings.Contains(string(result.Items[1]), "ANALYSIS_COLUMN_INVALID") {
		t.Fatal(string(result.Items[1]))
	}
	if _, err = s.PlanAnalysis(ctx, extension); err != nil {
		t.Fatal(err)
	}
	replay, err := s.ReadSummary(ctx, scope, plan.ID, plan.Manifest, pos, false)
	if err != nil || replay.Seq != result.Seq {
		t.Fatal(replay, err)
	}
	observe("semantic.discovery.candidates", `{"state":"SUCCEEDED","candidateCount":0,"candidates":[]}`)
	expired, err := s.ReadSummary(ctx, scope, plan.ID, plan.Manifest, pos, true)
	if err != nil || !expired.Hint.DecisionRequired || expired.Hint.RequestID != request ||
		expired.Counts.Ready != 0 || expired.Counts.Blocked != 3 || expired.State() != "RUNNING" {
		t.Fatal("expired listed candidates did not reopen the column decision", expired, err)
	}
	conflict := s.ObserveDiscovery(ctx, scope, plan.ID, plan.Manifest, "semantic.discovery.status", request, []byte(`{"state":"SUCCEEDED","completedCandidateCount":0}`), time.Time{})
	if !errors.Is(conflict, analysis.ErrProfile) {
		t.Fatal("changed immutable count", conflict)
	}
	hold, err := s.ReadSummary(ctx, scope, plan.ID, plan.Manifest, pos, false)
	if err != nil || hold.State() != "BLOCKED" || hold.Hint.Reason != "ANALYSIS_PROFILE_CHANGED" || hold.Seq != expired.Seq+1 {
		t.Fatal(hold, err)
	}

}

func TestLegacyEmptyPageResumesAndReconcilesOriginalChoice(t *testing.T) {
	for _, mode := range []string{"legacy receipt", "021_empty_candidate_resume.sql", "022_expired_candidate_decision.sql"} {
		t.Run(mode, func(t *testing.T) { checkLegacyEmptyChoice(t, mode) })
	}
}

func checkLegacyEmptyChoice(t *testing.T, mode string) {
	t.Helper()
	ctx, s, scope, profile, m := analysisFixture(t, 1)
	p, err := analysis.Build(scope, profile, m, 0, []analysis.Work{{Tool: "semantic.discovery.request", Columns: []int{0},
		Arguments: json.RawMessage(`{"intent":"fixture","preferredLanguages":["it"],"requestedArtifactType":"PROPERTY"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	p.Headers = []string{"fixture"}
	if _, err = s.PlanAnalysis(ctx, p); err != nil {
		t.Fatal(err)
	}
	request, key := uuid.NewString(), uuid.NewString()
	// Seed a receipt from the previous implementation, which left the column open.
	service := orchestration.Service{Auth: controlPolicyFixture{}, Gateway: &repeatabilityGateway{body: "{}"}, Admission: s, FingerprintKey: make([]byte, 32)}
	raw, _ := json.Marshal(map[string]any{"assetId": profile.AssetID, "profileId": profile.ProfileID,
		"fields": []map[string]string{{"fieldName": "fixture", "extractionDecision": "EXCLUDE"}}})
	const capability = "ouf.managed-source.onboarding.create"
	in := orchestration.Invocation{Identity: orchestration.Identity{ServicePrincipalID: scope.Service, PrincipalID: scope.Principal, TenantID: scope.Tenant},
		CapabilityID: capability, Owner: "onboarding", OperationClass: "COMMAND", GatewayBindingRef: "capability://" + capability,
		IdempotencyKey: key, Arguments: raw, ManifestChecksum: p.Manifest, CorrelationID: uuid.NewString(), RetryThreshold: 3,
		Timeout: time.Second, Window: time.Minute, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 1, ResultBytes: 262144}}
	if res, err := service.Call(ctx, in); err != nil || res.Replay {
		t.Fatal(res, err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	// Reproduce the previous projection, including its immutable version
	// and the keys delivered before the client lost the reply.
	old := analysis.Outcome{State: "BLOCKED", Reason: "DISCOVERY_EVIDENCE_UNAVAILABLE", RequestID: request}
	if err = projectOutcome(ctx, tx, p.ID, []int{0}, request, old, ""); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `update ouf_mcp.analysis_column set empty_candidate_page=true,
		proposal_choice=gen_random_uuid(),exclusion_choice=$2 where analysis_id=$1`, p.ID, key); err != nil {
		t.Fatal(err)
	}
	if mode == "022_expired_candidate_decision.sql" {
		if _, err := tx.Exec(ctx, `insert into ouf_mcp.analysis_job_completion(analysis_id,request_id,completed_count)
			values($1,$2,1)`, p.ID, request); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	pos := analysis.Position{Seq: math.MaxInt64, Last: -1, Limit: 1}
	if mode != "legacy receipt" {
		sql, err := migrationFiles.ReadFile("migrations/" + mode)
		if err != nil {
			t.Fatal(err)
		}
		for pass := range 2 {
			tag, err := s.pool.Exec(ctx, string(sql))
			if err != nil || (pass == 1 && tag.RowsAffected() != 0) {
				t.Fatal("backfill was not idempotent", tag, err)
			}
		}
		status, err := s.ReadSummary(ctx, scope, p.ID, p.Manifest, pos, false)
		if err != nil || status.State() != "RUNNING" || status.Hint.RequestID != request || status.Hint.Tool != "semantic.discovery.candidates" {
			t.Fatal(status, err)
		}
		if mode == "022_expired_candidate_decision.sql" && !status.Hint.DecisionRequired {
			t.Fatal("migrated expiry did not request a decision", status)
		}
		if err := s.ObserveDiscovery(ctx, scope, p.ID, p.Manifest, "semantic.discovery.candidates", request,
			[]byte(`{"state":"SUCCEEDED","candidateCount":0,"candidates":[]}`), time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	if res, err := service.Call(ctx, in); err != nil || !res.Replay {
		t.Fatal("lost legacy receipt caused a new dispatch", res, err)
	}
	status, err := s.ReadSummary(ctx, scope, p.ID, p.Manifest, pos, false)
	if err != nil || status.State() != "COMPLETE" || status.Counts.Zero != 1 {
		t.Fatal(status, err)
	}
}
