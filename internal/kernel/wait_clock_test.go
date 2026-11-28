package kernel

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestWaitRateFor100DistinctReaders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, err := configuredWait(nil)
		if err != nil {
			t.Fatal(err)
		}
		dispatched := make(chan time.Time, 100)
		for range 100 {
			go func() {
				if err := w.reads.Wait(context.Background()); err != nil {
					t.Error(err)
				}
				dispatched <- time.Now()
			}()
		}
		var times []time.Time
		for range 100 {
			times = append(times, <-dispatched)
		}
		slices.SortFunc(times, time.Time.Compare)
		for i := 20; i < len(times); i++ {
			if times[i].Sub(times[i-20]) < time.Second {
				t.Fatal("more than 20 owner reads in a second")
			}
		}
	})
}

func TestWaitUnchangedCoalescesAndExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.WithValue(context.Background(), identityKey{}, requestIdentity{
			Identity: orchestration.Identity{PrincipalID: "reader", TenantID: "tenant", Delegation: "fixture-proof"},
		})
		snapshot, err := manifest.Load()
		if err != nil {
			t.Fatal(err)
		}
		s, err := newWorkflowServer(nil, snapshot, make([]byte, 32))
		if err != nil {
			t.Fatal(err)
		}
		const name = "source.onboarding.read"
		contract := nextContract(t)
		version := "sha256:" + strings.Repeat("a", 64)
		var reads atomic.Int32
		for _, c := range snapshot.ToolEligible() {
			if c.ToolName != name {
				continue
			}
			var schema jsonschema.Schema
			if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
				t.Fatal(err)
			}
			input, err := schema.Resolve(nil)
			if err != nil {
				t.Fatal(err)
			}
			s.tools[name] = registeredTool{capability: c, input: input, run: func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				reads.Add(1)
				time.Sleep(time.Millisecond)
				return &mcp.CallToolResult{StructuredContent: nextstep.Object{"unchanged": true, "observationVersion": version}}, nil
			}}
		}
		const id = "11111111-1111-4111-8111-111111111111"
		var charges atomic.Int32
		s.waits.charge = func(context.Context, orchestration.Identity) error {
			charges.Add(1)
			return nil
		}
		owner := nextstep.Object{"reviewUrl": "https://fixture.invalid/review", "expiresAt": time.Now().Add(time.Hour).Format(time.RFC3339),
			"lockVersion": 1, "observationVersion": version}
		next := s.waits.signer.Issue(waitBinding(ctx), manifest.Rule{Tool: name, Reason: "SOURCE_REVIEW", Decision: "APPROVE_OR_REJECT"},
			nextstep.Object{"sourceId": "teatri", "onboardingVersionId": id}, owner, nextstep.LinkResult{}, time.Now())
		token := next["arguments"].(nextstep.Object)["waitRef"].(string)
		var req mcp.CallToolRequest
		if err := json.Unmarshal([]byte(`{"params":{"name":"ouf.wait","arguments":{}}}`), &req); err != nil || req.Params == nil {
			t.Fatal("invalid SDK request fixture")
		}
		start := time.Now()
		results := make(chan *mcp.CallToolResult, 2)
		for range 2 {
			go func() { results <- s.wait(ctx, &req, token) }()
		}
		for range 2 {
			result := <-results
			body := ownerObject(result)
			again := checkNext(t, contract, result)
			if body["changed"] != false || again["action"] != "WAIT_FOR_HUMAN" || again["deadline"] != owner["expiresAt"] || again["arguments"].(nextstep.Object)["waitRef"] != token {
				t.Fatal("wait extended or replaced an unchanged reference")
			}
		}
		if time.Since(start) != 45*time.Second || reads.Load() != 8 || charges.Load() != 2 {
			t.Fatalf("duration %s, shared reads %d", time.Since(start), reads.Load())
		}
		// Interrupt a live tranche after an owner read, then reconnect to a new server.
		inFlight, interrupt := context.WithCancel(ctx)
		disconnected := make(chan *mcp.CallToolResult, 1)
		go func() { disconnected <- s.wait(inFlight, &req, token) }()
		time.Sleep(s.waits.interval + time.Second)
		interrupt()
		if stopped := checkNext(t, contract, <-disconnected); stopped["reasonCode"] != "WAIT_OBSERVATION_UNAVAILABLE" || reads.Load() != 9 {
			t.Fatal("disconnect did not cancel the in-flight tranche", stopped)
		}
		restarted, err := newWorkflowServer(nil, snapshot, make([]byte, 32))
		if err != nil {
			t.Fatal(err)
		}
		restarted.tools[name] = s.tools[name]
		restarted.waits.charge = s.waits.charge
		s = restarted
		again := checkNext(t, contract, s.wait(ctx, &req, token))
		if time.Since(start) != 96*time.Second || again["action"] != "WAIT_FOR_HUMAN" || again["arguments"].(nextstep.Object)["waitRef"] != token || charges.Load() != 4 || reads.Load() != 17 {
			t.Fatal("restart or minute rollover changed the wait", again)
		}
		time.Sleep(time.Hour - time.Since(start) - time.Second)
		expired := checkNext(t, contract, s.wait(ctx, &req, token))
		if expired["reasonCode"] != "WAIT_DEADLINE_EXPIRED" || time.Since(start) != time.Hour || reads.Load() != 17 {
			t.Fatal("expired reference made another owner read")
		}
	})
}

func TestPickerWaitOwnerTransitions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		snapshot, err := manifest.Load()
		if err != nil {
			t.Fatal(err)
		}
		sdk := mcp.NewServer(&mcp.Implementation{Name: "fixture", Version: "1"}, nil)
		s, err := newWorkflowServer(sdk, snapshot, make([]byte, 32))
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range snapshot.ToolEligible() {
			switch c.ToolName {
			case "source.file.upload":
				registerPickerUploadTool(s, c, nil)
				registerPickerHandoffStatusTool(s, c, nil)
			case "ouf.wait":
				s.registerWait(c)
			case "source.file.profile":
				registerUnavailableTool(s, c)
			}
		}
		identity := requestIdentity{Identity: orchestration.Identity{ActorType: "HUMAN", TenantID: "t", PrincipalID: "reader", Delegation: "fixture"}}
		ctx := context.WithValue(context.Background(), identityKey{}, identity)
		contract := nextContract(t)
		const read = "source.file.upload.status"
		const id = "11111111-1111-4111-8111-111111111111"
		start := time.Now()
		until := start.Add(time.Hour).Format(time.RFC3339)
		args := nextstep.Object{"handoffId": id}
		issued := nextstep.Object{"handoffId": id, "pickerUrl": "https://owner.invalid/choose", "state": "PENDING",
			"issuedAt": start.Format(time.RFC3339), "waitUntil": until, "observationVersion": "1"}
		project := func(tool string, input, owner nextstep.Object) nextstep.Object {
			t.Helper()
			return checkNext(t, contract, s.decorate(ctx, s.tools[tool], input, &mcp.CallToolResult{StructuredContent: owner}))
		}
		next := project("source.file.upload", nextstep.Object{}, issued)
		if next["action"] != "WAIT_FOR_HUMAN" || next["deadline"] != until || next["human"].(nextstep.Object)["expiresAt"] != until {
			t.Fatal(next)
		}
		token := next["arguments"].(nextstep.Object)["waitRef"].(string)
		var req mcp.CallToolRequest
		if err := json.Unmarshal([]byte(`{"params":{"name":"ouf.wait","arguments":{}}}`), &req); err != nil {
			t.Fatal(err)
		}
		var reads int
		receipt := nextstep.Object{"issuedAt": issued["issuedAt"], "waitUntil": until, "state": "STAGED", "status": "STAGED",
			"assetId": id, "observationVersion": "2"}
		reg := s.tools[read]
		reg.run = func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			reads++
			if ctx.Value(internalWaitReadKey{}) != true || string(req.Params.Arguments) != `{"handoffId":"11111111-1111-4111-8111-111111111111"}` {
				t.Error("wrong internal owner read")
			}
			// The human completes the picker one millisecond after the first read.
			if time.Now().Before(start.Add(s.waits.interval + time.Millisecond)) {
				return &mcp.CallToolResult{StructuredContent: nextstep.Object{"state": "PENDING", "status": "PENDING", "issuedAt": issued["issuedAt"], "waitUntil": until, "observationVersion": "1"}}, nil
			}
			return &mcp.CallToolResult{StructuredContent: receipt}, nil
		}
		s.tools[read] = reg
		otherTenant := identity
		otherTenant.TenantID = "other"
		for _, invalid := range []struct {
			token string
			id    requestIdentity
		}{{"!" + token[1:], identity}, {token, otherTenant}} {
			other := context.WithValue(ctx, identityKey{}, invalid.id)
			result := s.wait(other, &req, invalid.token)
			next := checkNext(t, contract, s.decorate(other, s.tools["ouf.wait"], nextstep.Object{"waitRef": invalid.token}, result))
			if !result.IsError || next["reasonCode"] != "WAIT_REFERENCE_INVALID" || reads != 0 {
				t.Fatal(next)
			}
		}
		result := s.wait(ctx, &req, token)
		next = checkNext(t, contract, result)
		if reads != 2 || time.Since(start) != 2*s.waits.interval || next["tool"] != "source.file.profile" || ownerObject(result)["changed"] != true {
			t.Fatal("did not observe the first change at the next tick", next)
		}
		time.Sleep(time.Hour)
		next = checkNext(t, contract, s.wait(ctx, &req, token))
		if next["reasonCode"] != "WAIT_DEADLINE_EXPIRED" || len(next["alternatives"].([]any)) != 2 || reads != 2 {
			t.Fatal(next)
		}
		if next = project(read, args, receipt); next["tool"] != "source.file.profile" {
			t.Fatal("late authorized read lost the recorded result", next)
		}
		for _, state := range []string{"PENDING", "STAGED", "REJECTED"} {
			next = project(read, args, nextstep.Object{"legacy": true, "status": state})
			alt := next["alternatives"].([]any)[0].(nextstep.Object)
			if next["reasonCode"] != "HANDOFF_NOT_ISSUED" || next["actor"] != "AGENT" || alt["tool"] != "source.file.upload" || len(alt["arguments"].(nextstep.Object)) != 0 {
				t.Fatal(next)
			}
		}
		delete(issued, "waitUntil")
		next = project("source.file.upload", nextstep.Object{}, issued)
		if next["reasonCode"] != "HANDOFF_DEADLINE_REQUIRED" || next["actor"] != "OPERATOR" || !strings.Contains(next["userMessage"].(string), "Onboarding") {
			t.Fatal(next)
		}
	})
}

type waitOwnerClock struct {
	orchestration.AdmissionPort
}

func (waitOwnerClock) ReadOwnerObservation(ctx context.Context, _ orchestration.Identity, _ string) (orchestration.OwnerObservation, error) {
	return orchestration.OwnerObservation{}, ctx.Err()
}

func TestBatchReviewWaitExpiresDuringOwnerRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		snapshot, err := manifest.Load()
		if err != nil {
			t.Fatal(err)
		}
		s, err := newWorkflowServer(nil, snapshot, make([]byte, 32))
		if err != nil {
			t.Fatal(err)
		}
		s.analysisService = &orchestration.Service{Admission: waitOwnerClock{}}
		s.waits.interval, s.waits.duration = time.Second, 5*time.Second
		const read = "semantic.proposal.read"
		const batch = "11111111-1111-4111-8111-111111111111"
		id := strings.Repeat("a", 64)
		args := nextstep.Object{"analysisId": id, "projection": analysis.BatchMode, "reviewBatchId": batch}
		expiry := time.Now().Add(2 * time.Second)
		deadline := expiry.Format(time.RFC3339Nano)
		reads := make(chan error, 1)
		for _, c := range snapshot.ToolEligible() {
			if c.ToolName != read {
				continue
			}
			var schema jsonschema.Schema
			if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
				t.Fatal(err)
			}
			input, err := schema.Resolve(nil)
			if err != nil {
				t.Fatal(err)
			}
			s.tools[read] = registeredTool{capability: c, input: input, run: func(ctx context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				if !time.Now().Before(expiry) {
					t.Error("owner read started after the THS deadline")
				}
				<-ctx.Done()
				reads <- ctx.Err()
				return &mcp.CallToolResult{StructuredContent: nextstep.Object{"reviewBatchId": batch, "state": "DECIDED"}}, nil
			}}
		}
		ctx := context.WithValue(t.Context(), identityKey{}, requestIdentity{Identity: orchestration.Identity{
			PrincipalID: "reader", TenantID: "tenant", Delegation: "fixture-proof",
		}})
		issued := s.waits.signer.Issue(waitBinding(ctx), manifest.Rule{Action: "WAIT_FOR_HUMAN", Tool: read, Reason: nextstep.ReviewBatch, Decision: "APPROVE_EACH_ROW"},
			args, nextstep.Object{"state": "UNDER_REVIEW", "reviewBatchId": batch, "version": strings.Repeat("b", 64), "expiresAt": deadline},
			nextstep.LinkResult{Status: nextstep.LinkAvailable, URL: "https://fixture.invalid/review", ExpiresAt: deadline}, time.Now())
		if issued["action"] != "WAIT_FOR_HUMAN" {
			t.Fatal("failed to issue the batch wait", issued)
		}
		token := issued["arguments"].(nextstep.Object)["waitRef"].(string)
		var req mcp.CallToolRequest
		if err := json.Unmarshal([]byte(`{"params":{"name":"ouf.wait","arguments":{}}}`), &req); err != nil {
			t.Fatal(err)
		}
		result := s.wait(ctx, &req, token)
		body := ownerObject(result)
		next := checkNext(t, nextContract(t), result)
		if result.IsError || body["changed"] != true || body["observation"] != nil ||
			next["action"] != "CALL_TOOL" || next["reasonCode"] != nextstep.ReviewBatchExpired || next["tool"] != read {
			t.Fatal("expiry lost the batch reread or exposed the late response", body)
		}
		reread, _ := next["arguments"].(nextstep.Object)
		if !maps.Equal(reread, args) {
			t.Fatal("expiry changed the batch selector", reread)
		}
		if err := <-reads; err != context.DeadlineExceeded {
			t.Fatal("the in-flight read did not end at the THS deadline", err)
		}
	})
}

func TestAdmissionConfigurationRejectsInvalidValues(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "1.5", "invalid", "2147483648"} {
		t.Setenv("OUF_ADMISSION_CALLS_PER_MINUTE", value)
		if _, err := NewHTTPHandler(nil); err == nil {
			t.Fatalf("server accepted admissionCallsPerMinute=%q", value)
		}
	}
	t.Setenv("OUF_ADMISSION_CALLS_PER_MINUTE", "7")
	w, err := configuredWait(nil)
	if err != nil || w.callsPerMinute != 7 {
		t.Fatalf("configured budget missing: %v", err)
	}
}
