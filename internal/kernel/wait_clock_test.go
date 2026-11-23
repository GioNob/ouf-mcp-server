package kernel

import (
	"context"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

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
		owner := nextstep.Object{"reviewUrl": "https://fixture.invalid/review", "expiresAt": time.Now().Add(time.Minute).Format(time.RFC3339),
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
			body := ownerObject(<-results)
			again := body["next"].(nextstep.Object)
			if body["changed"] != false || again["action"] != "POLL" || again["pollAfterSeconds"].(json.Number).String() != strconv.Itoa(orchestration.PollIntervalSeconds) ||
				len(again["terminalStates"].([]any)) == 0 || again["deadline"] != owner["expiresAt"] || again["arguments"].(nextstep.Object)["waitRef"] != token {
				t.Fatal("wait extended or replaced an unchanged reference")
			}
		}
		if time.Since(start) != 45*time.Second || reads.Load() != 1 {
			t.Fatalf("duration %s, shared reads %d", time.Since(start), reads.Load())
		}
		time.Sleep(16 * time.Second)
		expired := ownerObject(s.wait(ctx, &req, token))["next"].(nextstep.Object)
		if expired["reasonCode"] != "WAIT_DEADLINE_EXPIRED" || reads.Load() != 1 {
			t.Fatal("expired reference made another owner read")
		}
	})
}
