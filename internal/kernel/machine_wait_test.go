package kernel

import (
	"context"
	"encoding/json"
	"maps"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// A schema-valid call assembled outside the engine lacks the in-process
// witness, even when assigned to a variable before returning it.
func TestOwnerCallWireCopyRejectedAtEmission(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	s, err := newWorkflowServer(nil, m, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	next := (nextstep.Engine{}).ExpiredSummary(strings.Repeat("a", 64))
	raw, err := json.Marshal(next)
	if err != nil {
		t.Fatal(err)
	}
	var bypass nextstep.Object
	if err := json.Unmarshal(raw, &bypass); err != nil {
		t.Fatal(err)
	}
	result := s.validateNext("source.analysis.read", workflowError(nextstep.Object{"next": bypass}))
	if ownerObject(result)["code"] != "OWNER_CLOCK_UNCHECKED" {
		t.Fatal("unverified owner call passed emission boundary", result)
	}
}

// Exercise the manifest's actual producer policies and target schemas with
// an owner clock that gives conflicting relative and absolute lower bounds.
func TestMachineWaitOwnerFloor(t *testing.T) {
	const id = "11111111-1111-4111-8111-111111111111"
	analysisID := strings.Repeat("a", 64)
	type obj = nextstep.Object
	profile := obj{"assetId": id, "jobId": id}
	job := obj{"status": "RUNNING", "assetId": id, "jobId": id}
	discovery := obj{"requestId": id, "analysisId": analysisID}
	retry := obj{"state": "RUNNING", "requestId": id, "recoveryState": "RETRY_WAIT", "errorCode": "SEM_DISCOVERY_RATE_LIMITED"}
	source := obj{"sourceId": "source", "onboardingVersionId": id}
	checking := obj{"state": "APPROVED", "allowedActions": []any{"READ_AGAIN"}}
	pending := obj{"outcome": "MATERIALIZATION_PENDING"}
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	covered := map[string]bool{}
	for _, tc := range []struct {
		producer, target string
		input, owner     obj
	}{
		{"source.file.profile", "source.file.preview", obj{"assetId": id}, job},
		{"source.file.preview", "source.file.preview", profile, job},
		{"source.analysis.step", "source.analysis.step", obj{"analysisId": analysisID, "taskId": analysisID, "commandKey": id}, obj{"state": "WAITING"}},
		{"source.analysis.step", "semantic.discovery.status", obj{"analysisId": analysisID, "taskId": analysisID, "commandKey": id}, obj{"state": "SUCCEEDED", "result": obj{"requestId": id}}},
		{"semantic.discovery.request", "semantic.discovery.status", obj{}, retry},
		{"semantic.discovery.status", "semantic.discovery.status", discovery, retry},
		{"semantic.approval.request", "semantic.proposal.read", obj{"commandKey": id}, obj{"state": "APPROVED"}},
		{"semantic.proposal.read", "semantic.proposal.read", obj{"commandKey": id}, obj{"state": "APPROVED"}},
		{"source.onboarding.create", "source.onboarding.read", source, checking},
		{"source.onboarding.review.prepare", "source.onboarding.read", source, checking},
		{"source.onboarding.read", "source.onboarding.read", source, checking},
		{"source.onboarding.revision.prepare", "source.onboarding.read", source, checking},
		{"source.onboarding.revision.read", "source.onboarding.read", source, checking},
		{"ouf.ingestion.status", "ouf.ingestion.status", obj{"sourceId": "source"}, obj{"runState": "RUNNING", "sourceId": "source"}},
		{"urban.object.search", "urban.object.search", obj{"type": "urn:test:Place"}, pending},
		{"urban.acquisition.read", "urban.acquisition.read", obj{"sourceId": "source", "runId": id}, pending},
	} {
		covered[tc.producer] = true
		t.Run(tc.producer, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s, err := newWorkflowServer(nil, m, make([]byte, 32))
				if err != nil {
					t.Fatal(err)
				}
				var producer manifest.Capability
				for _, c := range m.ToolEligible() {
					if c.ToolName == tc.producer {
						producer = c
					}
					var schema jsonschema.Schema
					if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
						t.Fatal(err)
					}
					resolved, err := schema.Resolve(nil)
					if err != nil {
						t.Fatal(err)
					}
					s.tools[c.ToolName] = registeredTool{capability: c, input: resolved}
				}
				ctx := context.WithValue(context.Background(), identityKey{}, requestIdentity{Identity: orchestration.Identity{
					TenantID: "tenant", PrincipalID: "reader", Delegation: "fixture",
				}})
				start := time.Now()
				due := start.Add(72 * time.Second)
				if tc.producer != "source.analysis.step" || tc.target == tc.producer {
					engine := s.engine(ctx)
					engine.PollIntervalSeconds = 78
					next := engine.Evaluate(producer, m.NextPolicies[producer.NextPolicy], tc.input, tc.owner, false)
					args, _ := next["arguments"].(obj)
					token, _ := args["waitRef"].(string)
					ref, err := engine.Signer.Open(token, engine.Binding)
					if err != nil || ref.Read != tc.target || ref.NotBefore != start.Add(78*time.Second).Format(time.RFC3339Nano) {
						t.Fatal("static poll lost its local floor", next, ref, err)
					}
				}
				// One hint at a time: a second (recognized) field must never mask
				// a missing field in one of the producer paths.
				for _, field := range []string{"nextAttemptAt", "notBefore", "next_retry_at", "retryAt", "pollAfterSeconds", "retryAfterSeconds", "RetryAfter", "Retry-After", "recoveryState"} {
					for _, nested := range []bool{false, true} {
						owner := maps.Clone(tc.owner)
						fields := owner
						if nested {
							result, _ := owner["result"].(obj)
							fields = maps.Clone(result)
							if fields == nil {
								fields = obj{}
							}
							owner["result"] = fields
						}
						want := due
						switch field {
						case "pollAfterSeconds", "retryAfterSeconds", "RetryAfter", "Retry-After":
							fields[field] = 72
						case "recoveryState":
							fields[field] = "RETRY_WAIT"
							want = start.Add(time.Duration(orchestration.PollIntervalSeconds) * time.Second)
						default:
							fields[field] = want.Format(time.RFC3339Nano)
						}
						engine := s.engine(ctx)
						next := engine.Evaluate(producer, m.NextPolicies[producer.NextPolicy], tc.input, owner, false)
						args, _ := next["arguments"].(obj)
						token, _ := args["waitRef"].(string)
						ref, err := engine.Signer.Open(token, engine.Binding)
						if err != nil || next["tool"] != "ouf.wait" || ref.Read != tc.target || ref.NotBefore != want.Format(time.RFC3339Nano) {
							t.Fatalf("%s nested=%t: next=%v ref=%v err=%v", field, nested, next, ref, err)
						}
					}
				}
				owner := maps.Clone(tc.owner)
				timing := owner
				if receipt, ok := owner["result"].(obj); ok {
					timing = maps.Clone(receipt)
					owner["result"] = timing
				}
				timing["pollAfterSeconds"] = 5
				timing["retryAt"] = start.Add(time.Second).Format(time.RFC3339)
				timing["notBefore"] = start.Add(50 * time.Second).Format(time.RFC3339)
				timing["nextAttemptAt"] = due.Format(time.RFC3339)
				next := s.engine(ctx).Evaluate(producer, m.NextPolicies[producer.NextPolicy], tc.input, owner, false)
				reg := s.tools[tc.target]
				reads := 0
				reg.run = func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
					reads++
					if time.Now().Before(due) || req.Params.Name != tc.target {
						t.Error("owner contacted early or with wrong selector")
					}
					return &mcp.CallToolResult{StructuredContent: obj{"state": "SUCCEEDED", "requestId": id, "allowedActions": []any{"LIST_CANDIDATES"}}}, nil
				}
				s.tools[tc.target] = reg
				contract := nextContract(t)
				wait := func() obj {
					t.Helper()
					args, ok := next["arguments"].(obj)
					if next["action"] != "POLL" || next["tool"] != "ouf.wait" || !ok || !s.valid("ouf.wait", args) {
						t.Fatal("missing executable wait", next)
					}
					raw, _ := json.Marshal(obj{"params": obj{"name": "ouf.wait", "arguments": args}})
					var req mcp.CallToolRequest
					if err := json.Unmarshal(raw, &req); err != nil {
						t.Fatal(err)
					}
					return checkNext(t, contract, s.wait(ctx, &req, args["waitRef"].(string)))
				}
				next = wait()
				if time.Since(start) != s.waits.duration || reads != 0 || next["pollAfterSeconds"] != float64(27) {
					t.Fatal("first tranche contacted owner or lost the remaining wait", next, reads)
				}
				next = wait()
				if !time.Now().Equal(due) {
					t.Fatal("wrong observation time", time.Since(start))
				}
				if reg.capability.OperationClass == "COMMAND" {
					if reads != 0 || next["tool"] != tc.target || next["action"] != "CALL_TOOL" {
						t.Fatal("wait dispatched or lost a command", next)
					}
				} else if reads != 1 {
					t.Fatal("expected a single fresh read", reads)
				}
				if tc.target == "semantic.discovery.status" && next["tool"] != "semantic.discovery.candidates" {
					t.Fatal("discovery did not advance", next)
				}
			})
		})
	}
	for _, c := range m.ToolEligible() {
		for _, rule := range m.NextPolicies[c.NextPolicy].Rules {
			if rule.Action == "POLL" && !covered[c.ToolName] {
				t.Errorf("add %s to the producer timing table", c.ToolName)
				break
			}
		}
	}
}
