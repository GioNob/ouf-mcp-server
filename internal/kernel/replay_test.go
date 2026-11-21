package kernel

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type retainedAttemptAdmission struct {
	statusAdmission
	decision orchestration.AdmissionDecision
}

func (a *retainedAttemptAdmission) Reserve(context.Context, orchestration.AdmissionRequest) (orchestration.AdmissionDecision, error) {
	return a.decision, nil
}

func TestReplayNamesOriginalAttemptAndAuthorizedRead(t *testing.T) {
	attempt, ref := uuid.New(), uuid.New()
	snapshot, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	identity := requestIdentity{Identity: orchestration.Identity{ServicePrincipalID: "mcp", PrincipalID: "mapper", TenantID: "tenant", ActorType: "HUMAN", Delegation: "fixture"}}
	ctx := context.WithValue(context.Background(), identityKey{}, identity)
	for _, tc := range []struct {
		tool, operation, read, selector string
	}{
		{"source.file.profile", "COMMAND", "source.file.preview", "jobId"},
		{"source.onboarding.create", "COMMAND", "source.onboarding.read", "onboardingVersionId"},
		{"semantic.discovery.request", "COMMAND", "semantic.discovery.status", "requestId"},
		{"source.file.upload", "COMMAND", "", "assetId"},
		{"semantic.discovery.candidates", "READ", "semantic.discovery.candidates", ""},
	} {
		for _, state := range []string{"ADMITTED", "RUNNING", "FAILED", "SUCCEEDED"} {
			in := orchestration.Result{Replay: true, AttemptID: attempt, AttemptState: state}
			successRef := state == "SUCCEEDED" && tc.operation == "COMMAND"
			if successRef {
				in.ResultID = &ref
			}
			cap, ok := analysis.Find(snapshot, tc.tool)
			if !ok {
				t.Fatal(tc.tool)
			}
			args := []byte(`{"assetId":"asset","sourceId":"source","idempotencyKey":"owner-command-key"}`)
			var got *mcp.CallToolResult
			if tc.tool == "source.file.upload" {
				got = replayResult(cap, args, in)
			} else {
				admission := &retainedAttemptAdmission{decision: orchestration.AdmissionDecision{Replay: true, AttemptID: attempt, AttemptState: state, ResultID: in.ResultID}}
				service := &orchestration.Service{Auth: analysisSDKAuth{}, Admission: admission, FingerprintKey: make([]byte, 32)}
				got, _, err = invoke(ctx, cap, "fixture", args, service, 100, 262144)
				if err != nil {
					t.Fatal(err)
				}
			}
			var body struct {
				AttemptID                            uuid.UUID
				AttemptState, ReadTool, AssetID      string
				ResultRef                            *uuid.UUID
				ReadArguments                        map[string]string
				UseOriginalArgumentsWithFreshReadKey bool
			}
			raw := got.Content[0].(*mcp.TextContent).Text
			if json.Unmarshal([]byte(raw), &body) != nil || body.AttemptID != attempt || body.AttemptState != state || got.IsError == successRef {
				t.Fatal(tc.tool, state, raw)
			}
			if successRef {
				if body.ResultRef == nil || *body.ResultRef != ref || body.ReadTool != tc.read {
					t.Fatal("missing owner reference", raw)
				}
				if tc.tool == "source.file.upload" {
					if body.AssetID != ref.String() {
						t.Fatal("upload asset lost", raw)
					}
				} else if body.ReadArguments[tc.selector] != ref.String() {
					t.Fatal("wrong exact read", raw)
				}
			} else if state == "SUCCEEDED" && (body.ReadTool != tc.read || !body.UseOriginalArgumentsWithFreshReadKey) {
				t.Fatal("READ replay lost exact selectors", raw)
			}
		}
	}
}
