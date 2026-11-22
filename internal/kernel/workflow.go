package kernel

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"maps"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type rawTool func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error)

type registeredTool struct {
	capability manifest.Capability
	input      *jsonschema.Resolved
	forms      []string
	limit      int
	run        rawTool
}

type workflowServer struct {
	*mcp.Server
	manifest *manifest.Snapshot
	tools    map[string]registeredTool
	waits    *waitService
	runLink  func(context.Context, nextstep.Object) nextstep.Object
	key      []byte
}

func newWorkflowServer(sdk *mcp.Server, snapshot *manifest.Snapshot, installationKey []byte) (*workflowServer, error) {
	waits, err := configuredWait(installationKey)
	if err != nil {
		return nil, err
	}
	if len(installationKey) < 32 {
		installationKey = make([]byte, 32)
		rand.Read(installationKey)
	}
	return &workflowServer{Server: sdk, manifest: snapshot, tools: map[string]registeredTool{}, waits: waits, key: installationKey}, nil
}

// A pseudorandom UUIDv4 for this intent. Reprojecting the same observation
// after a lost read response must not open a second owner command.
func (s *workflowServer) commandKey(ctx context.Context, tool string, args, obs, input nextstep.Object) string {
	mac := hmac.New(sha256.New, s.key)
	seed := []any{"ouf.next-step.v1/command-key", waitBinding(ctx), tool, args}
	if tool != "source.analysis.plan" && tool != "source.analysis.step" {
		seed = append(seed, nextstep.Version(obs), obs["challengeId"], input["commandKey"])
	}
	_ = json.NewEncoder(mac).Encode(seed)
	var id uuid.UUID
	copy(id[:], mac.Sum(nil))
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	return id.String()
}

func (s *workflowServer) valid(name string, args nextstep.Object) bool {
	t, ok := s.tools[name]
	if !ok || args == nil {
		return false
	}
	// Validate JSON values, including nested typed slices built by the planner.
	raw, err := json.Marshal(args)
	var v map[string]any
	return err == nil && json.Unmarshal(raw, &v) == nil && t.input.Validate(v) == nil
}

// Keep argument diagnostics inside the tool result. Typed SDK registration
// otherwise rejects a oneOf before the application's handler can supply next.
func addWorkflowTool[T any](s *workflowServer, tool *mcp.Tool, handle func(context.Context, *mcp.CallToolRequest, T) (*mcp.CallToolResult, any, error)) {
	var output *jsonschema.Resolved
	if schema, ok := tool.OutputSchema.(*jsonschema.Schema); ok {
		var err error
		output, err = schema.Resolve(nil)
		if err != nil {
			panic(err)
		}
	}
	s.register(tool, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var input T
		if err := json.Unmarshal(req.Params.Arguments, &input); err != nil {
			return errorResult("INVALID_TOOL_ARGUMENTS", false), nil
		}
		result, structured, err := handle(ctx, req, input)
		if structured != nil && err == nil {
			body, e := json.Marshal(structured)
			var v map[string]any
			if e != nil || json.Unmarshal(body, &v) != nil || (output != nil && output.Validate(v) != nil) {
				return errorResult("OWNER_RESULT_INVALID", false), nil
			}
			result.StructuredContent = structured
		}
		return result, err
	})
}

func (s *workflowServer) register(tool *mcp.Tool, run rawTool) {
	name := tool.Name
	source := name
	if name == "source.file.upload.status" || name == "source.file.attachment_origin_probe" {
		source = "source.file.upload"
	}
	var c manifest.Capability
	for _, candidate := range s.manifest.Capabilities {
		if candidate.ToolName == source {
			c = candidate
			break
		}
	}
	schema := tool.InputSchema.(*jsonschema.Schema)
	input, err := schema.Resolve(nil)
	if err != nil {
		panic(err)
	}
	var budget struct{ HardMaxResultBytes int }
	if json.Unmarshal(c.BaseCostEnvelope, &budget) != nil || budget.HardMaxResultBytes < 1024 {
		panic("missing result budget: " + name)
	}
	raw, _ := json.Marshal(schema)
	if !strings.HasSuffix(tool.Description, manifest.FollowNext) {
		tool.Description += " " + manifest.FollowNext
	}
	t := registeredTool{c, input, manifest.InputForms(raw), budget.HardMaxResultBytes, run}
	s.tools[name] = t
	s.Server.AddTool(tool, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args map[string]any
		var result *mcp.CallToolResult
		if len(req.Params.Arguments) == 0 {
			req.Params.Arguments = json.RawMessage("{}")
		}
		if json.Unmarshal(req.Params.Arguments, &args) != nil || args == nil || input.Validate(args) != nil {
			result = errorResult("INVALID_TOOL_ARGUMENTS", false)
		} else {
			var err error
			result, err = run(ctx, req)
			if err != nil || result == nil {
				result = errorResult("TOOL_EXECUTION_FAILED", false)
			}
		}
		// Keep numeric command arguments exact when returning a same-key retry.
		if args != nil {
			decoder := json.NewDecoder(bytes.NewReader(req.Params.Arguments))
			decoder.UseNumber()
			_ = decoder.Decode(&args)
		}
		if name == "ouf.wait" && result.StructuredContent != nil {
			out := boundedResult(result, t.limit, s.manifest.NextPolicies[c.NextPolicy].Read)
			if out != result {
				body := out.StructuredContent.(map[string]any)
				body["changed"], body["observation"] = false, nil
			}
			return out, nil
		}
		return s.decorate(ctx, t, args, result), nil
	})
}

func ownerObject(result *mcp.CallToolResult) nextstep.Object {
	var raw []byte
	if result.StructuredContent != nil {
		raw, _ = json.Marshal(result.StructuredContent)
	} else if len(result.Content) == 1 {
		if t, ok := result.Content[0].(*mcp.TextContent); ok {
			raw = []byte(t.Text)
		}
	}
	if !json.Valid(raw) {
		return nil
	}
	if trimmed := bytes.TrimSpace(raw); trimmed[0] == '[' {
		var items []json.RawMessage
		if json.Unmarshal(raw, &items) == nil {
			return nextstep.Object{"legacyList": true, "items": items}
		}
		return nil
	}
	var obj map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&obj) == nil {
		return obj
	}
	return nil
}

func (s *workflowServer) engine(ctx context.Context) nextstep.Engine {
	return nextstep.Engine{Errors: s.manifest.NextErrors, Valid: s.valid, Now: time.Now, Issue: func(r manifest.Rule, args, obs nextstep.Object) nextstep.Object {
		t, ok := s.tools[r.Tool]
		if !ok || t.capability.OperationClass != "READ" || r.Tool == "ouf.wait" {
			return nextstep.Block("WAIT_READ_REQUIRED", r.Tool, "Controlla la lettura associata alla decisione.")
		}
		if r.Tool == "ouf.ingestion.status" {
			var link nextstep.Object
			if s.runLink != nil {
				version := obs["expectedVersion"]
				if version == nil {
					version = obs["control_version"]
				}
				link = s.runLink(ctx, nextstep.Object{"sourceId": args["sourceId"], "runId": nextstep.Resource(obs), "expectedVersion": version})
			}
			// Only Onboarding can supply this URL. Keep both owner receipts intact.
			obs = maps.Clone(obs)
			delete(obs, "currentChallenge")
			delete(obs, "approvalPath")
			obs["reviewUrl"] = link["reviewUrl"]
			obs["linkExpiresAt"] = link["expiresAt"]
		}
		return s.waits.signer.Issue(waitBinding(ctx), r, args, obs, time.Now())
	}, CommandKey: func(tool string, args, obs, input nextstep.Object) string {
		return s.commandKey(ctx, tool, args, obs, input)
	}, AnalysisID: func(asset, profile string) string {
		identity, _ := ctx.Value(identityKey{}).(requestIdentity)
		scope := analysis.IdentityScope(identity.Identity)
		if !scope.Valid() {
			return ""
		}
		return analysis.AnalysisID(scope, analysis.Profile{AssetID: asset, ProfileID: profile})
	}}
}

func (s *workflowServer) decorate(ctx context.Context, t registeredTool, args map[string]any, result *mcp.CallToolResult) *mcp.CallToolResult {
	c := t.capability
	p := s.manifest.NextPolicies[c.NextPolicy]
	obs := ownerObject(result)
	structured := map[string]any{}
	if obs != nil {
		for k, v := range obs {
			structured[k] = v
		}
	}
	if result.IsError && obs["reasonCode"] == "NO_APPLICABLE_GRANT" {
		structured["requiredCapability"] = c.RequiredAuthorization
		if isAcquisitionRead(c.CapabilityID) {
			identity, _ := ctx.Value(identityKey{}).(requestIdentity)
			raw, _ := json.Marshal(args)
			_, resource, err := acquisitionReadAuthorization(c, raw, identity.TenantID)
			if err == nil {
				structured["requestedConstraints"] = map[string]any{
					"resourceType": resource.ResourceType, "resourceId": resource.ResourceID,
					"resourceAttributes": resource.Attributes,
				}
			}
		}
	}
	if result.IsError && obs["code"] == "INVALID_TOOL_ARGUMENTS" {
		structured["allowedForms"] = t.forms
	}
	structured["next"] = s.engine(ctx).Evaluate(c, p, args, obs, result.IsError)
	if result.IsError && structured["code"] == nil {
		structured["code"] = structured["next"].(nextstep.Object)["reasonCode"]
	}
	if c.ToolName == "ouf.wait" {
		structured["changed"], structured["observation"] = false, nil
	}
	result.StructuredContent = structured
	out := boundedResult(result, t.limit, p.Read)
	if out != result && !result.IsError && c.ToolName == "source.file.preview" {
		// A wave-2 column page can occupy 768 KiB on its own. Keep the
		// complete structured page when duplicating it as text exceeds the
		// wire limit. Older, unpaged owner receipts keep their text form.
		next := structured["next"].(nextstep.Object)
		if args["distinctValuesCursor"] != nil || next["reasonCode"] == "DISTINCT_VALUES_PAGE" || hasPagedValues(obs) {
			page := *result
			page.Content = []mcp.Content{&mcp.TextContent{Text: "La pagina completa dei valori e il passo successivo sono in structuredContent."}}
			return boundedResult(&page, t.limit, p.Read)
		}
	}
	return out
}

func hasPagedValues(obs nextstep.Object) bool {
	columns, _ := obs["columns"].([]any)
	for _, item := range columns {
		column, _ := item.(nextstep.Object)
		if _, ok := column["distinctValuesComplete"].(bool); ok {
			return true
		}
	}
	return false
}

// Measure the final wire result (including escaping and legacy text) before
// release. A command whose owner effect already happened must be reconciled.
func boundedResult(result *mcp.CallToolResult, limit int, read string) *mcp.CallToolResult {
	if body, err := json.Marshal(result); err == nil && len(body) <= limit {
		return result
	}
	small := errorResult("TOOL_RESULT_LIMIT", false)
	small.StructuredContent = map[string]any{"next": nextstep.Block("TOOL_RESULT_LIMIT", read, "Consulta la risorsa con una lettura circoscritta.")}
	return small
}

func waitBinding(ctx context.Context) []byte {
	i, ok := ctx.Value(identityKey{}).(requestIdentity)
	if !ok || i.PrincipalID == "" || i.TenantID == "" || i.Delegation == "" {
		return nil
	}
	b, _ := json.Marshal([]string{nextstep.Contract, i.Issuer, i.Audience, i.ActorType, i.ServicePrincipalID, i.TenantID, i.PrincipalID})
	return b
}
