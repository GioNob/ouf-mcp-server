package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/hostfiles"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/GioNob/ouf-mcp-server/internal/trustedclaims"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	ProtocolVersion = "2026-07-28"
	maxRequestBytes = 4 << 20
)

type unavailableResult struct {
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
}

type objectSearchInput struct {
	Type     string `json:"type"`
	PageSize *int   `json:"pageSize,omitempty"`
	Cursor   string `json:"cursor,omitempty"`
}

type relatedSearchInput struct {
	AnchorObjectID        string   `json:"anchorObjectId"`
	AnchorTypeCode        string   `json:"anchorTypeCode"`
	RelationIRI           string   `json:"relationIri"`
	Direction             string   `json:"direction"`
	TargetTypeCodes       []string `json:"targetTypeCodes"`
	Limit                 int      `json:"limit"`
	ApplicationSessionRef string   `json:"applicationSessionRef,omitempty"`
}

type operationalInput struct {
	Coverage   bool   `json:"coverage,omitempty"`
	ResumeFrom string `json:"resumeFrom,omitempty"`
	Snapshot   bool   `json:"snapshot,omitempty"`
	Cursor     string `json:"cursor,omitempty"`
	IncidentID string `json:"incidentId,omitempty"`
	SourceID   string `json:"sourceId,omitempty"`
	State      string `json:"state,omitempty"`
	Since      string `json:"since,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

func NewHTTPHandler(logger *slog.Logger) (http.Handler, error) {
	return newHTTPHandler(logger, nil, nil, false)
}
func NewGovernedHTTPHandler(logger *slog.Logger, service *orchestration.Service) (http.Handler, error) {
	if service == nil {
		return nil, fmt.Errorf("governed service is required")
	}
	return newHTTPHandler(logger, service, nil, false)
}
func NewGovernedHTTPHandlerWithHostFiles(logger *slog.Logger, service *orchestration.Service, fetcher *hostfiles.Fetcher) (http.Handler, error) {
	if service == nil || fetcher == nil {
		return nil, fmt.Errorf("governed service and host attachment origin are required")
	}
	return newHTTPHandler(logger, service, fetcher, false)
}

// The origin probe advertises the file parameter and reports only its host
// origin. Its widget may read bytes inside ChatGPT; the MCP server never
// fetches them and the probe never invokes the Gateway upload.
func NewGovernedHTTPHandlerWithHostOriginProbe(logger *slog.Logger, service *orchestration.Service) (http.Handler, error) {
	if service == nil {
		return nil, fmt.Errorf("governed service is required")
	}
	return newHTTPHandler(logger, service, nil, true)
}

// Picker mode retains the one source.file.upload tool. The browser chooses a
// local file on the first-party OUF site; MCP never receives its bytes or URL.
func NewGovernedHTTPHandlerWithFilePicker(logger *slog.Logger, service *orchestration.Service, pickerURL string) (http.Handler, error) {
	if service == nil {
		return nil, fmt.Errorf("governed service is required")
	}
	u, err := url.Parse(pickerURL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/trusted-human/managed-files/" {
		return nil, fmt.Errorf("invalid first-party managed-file picker URL")
	}
	return newHTTPHandlerWithPicker(logger, service, pickerURL)
}

func newHTTPHandlerWithPicker(logger *slog.Logger, service *orchestration.Service, pickerURL string) (http.Handler, error) {
	return newHTTPHandlerMode(logger, service, nil, false, pickerURL)
}

func newHTTPHandler(logger *slog.Logger, service *orchestration.Service, fetcher *hostfiles.Fetcher, originProbe bool) (http.Handler, error) {
	return newHTTPHandlerMode(logger, service, fetcher, originProbe, "")
}

func newHTTPHandlerMode(logger *slog.Logger, service *orchestration.Service, fetcher *hostfiles.Fetcher, originProbe bool, pickerURL string) (http.Handler, error) {
	snapshot, err := manifest.Load()
	if err != nil {
		return nil, fmt.Errorf("load capability manifest: %w", err)
	}
	sdk := mcp.NewServer(&mcp.Implementation{Name: "ouf-mcp-server", Version: "0.1.0"}, &mcp.ServerOptions{Capabilities: &mcp.ServerCapabilities{}, Instructions: "Governed OUF capabilities only. No SQL or arbitrary network access. Operational awareness exposes bounded semantic state, never raw logs. " + manifest.FollowNext, Logger: logger})
	var installationKey []byte
	if service != nil {
		installationKey = service.FingerprintKey
	}
	server, err := newWorkflowServer(sdk, snapshot, installationKey)
	if err != nil {
		return nil, err
	}
	if service != nil {
		if err := server.bindRecoveryLink(service); err != nil {
			return nil, err
		}
	}
	for _, capability := range snapshot.ToolEligible() {
		if capability.ToolName == "ouf.wait" {
			server.registerWait(capability)
			continue
		}
		if capability.ToolName == "source.file.upload" {
			if service != nil && pickerURL != "" {
				registerPickerUploadWidget(sdk, pickerURL)
				registerPickerUploadTool(server, capability, snapshot, service, pickerURL)
				registerPickerHandoffStatusTool(server, capability, snapshot, service)
			} else if service != nil && originProbe {
				registerHostOriginProbe(server, capability.InputSchema)
			} else if service != nil && fetcher != nil {
				registerUploadTool(server, capability, snapshot, service, fetcher)
			}
			continue
		}
		if isValueTypeTool(capability.ToolName) {
			registerValueTypeTool(server, capability, snapshot, service)
		} else if capability.ToolName == "source.onboarding.revision.read" {
			registerSourceRevisionRead(server, capability, snapshot, service)
		} else if service == nil {
			registerUnavailableTool(server, capability)
		} else {
			registerGovernedTool(server, capability, snapshot, service)
		}
	}
	streamable := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return sdk }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true, MaxRequestBodyBytes: maxRequestBytes, PropagateRequestCancellation: true})
	return modernOnly(streamable), nil
}

func registerPickerUploadTool(server *workflowServer, capability manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service, pickerURL string) {
	var schema jsonschema.Schema
	if err := json.Unmarshal([]byte(`{"type":"object","additionalProperties":false,"properties":{}}`), &schema); err != nil {
		panic(err)
	}
	addWorkflowTool(server, &mcp.Tool{
		Name:        "source.file.upload",
		Description: "Open the governed OUF file picker for an authenticated human. The user chooses a local file; OUF checks its format and reports unsupported formats. The handoffId identifies the status read while the user chooses a file.",
		InputSchema: &schema,
		Meta:        mcp.Meta{"ui": map[string]any{"resourceUri": pickerHandoffURI, "visibility": []string{"model", "app"}}, "openai/widgetAccessible": true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, any, error) {
		identity, ok := ctx.Value(identityKey{}).(requestIdentity)
		if !ok || identity.Delegation == "" || identity.ActorType != "HUMAN" {
			return actorOrDelegationDenial(identity, ok), nil, nil
		}
		id := uuid.NewString()
		url := pickerURL + "?handoff=" + id
		result := map[string]any{"status": "AWAITING_FILE_SELECTION", "pickerUrl": url, "handoffId": id}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "Apri " + url + " e scegli un file. OUF verificherà il formato; al momento il caricamento gestisce CSV. La chat attenderà l’esito."}}, StructuredContent: result}, nil, nil
	})
}

// This result read uses the upload capability's existing governed execution.
// It has no widget template, so calling it cannot launch a second picker.
func registerPickerHandoffStatusTool(server *workflowServer, capability manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service) {
	var schema jsonschema.Schema
	if err := json.Unmarshal([]byte(manifest.PickerStatusInputSchema), &schema); err != nil {
		panic(err)
	}
	addWorkflowTool(server, &mcp.Tool{
		Name:        "source.file.upload.status",
		Description: "Read the result of a first-party OUF file selection using the handoffId returned by source.file.upload. Call after the user completes the picker; PENDING means the file has not arrived yet. The owner is checked by OUF.",
		InputSchema: &schema,
		Meta:        mcp.Meta{"ui": map[string]any{"visibility": []string{"model", "app"}}, "openai/widgetAccessible": true},
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, any, error) {
		identity, ok := ctx.Value(identityKey{}).(requestIdentity)
		if !ok || identity.Delegation == "" || identity.ActorType != "HUMAN" {
			return actorOrDelegationDenial(identity, ok), nil, nil
		}
		value, ok := input["handoffId"].(string)
		if !ok || uuid.Validate(value) != nil {
			return errorResult("HANDOFF_ID_INVALID", false), nil, nil
		}
		args, _ := json.Marshal(map[string]string{"handoffId": value})
		checksum, _ := snapshot.Checksum()
		return invoke(ctx, capability, checksum, args, service, 1, 4096)
	})
}

func registerHostOriginProbe(server *workflowServer, inputSchema json.RawMessage) {
	var schema jsonschema.Schema
	if err := json.Unmarshal(inputSchema, &schema); err != nil {
		panic(err)
	}
	registerAttachmentProbeWidget(server.Server)
	addWorkflowTool(server, &mcp.Tool{
		Name:        "source.file.attachment_origin_probe",
		Description: "Inspect the origin and file ID of a ChatGPT-hosted CSV attachment. Its read-only widget checks whether the browser can read the bytes, without sending them to OUF. Never creates an asset or returns the private URL.",
		InputSchema: &schema,
		Meta:        mcp.Meta{"openai/fileParams": []string{"file"}, "ui": map[string]any{"resourceUri": attachmentProbeURI}},
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, any, error) {
		identity, ok := ctx.Value(identityKey{}).(requestIdentity)
		if !ok || identity.Delegation == "" || identity.ActorType != "HUMAN" {
			return errorResult("UNAUTHENTICATED", false), nil, nil
		}
		raw, _ := json.Marshal(input["file"])
		var descriptor hostfiles.Input
		if err := json.Unmarshal(raw, &descriptor); err != nil {
			return errorResult("ATTACHMENT_INVALID", false), nil, nil
		}
		origin, err := hostfiles.DescriptorOrigin(descriptor)
		if err != nil {
			return errorResult("ATTACHMENT_INVALID", false), nil, nil
		}
		body, _ := json.Marshal(map[string]string{"origin": origin, "fileId": descriptor.FileID})
		return &mcp.CallToolResult{
			Content:           []mcp.Content{&mcp.TextContent{Text: string(body)}},
			StructuredContent: map[string]any{"origin": origin, "fileId": descriptor.FileID},
		}, nil, nil
	})
}

func registerUploadTool(server *workflowServer, c manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service, fetcher *hostfiles.Fetcher) {
	var schema jsonschema.Schema
	if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
		panic(err)
	}
	checksum, _ := snapshot.Checksum()
	addWorkflowTool(server, &mcp.Tool{Name: c.ToolName, Description: c.Description(), InputSchema: &schema,
		Meta: mcp.Meta{"openai/fileParams": []string{"file"}}},
		func(ctx context.Context, _ *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, any, error) {
			identity, ok := ctx.Value(identityKey{}).(requestIdentity)
			if !ok || identity.Delegation == "" || identity.ActorType != "HUMAN" {
				return actorOrDelegationDenial(identity, ok), nil, nil
			}
			raw, _ := json.Marshal(input["file"])
			var descriptor hostfiles.Input
			if err := json.Unmarshal(raw, &descriptor); err != nil {
				return errorResult("ATTACHMENT_INVALID", false), nil, nil
			}
			staged, err := fetcher.Fetch(ctx, descriptor)
			if err != nil {
				return errorResult(hostfiles.FailureCode(err), false), nil, nil
			}
			defer staged.CloseAndRemove()
			args, _ := json.Marshal(map[string]any{"fileId": descriptor.FileID, "contentHash": staged.SHA256, "sizeBytes": staged.Size})
			if identity.CorrelationID == "" {
				identity.CorrelationID = uuid.NewString()
			}
			identity.IdempotencyKey = "upload:" + descriptor.FileID
			result, err := service.Call(ctx, orchestration.Invocation{Identity: identity.Identity, CapabilityID: c.CapabilityID,
				Owner: c.Owner, OperationClass: c.OperationClass, GatewayBindingRef: c.GatewayBindingRef,
				ManifestChecksum: checksum, Arguments: args, IdempotencyKey: identity.IdempotencyKey,
				CorrelationID: identity.CorrelationID, Window: time.Minute, Timeout: 30 * time.Second,
				RetryThreshold: 3, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 1, ResultBytes: 4096},
				Upload: &orchestration.UploadStream{Reader: staged.File, Size: staged.Size, SHA256: staged.SHA256, FileID: descriptor.FileID}})
			if err != nil {
				return governedCallErrorResult(err), nil, nil
			}
			if result.Replay {
				return replayResult(c, args, result), nil, nil
			}
			if denial := ownerDenialResult(result.Problem); denial != nil {
				return denial, nil, nil
			}
			if result.Problem != nil {
				return ownerProblemResult(result.Problem, result.Body), nil, nil
			}
			safe, ok := safeUploadAsset(result.Body)
			if !ok {
				return errorResult("UPLOAD_OWNER_RESPONSE_INVALID", false), nil, nil
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(safe)}}}, nil, nil
		})
}

func safeUploadAsset(body []byte) ([]byte, bool) {
	var response struct {
		AssetID string `json:"assetId"`
		Status  string `json:"status"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, false
	}
	if _, err := uuid.Parse(response.AssetID); err != nil {
		return nil, false
	}
	if response.Status != "STAGED" && response.Status != "PROFILED" && response.Status != "QUARANTINED" {
		return nil, false
	}
	filtered, _ := json.Marshal(map[string]string{"assetId": response.AssetID, "status": response.Status})
	return filtered, true
}

type responseDeadlineKey struct{}
type identityKey struct{}
type requestIdentity struct {
	orchestration.Identity
	CorrelationID, IdempotencyKey string
}

func registerGovernedTool(server *workflowServer, c manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service) {
	if strings.HasPrefix(c.ToolName, "source.analysis.") {
		registerAnalysisTool(server, c, snapshot, service)
		return
	}
	if isAcquisitionRead(c.CapabilityID) {
		registerAcquisitionReadTool(server, c, snapshot, service)
		return
	}
	if strings.HasPrefix(c.ToolName, "semantic.") || strings.HasPrefix(c.ToolName, "authorization.") || strings.HasPrefix(c.ToolName, "source.file.") || c.ToolName == "source.onboarding.review.prepare" || c.ToolName == "source.onboarding.read" {
		var inputSchema jsonschema.Schema
		if err := json.Unmarshal(c.InputSchema, &inputSchema); err != nil {
			panic(err)
		}
		checksum, _ := snapshot.Checksum()
		addWorkflowTool(server, &mcp.Tool{Name: c.ToolName, Description: c.Description(), InputSchema: &inputSchema, Annotations: &mcp.ToolAnnotations{ReadOnlyHint: c.OperationClass == "READ"}}, func(ctx context.Context, _ *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, any, error) {
			if inputSchema.Properties["analysisId"] != nil {
				delete(input, "analysisId")
			}
			if key, ok := input["commandKey"].(string); ok && c.ToolName == "source.file.profile" {
				if identity, present := ctx.Value(identityKey{}).(requestIdentity); present {
					identity.IdempotencyKey = key
					ctx = context.WithValue(ctx, identityKey{}, identity)
				}
				delete(input, "commandKey")
			}
			args, _ := json.Marshal(input)
			maxObjects, maxBytes := 200, 1<<20
			if strings.HasPrefix(c.ToolName, "semantic.") {
				maxObjects, maxBytes = 1, 262144
				if c.ToolName == "semantic.search" || (strings.HasPrefix(c.ToolName, "semantic.discovery.") && c.ToolName != "semantic.discovery.adopt") {
					maxObjects = 100
				}
			}
			if c.ToolName == "source.onboarding.review.prepare" || c.ToolName == "source.onboarding.read" {
				maxObjects, maxBytes = 1, 65536
			}
			return invoke(ctx, c, checksum, args, service, maxObjects, maxBytes)
		})
		return
	}
	if strings.HasPrefix(c.ToolName, "ouf.") {
		registerOperationalTool(server, c, snapshot, service)
		return
	}
	if c.CapabilityID == "urban.object.search" {
		registerObjectSearchTool(server, c, snapshot, service)
		return
	}
	var inputSchema jsonschema.Schema
	if err := json.Unmarshal(c.InputSchema, &inputSchema); err != nil {
		panic(err)
	}
	checksum, _ := snapshot.Checksum()
	addWorkflowTool(server, &mcp.Tool{Name: c.ToolName, Description: c.Description(), InputSchema: &inputSchema}, func(ctx context.Context, _ *mcp.CallToolRequest, input relatedSearchInput) (*mcp.CallToolResult, any, error) {
		args, _ := json.Marshal(input)
		return invoke(ctx, c, checksum, args, service, 200, 1<<20)
	})
}

func registerObjectSearchTool(server *workflowServer, c manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service) {
	var inputSchema jsonschema.Schema
	if err := json.Unmarshal(c.InputSchema, &inputSchema); err != nil {
		panic(err)
	}
	checksum, _ := snapshot.Checksum()
	addWorkflowTool(server, &mcp.Tool{Name: c.ToolName, Description: c.Description(), InputSchema: &inputSchema, Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true}}, func(ctx context.Context, _ *mcp.CallToolRequest, input objectSearchInput) (*mcp.CallToolResult, any, error) {
		args, _ := json.Marshal(input)
		return invoke(ctx, c, checksum, args, service, 100, 262144)
	})
}

func registerOperationalTool(server *workflowServer, c manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service) {
	var inputSchema jsonschema.Schema
	if err := json.Unmarshal(c.InputSchema, &inputSchema); err != nil {
		panic(err)
	}
	checksum, _ := snapshot.Checksum()
	addWorkflowTool(server, &mcp.Tool{Name: c.ToolName, Description: c.Description(), InputSchema: &inputSchema}, func(ctx context.Context, _ *mcp.CallToolRequest, input operationalInput) (*mcp.CallToolResult, any, error) {
		args, _ := json.Marshal(input)
		return invoke(ctx, c, checksum, args, service, 200, 512<<10)
	})
}

func invoke(ctx context.Context, c manifest.Capability, checksum string, args []byte, service *orchestration.Service, maxObjects, maxBytes int) (*mcp.CallToolResult, any, error) {
	identity, ok := ctx.Value(identityKey{}).(requestIdentity)
	if !ok {
		return errorResult("UNAUTHENTICATED", false), nil, nil
	}
	if len(c.AllowedActors) > 0 && !slices.Contains(c.AllowedActors, identity.ActorType) {
		return governedCallErrorResult(&orchestration.AuthorizationDeniedError{ReasonCode: "ACTOR_NOT_ALLOWED"}), nil, nil
	}
	if c.RequiredScope != "" && !slices.Contains(identity.Scopes, c.RequiredScope) {
		return governedCallErrorResult(&orchestration.AuthorizationDeniedError{ReasonCode: "SCOPE_MISSING"}), nil, nil
	}
	if identity.CorrelationID == "" {
		identity.CorrelationID = uuid.NewString()
	}
	if identity.IdempotencyKey == "" {
		identity.IdempotencyKey = uuid.NewString()
	}
	if orchestration.IsOperationalControlRead(c.CapabilityID, c.OperationClass) {
		maxObjects = 100
	}
	argumentLimit := 65536
	if c.CapabilityID == "ouf.managed-source.onboarding.create" || c.CapabilityID == "ouf.managed-source.onboarding.review.prepare" {
		argumentLimit = 2 << 20
	}
	if len(args) > argumentLimit && orchestration.IsValueMapProposal(c.CapabilityID, args) {
		argumentLimit = maxValueMapArgumentsBytes
	}
	if (isSemanticProposalCapability(c.CapabilityID) || isSourceReviewCapability(c.CapabilityID) || c.CapabilityID == "ouf.managed-source.onboarding.create") && len(args) > argumentLimit {
		return errorResult("OWNER_COMMAND_ARGUMENTS_TOO_LARGE", false), nil, nil
	}
	authorizationCapability, resource, err := ingestionReadAuthorization(c, args, identity.TenantID)
	if c.CapabilityID == "ouf.managed-source.preview" {
		authorizationCapability, resource, err = managedProfileJobAuthorization(c, args, identity.TenantID)
	}
	if isAcquisitionRead(c.CapabilityID) {
		authorizationCapability, resource, err = acquisitionReadAuthorization(c, args, identity.TenantID)
	}
	if isSemanticProposalCapability(c.CapabilityID) {
		authorizationCapability, resource, err = semanticProposalAuthorization(c, args, identity.TenantID)
		if c.OperationClass == "COMMAND" {
			var command struct {
				CommandKey string `json:"commandKey"`
			}
			if parseErr := json.Unmarshal(args, &command); parseErr != nil || command.CommandKey == "" {
				return errorResult("SEMANTIC_COMMAND_KEY_REQUIRED", false), nil, nil
			}
			identity.IdempotencyKey = command.CommandKey
		}
	}
	if isSourceReviewCapability(c.CapabilityID) {
		authorizationCapability, resource, err = sourceReviewAuthorization(c, args, identity.TenantID)
	}
	if isSourcePreparationCapability(c.CapabilityID) {
		var command struct {
			CommandKey string `json:"commandKey"`
		}
		if parseErr := json.Unmarshal(args, &command); parseErr != nil || command.CommandKey == "" {
			return errorResult("SOURCE_REVIEW_COMMAND_KEY_REQUIRED", false), nil, nil
		}
		identity.IdempotencyKey = command.CommandKey
	}
	if err != nil {
		code := "INVALID_INGESTION_READ_CONTEXT"
		if c.CapabilityID == "ouf.managed-source.preview" {
			code = "INVALID_MANAGED_PROFILE_JOB_CONTEXT"
		}
		if isAcquisitionRead(c.CapabilityID) {
			code = "INVALID_ACQUISITION_READ_CONTEXT"
		}
		if isSemanticProposalCapability(c.CapabilityID) {
			code = "INVALID_SEMANTIC_PROPOSAL_CONTEXT"
		}
		if isSourceReviewCapability(c.CapabilityID) {
			code = "INVALID_SOURCE_REVIEW_CONTEXT"
		}
		return errorResult(code, false), nil, nil
	}
	timeout := 3 * time.Second
	if isAcquisitionRead(c.CapabilityID) {
		timeout = 12 * time.Second
	}
	if isSourcePreparationCapability(c.CapabilityID) || c.CapabilityID == "ouf.managed-source.preview" || c.CapabilityID == "ouf.managed-source.onboarding.create" {
		timeout = 10 * time.Second
	}
	in := orchestration.Invocation{AuthorizationCapabilityID: authorizationCapability, Resource: resource, Identity: identity.Identity, CapabilityID: c.CapabilityID, Owner: c.Owner, OperationClass: c.OperationClass, GatewayBindingRef: c.GatewayBindingRef, ManifestChecksum: checksum, Arguments: args, IdempotencyKey: identity.IdempotencyKey, CorrelationID: identity.CorrelationID, Window: time.Minute, Timeout: timeout, RetryThreshold: 3, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: int64(maxObjects), ResultBytes: int64(maxBytes)}}
	if c.ToolName == "semantic.discovery.adopt" {
		return callDiscoveryAdoption(ctx, service, in), nil, nil
	}
	result, err := service.Call(ctx, in)
	if err != nil {
		return governedCallErrorResult(err), nil, nil
	}
	if result.Replay {
		return replayResult(c, args, result), nil, nil
	}
	if denial := ownerDenialResult(result.Problem); denial != nil {
		return denial, nil, nil
	}
	if result.Problem != nil {
		if c.CapabilityID == "ouf.onboarding.revision.prepare" && result.Problem.Status == http.StatusUnprocessableEntity && result.Problem.Code == "ONB_REVISION_CSV_INCOMPATIBLE" {
			return revisionCSVError(result.Body), nil, nil
		}
		problem := result.Problem
		if isValueTypeTool(c.ToolName) && (problem.Status == http.StatusBadRequest || problem.Status == http.StatusUnprocessableEntity) {
			// Owner validation details can contain source data. Preserve the
			// stable problem fields and the existing error envelope.
			problem = &orchestration.Problem{Status: problem.Status, Code: problem.Code, RetryAfter: problem.RetryAfter}
			if problem.Code == "" {
				problem.Code = "OWNER_ARGUMENTS_REJECTED"
			}
		}
		return ownerProblemResult(problem, result.Body), nil, nil
	}
	content := &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(result.Body)}}}
	if isSourceRevisionCapability(c.CapabilityID) {
		return content, json.RawMessage(result.Body), nil
	}
	return content, nil, nil
}

func ownerProblemResult(problem *orchestration.Problem, body []byte) *mcp.CallToolResult {
	var flags struct {
		Retryable *bool `json:"retryable"`
	}
	_ = json.Unmarshal(body, &flags)
	projected := struct {
		orchestration.Problem
		Retryable *bool `json:"retryable,omitempty"`
	}{*problem, flags.Retryable}
	if projected.Code == "" {
		projected.Code = "OWNER_REQUEST_FAILED"
	}
	raw, _ := json.Marshal(projected)
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(raw)}}, IsError: true}
}

func errorResult(code string, retryable bool) *mcp.CallToolResult {
	body, _ := json.Marshal(unavailableResult{Code: code, Retryable: retryable})
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(body)}}, IsError: true}
}

// Project only schema differences from the owner, in the reported order.
func revisionCSVError(body []byte) *mcp.CallToolResult {
	var problem struct {
		Code        string `json:"code"`
		Diagnostics []struct {
			Code     string          `json:"code"`
			Column   *string         `json:"column"`
			Expected json.RawMessage `json:"expected"`
			Actual   json.RawMessage `json:"actual"`
		} `json:"diagnostics"`
	}
	if json.Unmarshal(body, &problem) != nil || len(problem.Diagnostics) == 0 {
		return errorResult("SOURCE_REVISION_DIAGNOSTICS_INVALID", false)
	}
	for _, d := range problem.Diagnostics {
		if d.Code == "" {
			return errorResult("SOURCE_REVISION_DIAGNOSTICS_INVALID", false)
		}
		for _, value := range []json.RawMessage{d.Expected, d.Actual} {
			if len(value) > 0 && (value[0] == '{' || value[0] == '[') {
				return errorResult("SOURCE_REVISION_DIAGNOSTICS_INVALID", false)
			}
		}
	}
	filtered, _ := json.Marshal(problem)
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(filtered)}}, IsError: true}
}

func registerUnavailableTool(server *workflowServer, c manifest.Capability) {
	var inputSchema jsonschema.Schema
	if err := json.Unmarshal(c.InputSchema, &inputSchema); err != nil {
		panic(fmt.Sprintf("validated schema for %s cannot be decoded: %v", c.ToolName, err))
	}
	addWorkflowTool(server, &mcp.Tool{Name: c.ToolName, Description: c.Description(), InputSchema: &inputSchema}, func(context.Context, *mcp.CallToolRequest, map[string]any) (*mcp.CallToolResult, any, error) {
		body, _ := json.Marshal(unavailableResult{Code: "MCP_1A_BACKEND_NOT_BOUND", Retryable: false})
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(body)}}, IsError: true}, nil, nil
	})
}

func modernOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.Method != http.MethodPost {
			http.Error(w, "MCP modern profile accepts POST only", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Mcp-Session-Id") != "" {
			http.Error(w, "transport sessions are disabled", http.StatusBadRequest)
			return
		}
		if r.Header.Get("Mcp-Protocol-Version") != ProtocolVersion {
			http.Error(w, "unsupported MCP protocol version", http.StatusBadRequest)
			return
		}
		if strings.TrimSpace(r.Header.Get("Mcp-Method")) == "" {
			http.Error(w, "missing Mcp-Method", http.StatusBadRequest)
			return
		}
		if r.Header.Get("X-OUF-Gateway-Verified") != "true" {
			http.Error(w, "trusted Gateway context required", http.StatusUnauthorized)
			return
		}
		delegation := r.Header.Get("X-OUF-Delegation")
		if len(delegation) > 16384 || strings.ContainsAny(delegation, "\r\n") {
			http.Error(w, "invalid delegation context", http.StatusBadRequest)
			return
		}
		identity := requestIdentity{
			Identity: orchestration.Identity{
				Delegation:               delegation,
				ServicePrincipalID:       r.Header.Get("X-OUF-Service-Principal"),
				PrincipalID:              r.Header.Get("X-OUF-Principal-ID"),
				TenantID:                 r.Header.Get("X-OUF-Tenant-ID"),
				ActorType:                r.Header.Get("X-OUF-Actor-Type"),
				AuthenticationContextRef: r.Header.Get("X-OUF-Authentication-Context-Ref"),
				Issuer:                   r.Header.Get("X-OUF-Token-Issuer"),
				Audience:                 r.Header.Get("X-OUF-Token-Audience"),
				Scopes:                   strings.Fields(r.Header.Get("X-OUF-Granted-Scopes")),
			},
			CorrelationID:  r.Header.Get("X-Correlation-ID"),
			IdempotencyKey: r.Header.Get("Idempotency-Key"),
		}
		claims, err := trustedclaims.Read(r)
		if err != nil {
			http.Error(w, "invalid trusted claims", http.StatusBadRequest)
			return
		}
		identity.Identity.Claims = claims
		ctx := context.WithValue(r.Context(), identityKey{}, identity)
		ctx = context.WithValue(ctx, responseDeadlineKey{}, http.NewResponseController(w).SetWriteDeadline)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
