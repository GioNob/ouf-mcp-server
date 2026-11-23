package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/hostfiles"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
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
		service.AdmissionCallsPerMinute = server.waits.callsPerMinute
		checksum, err := snapshot.Checksum()
		if err != nil {
			return nil, err
		}
		admission, hasWaitAdmission := service.Admission.(waitAdmission)
		if hasWaitAdmission {
			server.waits.claimRead = admission.ClaimWaitRead
		}
		server.waits.charge = func(ctx context.Context, identity orchestration.Identity) error {
			if !hasWaitAdmission {
				return fmt.Errorf("WAIT_ADMISSION_UNAVAILABLE")
			}
			return admission.ChargeWait(ctx, identity, checksum, server.waits.callsPerMinute)
		}
		if err := server.bindTHSLinks(service); err != nil {
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
				index := slices.IndexFunc(snapshot.Capabilities, func(c manifest.Capability) bool {
					return c.CapabilityID == orchestration.HandoffIssueCapability && c.MCPClass == "MCP_INTERNAL" && c.PublicationState == "ACTIVE" && c.OperationClass == "COMMAND" && !c.ToolEligible
				})
				if index < 0 {
					return nil, fmt.Errorf("missing governed handoff emission")
				}
				registerPickerUploadTool(server, snapshot.Capabilities[index], snapshot, service)
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

func registerPickerUploadTool(server *workflowServer, capability manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service) {
	checksum, _ := snapshot.Checksum()
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
		key := uuid.NewString()
		if identity.IdempotencyKey != "" {
			key = server.commandKey(ctx, capability.CapabilityID, nil, nil, map[string]any{"commandKey": identity.IdempotencyKey})
		}
		identity.IdempotencyKey = key
		ctx = context.WithValue(ctx, identityKey{}, identity)
		args, _ := json.Marshal(map[string]string{"commandKey": key})
		result, _, err := invoke(ctx, capability, checksum, args, service, 1, 4096)
		if err == nil && result != nil && !result.IsError {
			owner := ownerObject(result)
			if waitUntil, _ := owner["waitUntil"].(string); waitUntil == "" && owner["replay"] != true {
				return errorResult("HANDOFF_DEADLINE_REQUIRED", false), nil, nil
			}
		}
		return result, nil, err
	})
}

// This result read uses the upload capability's existing governed execution.
// It has no widget template, so calling it cannot launch a second picker.
func registerPickerHandoffStatusTool(server *workflowServer, capability manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service) {
	checksum, _ := snapshot.Checksum()
	var schema jsonschema.Schema
	if err := json.Unmarshal(capability.HandoffInputSchema, &schema); err != nil {
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
				CorrelationID: identity.CorrelationID, Window: orchestration.AdmissionWindow, Timeout: 30 * time.Second,
				RetryThreshold: orchestration.AdmissionRetryThreshold, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 1, ResultBytes: 4096},
				Upload: &orchestration.UploadStream{Reader: staged.File, Size: staged.Size, SHA256: staged.SHA256, FileID: descriptor.FileID}})
			if err != nil {
				return governedCallErrorResult(err), nil, nil
			}
			if result.Replay {
				return replayResult(c, args, result), nil, nil
			}
			if denial := ownerDenialResult(result.Problem, c.RequiredAuthorization); denial != nil {
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

// Measure both the incoming arguments (before schema validation) and the
// serialized arguments sent to the Gateway, which may contain JSON escapes.
func exceedsOnboardingArgumentLimit(c manifest.Capability, size int) bool {
	switch c.CapabilityID {
	case "ouf.managed-source.onboarding.create", "ouf.managed-source.onboarding.review.prepare":
		var limits struct{ MaxArgumentsBytes int }
		return json.Unmarshal(c.OperationalLimits, &limits) != nil || size > limits.MaxArgumentsBytes
	}
	return false
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
	if exceedsOnboardingArgumentLimit(c, len(args)) {
		return errorResult("INVALID_TOOL_ARGUMENTS", false), nil, nil
	}
	argumentLimit := 65536
	if len(args) > argumentLimit && orchestration.IsValueMapProposal(c.CapabilityID, args) {
		argumentLimit = maxValueMapArgumentsBytes
	}
	if (isSemanticProposalCapability(c.CapabilityID) || isSourceRevisionCapability(c.CapabilityID) || c.CapabilityID == "ouf.managed-source.onboarding.read") && len(args) > argumentLimit {
		return errorResult("OWNER_COMMAND_ARGUMENTS_TOO_LARGE", false), nil, nil
	}
	authorizationCapability, resource, err := ingestionReadAuthorization(c, args, identity.TenantID)
	if c.CapabilityID == orchestration.HandoffIssueCapability {
		authorizationCapability = c.RequiredAuthorization
	}
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
	internalRead, _ := ctx.Value(internalWaitReadKey{}).(bool)
	in := orchestration.Invocation{InternalWaitRead: internalRead, AuthorizationCapabilityID: authorizationCapability, Resource: resource, Identity: identity.Identity, CapabilityID: c.CapabilityID, Owner: c.Owner, OperationClass: c.OperationClass, GatewayBindingRef: c.GatewayBindingRef, ManifestChecksum: checksum, Arguments: args, IdempotencyKey: identity.IdempotencyKey, CorrelationID: identity.CorrelationID, Window: orchestration.AdmissionWindow, Timeout: timeout, RetryThreshold: orchestration.AdmissionRetryThreshold, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: int64(maxObjects), ResultBytes: int64(maxBytes)}}
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
	if denial := ownerDenialResult(result.Problem, c.RequiredAuthorization); denial != nil {
		return denial, nil, nil
	}
	if result.Problem != nil {
		if c.CapabilityID == orchestration.HandoffIssueCapability && (result.Problem.Status == http.StatusNotFound || result.Problem.Status == http.StatusMethodNotAllowed) {
			return errorResult("HANDOFF_DEADLINE_REQUIRED", false), nil, nil
		}
		if c.CapabilityID == "ouf.onboarding.revision.prepare" && result.Problem.Status == http.StatusUnprocessableEntity && result.Problem.Code == "ONB_REVISION_CSV_INCOMPATIBLE" {
			return revisionCSVError(result.Body), nil, nil
		}
		problem := result.Problem
		if projected := onboardingProblem(c.ToolName, problem, result.Body); projected != nil {
			return projected, nil, nil
		}
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

var onboardingReaderCode = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)
var onboardingSourceID = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,160}$`)

type readerDiagnostic struct {
	FieldName         string   `json:"fieldName"`
	Transform         string   `json:"transform"`
	AllowedTransforms []string `json:"allowedTransforms"`
}

// Each Onboarding refusal has its own public diagnostic. Never include the
// owner's free-form problem detail, even on an internal review failure.
func onboardingProblem(tool string, p *orchestration.Problem, body []byte) *mcp.CallToolResult {
	out := map[string]any{"code": p.Code, "Status": p.Status}
	var fields struct {
		MissingSections []any  `json:"missingSections"`
		Index           *int   `json:"index"`
		SemanticRef     string `json:"semanticRef"`
	}
	switch {
	case tool == "source.onboarding.review.prepare" && p.Status == 422 && p.Code == "ONB_EXECUTION_PROPOSAL_REQUIRED":
		if json.Unmarshal(body, &fields) != nil {
			return errorResult("OWNER_PROBLEM_INVALID", false)
		}
		out["missingSections"] = nextstep.ProposalSections(fields.MissingSections)
	case tool == "source.onboarding.create" && p.Status == 422 && p.Code == "ONB_SEMANTIC_REF_FORM_INVALID":
		if json.Unmarshal(body, &fields) != nil || fields.Index == nil || *fields.Index < 0 {
			return errorResult("OWNER_PROBLEM_INVALID", false)
		}
		out["index"] = *fields.Index
	case tool == "source.onboarding.review.prepare" && p.Status == 422 && p.Code == "ONB_SEMANTIC_REF_VERSION_MISMATCH":
		if json.Unmarshal(body, &fields) != nil || fields.SemanticRef == "" {
			return errorResult("OWNER_PROBLEM_INVALID", false)
		}
		out["semanticRef"] = fields.SemanticRef
	case tool == "source.onboarding.create" && p.Status == 422 && p.Code == "ONB_READER_UNSUPPORTED",
		tool == "source.onboarding.review.prepare" && p.Status == 422 && p.Code == "ONB_DATATYPE_UNRESOLVED",
		tool == "source.onboarding.create" && p.Status == 409 && p.Code == "ONB_REVISION_ASSET_SOURCE_CONFLICT":
		var problem struct {
			Fields        []readerDiagnostic `json:"fields"`
			FieldName     string             `json:"fieldName"`
			Resolutions   []string           `json:"resolutions"`
			BoundSourceID string             `json:"boundSourceId"`
		}
		if json.Unmarshal(body, &problem) != nil {
			return errorResult("OWNER_PROBLEM_INVALID", false)
		}
		if p.Status == 422 && problem.Fields != nil {
			for _, f := range problem.Fields {
				if f.FieldName == "" || utf8.RuneCountInString(f.FieldName) > 256 || !onboardingReaderCode.MatchString(f.Transform) || f.AllowedTransforms == nil ||
					slices.ContainsFunc(f.AllowedTransforms, func(reader string) bool { return !onboardingReaderCode.MatchString(reader) }) {
					return errorResult("OWNER_PROBLEM_INVALID", false)
				}
			}
			out["fields"] = problem.Fields
		}
		if p.Code == "ONB_READER_UNSUPPORTED" {
			if problem.Fields == nil {
				if problem.FieldName == "" {
					return errorResult("OWNER_PROBLEM_INVALID", false)
				}
				out["fieldName"] = problem.FieldName // Pre-52d owner.
			}
			break
		}
		actions := slices.DeleteFunc(problem.Resolutions, func(action string) bool {
			return action != "CREATE_REVISION" && action != "UPLOAD_NEW_FILE" && !(p.Code == "ONB_DATATYPE_UNRESOLVED" && action == "REPLACE_DRAFT")
		})
		if len(actions) > 0 {
			out["resolutions"] = actions
		}
		if slices.Contains(actions, "CREATE_REVISION") && onboardingSourceID.MatchString(problem.BoundSourceID) {
			out["boundSourceId"] = problem.BoundSourceID
		}
	case tool == "source.onboarding.create" && p.Status == 412 && p.Code == "ONB_REVIEW_STALE_VERSION":
	case tool == "source.onboarding.review.prepare" && p.Status == 500 && p.Code == "ONB_REVIEW_INTERNAL_ERROR":
	default:
		return nil
	}
	return workflowError(out)
}

func ownerProblemResult(problem *orchestration.Problem, body []byte) *mcp.CallToolResult {
	// Only typed timing fields may accompany an owner problem on the retry rail.
	type timing struct {
		Retryable         *bool      `json:"retryable,omitempty"`
		PollAfterSeconds  *int       `json:"pollAfterSeconds,omitempty"`
		RetryAfterSeconds *int       `json:"retryAfterSeconds,omitempty"`
		WaitUntil         *time.Time `json:"waitUntil,omitempty"`
		Deadline          *time.Time `json:"deadline,omitempty"`
		ExpiresAt         *time.Time `json:"expiresAt,omitempty"`
		CreatedAt         *time.Time `json:"createdAt,omitempty"`
		StartedAt         *time.Time `json:"startedAt,omitempty"`
		BudgetDeadline    *time.Time `json:"budgetDeadline,omitempty"`
	}
	var flags timing
	if json.Valid(body) && json.Unmarshal(body, &flags) != nil {
		return errorResult("OWNER_PROBLEM_INVALID", false)
	}
	projected := struct {
		orchestration.Problem
		timing
	}{*problem, flags}
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
