package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	ProtocolVersion = "2026-07-28"
	maxRequestBytes = 1 << 20
)

type unavailableResult struct {
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
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

func NewHTTPHandler(logger *slog.Logger) (http.Handler, error) {
	return newHTTPHandler(logger, nil)
}

func NewGovernedHTTPHandler(logger *slog.Logger, service *orchestration.Service) (http.Handler, error) {
	if service == nil {
		return nil, fmt.Errorf("governed service is required")
	}
	return newHTTPHandler(logger, service)
}

func newHTTPHandler(logger *slog.Logger, service *orchestration.Service) (http.Handler, error) {
	snapshot, err := manifest.Load()
	if err != nil {
		return nil, fmt.Errorf("load capability manifest: %w", err)
	}

	server := mcp.NewServer(&mcp.Implementation{Name: "ouf-mcp-server", Version: "0.1.0"}, &mcp.ServerOptions{
		Capabilities: &mcp.ServerCapabilities{},
		Instructions: "Governed OUF capabilities only. No SQL or arbitrary network access.",
		Logger:       logger,
	})
	for _, capability := range snapshot.ToolEligible() {
		if service == nil {
			registerUnavailableTool(server, capability)
		} else {
			registerGovernedTool(server, capability, snapshot, service)
		}
	}

	streamable := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{
		Stateless:                    true,
		JSONResponse:                 true,
		MaxRequestBodyBytes:          maxRequestBytes,
		PropagateRequestCancellation: true,
	})
	return modernOnly(streamable), nil
}

type identityKey struct{}
type requestIdentity struct {
	orchestration.Identity
	CorrelationID, IdempotencyKey string
}

func registerGovernedTool(server *mcp.Server, c manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service) {
	var inputSchema jsonschema.Schema
	if err := json.Unmarshal(c.InputSchema, &inputSchema); err != nil {
		panic(err)
	}
	checksum, _ := snapshot.Checksum()
	mcp.AddTool(server, &mcp.Tool{Name: c.ToolName, Description: c.Description(), InputSchema: &inputSchema}, func(ctx context.Context, _ *mcp.CallToolRequest, input relatedSearchInput) (*mcp.CallToolResult, any, error) {
		identity, ok := ctx.Value(identityKey{}).(requestIdentity)
		if !ok {
			return errorResult("UNAUTHENTICATED", false), nil, nil
		}
		args, _ := json.Marshal(input)
		result, err := service.Call(ctx, orchestration.Invocation{Identity: identity.Identity, CapabilityID: c.CapabilityID, Owner: c.Owner, OperationClass: c.OperationClass, GatewayBindingRef: c.GatewayBindingRef, ManifestChecksum: checksum, Arguments: args, IdempotencyKey: identity.IdempotencyKey, CorrelationID: identity.CorrelationID, Window: time.Minute, Timeout: 3 * time.Second, RetryThreshold: 3, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 200, ResultBytes: 1 << 20}})
		if err != nil {
			return errorResult(err.Error(), false), nil, nil
		}
		if result.Problem != nil {
			body, _ := json.Marshal(result.Problem)
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(body)}}, IsError: true}, nil, nil
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(result.Body)}}}, nil, nil
	})
}

func errorResult(code string, retryable bool) *mcp.CallToolResult {
	body, _ := json.Marshal(unavailableResult{Code: code, Retryable: retryable})
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(body)}}, IsError: true}
}

func registerUnavailableTool(server *mcp.Server, c manifest.Capability) {
	var inputSchema jsonschema.Schema
	if err := json.Unmarshal(c.InputSchema, &inputSchema); err != nil {
		panic(fmt.Sprintf("validated schema for %s cannot be decoded: %v", c.ToolName, err))
	}
	mcp.AddTool(server, &mcp.Tool{
		Name:        c.ToolName,
		Description: c.Description(),
		InputSchema: &inputSchema,
	}, func(context.Context, *mcp.CallToolRequest, relatedSearchInput) (*mcp.CallToolResult, any, error) {
		result := unavailableResult{Code: "MCP_1A_BACKEND_NOT_BOUND", Retryable: false}
		body, _ := json.Marshal(result)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(body)}},
			IsError: true,
		}, nil, nil
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
		identity := requestIdentity{Identity: orchestration.Identity{ServicePrincipalID: r.Header.Get("X-OUF-Service-Principal"), PrincipalID: r.Header.Get("X-OUF-Principal-ID"), TenantID: r.Header.Get("X-OUF-Tenant-ID"), ActorType: r.Header.Get("X-OUF-Actor-Type"), AuthenticationContextRef: r.Header.Get("X-OUF-Authentication-Context-Ref")}, CorrelationID: r.Header.Get("X-Correlation-ID"), IdempotencyKey: r.Header.Get("Idempotency-Key")}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), identityKey{}, identity)))
	})
}
