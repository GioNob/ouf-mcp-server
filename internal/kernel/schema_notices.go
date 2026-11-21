package kernel

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	schemaNoticesCapability = "ouf.onboarding.schema-notices.read"
	schemaNoticesMaxBytes   = 65536
	emptySchemaNotices      = `{"notices":[],"next":null,"hasMore":false}`
)

//go:embed schema-notices-page.schema.json
var schemaNoticesPageJSON []byte

var schemaNoticesPage, resolvedSchemaNoticesPage = func() (*jsonschema.Schema, *jsonschema.Resolved) {
	var page jsonschema.Schema
	if err := json.Unmarshal(schemaNoticesPageJSON, &page); err != nil {
		panic(err)
	}
	resolved, err := page.Resolve(nil)
	if err != nil {
		panic(err)
	}
	return &page, resolved
}()

type schemaNoticesInput struct {
	State string `json:"state"`
	Limit int    `json:"limit"`
	After string `json:"after,omitempty"`
}

func registerSchemaNoticesTool(server *mcp.Server, c manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service) {
	var input jsonschema.Schema
	if err := json.Unmarshal(c.InputSchema, &input); err != nil {
		panic(err)
	}
	checksum, _ := snapshot.Checksum()
	mcp.AddTool(server, &mcp.Tool{Name: c.ToolName, Description: c.Description(), InputSchema: &input, OutputSchema: schemaNoticesPage, Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true}},
		func(ctx context.Context, _ *mcp.CallToolRequest, page schemaNoticesInput) (*mcp.CallToolResult, any, error) {
			if service == nil {
				return errorResult("MCP_1A_BACKEND_NOT_BOUND", false), nil, nil
			}
			page.State = "OPEN"
			if page.Limit == 0 {
				page.Limit = 20
			}
			args, _ := json.Marshal(page)
			return invoke(ctx, c, checksum, args, service, page.Limit, schemaNoticesMaxBytes)
		})
}

func schemaNoticesResult(result orchestration.Result, err error, args []byte) (*mcp.CallToolResult, any, error) {
	var denied *orchestration.AuthorizationDeniedError
	body := json.RawMessage(result.Body)
	switch {
	case errors.As(err, &denied) && denied.ReasonCode == "NO_APPLICABLE_GRANT":
		body = json.RawMessage(emptySchemaNotices)
	case errors.Is(err, orchestration.ErrUnauthorized):
		return errorResult("NOT_AUTHORIZED", false), nil, nil
	case errors.Is(err, orchestration.ErrResultLimit):
		return errorResult("RESULT_LIMIT_EXCEEDED", false), nil, nil
	case err != nil:
		return errorResult("SCHEMA_NOTICES_READ_FAILED", false), nil, nil
	case result.Replay:
		return errorResult("SCHEMA_NOTICES_REQUIRE_FRESH_READ", true), nil, nil
	case result.Problem != nil:
		return schemaNoticesProblem(result.Problem), nil, nil
	default:
		var request schemaNoticesInput
		var page map[string]any
		if json.Unmarshal(args, &request) != nil || json.Unmarshal(body, &page) != nil || resolvedSchemaNoticesPage.Validate(page) != nil {
			return errorResult("INVALID_SCHEMA_NOTICES_PAGE", false), nil, nil
		}
		rows := page["notices"].([]any)
		if len(rows) > request.Limit || page["next"] == request.After {
			return errorResult("INVALID_SCHEMA_NOTICES_PAGE", false), nil, nil
		}
		if len(rows) == 0 {
			body = json.RawMessage(emptySchemaNotices)
		} else {
			// Emit only the validated values. Raw JSON can still contain an
			// earlier, shadowed member; keep integer revision references exact.
			decoder := json.NewDecoder(bytes.NewReader(body))
			decoder.UseNumber()
			if err := decoder.Decode(&page); err != nil {
				return errorResult("INVALID_SCHEMA_NOTICES_PAGE", false), nil, nil
			}
			body, err = json.Marshal(page)
			if err != nil {
				return errorResult("INVALID_SCHEMA_NOTICES_PAGE", false), nil, nil
			}
			if len(body) > schemaNoticesMaxBytes {
				return errorResult("RESULT_LIMIT_EXCEEDED", false), nil, nil
			}
		}
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(body)}}}, body, nil
}

func schemaNoticesProblem(p *orchestration.Problem) *mcp.CallToolResult {
	switch p.Status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return errorResult("NOT_AUTHORIZED", false)
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return errorResult("INVALID_SCHEMA_NOTICES_REQUEST", false)
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		payload := map[string]any{"code": "SCHEMA_NOTICES_UNAVAILABLE", "retryable": true}
		if seconds, err := strconv.ParseUint(p.RetryAfter, 10, 63); err == nil {
			payload["retryAfterSeconds"] = seconds
		} else if when, err := http.ParseTime(p.RetryAfter); err == nil {
			payload["retryAfter"] = when.UTC().Format(http.TimeFormat)
		}
		body, _ := json.Marshal(payload)
		return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: string(body)}}}
	default:
		return errorResult("SCHEMA_NOTICES_READ_FAILED", false)
	}
}
