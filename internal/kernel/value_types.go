package kernel

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	maxValueMapArgumentsBytes = 1 << 20
	maxValueMapDefinitionBytes = 768 << 10
)

func isValueTypeTool(name string) bool {
	return name == "semantic.propose" || name == "source.onboarding.create" || name == "source.onboarding.revision.prepare"
}

// Schema diagnostics may quote the offending value. Use the same advertised
// schema for validation and expose only a stable code before owner dispatch.
func registerValueTypeTool(server *mcp.Server, c manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service) {
	var schema jsonschema.Schema
	if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
		panic(err)
	}
	input, err := schema.Resolve(nil)
	if err != nil {
		panic(err)
	}
	tool := &mcp.Tool{Name: c.ToolName, Description: c.Description(), InputSchema: &schema, Annotations: &mcp.ToolAnnotations{}}
	var receipt *jsonschema.Resolved
	maxObjects, maxBytes := 200, 1<<20
	switch c.ToolName {
	case "semantic.propose":
		maxObjects, maxBytes = 1, 262144
	case "source.onboarding.revision.prepare":
		maxObjects, maxBytes = 1, 65536
		output := sourceRevisionReceiptSchema(snapshot)
		tool.OutputSchema = output
		receipt, err = output.Resolve(nil)
		if err != nil {
			panic(err)
		}
	}
	checksum, _ := snapshot.Checksum()
	server.AddTool(tool, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args map[string]any
		raw := req.Params.Arguments
		if json.Unmarshal(raw, &args) != nil || args == nil || input.Validate(args) != nil {
			return errorResult("INVALID_TOOL_ARGUMENTS", false), nil
		}
		valueMap := c.ToolName == "semantic.propose" && args["artifactType"] == "VALUE_MAP"
		if valueMap {
			definition := args["definition"].(map[string]any)
			if !validValueMapDefinition(definition) {
				return errorResult("INVALID_TOOL_ARGUMENTS", false), nil
			}
			if len(raw) > maxValueMapArgumentsBytes {
				return errorResult("OWNER_COMMAND_ARGUMENTS_TOO_LARGE", false), nil
			}
		}
		if service == nil {
			return errorResult("MCP_1A_BACKEND_NOT_BOUND", false), nil
		}
		// Keep the canonical key ordering used by deployed request hashes.
		// DATATYPE is new and can carry exact numeric component constraints.
		if args["artifactType"] == "DATATYPE" {
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			if err := decoder.Decode(&args); err != nil {
				return errorResult("INVALID_TOOL_ARGUMENTS", false), nil
			}
		}
		marshal := json.Marshal
		if valueMap {
			marshal = marshalValueMapJSON
		}
		body, err := marshal(args)
		if err != nil {
			return errorResult("INVALID_TOOL_ARGUMENTS", false), nil
		}
		if receipt != nil && (args["newColumns"] != nil || args["compositions"] != nil) && len(body)+len(`{"Arguments":}`) > 65536 {
			return errorResult("OWNER_COMMAND_ARGUMENTS_TOO_LARGE", false), nil
		}
		result, structured, err := invoke(ctx, c, checksum, body, service, maxObjects, maxBytes)
		if err != nil || structured == nil {
			return result, err
		}
		var owner map[string]any
		payload, ok := structured.(json.RawMessage)
		if !ok || receipt == nil || json.Unmarshal(payload, &owner) != nil || owner == nil || receipt.Validate(owner) != nil {
			return errorResult("SOURCE_REVIEW_READ_REQUIRED", false), nil
		}
		result.StructuredContent = payload
		return result, nil
	})
}

// The input schema validates the shape first. Definition size and uniqueness
// by code are checked here because JSON Schema cannot express them.
func validValueMapDefinition(definition map[string]any) bool {
	entries := definition["entries"].([]any)
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		code := entry.(map[string]any)["code"].(string)
		if _, duplicate := seen[code]; duplicate {
			return false
		}
		seen[code] = struct{}{}
	}
	encoded, err := marshalValueMapJSON(definition)
	return err == nil && len(encoded) <= maxValueMapDefinitionBytes
}

// Go's compact JSON escapes HTML and script separators. Emit those characters
// directly for UTF-8 byte limits, preserving literal backslashes and other
// required escapes. Sorting entries would leave the byte count unchanged.
func marshalValueMapJSON(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	compact := encoded[:0]
	for {
		escape := bytes.IndexByte(encoded, '\\')
		if escape < 0 {
			return append(compact, encoded...), nil
		}
		compact = append(compact, encoded[:escape]...)
		encoded = encoded[escape:]
		if encoded[1] != 'u' {
			compact = append(compact, encoded[:2]...)
			encoded = encoded[2:]
			continue
		}
		switch string(encoded[:6]) {
		case `\u0026`:
			compact = append(compact, '&')
		case `\u003c`:
			compact = append(compact, '<')
		case `\u003e`:
			compact = append(compact, '>')
		case `\u2028`:
			compact = append(compact, "\u2028"...)
		case `\u2029`:
			compact = append(compact, "\u2029"...)
		default:
			compact = append(compact, encoded[:6]...)
		}
		encoded = encoded[6:]
	}
}
