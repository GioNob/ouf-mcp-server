package kernel

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"unicode/utf8"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	maxValueMapArgumentsBytes  = 1 << 20
	maxValueMapDefinitionBytes = 768 << 10
)

func isValueTypeTool(name string) bool {
	return name == "semantic.propose" || name == "source.onboarding.create" || name == "source.onboarding.revision.prepare"
}

// Schema diagnostics may quote the offending value. Use the same advertised
// schema for validation and expose only a stable code before owner dispatch.
func registerValueTypeTool(server *workflowServer, c manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service) {
	var schema jsonschema.Schema
	if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
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
		var err error
		receipt, err = output.Resolve(nil)
		if err != nil {
			panic(err)
		}
	}
	checksum, _ := snapshot.Checksum()
	server.register(tool, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args map[string]any
		raw := req.Params.Arguments
		if json.Unmarshal(raw, &args) != nil || args == nil {
			return errorResult("INVALID_TOOL_ARGUMENTS", false), nil
		}
		valueMap := c.ToolName == "semantic.propose" && args["artifactType"] == "VALUE_MAP"
		if valueMap {
			definition := args["definition"].(map[string]any)
			if !validValueMapDefinition(definition) || !validJCSInput(raw) {
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

// encoding/json accepts duplicate keys and replaces invalid Unicode with U+FFFD.
// Neither transformation may silently change a VALUE_MAP before measuring it.
// The caller has already checked JSON syntax and the closed VALUE_MAP schema.
func validJCSInput(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}
	for rest := raw; len(rest) > 0; {
		i := bytes.IndexByte(rest, '\\')
		if i < 0 {
			break
		}
		rest = rest[i+1:]
		if rest[0] != 'u' {
			rest = rest[1:]
			continue
		}
		unit, _ := strconv.ParseUint(string(rest[1:5]), 16, 16)
		rest = rest[5:]
		if unit < 0xd800 || unit > 0xdfff {
			continue
		}
		if unit > 0xdbff || len(rest) < 6 || rest[0] != '\\' || rest[1] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(rest[2:6]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		rest = rest[6:]
	}
	return uniqueJSONMembers(json.NewDecoder(bytes.NewReader(raw)))
}

func uniqueJSONMembers(d *json.Decoder) bool {
	token, err := d.Token()
	if err != nil {
		return false
	}
	delim, compound := token.(json.Delim)
	if !compound {
		return true
	}
	var keys map[string]bool
	if delim == '{' {
		keys = make(map[string]bool)
	}
	for d.More() {
		if keys != nil {
			key, err := d.Token()
			name, ok := key.(string)
			if err != nil || !ok || keys[name] {
				return false
			}
			keys[name] = true
		}
		if !uniqueJSONMembers(d) {
			return false
		}
	}
	_, err = d.Token()
	return err == nil
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

// VALUE_MAP definitions have only closed ASCII property names, arrays and
// strings. JSON's key order therefore coincides with JCS UTF-16 order here.
// RFC 8785 requires five short control escapes, lowercase hex for the other
// controls and literal UTF-8 for HTML characters and line separators.
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
		case `\u0008`:
			compact = append(compact, `\b`...)
		case `\u0009`:
			compact = append(compact, `\t`...)
		case `\u000a`:
			compact = append(compact, `\n`...)
		case `\u000c`:
			compact = append(compact, `\f`...)
		case `\u000d`:
			compact = append(compact, `\r`...)
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
