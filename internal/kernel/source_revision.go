package kernel

import (
	"context"
	"encoding/json"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Both tools return native owner objects. The decision schema is the same on
// submission and receipt, while version reads and ABSENT receipts can omit it.
func sourceRevisionReceiptSchema(snapshot *manifest.Snapshot) *jsonschema.Schema {
	for _, c := range snapshot.Capabilities {
		if c.CapabilityID == "ouf.onboarding.revision.prepare" {
			var schema jsonschema.Schema
			if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
				panic(err)
			}
			if columns := schema.Properties["newColumns"]; columns != nil {
				return &jsonschema.Schema{Type: "object", Defs: schema.Defs, Properties: map[string]*jsonschema.Schema{
					"newColumns": columns, "compositions": schema.Properties["compositions"],
				}}
			}
		}
	}
	panic("revision.prepare must define newColumns")
}

func registerSourceRevisionRead(server *workflowServer, c manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service) {
	var inputSchema jsonschema.Schema
	if err := json.Unmarshal(c.InputSchema, &inputSchema); err != nil {
		panic(err)
	}
	checksum, _ := snapshot.Checksum()
	tool := &mcp.Tool{
		Name:         c.ToolName,
		Description:  c.Description(),
		InputSchema:  &inputSchema,
		OutputSchema: sourceRevisionReceiptSchema(snapshot),
		Annotations:  &mcp.ToolAnnotations{ReadOnlyHint: c.OperationClass == "READ"},
	}
	addWorkflowTool(server, tool, func(ctx context.Context, _ *mcp.CallToolRequest, input map[string]any) (*mcp.CallToolResult, any, error) {
		if service == nil {
			return errorResult("MCP_1A_BACKEND_NOT_BOUND", false), nil, nil
		}
		args, err := json.Marshal(input)
		if err != nil {
			return nil, nil, err
		}
		return invoke(ctx, c, checksum, args, service, 1, 65536)
	})
}
