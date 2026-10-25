package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type acquisitionReadInput struct {
	SourceID       string `json:"sourceId"`
	RunID          string `json:"runId"`
	Limit          int    `json:"limit,omitempty"`
	AfterLineageID string `json:"afterLineageId,omitempty"`
	AfterHandoffID string `json:"afterHandoffId,omitempty"`
}

func isAcquisitionRead(id string) bool {
	return id == "ouf.ingestion.acquisition.read" || id == "urban.acquisition.read"
}

func registerAcquisitionReadTool(server *mcp.Server, c manifest.Capability, snapshot *manifest.Snapshot, service *orchestration.Service) {
	var schema jsonschema.Schema
	if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
		panic(err)
	}
	checksum, _ := snapshot.Checksum()
	mcp.AddTool(server, &mcp.Tool{Name: c.ToolName, Description: c.Description(), InputSchema: &schema, Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true}},
		func(ctx context.Context, _ *mcp.CallToolRequest, input acquisitionReadInput) (*mcp.CallToolResult, any, error) {
			args, _ := json.Marshal(input)
			return invoke(ctx, c, checksum, args, service, 100, 1<<20)
		})
}

// Dedicated minimized projection only. Protected historical APIs stay unavailable.
// Kernel preflight is not owner authority: every contributing permission/label is
// reevaluated by the selected owner before a row or property may be released.
func acquisitionReadAuthorization(c manifest.Capability, args []byte, tenant string) (string, orchestration.ResourceContext, error) {
	module, owner, permission := "INGESTION", "ingestion", "ingestion.lineage.read"
	if c.CapabilityID == "urban.acquisition.read" {
		module, owner, permission = "UDP", "udp", "lineage.object.read"
	}
	if !isAcquisitionRead(c.CapabilityID) || c.Owner != owner || c.OperationClass != "READ" || c.RequiredAuthorization != permission {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid acquisition manifest binding")
	}
	var input acquisitionReadInput
	decoder := json.NewDecoder(strings.NewReader(string(args)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return "", orchestration.ResourceContext{}, err
	}
	run, err := uuid.Parse(input.RunID)
	if err != nil || run == uuid.Nil || run.String() != input.RunID || strings.TrimSpace(input.SourceID) == "" || len(input.SourceID) > 200 || strings.IndexFunc(input.SourceID, unicode.IsControl) >= 0 || input.Limit < 0 || input.Limit > 100 {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid acquisition source/run")
	}
	if owner == "ingestion" && input.AfterHandoffID != "" || owner == "udp" && input.AfterLineageID != "" {
		return "", orchestration.ResourceContext{}, fmt.Errorf("wrong owner cursor")
	}
	if input.AfterLineageID != "" {
		id, parseErr := uuid.Parse(input.AfterLineageID)
		if parseErr != nil || id == uuid.Nil || id.String() != input.AfterLineageID {
			return "", orchestration.ResourceContext{}, fmt.Errorf("invalid lineage cursor")
		}
	}
	if len(input.AfterHandoffID) > 200 || strings.IndexFunc(input.AfterHandoffID, unicode.IsControl) >= 0 {
		return "", orchestration.ResourceContext{}, fmt.Errorf("invalid handoff cursor")
	}
	return permission, orchestration.ResourceContext{ResourceType: "operational", ResourceID: input.RunID, TenantID: tenant,
		Attributes: map[string]string{"module": module, "sourceRef": input.SourceID, "jobRef": input.RunID, "dataAccessLabel": "OPEN", "detailLevel": "TENANT_OPERATIONAL"}}, nil
}
