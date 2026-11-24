package kernel

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Only sheet analyses need a scoped read of the unit pin beyond the
// asset and profile already available in the legacy summary.
func bindSummaryUnit(ctx context.Context, c analysis.Coordinator, scope analysis.Scope, id, checksum string, hint *analysis.Hint) error {
	if analysis.AnalysisID(scope, analysis.Profile{AssetID: hint.AssetID, ProfileID: hint.ProfileID}) == id {
		return nil
	}
	view, err := c.Store.ReadAnalysis(ctx, scope, id, checksum, 0, 1)
	if err != nil {
		return err
	}
	p := view.Profile
	ref := analysis.UnitProfileRef{ProfileID: p.ProfileID, Version: p.Version, UnitRef: p.UnitRef, SchemaHash: p.SchemaHash}
	if !ref.Valid() || p.ProfileID != hint.ProfileID || p.AssetID != hint.AssetID || analysis.AnalysisID(scope, p) != id {
		return analysis.ErrProfile
	}
	hint.UnitProfileRef = &ref
	return nil
}

func unitProblem(p *orchestration.Problem, body []byte) *mcp.CallToolResult {
	if p == nil || analysis.OwnerUnitError(p.Code) == nil {
		return nil
	}
	if p.Code == "ONB_UNIT_REJECTED" {
		var problem struct{ Rejection struct{ Code string } }
		if json.Unmarshal(body, &problem) == nil && nextstep.UnitRejectionMessage(problem.Rejection.Code) != "" {
			return workflowError(nextstep.Object{"code": p.Code, "retryable": false, "rejection": nextstep.Object{"code": problem.Rejection.Code}})
		}
	}
	return errorResult(p.Code, false)
}

// A schema view may contain bounded distinct values. Row samples and owner
// extensions have no place in the projection, including inside column metadata.
func projectUnitPreview(args, body []byte) ([]byte, error) {
	var q struct {
		View, ProfileID, AssetID, UnitRef, DistinctValuesCursor string
		Limit, Offset                                           int
	}
	if json.Unmarshal(args, &q) != nil {
		return nil, analysis.ErrInvalid
	}
	if q.View != "UNIT" && q.View != "UNITS" {
		return body, nil
	}
	var meta struct {
		ProfileVersion int
		HasMore        *bool
		NextOffset     *int
		ColumnCount    int
		Offset         *int
	}
	var obj map[string]any
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	if !analysis.ValidEvidenceJSON(body) || json.Unmarshal(body, &meta) != nil || meta.ProfileVersion < 1 || meta.HasMore == nil || d.Decode(&obj) != nil {
		return nil, analysis.ErrProfile
	}
	origin := unitFields(obj["origin"], "assetId mediaType")
	asset, _ := origin["assetId"].(string)
	if obj["profileId"] != q.ProfileID || !analysis.CanonicalUUID(asset) || q.AssetID != "" && asset != q.AssetID || obj["sourceKind"] != "FILE" {
		return nil, analysis.ErrProfile
	}
	out := unitFields(obj, "schema sourceKind profileId profileVersion nextOffset hasMore")
	out["origin"] = origin
	var field, keys string
	switch q.View {
	case "UNITS":
		if obj["schema"] != "ouf.source-unit-inventory.v1" {
			return nil, analysis.ErrProfile
		}
		out["readerVersion"], out["inventoryComplete"] = obj["readerVersion"], obj["inventoryComplete"]
		out["unitSummary"] = unitFields(obj["unitSummary"], "unitCount readyUnits singleReadyUnitRef")
		field, keys = "units", "unitRef unitKind name hidden status recordCount columnCount hasGeometry"
		if q.Limit == 0 {
			q.Limit = 64
		}
	case "UNIT":
		if obj["schema"] != "ouf.source-unit-profile.v1" || obj["view"] != "UNIT" || obj["unitRef"] != q.UnitRef || obj["status"] != "READY" {
			return nil, analysis.ErrProfile
		}
		for k, v := range unitFields(obj, "view unitRef unitKind unitName status recordModel schemaHash columnsHash limitsProfile recordCount columnCount offset") {
			out[k] = v
		}
		out["keys"] = unitFields(obj["keys"], "declared candidates")
		out["geometry"] = nil
		if obj["geometry"] != nil {
			g := unitFields(obj["geometry"], "column geometryType dimensions srsId sourceCrs")
			if geometry, ok := obj["geometry"].(map[string]any); ok && geometry["extent"] != nil {
				g["extent"] = unitFields(geometry["extent"], "minX minY maxX maxY")
			}
			out["geometry"] = g
		}
		params := map[string]any{}
		source, _ := obj["readParameters"].(map[string]any)
		for format, fields := range map[string]string{"csv": "delimiter encoding headerRow", "xlsx": "sheet sheetIndex headerRow dateSystem timeZone cellText", "geopackage": "layer geometryColumn sourceCrs"} {
			if source[format] != nil {
				params[format] = unitFields(source[format], fields)
			}
		}
		out["readParameters"] = params
		field, keys = "columns", "ordinal name nativeType dataType baseType nullable distinctCount distinctValues distinctValuesComplete distinctValuesCursor role"
		if q.Limit == 0 {
			q.Limit = 128
		}
	}
	rows, ok := obj[field].([]any)
	if !ok || len(rows) > q.Limit {
		return nil, analysis.ErrProfile
	}
	end := q.Offset + len(rows)
	if *meta.HasMore && (len(rows) == 0 || meta.NextOffset == nil || *meta.NextOffset != end) {
		return nil, analysis.ErrProfile
	}
	if field == "columns" && q.DistinctValuesCursor == "" {
		if meta.Offset == nil || *meta.Offset != q.Offset || len(rows) == 0 || end > meta.ColumnCount || meta.ColumnCount > 1024 || *meta.HasMore != (end < meta.ColumnCount) {
			return nil, analysis.ErrProfile
		}
	}
	for i, item := range rows {
		row := unitFields(item, keys)
		if row == nil {
			return nil, analysis.ErrProfile
		}
		if field == "units" && row["status"] != "READY" && row["status"] != "REJECTED" {
			return nil, analysis.ErrProfile
		}
		if field == "units" && row["status"] == "REJECTED" {
			row["rejection"] = unitFields(item.(map[string]any)["rejection"], "code at")
		}
		rows[i] = row
	}
	out[field] = rows
	return json.Marshal(out)
}

func unitFields(value any, fields string) map[string]any {
	obj, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	out := map[string]any{}
	for _, key := range strings.Fields(fields) {
		if v, exists := obj[key]; exists {
			out[key] = v
		}
	}
	return out
}
