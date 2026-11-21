package kernel

import (
	"context"
	"encoding/json"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

// The owner also serves a redacted sample to other clients. The chat schema
// views publish only inventory and schema fields, even on the first page.
func projectUnitPreview(ctx context.Context, service *orchestration.Service, identity orchestration.Identity, args, body []byte) ([]byte, error) {
	var query struct {
		View, AssetID, ProfileID, UnitRef string
		Offset, Limit                     int
	}
	query.Limit = 128
	if json.Unmarshal(args, &query) != nil {
		return nil, analysis.ErrInvalid
	}
	if query.View != "UNIT" && query.View != "UNITS" {
		return body, nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return nil, analysis.ErrScope
	}
	keys := []string{"profileId", "profileVersion", "nextOffset", "hasMore"}
	rowField := "columns"
	rowKeys := []string{"name", "nativeType", "inferredType", "nullable", "distinctValues", "role"}
	if query.View == "UNITS" {
		var page struct {
			AssetID, ProfileID string
			ProfileVersion     int
			Units              []json.RawMessage
			NextOffset         *int
			HasMore            *bool
		}
		if json.Unmarshal(body, &page) != nil || page.ProfileID != query.ProfileID || page.AssetID != query.AssetID ||
			page.ProfileVersion < 1 || page.HasMore == nil || page.Units == nil || len(page.Units) > query.Limit ||
			(*page.HasMore && (len(page.Units) == 0 || page.NextOffset == nil || *page.NextOffset != query.Offset+len(page.Units))) {
			return nil, analysis.ErrScope
		}
		keys = append(keys, "assetId", "units", "inventoryComplete")
		rowField = "units"
		rowKeys = []string{"unitRef", "unitKind", "name", "recordCount", "columnCount", "hasGeometry"}
	} else {
		if _, err := analysis.UnitPage(body, query.AssetID, query.ProfileID, query.UnitRef, query.Offset, query.Limit); err != nil {
			return nil, err
		}
		keys = append(keys, "schema", "schemaHash", "sourceKind", "origin", "unitRef", "unitKind", "unitName", "recordModel", "columnCount", "columns", "keys", "geometry", "readParameters")
	}
	var rows []map[string]json.RawMessage
	if json.Unmarshal(fields[rowField], &rows) != nil {
		return nil, analysis.ErrScope
	}
	for i, row := range rows {
		rows[i] = schemaFields(row, rowKeys)
	}
	fields[rowField], _ = json.Marshal(rows)
	out, err := json.Marshal(schemaFields(fields, keys))
	if err != nil || len(out) > 1<<20 {
		return nil, analysis.ErrScope
	}
	origins, ok := service.Admission.(analysis.UnitOrigins)
	if !ok {
		return nil, analysis.ErrUnavailable
	}
	if err := origins.RememberUnitOrigin(ctx, analysis.IdentityScope(identity), query.ProfileID, query.AssetID); err != nil {
		return nil, err
	}
	return out, nil
}

func schemaFields(fields map[string]json.RawMessage, keys []string) map[string]json.RawMessage {
	schema := make(map[string]json.RawMessage, len(keys))
	for _, k := range keys {
		if v, ok := fields[k]; ok {
			schema[k] = v
		}
	}
	return schema
}
