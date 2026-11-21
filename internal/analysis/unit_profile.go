package analysis

import (
	"encoding/json"
	"strings"

	"github.com/google/uuid"
)

type ProfileSelection struct {
	AssetID        string          `json:"assetId"`
	ProfileID      string          `json:"profileId"`
	UnitProfileRef *UnitProfileRef `json:"unitProfileRef"`
}

func (s ProfileSelection) valid() bool {
	ref := s.UnitProfileRef
	if ref == nil {
		return uuid.Validate(s.AssetID) == nil && uuid.Validate(s.ProfileID) == nil
	}
	return s.ProfileID == "" && (s.AssetID == "" || uuid.Validate(s.AssetID) == nil) && ref.valid()
}

func (ref UnitProfileRef) valid() bool {
	return uuid.Validate(ref.ProfileID) == nil && ref.Version > 0 && strings.TrimSpace(ref.UnitRef) != "" &&
		validSchemaHash(ref.SchemaHash) && ref.SchemaHash == strings.ToLower(ref.SchemaHash)
}

// The UNIT owner response carries the asset under origin and has no view or
// offset echo. Verify the requested page using the owner's pagination fields.
func UnitPage(body []byte, asset, profile, unit string, offset, limit int) (Profile, error) {
	var page struct {
		Profile
		Schema     string `json:"schema"`
		SourceKind string `json:"sourceKind"`
		UnitKind   string `json:"unitKind"`
		Origin     struct {
			AssetID string `json:"assetId"`
		} `json:"origin"`
		Columns    []json.RawMessage `json:"columns"`
		HasMore    *bool             `json:"hasMore"`
		NextOffset *int              `json:"nextOffset"`
	}
	if json.Unmarshal(body, &page) != nil || page.Schema != "ouf.source-unit-profile.v1" || page.SourceKind != "FILE" ||
		page.Origin.AssetID != asset || page.ProfileID != profile || page.UnitRef != unit || unit == "" {
		return Profile{}, ErrScope
	}
	p := page.Profile
	p.AssetID = page.Origin.AssetID
	if page.UnitKind != "TABLE" && page.UnitKind != "SHEET" && page.UnitKind != "LAYER" ||
		(unit == "table:0") != (page.UnitKind == "TABLE") {
		return Profile{}, ErrScope
	}
	end := min(offset+limit, p.ColumnCount)
	if uuid.Validate(asset) != nil || !p.Reference().valid() ||
		p.ColumnCount < 0 || p.ColumnCount > 1024 || offset < 0 || (offset != 0 && offset >= p.ColumnCount) || limit < 1 || limit > 128 ||
		page.Columns == nil || len(page.Columns) != end-offset || page.HasMore == nil || *page.HasMore != (end < p.ColumnCount) ||
		(*page.HasMore && (page.NextOffset == nil || *page.NextOffset != end)) {
		return Profile{}, ErrScope
	}
	return p, nil
}
