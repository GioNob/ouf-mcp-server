package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

var unitPattern = regexp.MustCompile(`^(table|sheet|layer|collection):(0|[1-9][0-9]{0,3})$`)
var unitHash = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type UnitProfileRef struct {
	ProfileID  string `json:"profileId"`
	Version    int    `json:"profileVersion"`
	UnitRef    string `json:"unitRef"`
	SchemaHash string `json:"schemaHash"`
}

func (r UnitProfileRef) Valid() bool {
	return CanonicalUUID(r.ProfileID) && r.Version > 0 && r.Version <= 2147483647 && len(r.UnitRef) <= 16 && unitPattern.MatchString(r.UnitRef) && unitHash.MatchString(r.SchemaHash)
}

// UnitError carries only contract-defined codes across the owner boundary.
type UnitError string

func (e UnitError) Error() string { return string(e) }

func OwnerUnitError(code string) error {
	switch code {
	case "ONB_UNIT_SCHEMA_CHANGED", "ONB_UNIT_PROFILE_CHANGED", "ONB_PROFILE_UNIT_NOT_FOUND",
		"ONB_UNIT_REJECTED", "ONB_UNIT_SELECTION_CHANGED", "ONB_UNIT_ALREADY_BOUND",
		"ONB_UNIT_ASSET_ALREADY_BOUND", "ONB_UNIT_PROFILE_OUTDATED", "ONB_UNIT_FORMAT_UNSUPPORTED",
		"ONB_FILE_MEDIA_TYPE_MISMATCH", "ONB_FILE_MEDIA_TYPE_UNSUPPORTED", "ONB_FILE_LIMIT_EXCEEDED",
		"ONB_READ_OPTIONS_UNSUPPORTED", "ONB_FILE_PROFILE_NOT_FOUND":
		return UnitError(code)
	}
	return nil
}

func (c Coordinator) PlanUnit(ctx context.Context, i orchestration.Identity, unit UnitProfileRef, expected int64, work []Work) (View, error) {
	if !unit.Valid() || strings.HasPrefix(unit.UnitRef, "collection:") {
		return View{}, ErrInvalid
	}
	return c.plan(ctx, i, "", unit.ProfileID, &unit, expected, work)
}

func (c Coordinator) readProfilePage(ctx context.Context, i orchestration.Identity, preview manifest.Capability, checksum, asset, profile string, unit *UnitProfileRef, pin Profile, offset, limit int) (Profile, []string, error) {
	query := map[string]any{"profileId": profile, "offset": offset, "limit": limit}
	if unit == nil {
		query["view"], query["assetId"] = "COLUMNS", asset
	} else {
		query["view"], query["unitRef"] = "UNIT", unit.UnitRef
	}
	args, _ := json.Marshal(query)
	r, e := c.Service.Call(ctx, orchestration.Invocation{BudgetProfile: orchestration.ManagedSchemaBudgetProfile, Identity: i, CapabilityID: preview.CapabilityID, Owner: preview.Owner, OperationClass: "READ", GatewayBindingRef: preview.GatewayBindingRef, ManifestChecksum: checksum, Arguments: args, IdempotencyKey: uuid.NewString(), CorrelationID: uuid.NewString(), Window: orchestration.AdmissionWindow, Timeout: 10 * time.Second, RetryThreshold: orchestration.AdmissionRetryThreshold, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 1, ResultBytes: 1 << 20}})
	if e != nil {
		return Profile{}, nil, e
	}
	if r.Problem != nil && r.Problem.Status == http.StatusForbidden {
		return Profile{}, nil, &OwnerDenial{ReasonCode: r.Problem.Code, CapabilityID: preview.CapabilityID}
	}
	if r.Problem != nil && (r.Problem.Status == 503 || r.Problem.Status == 429) {
		return Profile{}, nil, ErrUnavailable
	}
	if r.Problem != nil && unit != nil {
		if err := OwnerUnitError(r.Problem.Code); err != nil {
			return Profile{}, nil, err
		}
	}
	if r.Replay || r.Problem != nil {
		return Profile{}, nil, ErrProfile
	}
	return parseProfilePage(r.Body, asset, profile, unit, pin, offset, limit)
}

// Storage accepts the presented manifest and its direct predecessor. Also
// preserve the historical plans that the deployed predecessor could resume.
func ReadCompatible[T any](checksum string, read func(string) (T, error)) (T, string, error) {
	v, err := read(checksum)
	old := manifest.AnalysisPredecessor(checksum)
	if errors.Is(err, ErrScope) && manifest.AnalysisPredecessor(old) != "" {
		v, err = read(old)
		if err == nil {
			return v, old, nil
		}
	}
	return v, checksum, err
}

type profilePage struct {
	Profile
	Schema, Status, SourceKind, ColumnsHash string
	Origin                                  struct{ AssetID string }
	View                                    string
	Offset, NextOffset                      *int
	HasMore                                 *bool
	Columns                                 []json.RawMessage
}

func (page *profilePage) normalize(unit *UnitProfileRef) (string, error) {
	if unit == nil {
		if page.UnitRef != "" {
			return "", ErrProfile
		}
		return "COLUMNS", nil
	}
	if page.Schema != "ouf.source-unit-profile.v1" || page.SourceKind != "FILE" || page.UnitRef != unit.UnitRef || !CanonicalUUID(page.Origin.AssetID) {
		return "", ErrProfile
	}
	if page.Version != unit.Version {
		return "", UnitError("ONB_UNIT_PROFILE_CHANGED")
	}
	if page.Status == "REJECTED" {
		return "", UnitError("ONB_UNIT_REJECTED")
	}
	if page.Status != "READY" {
		return "", ErrProfile
	}
	if page.SchemaHash != unit.SchemaHash {
		return "", UnitError("ONB_UNIT_SCHEMA_CHANGED")
	}
	page.AssetID = page.Origin.AssetID
	if unit.UnitRef == "table:0" {
		page.UnitRef, page.SchemaHash = "", page.ColumnsHash
	}
	return "UNIT", nil
}
