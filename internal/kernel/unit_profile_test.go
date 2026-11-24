package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

const unitAsset = "00000000-0000-4000-8000-000000000001"
const unitProfile = "00000000-0000-4000-8000-000000000002"

var columnsHash = "sha256:" + strings.Repeat("c", 64)
var structuralHash = "sha256:" + strings.Repeat("a", 64)

func fixtureUnit(ref string) analysis.UnitProfileRef {
	return analysis.UnitProfileRef{ProfileID: unitProfile, Version: 2, UnitRef: ref, SchemaHash: structuralHash}
}

type unitPlanStore struct {
	analysisSDKStore
	saved    analysis.Plan
	progress analysis.Profile
	headers  []string
	stop     bool
}

func (s *unitPlanStore) PreviewProgress(_ context.Context, _ analysis.Scope, _, _ string) (analysis.Profile, []string, error) {
	return s.progress, slices.Clone(s.headers), nil
}

func (s *unitPlanStore) SavePreviewPage(_ context.Context, _ analysis.Scope, _, _ string, pin analysis.Profile, offset int, names []string) error {
	if s.stop && offset == 1 {
		s.stop = false
		return errors.New("fixture interruption")
	}
	if offset != len(s.headers) || offset != 0 && s.progress != pin {
		return analysis.ErrProfile
	}
	s.progress = pin
	s.headers = append(s.headers, names...)
	return nil
}

func (s *unitPlanStore) PlanAnalysis(ctx context.Context, p analysis.Plan) (analysis.View, error) {
	s.saved = p
	return s.analysisSDKStore.PlanAnalysis(ctx, p)
}

func (s *unitPlanStore) ReadAnalysis(_ context.Context, _ analysis.Scope, id, _ string, _, _ int) (analysis.View, error) {
	if s.saved.ID == id {
		return analysis.View{ID: id, Profile: s.saved.Profile, Revision: 1}, nil
	}
	return analysis.View{}, analysis.ErrScope
}

type unitPageOwner struct {
	offsets []int
}

func (o *unitPageOwner) Execute(_ context.Context, r orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	var q struct {
		View, ProfileID, AssetID, UnitRef string
		Limit, Offset                     int
	}
	if json.Unmarshal(r.Arguments, &q) != nil || r.CapabilityID != "ouf.managed-source.preview" || q.ProfileID != unitProfile || q.Limit != 1 {
		return orchestration.GatewayResponse{}, errors.New("unexpected preview")
	}
	o.offsets = append(o.offsets, q.Offset)
	page := railObject{"view": q.View, "profileId": q.ProfileID, "profileVersion": 2, "columnCount": 3, "offset": q.Offset,
		"columns": []railObject{{"ordinal": q.Offset, "name": fmt.Sprintf("column_%d", q.Offset)}}, "nextOffset": q.Offset + 1, "hasMore": q.Offset+1 < 3}
	switch q.View {
	case "COLUMNS":
		if q.AssetID != unitAsset {
			return orchestration.GatewayResponse{}, errors.New("wrong asset")
		}
		page["assetId"], page["schemaHash"] = unitAsset, columnsHash
	case "UNIT":
		if q.AssetID != "" {
			return orchestration.GatewayResponse{}, errors.New("client supplied an asset")
		}
		page["origin"], page["sourceKind"], page["schema"] = railObject{"assetId": unitAsset}, "FILE", "ouf.source-unit-profile.v1"
		page["schemaHash"], page["columnsHash"], page["unitRef"], page["status"] = structuralHash, columnsHash, q.UnitRef, "READY"
	default:
		return orchestration.GatewayResponse{}, errors.New("wrong view")
	}
	body, err := json.Marshal(page)
	return orchestration.GatewayResponse{Status: 200, Body: body}, err
}

func TestUnitPlansUseVerifiedPagesAndResume(t *testing.T) { // MCP-62E-04
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	i := orchestration.Identity{ServicePrincipalID: "mcp", PrincipalID: "reader", TenantID: "tenant", ActorType: "HUMAN", Delegation: "fixture"}
	work := []analysis.Work{{Tool: "semantic.search", Arguments: json.RawMessage(`{"q":"column_2"}`), Columns: []int{2}}}
	var legacy analysis.Plan
	for _, name := range []string{"COLUMNS", "table:0", "sheet:1"} {
		t.Run(name, func(t *testing.T) {
			s := &unitPlanStore{stop: name == "sheet:1"}
			owner := &unitPageOwner{}
			c := analysis.Coordinator{Store: s, Manifest: m, PreviewPageSize: 1,
				Service: &orchestration.Service{Auth: analysisSDKAuth{}, Admission: s, Gateway: owner, FingerprintKey: make([]byte, 32)}}
			ref := fixtureUnit(name)
			if name == "COLUMNS" {
				_, err := c.Plan(ctx, i, unitAsset, unitProfile, 0, work)
				if err != nil {
					t.Fatal(err)
				}
				legacy = s.saved
				sum, _ := m.Checksum()
				if legacy.Manifest != manifest.AnalysisPredecessor(sum) {
					t.Fatal("new CSV plan cannot be read after rollback", legacy.Manifest)
				}
			} else {
				if s.stop {
					if _, err := c.PlanUnit(ctx, i, ref, 0, work); err == nil || len(s.headers) != 1 || s.plans != 0 {
						t.Fatal("incomplete page committed", err)
					}
					owner.offsets = nil
					wrong := ref
					wrong.SchemaHash = columnsHash
					if _, err := c.PlanUnit(ctx, i, wrong, 0, work); err == nil || len(s.headers) != 1 || s.plans != 0 {
						t.Fatal("wrong pin resumed a checkpoint")
					}
				}
				if _, err := c.PlanUnit(ctx, i, ref, 0, work); err != nil {
					t.Fatal(err)
				}
			}
			if !slices.Equal(s.saved.Headers, []string{"column_0", "column_1", "column_2"}) {
				t.Fatal("missing verified headers", s.saved)
			}
			want := []int{0, 1, 2}
			if name == "sheet:1" {
				want = []int{1, 2}
				id := analysis.Digest([]any{analysis.ProfileVersion, analysis.IdentityScope(i), "unit", unitProfile, name})
				if s.saved.Profile.UnitRef != name || s.saved.Profile.SchemaHash != structuralHash || s.saved.ID != id {
					t.Fatal(s.saved)
				}
			} else if !reflect.DeepEqual(s.saved, legacy) || s.saved.Profile.SchemaHash != columnsHash || s.saved.Profile.UnitRef != "" {
				t.Fatal("table route changed the plan, ID, manifest or profile")
			}
			if !slices.Equal(owner.offsets, want) {
				t.Fatalf("read offsets %v, want %v", owner.offsets, want)
			}
			if name != "COLUMNS" {
				ref.SchemaHash = columnsHash
				if _, err := c.PlanUnit(ctx, i, ref, 0, work); analysis.SafeError(err) != "ONB_UNIT_SCHEMA_CHANGED" || s.plans != 1 {
					t.Fatal("saved plan accepted a different reference", err)
				}
			}
		})
	}
}
