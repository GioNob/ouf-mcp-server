package kernel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type unitTestStore struct {
	analysisSDKStore
	scope          analysis.Scope
	profile, asset string
}

func (s *unitTestStore) RememberUnitOrigin(_ context.Context, scope analysis.Scope, profile, asset string) error {
	s.scope, s.profile, s.asset = scope, profile, asset
	return nil
}

func (s *unitTestStore) UnitOrigin(_ context.Context, scope analysis.Scope, profile string) (string, error) {
	if s.scope != scope || s.profile != profile {
		return "", analysis.ErrUnitPreviewRequired
	}
	return s.asset, nil
}

func TestUnitProfileChatThroughGateway(t *testing.T) {
	const asset = "00000000-0000-4000-8000-000000000001"
	const profile = "00000000-0000-4000-8000-000000000002"
	hash := "sha256:" + strings.Repeat("a", 64)
	ref := analysis.UnitProfileRef{ProfileID: profile, Version: 2, UnitRef: "sheet:1", SchemaHash: hash}
	var conflict atomic.Bool
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in orchestration.GatewayRequest
		var args struct {
			AssetID, ProfileID, View, UnitRef string
			UnitProfileRef                    analysis.UnitProfileRef
			Limit, Offset                     int
		}
		if json.NewDecoder(r.Body).Decode(&in) != nil || json.Unmarshal(in.Arguments, &args) != nil {
			t.Error("invalid envelope")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/internal/capabilities/v1/execute/managed.file/preview":
			if args.AssetID != asset || args.ProfileID != profile || args.Limit != 1 {
				t.Error("missing exact owner locator or page bound")
			}
			page := map[string]any{"profileId": profile, "profileVersion": 2, "nextOffset": nil, "hasMore": false}
			switch args.View {
			case "UNITS":
				page["assetId"] = asset
				page["units"] = []any{map[string]any{"unitRef": ref.UnitRef, "unitKind": "SHEET", "name": "Places", "columnCount": 2, "recordCount": 4, "hasGeometry": false}}
			case "UNIT":
				if args.UnitRef != ref.UnitRef {
					t.Error("wrong unit")
				}
				page["schema"], page["sourceKind"], page["origin"] = "ouf.source-unit-profile.v1", "FILE", map[string]string{"assetId": asset}
				page["schemaHash"], page["unitKind"], page["unitRef"] = hash, "SHEET", ref.UnitRef
				page["columnCount"], page["columns"] = 2, []any{map[string]any{"name": "id", "sample": "private-value"}}
				page["readParameters"] = map[string]any{"xlsx": map[string]any{"sheet": "Places", "headerRow": 1}}
				if args.Offset == 0 {
					page["hasMore"], page["nextOffset"] = true, 1
				}
			default:
				t.Error("unexpected preview")
			}
			page["sample"] = []any{map[string]string{"id": "private-value"}}
			_ = json.NewEncoder(w).Encode(page)
			return
		case "/internal/capabilities/v1/execute/source.revision/prepare":
			if r.Header.Get("If-Match") != `W/"ov:7"` {
				t.Error("missing parent ETag")
			}
		case "/internal/capabilities/v1/execute/managed.file/create", "/internal/capabilities/v1/execute/managed.file/review-prepare":
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
		}
		if args.UnitProfileRef != ref || args.AssetID != "" || args.ProfileID != "" {
			t.Error("changed command selection")
		}
		if conflict.Load() {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"code":"ONB_UNIT_SCHEMA_CHANGED","detail":"private-value"}`))
			return
		}
		_, _ = w.Write([]byte(`{"state":"DRAFT"}`))
	}))
	defer gateway.Close()
	client, err := httpclient.NewGateway(gateway.URL+"/internal/capabilities/v1/execute", "workload")
	if err != nil {
		t.Fatal(err)
	}
	client.Client = gateway.Client()
	store := &unitTestStore{}
	service := &orchestration.Service{RequireDelegation: true, Auth: analysisSDKAuth{}, Admission: store, Gateway: client, FingerprintKey: make([]byte, 32)}
	session := revisionSession(t, service, "ouf.onboarding.revision.prepare ouf.managed-source.onboarding.review.prepare")
	call := func(tool string, args map[string]any, wantError bool) string {
		t.Helper()
		res, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
		if err != nil || res.IsError != wantError || len(res.Content) != 1 {
			t.Fatalf("%s: %v %v", tool, res, err)
		}
		return res.Content[0].(*mcp.TextContent).Text
	}
	query := map[string]any{"view": "UNITS", "assetId": asset, "profileId": profile, "limit": 1}
	assertSchema := func(want string) {
		t.Helper()
		got := call("source.file.preview", query, false)
		if !strings.Contains(got, want) || strings.Contains(got, "sample") || strings.Contains(got, "private-value") {
			t.Fatal("invalid schema projection", got)
		}
	}
	assertSchema("Places")
	query["view"], query["unitRef"], query["offset"] = "UNIT", ref.UnitRef, 1
	assertSchema("readParameters")
	plan := map[string]any{"unitProfileRef": ref, "expectedRevision": 0, "work": []any{map[string]any{"tool": "semantic.search", "arguments": map[string]any{"q": "name"}, "columns": []int{1}}}}
	if got := call("source.analysis.plan", plan, false); !strings.Contains(got, ref.UnitRef) || store.plans != 1 {
		t.Fatal("unit plan missing", got)
	}
	changed := ref
	changed.SchemaHash = "sha256:" + strings.Repeat("b", 64)
	plan["unitProfileRef"] = changed
	const drift = `{"code":"ONB_UNIT_SCHEMA_CHANGED","retryable":false}`
	if got := call("source.analysis.plan", plan, true); got != drift || store.plans != 1 {
		t.Fatal("hash mismatch changed plan", got)
	}
	commands := map[string]map[string]any{
		"source.onboarding.create": {
			"unitProfileRef": ref, "sourceId": "places", "name": "Places", "owner": "Comune",
			"targetClassIri": "urn:test:Place", "semanticRefs": []string{"core@1"}, "sourceObjectKeyFields": []string{"id"},
			"fields": []any{map[string]any{"fieldName": "id", "extractionDecision": "INCLUDE", "dataAccessLabel": "RESTRICTED", "targetPropertyIri": "urn:test:id"}},
		},
		"source.onboarding.review.prepare": {
			"unitProfileRef": ref, "sourceId": "places", "onboardingVersionId": profile, "expectedVersion": 2, "commandKey": "review-places",
		},
		"source.onboarding.revision.prepare": {
			"unitProfileRef": ref, "sourceId": "places", "parentVersionId": profile, "expectedVersion": 7,
			"configurationHash": hash, "commandKey": "revise-places", "publicationEffect": "NEW_PUBLICATION_MAY_START_ACQUISITION",
		},
	}
	for tool, args := range commands {
		conflict.Store(false)
		call(tool, args, false)
		conflict.Store(true)
		if got := call(tool, args, true); got != drift {
			t.Fatal("unsafe conflict projection", tool, got)
		}
	}
}
