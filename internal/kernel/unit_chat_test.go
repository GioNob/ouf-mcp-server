package kernel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/hostfiles"
	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Supply completed semantic evidence at the store boundary. Planning and all
// file/onboarding requests go through the coordinator and Gateway client.
type completedUnitAnalysis struct{ analysisSDKStore }

func (s *completedUnitAnalysis) PlanAnalysis(ctx context.Context, p analysis.Plan) (analysis.View, error) {
	for i := range p.Tasks {
		p.Tasks[i].State = "SUCCEEDED"
	}
	return s.analysisSDKStore.PlanAnalysis(ctx, p)
}

func TestXLSXUploadToReview(t *testing.T) { // MCP-62E-05
	ctx := context.Background()
	ref := fixtureUnit("sheet:1")
	var proposal railObject
	if err := json.Unmarshal([]byte(reviewExecutionJSON), &proposal); err != nil {
		t.Fatal(err)
	}
	proposal["contract"] = "ouf.managed-unit-execution-proposal.v1"
	options := railObject{"xlsx": railObject{"headerRows": railObject{"sheet:1": float64(3)}}}
	file, err := os.ReadFile("testdata/two-worksheets.xlsx")
	if err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/managed.file/upload" {
			b, err := io.ReadAll(r.Body)
			if err != nil || !bytes.Equal(b, file) || r.Header.Get("Content-Type") != hostfiles.XLSXMediaType {
				t.Error("workbook transfer", err)
			}
			json.NewEncoder(w).Encode(railObject{"status": "STAGED", "assetId": unitAsset})
			return
		}
		var in orchestration.GatewayRequest
		var args railObject
		if json.NewDecoder(r.Body).Decode(&in) != nil || json.Unmarshal(in.Arguments, &args) != nil {
			t.Error("invalid owner envelope")
			return
		}
		summary := railObject{"unitCount": 2, "readyUnits": 2}
		result := railObject{"profileId": unitProfile, "profileVersion": 2, "sourceKind": "FILE", "origin": railObject{"assetId": unitAsset}}
		switch r.URL.Path {
		case "/managed.file/profile":
			if !reflect.DeepEqual(args["readOptions"], options) {
				t.Error("readOptions changed", args)
			}
			result["unitSummary"], result["workflowState"] = summary, "SUCCEEDED"
		case "/managed.file/preview":
			result["hasMore"], result["nextOffset"] = false, nil
			if args["view"] == "UNITS" {
				result["schema"], result["unitSummary"] = "ouf.source-unit-inventory.v1", summary
				result["units"] = []railObject{{"unitRef": "sheet:0", "status": "READY"}, {"unitRef": "sheet:1", "status": "READY"}}
			} else {
				if args["unitRef"] != ref.UnitRef || args["view"] != "UNIT" || args["assetId"] != nil {
					t.Error("wrong sheet selector", args)
				}
				result["schema"], result["view"], result["schemaHash"] = "ouf.source-unit-profile.v1", "UNIT", structuralHash
				result["unitRef"], result["status"], result["unitKind"] = ref.UnitRef, "READY", "SHEET"
				result["columnCount"], result["offset"] = 1, 0
				result["columns"] = []railObject{{"ordinal": 0, "name": "name", "sample": "private-fixture"}}
				result["sample"] = "private-fixture"
			}
		case "/managed.file/create", "/managed.file/review-prepare", "/source.revision/prepare":
			raw, _ := json.Marshal(args["unitProfileRef"])
			var got analysis.UnitProfileRef
			if json.Unmarshal(raw, &got) != nil || got != ref || args["assetId"] != nil || args["profileId"] != nil {
				t.Error("lost unit selection", args)
			}
			result = railObject{"sourceId": "theatres", "onboardingVersionId": unitProfile, "lockVersion": 1}
			if r.URL.Path == "/source.revision/prepare" {
				if r.Header.Get("If-Match") != `W/"ov:7"` || args["changes"] != nil || args["assetRef"] != nil {
					t.Error("revision replacement lost its precondition", args)
				}
				result["state"] = "IN_REVIEW"
			} else if r.URL.Path == "/managed.file/create" {
				result["workflowState"], result["allowedActions"], result["reasonCode"] = "DRAFT", []string{"PREPARE_REVIEW"}, "EXECUTION_PROPOSAL_REQUIRED"
			} else {
				if !reflect.DeepEqual(args["executionProposal"], proposal) {
					t.Error("altered execution proposal", args)
				}
				result["workflowState"] = "IN_REVIEW"
			}
		default:
			t.Error("unexpected route", r.URL.Path)
		}
		json.NewEncoder(w).Encode(result)
	}))
	defer gateway.Close()
	client, err := httpclient.NewGateway(gateway.URL, "workload")
	if err != nil {
		t.Fatal(err)
	}
	client.Client = gateway.Client()
	media := (hostfiles.Input{FileName: "fixture.XLSX"}).MediaType()
	raw, _ := json.Marshal(railObject{"mediaType": media})
	staged, err := client.Execute(ctx, orchestration.GatewayRequest{CapabilityID: "ouf.managed-source.file.upload", Arguments: raw,
		Identity: orchestration.Identity{Delegation: "fixture"}, IdempotencyKey: "upload:file_test",
		Upload: &orchestration.UploadStream{Reader: bytes.NewReader(file), Size: int64(len(file)), SHA256: fmt.Sprintf("sha256:%x", sha256.Sum256(file)), FileID: "file_test"}}, time.Second)
	if err != nil || staged.Status != 200 {
		t.Fatal("upload", err, staged.Status)
	}
	store := &completedUnitAnalysis{}
	service := &orchestration.Service{Auth: analysisSDKAuth{}, Admission: store, Gateway: client, RequireDelegation: true, FingerprintKey: make([]byte, 32)}
	session := revisionSession(t, service, "ouf.managed-source.preview ouf.managed-source.onboarding.review.prepare ouf.onboarding.revision.prepare")
	call := func(name string, args railObject) railObject {
		t.Helper()
		res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
		if err != nil || res.IsError {
			t.Fatalf("%s: %v %v", name, res, err)
		}
		wire, _ := json.Marshal(res)
		if strings.Contains(string(wire), "private-fixture") {
			t.Fatal("sample escaped projection")
		}
		return ownerObject(res)
	}
	var asset railObject
	if json.Unmarshal(staged.Body, &asset) != nil {
		t.Fatal("invalid staged asset")
	}
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	upload, _ := analysis.Find(m, "source.file.upload")
	e := nextstep.Engine{Valid: func(tool string, args nextstep.Object) bool {
		return tool == "source.file.profile" && args["assetId"] == unitAsset
	}}
	first := e.Evaluate(upload, m.NextPolicies[upload.NextPolicy], nil, asset, false)
	profileArgs := first["arguments"].(railObject)
	profileArgs["readOptions"] = options
	next := call(first["tool"].(string), profileArgs)["next"].(railObject)
	follow := func(target string, next railObject) railObject {
		t.Helper()
		if next["tool"] != target {
			t.Fatalf("expected %s in %v", target, next)
		}
		return call(target, next["arguments"].(railObject))
	}
	choose := func(next railObject, target string, index int) railObject {
		t.Helper()
		for _, v := range next["alternatives"].([]any) {
			a := v.(railObject)
			if a["tool"] == target {
				if index == 0 {
					return a
				}
				index--
			}
		}
		t.Fatalf("missing %s in %v", target, next)
		return nil
	}
	inventory := follow("source.file.preview", next)
	sheet := follow("source.file.preview", choose(inventory["next"].(railObject), "source.file.preview", 1))
	view := follow("source.analysis.plan", sheet["next"].(railObject))
	changed := fixtureUnit("sheet:1")
	changed.Version++
	wrong := railObject{"unitProfileRef": changed, "expectedRevision": 0, "work": sheet["next"].(railObject)["arguments"].(railObject)["work"]}
	rejected, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "source.analysis.plan", Arguments: wrong})
	if err != nil || !rejected.IsError || ownerObject(rejected)["code"] != "ONB_UNIT_PROFILE_CHANGED" || store.plans != 1 {
		t.Fatal("stale unit reference accepted", rejected, err)
	}
	create := choose(view["next"].(railObject), "source.onboarding.create", 0)
	fields := create["arguments"].(railObject)
	fields["sourceId"], fields["name"], fields["owner"], fields["targetClassIri"] = "theatres", "Theatres", "Fixture", "urn:test:theatres"
	fields["semanticRefs"], fields["sourceObjectKeyFields"] = []string{"urn:test:theatres@1.0.0"}, []string{"name"}
	fields["fields"] = []railObject{{"fieldName": "name", "extractionDecision": "INCLUDE", "dataAccessLabel": "RESTRICTED", "targetPropertyIri": "urn:test:name"}}
	draft := follow("source.onboarding.create", create)
	prepare := choose(draft["next"].(railObject), "source.onboarding.review.prepare", 0)
	prepare["arguments"].(railObject)["executionProposal"] = proposal
	if got := follow("source.onboarding.review.prepare", prepare); got["workflowState"] != "IN_REVIEW" || store.plans != 1 {
		t.Fatal("review not reached", got, store.plans)
	}
	call("source.onboarding.revision.prepare", railObject{"sourceId": "theatres", "parentVersionId": unitProfile, "expectedVersion": 7,
		"configurationHash": structuralHash, "commandKey": "fixture-unit-replacement", "publicationEffect": "NEW_PUBLICATION_MAY_START_ACQUISITION", "unitProfileRef": ref})
}
