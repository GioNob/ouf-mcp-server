package manifest

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
)

// The fixture is shared verbatim with the Gateway contract. Compare keywords,
// references and required lists before resolving them against the tool root.
func TestExtractionUnitArgumentFragments(t *testing.T) { // MCP-62E-03
	var contract struct {
		Defs  map[string]any            `json:"$defs"`
		Tools map[string]map[string]any `json:"tools"`
	}
	raw, err := os.ReadFile("testdata/contract-extraction-unit-v1-schemas.json")
	if err != nil || json.Unmarshal(raw, &contract) != nil {
		t.Fatal("contract fixture unavailable", err)
	}
	m, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	ref := func(name string) any { return map[string]any{"$ref": "#/$defs/" + name} }
	count := 0
	for _, c := range m.Capabilities {
		delta, ok := contract.Tools[c.ToolName]
		if !ok {
			continue
		}
		count++
		t.Run(c.ToolName, func(t *testing.T) {
			equal := func(got, want any) {
				t.Helper()
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %v, want %v", got, want)
				}
			}
			var s map[string]any
			if err := json.Unmarshal(c.InputSchema, &s); err != nil {
				t.Fatal(err)
			}
			defs := s["$defs"].(map[string]any)
			props := s["properties"].(map[string]any)
			for k, v := range defs {
				if expected, ok := contract.Defs[k]; ok {
					equal(v, expected)
				}
			}
			switch c.ToolName {
			case "source.file.upload":
				file := props["file"].(map[string]any)["properties"].(map[string]any)
				equal(file["mime_type"], ref("uploadMediaType"))
				equal(defs["uploadMediaType"], contract.Defs["uploadMediaType"])
			case "source.file.profile":
				equal(props["readOptions"], ref("xlsxReadOptions"))
				equal(defs["xlsxReadOptions"], contract.Defs["xlsxReadOptions"])
			case "source.file.preview":
				branches := s["oneOf"].([]any)
				equal(props["unitRef"], ref("unitRef"))
				equal(defs["unitRef"], contract.Defs["unitRef"])
				equal(defs["uuid"], contract.Defs["uuid"])
				equal(branches[5:], delta["newOneOfBranches"])
				// Digest only the five unchanged historical branches.
				b, _ := json.Marshal(branches[:5])
				if fmt.Sprintf("%x", sha256.Sum256(b)) != "6e237f6562fe96099883e4102c4a5b6994e7378feac31de58c684ea1aa31b717" {
					t.Fatal("legacy preview branches changed")
				}
			default:
				equal(props["unitProfileRef"], ref("unitProfileRef"))
				for _, name := range []string{"uuid", "unitRef", "unitProfileRef"} {
					equal(defs[name], contract.Defs[name])
				}
				if v, ok := delta["oneOf"]; ok {
					equal(s["oneOf"], v)
				}
				if v, ok := delta["anyOf"]; ok {
					equal(s["anyOf"], v)
				}
				if c.ToolName == "source.onboarding.review.prepare" {
					arms := props["executionProposal"].(map[string]any)["oneOf"].([]any)
					legacy, unit := arms[0].(map[string]any), arms[1].(map[string]any)
					b, _ := json.Marshal(legacy)
					if fmt.Sprintf("%x", sha256.Sum256(b)) != "d1e43087b1940816c90eec6b4769b6d467716a2d19e26c055f3d1c54fc7f4b97" {
						t.Fatal("typed-csv:v2 changed")
					}
					equal(unit["$id"], "urn:ouf:managed-unit-execution-proposal:v1")
					equal(unit["properties"].(map[string]any)["contract"], map[string]any{"const": "ouf.managed-unit-execution-proposal.v1"})
					delete(unit["properties"].(map[string]any), "contract")
					equal(unit["properties"], legacy["properties"])
					equal(unit["required"], append(legacy["required"].([]any), "contract"))
				}
			}
		})
	}
	if count != len(contract.Tools) {
		t.Fatalf("covered %d of %d tools", count, len(contract.Tools))
	}
}
