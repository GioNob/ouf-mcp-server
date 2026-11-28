package manifest

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestPublishedBatchGatewaySnapshot(t *testing.T) { // MCP-50T-01
	raw, err := os.ReadFile("testdata/semantic-search-batch-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	if input, err := os.ReadFile("../../.codex-input/semantic-search-batch-v1.json"); err == nil {
		if !bytes.Equal(raw, input) {
			t.Fatal("task-51m snapshot drift")
		}
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var want Capability
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	m, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range m.Capabilities {
		if c.CapabilityID != want.CapabilityID {
			continue
		}
		var ceiling, expected struct{ HardMaxResultBytes int64 }
		if json.Unmarshal(c.BaseCostEnvelope, &ceiling) != nil || json.Unmarshal(want.BaseCostEnvelope, &expected) != nil || ceiling != expected || c.MCPClass != "MCP_INTERNAL" {
			t.Fatal("internal batch envelope drift")
		}
		var a, b any
		if json.Unmarshal(c.InputSchema, &a) != nil || json.Unmarshal(want.InputSchema, &b) != nil || !reflect.DeepEqual(a, b) ||
			c.Owner != want.Owner || c.RequiredAuthorization != want.RequiredAuthorization || c.ToolEligible ||
			c.GatewayBindingRef != want.GatewayBindingRef || c.OperationClass != want.OperationClass || !reflect.DeepEqual(c.AllowedActors, want.AllowedActors) {
			t.Fatal("batch dispatch contract differs from Gateway")
		}
		return
	}
	t.Fatal("missing published batch")
}

func TestAnalysisManifestCompatibilityIsPinnedPerPublicationState(t *testing.T) {
	m, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range m.Capabilities {
		if c.ToolName != "semantic.search.batch" {
			continue
		}
		batchDecide := []string{"e7e6f5813ddaa6093f7ca632c8747b5721cf666ceae70ee08870fba02a944cd8", "69068a4131dbbbf64547992769c0dc7459150644da5b159a3dc71b425b711acb", "af7ac12ca605d6668a9ec81e83bd693be7d03ee4c4e2c974e23c3426596f44b9"}
		for index, tc := range []struct{ state, current, service, discovery, history, predecessor string }{
			{"ACTIVE", "e69a3fe9b0247e784852ab9e1e6184230643ae9b4c6776e7faee78a6c7ed6218", servicePredecessorActive,
				"a5daebe093a64669ccd8213a5222f0e7488c2278b6b0614ac29e2db449b3a1e6", "7172623aaf380a14a7833b0ca57e814534afe6a427674f34955f2be606066b19", "0c624454825ebc571690961be309c38d27d78e96ab64e7903ea99c359c4f8daa"},
			{"INACTIVE", "cb5caa1d1962935cbee7ac673eff2ea7730bcc32660919d8e73a434d54906204", "1df860e9f539a0ca13b21c700cca7b30c9fd2bd2019808d4a6cd9bbe332f6de5",
				"c7fb1fb1603afce39efab853676e831870792d78fea4f7507a28aa17177ea8e6", "6148bea615ac223c448c17930a181ca8ea7b93a124d55bcbcc4331712ee4a8cb", "42cdc29936d851ef039488b4ba3488be3aadcb4e03b45906e4de9ae22b424e2f"},
			{"ABSENT", "5ce058ced8a3b35b8e12027c1a358d3a7576b5b7a8bccc1eb8212407bb91a79f", "3e41958b9fabcfbc02d640d82de5fb4211db56931ab6cb36fb31ac9f2ae0ad1b",
				"30e36b213832149b121d7e490e8b412e18cd6abb3626ef7ce9dd361596e01618", "5350664e3cca34f63afe279ff1d3a1e4a0ed25025886b5e428bd651bed35474e", "e7ee7a2c2ed3030133a96f20a7c416bd158c38cc9775cb1b59503842c2f008ab"},
		} {
			if tc.state == "ABSENT" {
				m.Capabilities = append(m.Capabilities[:i], m.Capabilities[i+1:]...)
			} else {
				m.Capabilities[i].PublicationState = tc.state
			}
			sum, err := m.Checksum()
			want := []string{tc.current, tc.service, tc.discovery, tc.history, tc.predecessor, "01d00a53be5614115924b4775e0d098938ebae2e8e713ef8dd5b4c772e520c24", batchDecide[index]}
			if err != nil || sum != tc.current || AnalysisPredecessor(sum) != tc.service || !reflect.DeepEqual(AnalysisManifests(sum), want) {
				t.Fatal(tc.state, sum, err)
			}
		}
		if got := AnalysisManifests("unknown"); !reflect.DeepEqual(got, []string{"unknown"}) {
			t.Fatal(got)
		}
		return
	}
	t.Fatal("batch descriptor missing")
}
