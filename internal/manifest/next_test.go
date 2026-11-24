package manifest

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestInputConstraintMatchesPublishedOneOfBranches(t *testing.T) {
	s, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"ouf.operations.incidents":          {"()", "(snapshot=true)", "(cursor)", "(coverage=true, snapshot=true)", "(coverage=true, cursor)", "(resumeFrom)"},
		"authorization.permissions.read":    {"(view=ROLES)", "(view=CAPABILITIES)", "(subjectId)", "(externalRoleRef)"},
		"authorization.permissions.propose": {"(grantId, operation, reason)", "(operation=REPLACE_ROLES, reason, roleCatalogue)"},
		"semantic.propose": {
			"(artifactType, authorityRef, commandKey, definition, labels, localName, namespace, ownerRef, semanticId, semanticVersion)",
			"(artifactType=ONTOLOGY, authorityRef, commandKey, labels, localName, mediaType=text/turtle, namespace, ownerRef, rdfTurtle, semanticId, semanticVersion)",
		},
		"source.file.preview":             {"(assetId, profileId)", "(assetId, jobId)", "(view=ASSETS)", "(assetId, profileId, view=COLUMNS)", "(assetId, distinctValuesCursor, profileId, view=COLUMNS)"},
		"source.onboarding.read":          {"(onboardingVersionId, sourceId)", "(sourceId, view=LATEST)"},
		"source.onboarding.revision.read": {"(onboardingVersionId, sourceId)", "(commandKey, parentVersionId, sourceId)"},
	}
	for _, c := range s.ToolEligible() {
		var shape struct{ OneOf []json.RawMessage }
		if err := json.Unmarshal(c.InputSchema, &shape); err != nil {
			t.Fatal(err)
		}
		desc := c.Description()
		if !strings.HasSuffix(desc, FollowNext) {
			t.Fatalf("%s missing next instruction", c.ToolName)
		}
		if len(shape.OneOf) == 0 {
			if strings.Contains(desc, "Input constraint:") {
				t.Fatal(c.ToolName, "unexpected union description")
			}
			continue
		}
		forms := InputForms(c.InputSchema)
		if !reflect.DeepEqual(forms, want[c.ToolName]) || !strings.Contains(desc, "Input constraint: "+strings.Join(forms, " or ")+".") {
			t.Errorf("%s: %v", c.ToolName, forms)
		}
		delete(want, c.ToolName)
	}
	if len(want) != 0 {
		t.Fatal("missing published unions", want)
	}
}

func TestWaitSchemaAndRuleChecksum(t *testing.T) {
	raw, err := os.ReadFile("../../contracts/ouf-next-step-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract struct {
		Wait struct{ InputSchema any } `json:"x-ouf-wait-tool"`
	}
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatal(err)
	}
	s, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range s.ToolEligible() {
		if c.ToolName == "ouf.wait" {
			var input any
			if json.Unmarshal(c.InputSchema, &input) != nil || !reflect.DeepEqual(input, contract.Wait.InputSchema) || c.Owner != "mcp" || c.OperationClass != "READ" {
				t.Fatal("wait descriptor differs from contract")
			}
		}
	}
	digest, _ := s.Checksum()
	s.NextPolicies["udp"].Rules[0].Reason = "CHANGED_FOR_TEST"
	modified, _ := s.Checksum()
	if digest == modified {
		t.Fatal("next policies missing from manifest checksum")
	}
}
