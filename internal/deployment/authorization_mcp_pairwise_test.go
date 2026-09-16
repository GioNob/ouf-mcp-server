package deployment

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

type authorizationSDKSchema struct {
	Properties map[string]struct {
		Required   []string `json:"required"`
		Properties map[string]struct {
			Enum []string `json:"enum"`
		} `json:"properties"`
	} `json:"properties"`
	Decision struct {
		Required []string `json:"required"`
	} `json:"x-ouf-decision"`
	IntegrationGate struct {
		ScenarioNeutral bool     `json:"scenarioNeutral"`
		External        []string `json:"external"`
	} `json:"x-ouf-integration-gate"`
}

type mcpManifest struct {
	Capabilities []struct {
		CapabilityID                  string `json:"capabilityId"`
		RequiredAuthorizationCapability string `json:"requiredAuthorizationCapability"`
		OperationClass                string `json:"operationClass"`
	} `json:"capabilities"`
}

type cachedAuthorizationFixture struct {
	requiredScope string
	decisionRef   string
}

func (f cachedAuthorizationFixture) Authorize(_ context.Context, in orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	if in.Identity.Issuer == "" || in.Identity.Audience == "" || in.Identity.AuthenticationContextRef == "" {
		return orchestration.AuthorizationDecision{}, errors.New("trusted principal context incomplete")
	}
	found := false
	for _, scope := range in.Identity.Scopes {
		if scope == f.requiredScope {
			found = true
			break
		}
	}
	if !found {
		return orchestration.AuthorizationDecision{Allowed: false, DecisionRef: f.decisionRef}, nil
	}
	return orchestration.AuthorizationDecision{Allowed: true, DecisionRef: f.decisionRef}, nil
}

type pairwiseAdmission struct {
	decisionRef string
}

func (a *pairwiseAdmission) Reserve(_ context.Context, in orchestration.AdmissionRequest) (orchestration.AdmissionDecision, error) {
	a.decisionRef = in.AuthorizationDecisionRef
	return orchestration.AdmissionDecision{AttemptID: in.AttemptID, LockVersion: 1}, nil
}
func (*pairwiseAdmission) Dispatch(context.Context, uuid.UUID, string, time.Duration, int64) error { return nil }
func (*pairwiseAdmission) Reconcile(context.Context, uuid.UUID, orchestration.Cost, orchestration.AttemptOutcome) error {
	return nil
}

type pairwiseGateway struct {
	decisionRef string
}

func (g *pairwiseGateway) Execute(_ context.Context, in orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	g.decisionRef = in.AuthorizationDecisionRef
	return orchestration.GatewayResponse{Status: 200, Body: []byte(`{"ok":true}`)}, nil
}

func TestAuthorizationMCPPairwise(t *testing.T) {
	root := os.Getenv("AUTHORIZATION_PAIRWISE_ROOT")
	if root == "" {
		t.Skip("AUTHORIZATION_PAIRWISE_ROOT not set")
	}

	raw, err := os.ReadFile(filepath.Join(root, "contracts", "authorization", "authorization-sdk-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var sdk authorizationSDKSchema
	if err := json.Unmarshal(raw, &sdk); err != nil {
		t.Fatal(err)
	}
	principal := sdk.Properties["principal"]
	for _, field := range []string{"subjectId", "tenantId", "actorType", "authenticationContextRef", "issuer", "audience", "scopes"} {
		if !contains(principal.Required, field) {
			t.Fatalf("Authorization SDK principal contract missing required field %s", field)
		}
	}
	for _, actor := range []string{"HUMAN", "SERVICE", "AI_AGENT"} {
		if !contains(principal.Properties["actorType"].Enum, actor) {
			t.Fatalf("Authorization SDK actor vocabulary missing %s", actor)
		}
	}
	resource := sdk.Properties["resource"]
	for _, field := range []string{"resourceType", "tenantId", "attributes"} {
		if !contains(resource.Required, field) {
			t.Fatalf("Authorization SDK resource contract missing required field %s", field)
		}
	}
	for _, field := range []string{"allowed", "decisionCode", "decisionRef", "bundleId", "bundleVersion"} {
		if !contains(sdk.Decision.Required, field) {
			t.Fatalf("Authorization decision contract missing %s", field)
		}
	}
	if !sdk.IntegrationGate.ScenarioNeutral {
		t.Fatal("Authorization SDK must remain scenario-neutral before IAM A/B/C selection")
	}

	manifestRaw, err := os.ReadFile(filepath.Join("..", "manifest", "capabilities.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest mcpManifest
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatal(err)
	}
	var requiredScope, operationClass string
	for _, capability := range manifest.Capabilities {
		if capability.CapabilityID == "ouf.system.status" {
			requiredScope = capability.RequiredAuthorizationCapability
			operationClass = capability.OperationClass
			break
		}
	}
	if requiredScope == "" || operationClass == "" {
		t.Fatal("MCP manifest does not expose Authorization vocabulary for ouf.system.status")
	}

	identity := orchestration.Identity{
		ServicePrincipalID:       "service:mcp",
		PrincipalID:              "user:pairwise",
		TenantID:                 "tenant:pairwise",
		ActorType:                "HUMAN",
		AuthenticationContextRef: "acr:mfa",
		Issuer:                   "https://issuer.example.invalid",
		Audience:                 "ouf",
		Scopes:                   []string{requiredScope},
	}
	decisionRef := "policy-bundle-pairwise:7:ouf.system.status"
	admission := &pairwiseAdmission{}
	gateway := &pairwiseGateway{}
	service := orchestration.Service{
		Auth:           cachedAuthorizationFixture{requiredScope: requiredScope, decisionRef: decisionRef},
		Admission:      admission,
		Gateway:        gateway,
		FingerprintKey: []byte("01234567890123456789012345678901"),
	}
	_, err = service.Call(context.Background(), orchestration.Invocation{
		Identity:           identity,
		CapabilityID:       "ouf.system.status",
		Owner:              "mcp",
		OperationClass:     operationClass,
		GatewayBindingRef:  "capability://ouf.system.status",
		ManifestChecksum:   "sha256:pairwise",
		Arguments:          json.RawMessage(`{}`),
		IdempotencyKey:     "pairwise-idempotency",
		CorrelationID:      "pairwise-correlation",
		Window:             time.Minute,
		Timeout:            time.Second,
		RetryThreshold:     1,
		Maximum:            orchestration.Cost{ToolCalls: 1, DistinctObjects: 20, ResultBytes: 1024},
	})
	if err != nil {
		t.Fatal(err)
	}
	if admission.decisionRef != decisionRef || gateway.decisionRef != decisionRef {
		t.Fatalf("Authorization decisionRef was not preserved across MCP admission/Gateway: admission=%q gateway=%q", admission.decisionRef, gateway.decisionRef)
	}
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
