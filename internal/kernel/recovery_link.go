package kernel

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
)

const (
	recoveryLinkID = "ouf.onboarding.ths-links.ingestion-run-recovery"
	semanticLinkID = "ouf.onboarding.ths-links.semantic-review"
)

func (s *workflowServer) bindTHSLinks(service *orchestration.Service) error {
	checksum, err := s.manifest.Checksum()
	if err != nil {
		return err
	}
	s.thsLinks = make(map[string]func(context.Context, nextstep.Object) nextstep.Object)
	for _, c := range s.manifest.Capabilities {
		if c.CapabilityID != recoveryLinkID && c.CapabilityID != semanticLinkID {
			continue
		}
		if c.ToolEligible || c.MCPClass != "MCP_INTERNAL" || c.Owner != "onboarding" || c.OperationClass != "READ" || c.PublicationState != "ACTIVE" {
			return fmt.Errorf("invalid THS link capability %s", c.CapabilityID)
		}
		var schema jsonschema.Schema
		if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
			return err
		}
		input, err := schema.Resolve(nil)
		if err != nil {
			return err
		}
		s.thsLinks[c.CapabilityID] = func(ctx context.Context, args nextstep.Object) nextstep.Object {
			identity, ok := ctx.Value(identityKey{}).(requestIdentity)
			if !ok || identity.ActorType != "HUMAN" || identity.Delegation == "" {
				return nil
			}
			raw, err := json.Marshal(args)
			var normalized nextstep.Object
			if err != nil || json.Unmarshal(raw, &normalized) != nil || input.Validate(normalized) != nil {
				return nil
			}
			if identity.CorrelationID == "" {
				identity.CorrelationID = uuid.NewString()
			}
			resource := orchestration.ResourceContext{TenantID: identity.TenantID}
			if c.CapabilityID == recoveryLinkID {
				resource.ResourceType = "source"
				resource.ResourceID = args["sourceId"].(string)
				resource.Attributes = map[string]string{"runId": args["runId"].(string)}
			} else {
				resource.ResourceType = "semantic.challenge"
				resource.ResourceID = args["challengeId"].(string)
			}
			response, err := service.Call(ctx, orchestration.Invocation{
				Identity:                  identity.Identity,
				CapabilityID:              c.CapabilityID,
				Owner:                     c.Owner,
				OperationClass:            c.OperationClass,
				AuthorizationCapabilityID: c.RequiredAuthorization,
				GatewayBindingRef:         c.GatewayBindingRef,
				ManifestChecksum:          checksum,
				Resource:                  resource,
				Arguments:                 raw,
				CorrelationID:             identity.CorrelationID,
				IdempotencyKey:            uuid.NewString(),
				Window:                    time.Minute,
				Timeout:                   3 * time.Second,
				RetryThreshold:            3,
				Maximum:                   orchestration.Cost{ToolCalls: 1, DistinctObjects: 1, ResultBytes: 8192},
			})
			var link nextstep.Object
			if err != nil || response.Problem != nil || response.Replay || json.Unmarshal(response.Body, &link) != nil {
				return nil
			}
			return link
		}
	}
	if len(s.thsLinks) != 2 {
		return fmt.Errorf("missing internal THS link capability")
	}
	return nil
}
