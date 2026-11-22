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

const recoveryLinkID = "ouf.onboarding.ths-links.ingestion-run-recovery"

func (s *workflowServer) bindRecoveryLink(service *orchestration.Service) error {
	for _, c := range s.manifest.Capabilities {
		if c.CapabilityID != recoveryLinkID {
			continue
		}
		if c.ToolEligible || c.Owner != "onboarding" || c.OperationClass != "READ" || c.PublicationState != "ACTIVE" {
			return fmt.Errorf("invalid recovery link capability")
		}
		var schema jsonschema.Schema
		if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
			return err
		}
		input, err := schema.Resolve(nil)
		if err != nil {
			return err
		}
		checksum, err := s.manifest.Checksum()
		if err != nil {
			return err
		}
		s.runLink = func(ctx context.Context, args nextstep.Object) nextstep.Object {
			identity, ok := ctx.Value(identityKey{}).(requestIdentity)
			if !ok || identity.ActorType != "HUMAN" || identity.Delegation == "" {
				return nil
			}
			raw, err := json.Marshal(args)
			var normalized nextstep.Object
			if err != nil || json.Unmarshal(raw, &normalized) != nil || input.Validate(normalized) != nil {
				return nil
			}
			source := args["sourceId"].(string)
			run := args["runId"].(string)
			if identity.CorrelationID == "" {
				identity.CorrelationID = uuid.NewString()
			}
			resource := orchestration.ResourceContext{
				ResourceType: "source",
				ResourceID:   source,
				TenantID:     identity.TenantID,
				Attributes:   map[string]string{"runId": run},
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
		return nil
	}
	return fmt.Errorf("missing recovery link capability")
}
