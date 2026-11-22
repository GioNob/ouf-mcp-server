package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
)

type linkLookup func(context.Context, nextstep.Object) nextstep.LinkResult

func classifyLink(capability string, response orchestration.Result, err error) nextstep.LinkResult {
	denial := func(reason string) nextstep.LinkResult {
		return nextstep.LinkResult{Status: nextstep.LinkDenied, Capability: capability, Reason: reason}
	}
	if errors.Is(err, orchestration.ErrUnauthorized) {
		var denied *orchestration.AuthorizationDeniedError
		if errors.As(err, &denied) {
			return denial(orchestration.SafeDenialReason(denied.ReasonCode, "AUTHORIZATION_DENIED"))
		}
		return denial("AUTHORIZATION_DENIED")
	}
	if err != nil {
		var window *orchestration.WindowBudgetExhaustedError
		if errors.As(err, &window) {
			return nextstep.LinkResult{Status: nextstep.LinkTransient, RetryAfter: fmt.Sprint(window.RetryAfterSeconds)}
		}
		if transientNetworkError(err) {
			return nextstep.LinkResult{Status: nextstep.LinkTransient}
		}
		return nextstep.LinkResult{}
	}
	if p := response.Problem; p != nil {
		switch p.Status {
		case 401, 403:
			return denial(orchestration.SafeDenialReason(p.Code, "OWNER_AUTHORIZATION_DENIED"))
		case 408, 429, 502, 503, 504:
			var flags struct{ Retryable *bool }
			_ = json.Unmarshal(response.Body, &flags)
			if flags.Retryable == nil || *flags.Retryable {
				return nextstep.LinkResult{Status: nextstep.LinkTransient, RetryAfter: p.RetryAfter}
			}
		}
		return nextstep.LinkResult{}
	}
	var link struct{ ReviewURL, ExpiresAt string }
	if response.Replay || json.Unmarshal(response.Body, &link) != nil {
		return nextstep.LinkResult{}
	}
	return nextstep.OwnerLink(link.ReviewURL, link.ExpiresAt, time.Now())
}

const (
	recoveryLinkID = "ouf.onboarding.ths-links.ingestion-run-recovery"
	semanticLinkID = "ouf.onboarding.ths-links.semantic-review"
)

func (s *workflowServer) bindTHSLinks(service *orchestration.Service) error {
	checksum, err := s.manifest.Checksum()
	if err != nil {
		return err
	}
	s.thsLinks = make(map[string]linkLookup)
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
		s.thsLinks[c.CapabilityID] = func(ctx context.Context, args nextstep.Object) nextstep.LinkResult {
			identity, ok := ctx.Value(identityKey{}).(requestIdentity)
			if !ok || identity.ActorType != "HUMAN" || identity.Delegation == "" {
				reason := "DELEGATION_REQUIRED"
				if ok && identity.Delegation != "" {
					reason = "ACTOR_NOT_ALLOWED"
				}
				return nextstep.LinkResult{Status: nextstep.LinkDenied, Capability: c.RequiredAuthorization, Reason: reason}
			}
			raw, err := json.Marshal(args)
			var normalized nextstep.Object
			if err != nil || json.Unmarshal(raw, &normalized) != nil || input.Validate(normalized) != nil {
				return nextstep.LinkResult{}
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
			return classifyLink(c.RequiredAuthorization, response, err)
		}
	}
	if len(s.thsLinks) != 2 {
		return fmt.Errorf("missing internal THS link capability")
	}
	return nil
}
