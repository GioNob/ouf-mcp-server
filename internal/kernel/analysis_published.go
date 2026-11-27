package kernel

import (
	"context"
	"errors"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
)

// Read a small, current continuation inside the already authorized public
// call. Only the next public tool and its validated arguments reach the client.
func (s *workflowServer) publishedNext(ctx context.Context, tool string, args, obs, original nextstep.Object) nextstep.Object {
	if s.analysisService == nil {
		return original
	}
	switch tool {
	case "source.analysis.plan", "source.analysis.step", "semantic.discovery.status", "semantic.discovery.candidates":
	default:
		return original
	}
	if original["tool"] != "source.analysis.read" && !(tool == "source.analysis.plan" && original["action"] == "BLOCKED") {
		return original
	}
	id, _ := args["analysisId"].(string)
	if id == "" && tool == "source.analysis.plan" {
		id, _ = obs["analysisId"].(string)
	}
	if id == "" {
		return original
	}
	summary, ok, err := s.publishedHint(ctx, id, "", false)
	if errors.Is(err, analysis.ErrEpochChanged) {
		return s.engine(ctx).ReplanNext(id, analysis.ErrEpochChanged.Error())
	}
	if err != nil {
		return nextstep.ManualBlock(analysis.SafeError(err), "Verifica lo scope corrente del piano.", "OPERATOR")
	}
	if !ok {
		return original
	}
	return s.engine(ctx).SummaryNext(id, "STATUS", "", summary)
}

type publishedHintStore interface {
	PublishedHint(context.Context, analysis.Scope, string, string, string) (analysis.Summary, bool, error)
}

func (s *workflowServer) publishedHint(ctx context.Context, id, request string, step bool) (analysis.Summary, bool, error) {
	var empty analysis.Summary
	if s.analysisService == nil {
		return empty, false, nil
	}
	store, ok := s.analysisService.Admission.(publishedHintStore)
	if !ok {
		return empty, false, nil
	}
	i, _ := ctx.Value(identityKey{}).(requestIdentity)
	checksum, err := s.manifest.Checksum()
	if err != nil {
		return empty, false, err
	}
	summary, ok, err := store.PublishedHint(analysis.WithOwnerObservationKey(ctx, s.analysisService.FingerprintKey), analysis.IdentityScope(i.Identity), id, checksum, request)
	if !ok || err != nil {
		return summary, ok, err
	}
	storeForPlan, _ := s.analysisService.Admission.(analysis.Store)
	c := analysis.Coordinator{Store: storeForPlan, Service: s.analysisService, Manifest: s.manifest}
	if _, _, err = c.SummaryAccess(ctx, i.Identity); err == nil {
		if err = c.CheckEpoch(ctx, i.Identity, summary.Epoch); err != nil {
			return summary, true, err
		}
		if step {
			err = c.CheckStepScope(ctx, i.Identity, id, checksum, summary.Visibility)
		} else {
			err = c.ScopeError(ctx, i.Identity, id, checksum, c.CheckPublishedScope(ctx, i.Identity, summary.Visibility))
		}
	}
	return summary, true, err
}

func (s *workflowServer) admitPublishedStep(ctx context.Context, cap manifest.Capability, args nextstep.Object, maximum int) (func(context.Context, int, int, bool) error, error) {
	id, _ := args["analysisId"].(string)
	if _, enrolled, err := s.publishedHint(ctx, id, "", true); err != nil || !enrolled {
		return nil, err
	}
	identity, _ := ctx.Value(identityKey{}).(requestIdentity)
	c := analysis.Coordinator{Service: s.analysisService, Manifest: s.manifest}
	checksum, decision, err := c.SummaryAccess(ctx, identity.Identity)
	if err != nil {
		return nil, err
	}
	return admitAnalysisCall(ctx, s.analysisService, identity.Identity, checksum, decision, args, maximum, cap.CapabilityID, cap.OperationClass)
}
