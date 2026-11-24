package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

type RunStore interface {
	CheckRunCommand(context.Context, Scope, string, string, string) error
	BeginRun(context.Context, RunSpec) (RunRecord, error)
	ReadRun(context.Context, Scope, string, string) (RunRecord, error)
	AttachMandate(context.Context, RunSpec, []byte) (RunRecord, error)
	ClaimRunIssue(context.Context, RunRecord) (RunRecord, int, error)
	RecordIssueFailure(context.Context, RunRecord, string, time.Time) error
}

func (c Coordinator) RunPolicy(ctx context.Context, i orchestration.Identity) (string, error) {
	batch, ok := Find(c.Manifest, BatchTool)
	if !ok || batch.PublicationState != "ACTIVE" {
		return "", ErrUnavailable
	}
	policy, _, err := c.publishedScope(ctx, i, batch)
	return policy, err
}

func (c Coordinator) StartRun(ctx context.Context, i orchestration.Identity, cfg RunInstallation, asset, profile, key string) (RunRecord, error) {
	var r RunRecord
	store, ok := c.Store.(RunStore)
	if !ok || !cfg.Enabled || !cfg.Limits.OwnerDedupQualified || c.Service.AnalysisMandates == nil {
		return r, ErrUnavailable
	}
	if !CanonicalUUID(asset) || !CanonicalUUID(profile) || !CanonicalUUID(key) || key[14] != '4' || i.Issuer == "" {
		return r, ErrInvalid
	}
	policy, err := c.RunPolicy(ctx, i)
	if err != nil {
		return r, err
	}
	scope := IdentityScope(i)
	id := AnalysisID(scope, Profile{AssetID: asset, ProfileID: profile})
	if err = store.CheckRunCommand(ctx, scope, i.Issuer, key, id); err != nil {
		return r, err
	}
	existing, err := store.ReadRun(ctx, scope, i.Issuer, id)
	pin, sourceFile := existing.Profile, existing.SourceFile
	if errors.Is(err, ErrScope) {
		// A single interactive owner read binds issue to the immutable
		// profile. Subsequent page reads are mediated by the mandate.
		pin, sourceFile, err = c.runProfile(ctx, i, asset, profile)
	}
	if err != nil {
		return r, err
	}
	spec := RunSpec{ProposalSeconds: cfg.Renewal.ProposalSeconds, SourceFile: sourceFile, Onboarding: c.Onboarding, Scope: scope, Issuer: i.Issuer, Workload: cfg.Workload, CommandKey: key, Manifest: cfg.Manifest, Policy: policy, Profile: pin, Limits: cfg.Limits}
	r, err = store.BeginRun(ctx, spec)
	if err != nil {
		return r, err
	}
	if r.State != "ISSUING" {
		if r.Mandate.ID == "" {
			return r, issueError(r.Reason)
		}
		return r, nil
	}
	var n int
	r, n, err = store.ClaimRunIssue(ctx, r)
	if errors.Is(err, ErrFenced) {
		current, e := store.ReadRun(ctx, scope, i.Issuer, id)
		if e == nil && current.Mandate.ID != "" && current.RunSpec == r.RunSpec {
			return current, nil
		}
	}
	if err != nil {
		return r, err
	}
	body, _ := json.Marshal(map[string]any{"contract": MandateContract, "analysisId": id, "profile": r.Profile, "manifestChecksum": r.Manifest, "commandKey": r.CommandKey})
	response, err := c.Service.AnalysisMandates.MandateCall(ctx, "issue", body, i.Delegation, 8192, 3*time.Second)
	// A crash here leaves an ISSUING intent. A later HUMAN request uses
	// the same Gateway command; a classified refusal is stored durably.
	code := "ANALYSIS_AUTHORITY_UNAVAILABLE"
	var retry time.Time
	if errors.Is(err, orchestration.ErrMandateTransportUncertain) {
		code = ErrRunPressure.Error()
		retry = RunRetryAt(time.Now(), n, "")
	}
	if err == nil {
		var problem struct{ Code string }
		_ = json.Unmarshal(response.Body, &problem)
		switch {
		case response.Status == 200 || response.Status == 201:
			attached, e := store.AttachMandate(ctx, r.RunSpec, response.Body)
			if !errors.Is(e, ErrInvalid) && !errors.Is(e, ErrRunDrift) {
				return attached, e
			}
			code = ErrRunDrift.Error()
		case response.Status == 403:
			code = "ANALYSIS_AUTHORITY_DENIED"
		case response.Status == 409 && problem.Code == "ANALYSIS_COMMAND_CONFLICT":
			code = ErrCommandConflict.Error()
		case response.Status == 410:
			code = ErrMandateExpired.Error()
		case response.Status == 429 && problem.Code == "ANALYSIS_BACKOFF", response.Status == 503 && problem.Code == "ANALYSIS_AUTHORITY_UNAVAILABLE":
			code = ErrRunPressure.Error()
			retry = RunRetryAt(time.Now(), n, response.RetryAfter)
		}
	}
	if e := store.RecordIssueFailure(ctx, r, code, retry); e != nil {
		return r, e
	}
	return r, issueError(code)
}

func issueError(code string) error {
	switch code {
	case "ANALYSIS_AUTHORITY_DENIED":
		return orchestration.ErrUnauthorized
	case "ANALYSIS_SCOPE_DRIFT":
		return ErrRunDrift
	case "ANALYSIS_COMMAND_CONFLICT":
		return ErrCommandConflict
	case "ANALYSIS_BACKOFF":
		return ErrRunPressure
	case "ANALYSIS_MANDATE_EXPIRED":
		return ErrMandateExpired
	case "ANALYSIS_BUDGET_EXHAUSTED":
		return ErrRunExhausted
	default:
		return ErrIssueUnavailable
	}
}

var ErrIssueUnavailable = errors.New("ANALYSIS_AUTHORITY_UNAVAILABLE")

func RunRetryAt(now time.Time, attempt int, header string) time.Time {
	delay := []time.Duration{5, 15, 60, 300, 900}[min(max(attempt-1, 0), 4)] * time.Second
	when := now.Add(delay + time.Duration(rand.Int64N(int64(delay)/5+1)))
	if owner := OwnerRetryAfter(header, now); owner.After(when) {
		return owner
	}
	return when
}

func (c Coordinator) runProfile(ctx context.Context, i orchestration.Identity, asset, profile string) (Profile, string, error) {
	cap, ok := Find(c.Manifest, "source.file.preview")
	if !ok || cap.PublicationState != "ACTIVE" {
		return Profile{}, "", ErrUnavailable
	}
	sum, err := c.Manifest.Checksum()
	if err != nil {
		return Profile{}, "", err
	}
	raw, _ := json.Marshal(map[string]any{"assetId": asset, "profileId": profile, "view": "COLUMNS", "offset": 0, "limit": 1})
	reply, err := c.Service.Call(ctx, orchestration.Invocation{Identity: i, CapabilityID: cap.CapabilityID, Owner: cap.Owner, OperationClass: "READ",
		GatewayBindingRef: cap.GatewayBindingRef, ManifestChecksum: sum, Arguments: raw, IdempotencyKey: uuid.NewString(), CorrelationID: uuid.NewString(),
		Window: orchestration.AdmissionWindow, Timeout: 3 * time.Second, RetryThreshold: orchestration.AdmissionRetryThreshold,
		Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 1, ResultBytes: 1 << 20}})
	if err != nil {
		return Profile{}, "", err
	}
	if reply.Problem != nil && reply.Problem.Status == 403 {
		return Profile{}, "", orchestration.ErrUnauthorized
	}
	if reply.Problem != nil || reply.Replay {
		return Profile{}, "", ErrUnavailable
	}
	p, _, err := previewPage(reply.Body, asset, profile, Profile{}, 0, 1)
	var metadata struct {
		FileName string `json:"fileName"`
	}
	if err == nil {
		_ = json.Unmarshal(reply.Body, &metadata)
	}
	return p, sourceBasename(metadata.FileName), err
}
