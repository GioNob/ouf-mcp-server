package orchestration

import (
	"context"
	"errors"
	"testing"
	"time"
)

func semanticReadRequest() AdmissionRequest {
	return AdmissionRequest{BudgetProfile: SemanticReadBudgetProfile, CapabilityID: "ouf.semantic.search", OperationClass: "READ", ManifestChecksum: "manifest", Window: time.Minute, RetryThreshold: 3, Maximum: Cost{ToolCalls: 1, ResultBytes: 262144, DistinctObjects: 100}}
}

func TestSemanticReadBudgetProfile(t *testing.T) {
	for _, capability := range []string{"ouf.semantic.search", "ouf.semantic.consultation.read"} {
		request := semanticReadRequest()
		request.CapabilityID = capability
		key, budget, err := AdmissionWindowBudget(request)
		if err != nil || key != "manifest:"+SemanticReadBudgetProfile || budget != (Cost{ToolCalls: 60, ResultBytes: 15 << 20, DistinctObjects: 6000}) {
			t.Fatal(key, budget, err)
		}
	}
	request := semanticReadRequest()
	request.BudgetProfile = ""
	request.CapabilityID = "ouf.semantic.propose"
	request.OperationClass = "COMMAND"
	key, budget, err := AdmissionWindowBudget(request)
	if err != nil || key != "manifest" || budget.ToolCalls != 3 {
		t.Fatal("command window changed", key, budget, err)
	}
}

func TestSemanticReadBudgetRejectsUnreviewedProfiles(t *testing.T) {
	changes := map[string]func(*AdmissionRequest){
		"command":          func(r *AdmissionRequest) { r.OperationClass = "COMMAND" },
		"other capability": func(r *AdmissionRequest) { r.CapabilityID = "ouf.managed-source.file.profile" },
		"control":          func(r *AdmissionRequest) { r.CapabilityID = "ouf.operations.incidents" },
		"empty manifest":   func(r *AdmissionRequest) { r.ManifestChecksum = "" },
		"window":           func(r *AdmissionRequest) { r.Window = time.Hour },
		"retries":          func(r *AdmissionRequest) { r.RetryThreshold = 60 },
		"calls":            func(r *AdmissionRequest) { r.Maximum.ToolCalls = 2 },
		"bytes":            func(r *AdmissionRequest) { r.Maximum.ResultBytes = 262145 },
		"zero bytes":       func(r *AdmissionRequest) { r.Maximum.ResultBytes = 0 },
		"objects":          func(r *AdmissionRequest) { r.Maximum.DistinctObjects = 101 },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			request := semanticReadRequest()
			change(&request)
			if _, _, err := AdmissionWindowBudget(request); err == nil {
				t.Fatal("unreviewed request admitted")
			}
		})
	}
}

type semanticReadAdmissionCapture struct {
	fakeAdmission
	profile string
}

func (s *semanticReadAdmissionCapture) Reserve(ctx context.Context, in AdmissionRequest) (AdmissionDecision, error) {
	s.profile = in.BudgetProfile
	return s.fakeAdmission.Reserve(ctx, in)
}

func TestSemanticReadBudgetServiceSelectionAndAuthorization(t *testing.T) {
	for _, capability := range []string{"ouf.semantic.search", "ouf.semantic.consultation.read"} {
		for _, allowed := range []bool{true, false} {
			in := invocation()
			in.CapabilityID = capability
			in.Owner = "semantic"
			in.OperationClass = "READ"
			in.Maximum = Cost{ToolCalls: 1, ResultBytes: 262144, DistinctObjects: 100}
			admission := &semanticReadAdmissionCapture{fakeAdmission: fakeAdmission{err: ErrToolSelectionStall}}
			gateway := &fakeGateway{}
			_, err := (Service{Auth: &fakeAuth{allowed}, Admission: admission, Gateway: gateway, FingerprintKey: make([]byte, 32)}).Call(context.Background(), in)
			if gateway.calls != 0 {
				t.Fatal("refused read reached gateway")
			}
			if allowed && (!errors.Is(err, ErrToolSelectionStall) || admission.profile != SemanticReadBudgetProfile) {
				t.Fatal("profile not selected", admission.profile, err)
			}
			if !allowed && (!errors.Is(err, ErrUnauthorized) || admission.reserves != 0) {
				t.Fatal("authorization did not precede admission", err)
			}
		}
	}
}

func TestSemanticReadBudgetRetryTiming(t *testing.T) {
	cause := errors.New("budget")
	now := time.Date(2026, 11, 7, 20, 7, 42, 100000000, time.UTC)
	end := now.Truncate(time.Minute).Add(time.Minute)
	err := NewSemanticReadBudgetExhaustedError(cause, "TOOL_CALLS", now, end)
	var exhausted *SemanticReadBudgetExhaustedError
	if !errors.Is(err, cause) || !errors.As(err, &exhausted) || exhausted.RetryAfterSeconds != 18 || !exhausted.WindowEnd.Equal(end) {
		t.Fatal(err, exhausted)
	}
}
