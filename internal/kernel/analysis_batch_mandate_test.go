package kernel

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

var mandateTestEpoch = analysis.Digest([]any{"fixture-bundle", int64(1)})

type mandateTestAuth struct {
	orchestration.AuthorizationPort
}

func (a mandateTestAuth) Authorize(ctx context.Context, in orchestration.AuthorizationRequest) (orchestration.AuthorizationDecision, error) {
	decision, err := a.AuthorizationPort.Authorize(ctx, in)
	decision.BundleID, decision.BundleVersion = "fixture-bundle", 1
	return decision, err
}

func testMandateBinding(domain string, data any) string {
	raw, _ := json.Marshal(data)
	payload := base64.RawURLEncoding.EncodeToString(raw)
	signature := hmac.New(sha256.New, []byte("synthetic-fixture-key"))
	_, _ = signature.Write([]byte(domain + "." + payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(signature.Sum(nil))
}

type mandateTestIssuer struct{}

func (*mandateTestIssuer) Prepare(_ context.Context, proof string) (analysis.BatchMandateView, error) {
	binding, ok := analysis.DecodeBatchStartBinding(proof)
	if !ok || proof != testMandateBinding("start-proof", binding) {
		return analysis.BatchMandateView{}, orchestration.ErrUnauthorized
	}
	return analysis.BatchMandateView{Ref: binding.StartRef, Generation: 1, State: "PENDING",
		ExpiresAtMs: time.Now().Add(time.Minute).UnixMilli(), StartBinding: testMandateBinding("start-binding", binding)}, nil
}

func (*mandateTestIssuer) Issue(_ context.Context, in analysis.BatchMandateIssue) (analysis.BatchMandateView, error) {
	binding, ok := analysis.DecodeBatchStartBinding(in.StartBinding)
	if !ok || in.StartBinding != testMandateBinding("start-binding", binding) ||
		in.StartProof != testMandateBinding("start-proof", binding) || in.CommandKey != binding.CommandKey || in.Ref != binding.StartRef ||
		in.PolicyEpoch != mandateTestEpoch {
		return analysis.BatchMandateView{}, orchestration.ErrUnauthorized
	}
	return testActiveMandate(in), nil
}

func testActiveMandate(in analysis.BatchMandateIssue) analysis.BatchMandateView {
	id := uuid.MustParse(in.Ref)
	id[15] ^= 1
	view := analysis.BatchMandateView{Ref: id.String(), Generation: 1, Version: 1, State: "ACTIVE", AnalysisID: in.AnalysisID,
		ExpiresAtMs: time.Now().Add(24 * time.Hour).UnixMilli()}
	view.MandateBinding = testMandateBinding("mandate", struct {
		analysis.BatchMandateView
		StartRef string `json:"startRef"`
	}{view, in.Ref})
	return view
}

func (*mandateTestIssuer) Read(context.Context, analysis.BatchMandateKey) (analysis.BatchMandateView, error) {
	return analysis.BatchMandateView{}, analysis.ErrUnavailable
}
func (*mandateTestIssuer) Expire(context.Context, analysis.BatchMandateKey, int) (analysis.BatchMandateView, error) {
	return analysis.BatchMandateView{}, analysis.ErrUnavailable
}
func (*mandateTestIssuer) Dispatch(context.Context, analysis.BatchMandateDispatch, int64) (orchestration.GatewayResponse, string, error) {
	return orchestration.GatewayResponse{}, "", analysis.ErrUnavailable
}

func mandateTestIngress(t *testing.T, issuer analysis.BatchMandateGateway, identity analysis.Scope, granted string) func(http.Handler) http.Handler {
	t.Helper()
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(raw))
			var call struct {
				Params struct {
					Name      string
					Arguments analysis.BatchStartBinding
				}
			}
			if json.Unmarshal(raw, &call) == nil && call.Params.Name == "source.analysis.plan" && call.Params.Arguments.Mode == analysis.BatchMode {
				b := call.Params.Arguments
				b.Installation, b.Workload, b.Issuer, b.Audience = "fixture", "mcp-workload", "issuer", "gateway"
				b.Client, b.Subject, b.Tenant = identity.Service, identity.Principal, identity.Tenant
				b.Scope = strings.Fields(granted)
				slices.Sort(b.Scope)
				b.StartRef = b.CommandKey
				r.Header.Set(analysis.BatchStartHeader, testMandateBinding("start-proof", b))
			}
			next.ServeHTTP(w, r.WithContext(analysis.WithBatchMandateGateway(r.Context(), issuer)))
		})
	}
}
