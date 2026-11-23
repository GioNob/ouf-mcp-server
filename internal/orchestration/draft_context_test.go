package orchestration

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

type draftGateway struct{ request GatewayRequest }

func (g *draftGateway) Execute(_ context.Context, request GatewayRequest, _ time.Duration) (GatewayResponse, error) {
	g.request = request
	return GatewayResponse{Status: 200, Body: []byte(`{}`)}, nil
}

func TestDraftContextPreservesOwnerRequest(t *testing.T) {
	const asset = `{"assetId":"11111111-1111-4111-8111-111111111111","profileId":"22222222-2222-4222-8222-222222222222"}`
	const read = `{"sourceId":"example","view":"LATEST"}`
	const prepare = `{"commandKey":"example-key","expectedVersion":0,"onboardingVersionId":"33333333-3333-4333-8333-333333333333","sourceId":"example"}`
	for _, tc := range []struct{ operation, capability, original, local string }{
		{"COMMAND", "ouf.managed-source.onboarding.review.prepare", prepare, "draftAssetRef"},
		{"READ", "ouf.managed-source.onboarding.read", read, "draftAssetRef"},
		{"READ", "ouf.managed-source.onboarding.read", read, "revisionAssetRef"},
	} {
		t.Run(tc.capability+"/"+tc.local, func(t *testing.T) {
			g := &draftGateway{}
			s := Service{Auth: &fakeAuth{true}, Admission: &fakeAdmission{}, Gateway: g, FingerprintKey: make([]byte, 32)}
			in := invocation()
			in.Owner, in.CapabilityID, in.GatewayBindingRef, in.OperationClass = "onboarding", tc.capability, "capability://"+tc.capability, tc.operation
			in.Arguments = json.RawMessage(tc.original[:len(tc.original)-1] + `,"` + tc.local + `":` + asset + "}")
			_, err := s.Call(context.Background(), in)
			wantHash := fmt.Sprintf("%x", sha256.Sum256([]byte(tc.original)))
			if err != nil || string(g.request.Arguments) != tc.original || g.request.RequestHash != wantHash || g.request.IdempotencyKey != in.IdempotencyKey {
				t.Fatal("MCP context changed the owner command", err, g.request)
			}
		})
	}
	// With no local context, do not reserialize a predecessor command.
	const original = `{ "view":"LATEST", "sourceId":"example" }`
	got, err := withoutDraftContext(json.RawMessage(original))
	if err != nil || string(got) != original {
		t.Fatal(string(got), err)
	}
}
