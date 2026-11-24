package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

type repeatabilityGateway struct {
	calls int
	body  string
}

func (g *repeatabilityGateway) Execute(_ context.Context, _ orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	g.calls++
	return orchestration.GatewayResponse{Status: 200, Body: []byte(g.body)}, nil
}

func TestDiscoveryTwoRoundsAndSameAdoptionAfterWindow(t *testing.T) {
	ctx, store, manifest := controlStore(t)
	identity := controlRequest(manifest, uuid.NewString()).Identity
	ref := uuid.New()
	gateway := &repeatabilityGateway{body: `{"requestId":"` + ref.String() + `"}`}
	service := orchestration.Service{Auth: controlPolicyFixture{}, Admission: store, Gateway: gateway, FingerprintKey: make([]byte, 32)}
	invoke := func(ctx context.Context, cap, op, key string, args map[string]string) (orchestration.Result, error) {
		raw, err := json.Marshal(args)
		if err != nil {
			t.Fatal(err)
		}
		maxObjects := int64(100)
		if cap == "ouf.semantic.discovery.adopt" {
			maxObjects = 1
		}
		return service.Call(ctx, orchestration.Invocation{Identity: identity, CapabilityID: cap, Owner: "semantic", OperationClass: op, GatewayBindingRef: "capability://" + cap, ManifestChecksum: manifest, Arguments: raw, IdempotencyKey: key, CorrelationID: uuid.NewString(), RetryThreshold: 3, Window: time.Minute, Timeout: time.Second, Maximum: orchestration.Cost{ToolCalls: 1, ResultBytes: 262144, DistinctObjects: maxObjects}})
	}
	for round := 0; round < 2; round++ {
		// The same intent gets a new owner key on the second round.
		command := map[string]string{"intent": "teatro", "idempotencyKey": uuid.NewString()}
		first, err := invoke(ctx, "ouf.semantic.discovery", "COMMAND", uuid.NewString(), command)
		if err != nil || first.Replay {
			t.Fatal("discovery request", round, err)
		}
		replay, err := invoke(ctx, "ouf.semantic.discovery", "COMMAND", uuid.NewString(), command)
		if err != nil || !replay.Replay || replay.AttemptID != first.AttemptID || replay.AttemptState != "SUCCEEDED" || replay.ResultID == nil || *replay.ResultID != ref {
			t.Fatal("second transport key lost original receipt", replay, err)
		}
		command["intent"] = "changed"
		if _, err = invoke(ctx, "ouf.semantic.discovery", "COMMAND", uuid.NewString(), command); !errors.Is(err, orchestration.ErrIdempotencyConflict) {
			t.Fatal("different intent admitted under the same owner key", err)
		}
		for _, read := range []string{"ouf.semantic.discovery.status", "ouf.semantic.discovery.candidates"} {
			if _, err = invoke(ctx, read, "READ", uuid.NewString(), map[string]string{"requestId": ref.String()}); err != nil {
				t.Fatal(read, round, err)
			}
		}
		key := uuid.NewString()
		if _, err = invoke(ctx, "ouf.semantic.discovery.adopt", "COMMAND", key, map[string]string{"requestId": ref.String(), "candidateId": uuid.NewString(), "commandKey": key}); err != nil {
			t.Fatal("nominal adoption", round, err)
		}
	}
	for n := 0; n < 4; n++ {
		if _, err := invoke(ctx, "ouf.semantic.discovery.status", "READ", uuid.NewString(), map[string]string{"requestId": uuid.NewString()}); err != nil {
			t.Fatal(err)
		}
	}
	key := uuid.NewString()
	args := map[string]string{"requestId": ref.String(), "candidateId": uuid.NewString(), "commandKey": key}
	_, err := invoke(ctx, "ouf.semantic.discovery.adopt", "COMMAND", key, args)
	var wait *orchestration.WindowBudgetExhaustedError
	if !errors.As(err, &wait) || wait.Dimension != "TOOL_CALLS" || wait.RetryAfterSeconds < 1 || wait.RetryAfterSeconds > 60 || gateway.calls != 12 {
		t.Fatal("13th dispatch or missing deadline", gateway.calls, err)
	}
	time.Sleep(time.Until(wait.BudgetWindowEnd()) + 100*time.Millisecond)
	resume, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := invoke(resume, "ouf.semantic.discovery.adopt", "COMMAND", key, args)
	if err != nil || result.Replay || gateway.calls != 13 {
		t.Fatal("adoption did not resume exactly once", result, err)
	}
}

func TestOrdinaryReplayTracksAttemptTransitionsAndPublicReference(t *testing.T) {
	ctx, store, manifest := controlStore(t)
	for _, capability := range []string{"ouf.managed-source.file.profile", "ouf.managed-source.onboarding.create", "ouf.ingestion.status"} {
		request := controlRequest(manifest, uuid.NewString())
		request.BudgetProfile = ""
		request.CapabilityID = capability
		request.Owner, request.OperationClass = "onboarding", "COMMAND"
		if capability == "ouf.ingestion.status" {
			request.Owner, request.OperationClass = "ingestion", "READ"
		}
		for _, success := range []bool{true, false} {
			request.AttemptID, request.IdempotencyKey = uuid.New(), uuid.NewString()
			first, err := store.Reserve(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			check := func(state string, ref *uuid.UUID) {
				t.Helper()
				got, err := store.Reserve(ctx, request)
				if err != nil || !got.Replay || got.AttemptState != state || got.AttemptID != first.AttemptID ||
					(ref == nil) != (got.ResultID == nil) || (ref != nil && *got.ResultID != *ref) {
					t.Fatal(capability, state, got, err)
				}
			}
			check("ADMITTED", nil)
			if err = store.Dispatch(ctx, first.AttemptID, uuid.NewString(), time.Second, first.LockVersion); err != nil {
				t.Fatal(err)
			}
			check("RUNNING", nil)
			outcome := orchestration.AttemptOutcome{Success: success}
			state := "FAILED"
			if success {
				state = "SUCCEEDED"
				if request.OperationClass == "COMMAND" {
					id := uuid.New()
					outcome.ResultID = &id
				}
			}
			if err = store.Reconcile(ctx, first.AttemptID, orchestration.Cost{ToolCalls: 1}, outcome); err != nil {
				t.Fatal(err)
			}
			check(state, outcome.ResultID)
		}
	}
}

func TestDiscoveryBindsExistingTransportClaimOnlyOnExactPayload(t *testing.T) {
	ctx, store, manifest := controlStore(t)
	request := controlRequest(manifest, uuid.NewString())
	request.CapabilityID, request.Owner, request.OperationClass = "ouf.semantic.discovery", "semantic", "COMMAND"
	request.BudgetProfile = ""
	request.RequestHash = hash64("original body including owner key")
	first, err := store.Reserve(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err = settleControlAdmission(ctx, store, first); err != nil {
		t.Fatal(err)
	}
	// Represent a pre-016 succeeded claim: original transport key, no public UUID.
	if _, err = store.pool.Exec(ctx, "update ouf_mcp.idempotency_claim set discovery_owner_key=null where attempt_id=$1", first.AttemptID); err != nil {
		t.Fatal(err)
	}
	request.IdempotencyKey = "body-key-different-from-transport"
	got, err := store.Reserve(ctx, request)
	if err != nil || !got.Replay || got.AttemptID != first.AttemptID || got.AttemptState != "SUCCEEDED" || got.ResultID != nil {
		t.Fatal("legacy command redispatched or receipt invented", got, err)
	}
	request.RequestHash = hash64("different intent")
	if _, err = store.Reserve(ctx, request); !errors.Is(err, orchestration.ErrIdempotencyConflict) {
		t.Fatal("legacy owner key failed to bind", err)
	}
}
