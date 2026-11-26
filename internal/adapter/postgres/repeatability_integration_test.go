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

func TestAdoptionReplayReadsReconciledRefusal(t *testing.T) {
	ctx, store, manifest := controlStore(t)
	request := controlRequest(manifest, uuid.NewString())
	request.BudgetProfile = ""
	request.CapabilityID, request.Owner, request.OperationClass = "ouf.semantic.discovery.adopt", "semantic", "COMMAND"
	first, err := store.Reserve(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Dispatch(ctx, first.AttemptID, uuid.NewString(), time.Second, first.LockVersion); err != nil {
		t.Fatal(err)
	}
	const code = "CANDIDATE_PUBLISHER_MISSING"
	if err := store.Reconcile(ctx, first.AttemptID, orchestration.Cost{ToolCalls: 1}, orchestration.AttemptOutcome{Code: code, AdoptionRefusalCode: code}); err != nil {
		t.Fatal(err)
	}
	replay, err := store.Reserve(ctx, request)
	if err != nil || !replay.Replay || replay.AttemptID != first.AttemptID || replay.AttemptState != "FAILED" || replay.AdoptionRefusalCode != code {
		t.Fatal(replay, err)
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

func TestWaitAndStatusShareOrdinaryAdmission(t *testing.T) {
	ctx, store, manifest := controlStore(t)
	identity := controlRequest(manifest, uuid.NewString()).Identity
	gateway := &repeatabilityGateway{body: "{}"}
	service := orchestration.Service{Auth: controlPolicyFixture{}, Admission: store, Gateway: gateway, FingerprintKey: make([]byte, 32)}
	in := orchestration.Invocation{Identity: identity, CapabilityID: "ouf.managed-source.file.upload",
		Owner: "onboarding", OperationClass: "COMMAND", GatewayBindingRef: "capability://ouf.managed-source.file.upload",
		ManifestChecksum: manifest, Arguments: json.RawMessage(`{"handoffId":"11111111-1111-4111-8111-111111111111"}`),
		Window: time.Minute, Timeout: time.Second, RetryThreshold: orchestration.AdmissionRetryThreshold,
		Maximum: orchestration.Cost{ToolCalls: 1, ResultBytes: 4096, DistinctObjects: 1}}
	// Competing replicas must not dispatch two reads for a signed wait in one interval.
	end := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	tokenHash := hash64(uuid.NewString())
	claims := make(chan bool, 2)
	for range 2 {
		go func() {
			ok, err := store.ClaimWaitRead(ctx, identity, tokenHash, 5*time.Second, end)
			if err != nil {
				t.Error(err)
			}
			claims <- ok
		}()
	}
	if first, second := <-claims, <-claims; first == second {
		t.Fatal("expected exactly one owner read slot", first, second)
	}
	const limit = orchestration.DefaultAdmissionCallsPerMinute
	if err := store.ChargeWait(ctx, identity, manifest, limit); err != nil {
		t.Fatal(err)
	}
	call := func(internal bool) error {
		in.InternalWaitRead = internal
		in.IdempotencyKey, in.CorrelationID = uuid.NewString(), uuid.NewString()
		_, err := service.Call(ctx, in)
		return err
	}
	for range 8 {
		if err := call(true); err != nil {
			t.Fatal("internal read failed", err)
		}
	}
	var charged, usedBytes int
	err := store.Pool().QueryRow(ctx, `select consumed_tool_calls,consumed_result_bytes from ouf_mcp.budget_window where tenant_id=$1 and policy_ref=$2`, identity.TenantID, manifest).Scan(&charged, &usedBytes)
	if err != nil || charged != 1 || usedBytes != 8*len(gateway.body) {
		t.Fatal("incorrect byte or call accounting", charged, usedBytes, err)
	}
	for n := 2; n <= limit; n++ {
		if err := call(false); err != nil {
			t.Fatalf("ordinary call %d failed: %v", n, err)
		}
	}
	for _, err := range []error{call(false), store.ChargeWait(ctx, identity, manifest, limit)} {
		var refusal *orchestration.WindowBudgetExhaustedError
		if !errors.As(err, &refusal) || refusal.Dimension != "TOOL_CALLS" {
			t.Fatal("missing window refusal", err)
		}
	}
	in.CapabilityID = orchestration.HandoffIssueCapability
	in.AuthorizationCapabilityID = "ouf.managed-source.file.upload"
	in.Identity.Delegation = "fixture"
	in.InternalWaitRead = false
	in.GatewayBindingRef = "capability://" + in.CapabilityID
	in.Identity.TenantID = uuid.NewString()
	for n := 1; n <= orchestration.AdmissionRetryThreshold; n++ {
		in.IdempotencyKey = uuid.NewString()
		in.Arguments, _ = json.Marshal(map[string]string{"commandKey": in.IdempotencyKey})
		_, err := service.Call(ctx, in)
		if n < orchestration.AdmissionRetryThreshold && err != nil || n == orchestration.AdmissionRetryThreshold && !errors.Is(err, orchestration.ErrToolSelectionStall) {
			t.Fatal("equivalent picker command", n, err)
		}
	}
}

type uncertainAdoptionGateway struct{ status int }

func (g uncertainAdoptionGateway) Execute(context.Context, orchestration.GatewayRequest, time.Duration) (orchestration.GatewayResponse, error) {
	return orchestration.GatewayResponse{Status: g.status, Problem: &orchestration.Problem{Code: "OWNER_UNAVAILABLE", Status: g.status, RetryAfter: "72"}}, nil
}

func TestAdoptionClockSurvivesUnknownRecovery(t *testing.T) {
	ctx, store, checksum := controlStore(t)
	for _, status := range []int{429, 503} {
		id := controlRequest(checksum, uuid.NewString()).Identity
		key := uuid.NewString()
		args, _ := json.Marshal(map[string]string{"commandKey": key, "candidateId": uuid.NewString(), "requestId": uuid.NewString()})
		in := orchestration.Invocation{Identity: id, CapabilityID: "ouf.semantic.discovery.adopt", OperationClass: "COMMAND",
			Owner: "semantic", GatewayBindingRef: "capability://ouf.semantic.discovery.adopt", ManifestChecksum: checksum,
			Arguments: args, IdempotencyKey: key, CorrelationID: uuid.NewString(), Window: time.Minute, Timeout: time.Second,
			RetryThreshold: 3, Maximum: orchestration.Cost{ToolCalls: 1, ResultBytes: 262144, DistinctObjects: 1}}
		s := orchestration.Service{Auth: controlPolicyFixture{}, Admission: store, Gateway: uncertainAdoptionGateway{status}, FingerprintKey: make([]byte, 32)}
		lost, err := s.Call(ctx, in)
		if status == 503 && !errors.Is(err, orchestration.ErrIdempotencyOutcomeUnknown) || status == 429 && err != nil {
			t.Fatal(status, err)
		}
		restarted := orchestration.Service{Auth: s.Auth, Admission: store, FingerprintKey: s.FingerprintKey}
		check := func() {
			t.Helper()
			res, err := restarted.Call(ctx, in)
			if err != nil && !errors.Is(err, orchestration.ErrIdempotencyOutcomeUnknown) || res.AttemptID != lost.AttemptID ||
				!res.Observation.Due.Equal(lost.Observation.Due) || res.Observation.Status != status {
				t.Fatal(status, res, err)
			}
			read, _ := json.Marshal(map[string]string{"commandKey": key})
			clock, err := restarted.SavedObservation(ctx, id, "ouf.semantic.proposal.read", "READ", read)
			if err != nil || !clock.Due.Equal(res.Observation.Due) || clock.Reason != "TRANSIENT_OWNER_WAIT" {
				t.Fatal(clock, err)
			}
		}
		check()
		if status == 503 {
			var state string
			if err = store.pool.QueryRow(ctx, `select state from ouf_mcp.tool_attempt where attempt_id=$1`, lost.AttemptID).Scan(&state); err != nil || state != "RUNNING" {
				t.Fatal("uncertain dispatch lost its lease", state, err)
			}
			_, err = store.pool.Exec(ctx, `update ouf_mcp.tool_attempt set state='UNKNOWN',dispatch_state='UNKNOWN',unknown_since=now(),lease_until=null where attempt_id=$1`, lost.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
			check()
		}
	}
}
