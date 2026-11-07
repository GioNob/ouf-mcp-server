package postgres

import (
	"errors"
	"os"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func semanticReadRequest(manifest, tenant string) orchestration.AdmissionRequest {
	request := controlRequest(manifest, tenant)
	request.BudgetProfile = orchestration.SemanticReadBudgetProfile
	request.CapabilityID = "ouf.semantic.search"
	request.Owner = "semantic"
	request.Maximum.ResultBytes = 262144
	return request
}

func TestSemanticReadBudgetSixtyDistinctReadsPersistAcrossConnections(t *testing.T) {
	ctx, store, manifest := controlStore(t)
	tenant := "semantic-capacity-" + uuid.NewString()
	for index := 0; index < 60; index++ {
		request := semanticReadRequest(manifest, tenant)
		if index%2 == 1 {
			request.CapabilityID = "ouf.semantic.consultation.read"
			request.Maximum.DistinctObjects = 1
		}
		admitted, err := store.Reserve(ctx, request)
		if err != nil {
			t.Fatal(index, err)
		}
		if err = settleControlAdmission(ctx, store, admitted); err != nil {
			t.Fatal(err)
		}
	}
	other, err := Open(ctx, os.Getenv("MCP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	_, err = other.Reserve(ctx, semanticReadRequest(manifest, tenant))
	var exhausted *orchestration.SemanticReadBudgetExhaustedError
	if !errors.Is(err, ErrBudgetExhausted) || !errors.As(err, &exhausted) || exhausted.Dimension != "TOOL_CALLS" || exhausted.RetryAfterSeconds < 1 || exhausted.RetryAfterSeconds > 60 {
		t.Fatal("61st read not bounded after reconnect", err)
	}
	var attempts, consumed int64
	if err = store.Pool().QueryRow(ctx, "select count(*) from ouf_mcp.tool_attempt where tenant_id=$1", tenant).Scan(&attempts); err != nil || attempts != 60 {
		t.Fatal("refusal wrote attempt or erased history", attempts, err)
	}
	if err = store.Pool().QueryRow(ctx, "select consumed_tool_calls from ouf_mcp.budget_window where tenant_id=$1", tenant).Scan(&consumed); err != nil || consumed != 60 {
		t.Fatal("consumption not retained", consumed, err)
	}
	for index := 0; index < 3; index++ {
		request := controlRequest(manifest, tenant)
		request.BudgetProfile = ""
		request.CapabilityID = "ouf.semantic.propose"
		request.OperationClass = "COMMAND"
		admitted, err := other.Reserve(ctx, request)
		if err != nil {
			t.Fatal("read use consumed command capacity", index, err)
		}
		if err = settleControlAdmission(ctx, other, admitted); err != nil {
			t.Fatal(err)
		}
	}
	command := controlRequest(manifest, tenant)
	command.BudgetProfile = ""
	command.CapabilityID = "ouf.semantic.propose"
	command.OperationClass = "COMMAND"
	if _, err = other.Reserve(ctx, command); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatal("command budget increased", err)
	}
	if _, err = other.Reserve(ctx, controlRequest(manifest, tenant)); err != nil {
		t.Fatal("read use consumed operational control capacity", err)
	}
}

func TestSemanticReadBudgetEquivalentGuardAndIdempotencyRemain(t *testing.T) {
	ctx, store, manifest := controlStore(t)
	tenant := "semantic-equivalence-" + uuid.NewString()
	base := semanticReadRequest(manifest, tenant)
	for index := 0; index < 2; index++ {
		request := base
		request.AttemptID = uuid.New()
		request.IdempotencyKey = uuid.NewString()
		admitted, err := store.Reserve(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		if err = settleControlAdmission(ctx, store, admitted); err != nil {
			t.Fatal(err)
		}
		replay, err := store.Reserve(ctx, request)
		if err != nil || !replay.Replay || replay.AttemptID != admitted.AttemptID {
			t.Fatal("idempotency changed", replay, err)
		}
	}
	base.AttemptID = uuid.New()
	base.IdempotencyKey = uuid.NewString()
	if _, err := store.Reserve(ctx, base); !errors.Is(err, orchestration.ErrToolSelectionStall) {
		t.Fatal("equivalent-query threshold changed", err)
	}
}

func TestSemanticReadBudgetForgedCommandWritesNoWindow(t *testing.T) {
	ctx, store, manifest := controlStore(t)
	tenant := "semantic-forgery-" + uuid.NewString()
	request := semanticReadRequest(manifest, tenant)
	request.OperationClass = "COMMAND"
	if _, err := store.Reserve(ctx, request); err == nil {
		t.Fatal("command acquired semantic read profile")
	}
	var windows int
	if err := store.Pool().QueryRow(ctx, "select count(*) from ouf_mcp.budget_window where tenant_id=$1", tenant).Scan(&windows); err != nil || windows != 0 {
		t.Fatal("invalid profile wrote window", windows, err)
	}
}
