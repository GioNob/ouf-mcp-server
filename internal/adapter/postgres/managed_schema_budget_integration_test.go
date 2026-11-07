package postgres

import (
	"errors"
	"os"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

func managedSchemaRequest(manifest, tenant string) orchestration.AdmissionRequest {
	request := controlRequest(manifest, tenant)
	request.BudgetProfile = orchestration.ManagedSchemaBudgetProfile
	request.CapabilityID = "ouf.managed-source.preview"
	request.Owner = "onboarding"
	request.Maximum.ResultBytes = 1 << 20
	return request
}

func TestManagedSchemaPagesPersistCapacityAcrossConnections(t *testing.T) {
	ctx, store, manifest := controlStore(t)
	tenant := "schema-pages-" + uuid.NewString()
	for index := 0; index < 24; index++ {
		request := managedSchemaRequest(manifest, tenant)
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
	_, err = other.Reserve(ctx, managedSchemaRequest(manifest, tenant))
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatal("25th page not bounded after reconnect", err)
	}
	var attempts, consumed int64
	if err = store.Pool().QueryRow(ctx, "select count(*) from ouf_mcp.tool_attempt where tenant_id=$1", tenant).Scan(&attempts); err != nil || attempts != 24 {
		t.Fatal("refusal wrote attempt or erased history", attempts, err)
	}
	if err = store.Pool().QueryRow(ctx, "select consumed_tool_calls from ouf_mcp.budget_window where tenant_id=$1", tenant).Scan(&consumed); err != nil || consumed != 24 {
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
