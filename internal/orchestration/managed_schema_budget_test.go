package orchestration

import (
	"encoding/json"
	"testing"
	"time"
)

func TestManagedSchemaPagesHaveSeparateFiniteBudget(t *testing.T) {
	in:=AdmissionRequest{BudgetProfile:ManagedSchemaBudgetProfile,CapabilityID:"ouf.managed-source.preview",OperationClass:"READ",ManifestChecksum:"manifest",Window:time.Minute,RetryThreshold:3,Maximum:Cost{ToolCalls:1,ResultBytes:1<<20,DistinctObjects:200}}
	key, budget, err:=AdmissionWindowBudget(in)
	if err!=nil || key!="manifest:"+ManagedSchemaBudgetProfile || budget.ToolCalls!=24 { t.Fatalf("wrong page capacity: %s %+v %v",key,budget,err) }
	for _, cap:=range []string{"ouf.managed-source.onboarding.create","ouf.semantic.search"} {
		bad:=in;bad.CapabilityID=cap
		if _,_,err:=AdmissionWindowBudget(bad);err==nil { t.Fatal("page budget leaked into other capability") }
	}
	for _, view:=range []string{"ASSETS","","COLUMNS"} {
		args,_:=json.Marshal(map[string]string{"view":view})
		matched:=isManagedSchemaPage(Invocation{CapabilityID:in.CapabilityID,OperationClass:"READ",Arguments:args})
		if matched!=(view=="COLUMNS") { t.Fatal("budget selection changed") }
	}
}
