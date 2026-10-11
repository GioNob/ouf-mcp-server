package orchestration

import (
	"testing"
	"time"
)

func TestOperationalControlBudgetDoesNotRaiseBusinessOrEquivalentRetryLimits(t *testing.T){
	request:=AdmissionRequest{BudgetProfile:OperationalControlBudgetProfile,CapabilityID:"ouf.operations.incidents",OperationClass:"READ",ManifestChecksum:"manifest",Window:time.Minute,RetryThreshold:3,Maximum:Cost{ToolCalls:1,ResultBytes:512<<10,DistinctObjects:100}}
	key,budget,err:=AdmissionWindowBudget(request)
	if err!=nil || key==request.ManifestChecksum || budget.ToolCalls!=24 || budget.ResultBytes!=12<<20 || budget.DistinctObjects!=2400 {t.Fatal(key,budget,err)}
	request.BudgetProfile="";request.CapabilityID="urban.object.related_search";request.OperationClass="SEARCH"
	key,budget,err=AdmissionWindowBudget(request)
	if err!=nil || key!="manifest" || budget.ToolCalls!=3 || budget.ResultBytes!=3*(512<<10) {t.Fatal("business budget changed",key,budget,err)}
}

func TestOperationalControlBudgetRejectsUnreviewedScopeAndLimits(t *testing.T){
	base:=AdmissionRequest{BudgetProfile:OperationalControlBudgetProfile,CapabilityID:"ouf.gateway.operations.incidents",OperationClass:"READ",ManifestChecksum:"manifest",Window:time.Minute,RetryThreshold:3,Maximum:Cost{ToolCalls:1,ResultBytes:512<<10,DistinctObjects:100}}
	for _,mode:=range []string{"profile","capability","operation","window","equivalent-retries","calls","bytes","objects"}{in:=base;switch mode {case "profile":in.BudgetProfile="arbitrary";case "capability":in.CapabilityID="urban.object.related_search";case "operation":in.OperationClass="COMMAND";case "window":in.Window=time.Second;case "equivalent-retries":in.RetryThreshold=128;case "calls":in.Maximum.ToolCalls=24;case "bytes":in.Maximum.ResultBytes=1<<20;case "objects":in.Maximum.DistinctObjects=200};if _,_,err:=AdmissionWindowBudget(in);err==nil {t.Fatal("unreviewed profile admitted",mode)}}
}
