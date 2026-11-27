package nextstep

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type ownerProducerCase struct {
	target string
	run    func(Engine) Object
}

// Each fixture enters the production function at its public boundary. The
// source walk below independently discovers which functions need fixtures.
var ownerProducers = map[string][]ownerProducerCase{}

func registerOwnerProducer(fn any, cases ...ownerProducerCase) {
	pc := reflect.ValueOf(fn).Pointer()
	producer := runtime.FuncForPC(pc)
	file, _ := producer.FileLine(pc)
	parts := strings.Split(producer.Name(), ".")
	site := filepath.Base(file) + "/" + parts[len(parts)-1]
	ownerProducers[site] = append(ownerProducers[site], cases...)
}

func init() {
	registerOwnerProducer(Engine.Evaluate, ownerProducerCase{"source.analysis.step", func(e Engine) Object {
		p := manifest.Policy{Rules: []manifest.Rule{{Action: "CALL_TOOL", Tool: "source.analysis.step",
			Reason: "ANALYSIS_CONTINUE", Args: map[string]string{"taskId": "#readyTask", "analysisId": "@analysisId"}}}}
		return e.Evaluate(manifest.Capability{ToolName: "source.analysis.read"}, p, Object{"analysisId": "analysis"},
			Object{"tasks": []any{Object{"taskId": "task", "state": "READY"}}}, false)
	}})
	registerOwnerProducer(Engine.Evaluate, ownerProducerCase{"source.analysis.step", func(e Engine) Object {
		e.AnalysisSuccessor = &analysis.TaskResume{ID: "successor", State: "READY"}
		e.CommandKey = func(string, Object, Object, Object) string { return "key" }
		return e.Evaluate(manifest.Capability{ToolName: "source.analysis.step"}, manifest.Policy{},
			Object{"analysisId": "analysis", "taskId": "predecessor"}, Object{"state": "SUCCEEDED"}, false)
	}})
	registerOwnerProducer(Engine.BatchNext, ownerProducerCase{"source.analysis.plan", func(e Engine) Object {
		e.CommandKey = func(string, Object, Object, Object) string { return "key" }
		return e.BatchNext("source.file.preview", Object{"mode": analysis.BatchMode},
			Object{"assetId": "asset", "profileId": "profile", "schemaHash": "hash"}, false)
	}})
	registerOwnerProducer(Engine.BatchStatusNext, ownerProducerCase{"source.analysis.read", func(e Engine) Object {
		return e.BatchStatusNext("analysis", "ANALYSIS_CONTINUE")
	}})
	registerOwnerProducer(Engine.BatchPageNext, ownerProducerCase{"source.analysis.read", func(e Engine) Object {
		return e.BatchPageNext(Object{"analysisId": "analysis", "view": "BATCH"}, false)
	}})
	registerOwnerProducer(Engine.AnalysisSuccessorFallback, ownerProducerCase{"source.analysis.read", func(e Engine) Object {
		return e.AnalysisSuccessorFallback("analysis")
	}})
	registerOwnerProducer(Engine.SummaryNext,
		ownerProducerCase{"source.analysis.read", func(e Engine) Object {
			return e.SummaryNext("analysis", "SUMMARY", "cursor", analysis.Summary{})
		}},
		ownerProducerCase{"source.analysis.read", func(e Engine) Object {
			return e.SummaryNext("analysis", "STATUS", "", analysis.Summary{})
		}},
		ownerProducerCase{"semantic.discovery.candidates", func(e Engine) Object {
			return e.SummaryNext("analysis", "STATUS", "", analysis.Summary{Hint: analysis.Hint{
				DecisionRequired: true, Tool: "semantic.discovery.candidates", RequestID: "request"}})
		}})
	registerOwnerProducer(Engine.ExpiredSummary, ownerProducerCase{"source.analysis.read", func(e Engine) Object {
		return e.ExpiredSummary("analysis")
	}})
	registerOwnerProducer(Engine.PreviewColumn, ownerProducerCase{"source.file.preview", func(e Engine) Object {
		return e.PreviewColumn(analysis.Hint{Ordinal: 2})
	}})
	registerOwnerProducer(WaitRef.Refresh, ownerProducerCase{"semantic.proposal.read", func(e Engine) Object {
		return (WaitRef{Read: "semantic.proposal.read", Args: Object{"commandKey": "key"}}).Refresh(e, nil)
	}})
	registerOwnerProducer(WaitRef.ResumeCommand, ownerProducerCase{"source.analysis.step", func(e Engine) Object {
		return (WaitRef{Reason: "ANALYSIS_CONTINUE", Read: "source.analysis.step", Args: Object{"taskId": "task"},
			NotBefore: time.Unix(1, 0).UTC().Format(time.RFC3339)}).ResumeCommand(e)
	}})
}

func checkOwnerProducer(t *testing.T, tc ownerProducerCase) {
	t.Helper()
	now := time.Unix(1800000000, 0).UTC()
	due := now.Add(72 * time.Second)
	e := signedEngine(t, Engine{Now: func() time.Time { return now }, Valid: func(string, Object) bool { return true },
		SavedObservation: func(tool string, _ Object) (orchestration.OwnerObservation, error) {
			if tool != tc.target {
				t.Fatalf("looked up %s, want %s", tool, tc.target)
			}
			return orchestration.OwnerObservation{Due: due}, nil
		}})
	ref := pollTarget(t, e, tc.run(e))
	if ref.Read != tc.target || ref.NotBefore != due.Format(time.RFC3339Nano) {
		t.Fatal("producer ignored persisted request clock", ref)
	}
	now = due
	next := tc.run(e)
	if next["tool"] != ref.Read || next["action"] != "CALL_TOOL" || !CheckedOwnerCall(next) ||
		!wireArgumentsEqual(ref.Args, next["arguments"]) {
		t.Fatal("expired clock did not release the original call", next, ref)
	}
	next["arguments"] = Object{"changed": true}
	if CheckedOwnerCall(next) {
		t.Fatal("witness accepted different arguments")
	}
}

// The engine's private rule builders feed Evaluate. Only manual alternatives
// and already signed waits can construct actions outside its emission boundary.
func TestOwnerContinuationConstructionBoundary(t *testing.T) {
	fset := token.NewFileSet()
	discovered := map[string]bool{}
	for _, dir := range []string{".", "../kernel"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range files {
			if strings.HasSuffix(path, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, decl := range file.Decls {
				site := filepath.Base(path) + "/package"
				if fn, ok := decl.(*ast.FuncDecl); ok {
					site = filepath.Base(path) + "/" + fn.Name.Name
				}
				ast.Inspect(decl, func(n ast.Node) bool {
					if sel, ok := n.(*ast.SelectorExpr); ok && (sel.Sel.Name == "ownerCall" || sel.Sel.Name == "ownerNext") && site != "engine.go/ownerCall" {
						discovered[site] = true
					}
					if bypassesOwnerTiming(site, n) {
						t.Errorf("%s: %s bypasses the owner timing boundary", fset.Position(n.Pos()), site)
					}
					return true
				})
			}
		}
	}
	for site := range discovered {
		if len(ownerProducers[site]) == 0 {
			t.Errorf("unregistered owner producer %s", site)
		}
		for _, tc := range ownerProducers[site] {
			t.Run(site+"/"+tc.target, func(t *testing.T) { checkOwnerProducer(t, tc) })
		}
	}
	for site := range ownerProducers {
		if !discovered[site] {
			t.Errorf("registered producer %s lost its timing boundary", site)
		}
	}
}

func bypassesOwnerTiming(site string, n ast.Node) bool {
	switch v := n.(type) {
	case *ast.ValueSpec:
		return containsCallAlias(v.Values)
	case *ast.AssignStmt:
		if containsCallAlias(v.Rhs) {
			return true
		}
		for _, lhs := range v.Lhs {
			if index, ok := lhs.(*ast.IndexExpr); ok {
				if key, ok := index.Index.(*ast.BasicLit); ok && key.Value == `"action"` {
					return true
				}
			}
		}
	case *ast.KeyValueExpr:
		key, _ := v.Key.(*ast.BasicLit)
		value, _ := v.Value.(*ast.BasicLit)
		if key == nil || key.Value != `"action"` || (value != nil && value.Value != `"CALL_TOOL"` && value.Value != `"POLL"`) {
			return false
		}
		switch site {
		case "engine.go/step":
			return false // The callers of this primitive are checked below.
		case "draft.go/alternative", "engine.go/Block", "engine.go/PermissionDenied", "engine.go/blockedRule", "waitref.go/Block":
			return false // Alternatives for an explicit decision.
		}
		return true
	case *ast.CallExpr:
		switch callee := v.Fun.(type) {
		case *ast.Ident:
			if callee.Name == "alternative" {
				switch site {
				case "batch.go/BatchDecisionNext", "discovery.go/discoveryDecisions", "discovery.go/reformulationOption", "draft.go/draftCreateDecision", "draft.go/draftFailure",
					"draft.go/afterDraftRecoveryRead", "draft.go/installationLimitDecision", "engine.go/failure",
					"summary.go/SummaryNext", "summary.go/ReplanNext", "summary.go/permissionHold":
					return false
				}
				return true
			}
			if callee.Name != "call" && callee.Name != "step" {
				return false
			}
			switch site {
			case "engine.go/evaluate", "engine.go/failure", "engine.go/ownerCall", "engine.go/call",
				"waitref.go/Issue", "waitref.go/pollNext", "waitref.go/Next":
				return false
			}
			action, _ := v.Args[0].(*ast.BasicLit)
			return action == nil || action.Value == `"CALL_TOOL"` || action.Value == `"POLL"`
		case *ast.SelectorExpr:
			if callee.Sel.Name == "evaluate" || callee.Sel.Name == "failure" {
				return site != "engine.go/Evaluate" && site != "engine.go/evaluate" && site != "engine.go/failure"
			}
		}
	case *ast.ReturnStmt:
		for _, result := range v.Results {
			if call, ok := result.(*ast.CallExpr); ok {
				if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "alternative" {
					return true
				}
			}
		}
		if site == "engine.go/Evaluate" || site == "engine.go/ownerCall" {
			guarded := false
			ast.Inspect(v, func(child ast.Node) bool {
				if call, ok := child.(*ast.CallExpr); ok {
					sel, ok := call.Fun.(*ast.SelectorExpr)
					guarded = guarded || (ok && sel.Sel.Name == "ownerNext")
				}
				return true
			})
			return !guarded
		}
	}
	return false
}

func containsCallAlias(values []ast.Expr) bool {
	for _, value := range values {
		if id, ok := ast.Unparen(value).(*ast.Ident); ok && id.Name == "call" {
			return true
		}
	}
	return false
}

func TestOwnerGuardRejectsCallAlias(t *testing.T) {
	for _, bind := range []string{"f := call", "var f = (call)"} {
		file, err := parser.ParseFile(token.NewFileSet(), "alias.go", "package nextstep\nfunc bypass(){"+bind+"; f()}", 0)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		ast.Inspect(file, func(n ast.Node) bool {
			found = found || bypassesOwnerTiming("alias.go/bypass", n)
			return !found
		})
		if !found {
			t.Fatal("undetected constructor alias", bind)
		}
	}
}
