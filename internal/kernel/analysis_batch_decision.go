package kernel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const maxBatchDecisionArgumentsBytes = analysis.BatchMaxItems*maxValueMapArgumentsBytes + (8 << 10)

// Only the envelope and action selectors are validated as a unit. Each action
// payload is checked independently so invalid values can appear in a receipt.
func validBatchDecisionEnvelope(args nextstep.Object, raw []byte) bool {
	id, _ := args["analysisId"].(string)
	batchID, _ := args["batchId"].(string)
	revision, ok := args["expectedRevision"].(float64)
	items, _ := args["decisions"].([]any)
	_, hasKey := args["commandKey"]
	if len(args) != 5 || analysis.ValidateID(id) != nil || id != strings.ToLower(id) || !analysis.CanonicalUUID(batchID) || !hasKey ||
		!ok || revision < 0 || revision > (1<<53)-1 || math.Trunc(revision) != revision ||
		len(items) < 1 || len(items) > analysis.BatchMaxItems {
		return false
	}
	exact, err := batchDecisionItems(raw)
	if err != nil {
		return false
	}
	for _, item := range exact {
		var fields []string
		switch item["action"] {
		case "ADOPT":
			fields = []string{"requestId", "candidateId"}
		case "REFORMULATE":
			fields = []string{"intent"}
		case "EXCLUDE":
			fields = []string{"reason"}
		case "PROPOSE":
			fields = []string{"proposal", "revisionId"}
		default:
			return false
		}
		for field := range item {
			if field != "ordinal" && field != "action" && !slices.Contains(fields, field) {
				return false
			}
		}
	}
	return true
}

func batchDecisionItems(raw []byte) ([]nextstep.Object, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var submitted struct{ Decisions []nextstep.Object }
	if err := decoder.Decode(&submitted); err != nil {
		return nil, analysis.ErrInvalid
	}
	seen := make(map[int64]bool, len(submitted.Decisions))
	for _, item := range submitted.Decisions {
		number, ok := item["ordinal"].(json.Number)
		ordinal, err := number.Int64()
		if !ok || err != nil || ordinal < 0 || ordinal >= 1024 || seen[ordinal] {
			return nil, analysis.ErrInvalid
		}
		seen[ordinal] = true
		item["ordinal"] = int(ordinal)
	}
	return submitted.Decisions, nil
}

func (s *workflowServer) decideAnalysisBatch(ctx context.Context, c analysis.Coordinator, i orchestration.Identity, input nextstep.Object, raw []byte) *mcp.CallToolResult {
	id, _ := input["analysisId"].(string)
	start, batch, err := batchStartForRead(ctx, c, i, id)
	if err != nil {
		return batchReadFailure(id, err)
	}
	if !batch {
		return batchReadFailure(id, analysis.ErrScope)
	}
	store, ok := c.Store.(analysis.BatchDecisionStore)
	if !ok {
		return batchUnavailable(id)
	}
	scope := analysis.IdentityScope(i)
	command, err := s.batchDecisionCommand(scope, input, raw)
	if err != nil {
		return batchReadFailure(id, err)
	}
	if c.Service.ObserveBatchDecision != nil || start.Mandate.StartRef != "" {
		for n := range command.Decisions {
			if command.Decisions[n].Action == "REFORMULATE" && command.Decisions[n].Rejection == "" {
				command.Decisions[n].Rejection = "ANALYSIS_FIRST_ROUND_MANDATE_REQUIRED"
			}
		}
	}
	checksum, err := c.Manifest.Checksum()
	if err != nil {
		return batchReadFailure(id, err)
	}
	commandCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	receipt, err := store.DecideBatch(commandCtx, scope, checksum, start, command)
	engine := s.engine(ctx)
	if err != nil {
		result := batchReadFailure(id, err)
		if errors.Is(err, analysis.ErrConflict) || errors.Is(err, analysis.ErrKeyReuse) {
			result.Meta[batchNextMeta] = engine.BatchDecisionReadNext(id, err.Error())
		}
		return result
	}
	if c.Service.ObserveBatchDecision != nil {
		c.Service.ObserveBatchDecision(id, i)
	}
	body := nextstep.Object{"analysisId": receipt.AnalysisID, "batchId": receipt.BatchID,
		"revision": receipt.Revision, "decisions": receipt.Decisions}
	return localBatchResult(body, engine.BatchDecisionReadNext(id, "ANALYSIS_CONTINUE"))
}

func (s *workflowServer) batchDecisionCommand(scope analysis.Scope, input nextstep.Object, raw []byte) (analysis.BatchDecide, error) {
	id, _ := input["analysisId"].(string)
	batchID, _ := input["batchId"].(string)
	key, _ := input["commandKey"].(string)
	command := analysis.BatchDecide{AnalysisID: id, BatchID: batchID, CommandKey: key, ExpectedRevision: int64(input["expectedRevision"].(float64))}
	submitted, err := batchDecisionItems(raw)
	if err != nil {
		return command, err
	}
	items := input["decisions"].([]any)
	for n, exact := range submitted {
		item := items[n].(nextstep.Object)
		d := analysis.BatchDecision{Ordinal: exact["ordinal"].(int), Action: item["action"].(string)}
		d.RequestID, _ = item["requestId"].(string)
		d.CandidateID, _ = item["candidateId"].(string)
		switch d.Action {
		case "ADOPT":
			if !analysis.CanonicalUUID(d.RequestID) || !analysis.CanonicalUUID(d.CandidateID) {
				d.Rejection = "ANALYSIS_CANDIDATE_INVALID"
			}
		case "REFORMULATE":
			if intent, ok := item["intent"].(string); ok {
				d.Intent = analysis.NormalizeBatchIntent(intent)
				exact["intent"] = d.Intent
			}
			if err := analysis.ValidateReformulationIntent(d.Intent); err != nil {
				d.Rejection = analysis.SafeError(err)
			}
		case "EXCLUDE":
			if reason, ok := item["reason"].(string); ok {
				d.Reason = strings.TrimSpace(reason)
				exact["reason"] = d.Reason
			}
			if d.Reason == "" || strings.ContainsRune(d.Reason, 0) || utf8.RuneCountInString(d.Reason) > 400 {
				d.Rejection = "ANALYSIS_EXCLUSION_REASON_INVALID"
			}
		case "PROPOSE":
			s.prepareBatchProposal(&d, item, exact, command.EffectKey(scope, d))
		}
		command.Decisions = append(command.Decisions, d)
	}
	if err := command.Normalize(); err != nil {
		return command, err
	}
	slices.SortFunc(submitted, func(a, b nextstep.Object) int { return a["ordinal"].(int) - b["ordinal"].(int) })
	command.InputFingerprint = analysis.Digest([]any{id, batchID, command.ExpectedRevision, submitted})
	return command, nil
}

func (s *workflowServer) prepareBatchProposal(d *analysis.BatchDecision, item, exact nextstep.Object, effectKey string) {
	proposal, hasProposal := item["proposal"]
	revision, hasRevision := item["revisionId"]
	d.Rejection = "ANALYSIS_PROPOSAL_INVALID"
	switch {
	case hasRevision && !hasProposal:
		d.RevisionID, _ = revision.(string)
		if analysis.CanonicalUUID(d.RevisionID) {
			d.Rejection = ""
		}
	case hasProposal && !hasRevision:
		tool, found := s.tools["semantic.propose"]
		p, ok := proposal.(nextstep.Object)
		if !found || !ok || tool.input.Validate(p) != nil {
			return
		}
		data := exact["proposal"].(nextstep.Object)
		// The effect supplies its own stable key and consumes MCP context locally.
		forward := make(nextstep.Object, len(data))
		for k, v := range data {
			forward[k] = v
		}
		forward["commandKey"] = effectKey
		delete(forward, "analysisId")
		encode := json.Marshal
		limit := 65536
		if p["artifactType"] == "VALUE_MAP" {
			if !validValueMapDefinition(p["definition"].(nextstep.Object)) {
				return
			}
			limit, encode = maxValueMapArgumentsBytes, marshalValueMapJSON
		}
		payload, err := encode(forward)
		turtle, _ := p["rdfTurtle"].(string)
		if err == nil && len(payload) <= limit && len(turtle) <= 49152 {
			d.Proposal, d.Rejection = payload, ""
		}
	}
}
