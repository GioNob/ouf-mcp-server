package nextstep

import (
	"strconv"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
)

func RunNext(r analysis.RunRecord, now time.Time, interval time.Duration) Object {
	read := Object{"analysisId": r.ID(), "view": "SUMMARY"}
	switch r.State {
	case "COMPLETE", "PARTIAL":
		return call("CALL_TOOL", "ANALYSIS_SUMMARY", "source.analysis.read", read)
	case "BLOCKED":
		return ManualBlock(r.Reason, "Verifica il motivo di arresto del piano.", "OPERATOR")
	}
	if !now.Before(r.Deadline) {
		return ManualBlock("WAIT_DEADLINE_EXPIRED", "Il tempo di attesa del piano è terminato.", "OPERATOR")
	}
	delay := min(max(interval, r.NotBefore.Sub(now)), r.Deadline.Sub(now))
	reason := r.Reason
	if reason == "" {
		reason = "ANALYSIS_RUNNING"
	}
	return Object{"contract": Contract, "action": "POLL", "reasonCode": reason, "tool": "source.analysis.read",
		"arguments": Object{"analysisId": r.ID(), "view": "STATUS"}, "pollAfterSeconds": int((delay + time.Second - 1) / time.Second),
		"terminalStates": []string{"COMPLETE", "PARTIAL", "BLOCKED"}, "deadline": r.Deadline.UTC().Format(time.RFC3339Nano),
		"userMessage": "L’analisi è in corso. Attendi il riepilogo del profilo."}
}

func (e Engine) RenewalNext(p analysis.RenewalProposal, reviewBase string) Object {
	const read = "source.analysis.read"
	args := Object{"analysisId": p.AnalysisID, "view": "STATUS"}
	message := "Il budget è esaurito. Apri la proposta di rinnovo nella THS."
	blocked := Block(analysis.ErrRunExhausted.Error(), read, message)
	blocked["actor"] = "HUMAN_THS"
	blocked["alternatives"].([]Object)[0]["arguments"] = args
	if p.State == "CHANGES_REQUESTED" {
		revise := Object{"analysisId": p.AnalysisID, "incidentId": p.IncidentID, "proposalVersion": p.Version, "requestedChanges": p.RequestedChanges}
		revise["commandKey"] = e.CommandKey(analysis.RenewalTool, revise, nil, nil)
		if e.Valid(analysis.RenewalTool, revise) {
			return call("CALL_TOOL", "ANALYSIS_CHANGES_REQUESTED", analysis.RenewalTool, revise)
		}
	}
	if p.State == "OPEN" && reviewBase != "" {
		obs := Object{"state": "BLOCKED", "version": strconv.FormatInt(p.ExpectedSeq, 10), "reviewUrl": reviewBase + p.ReviewRef, "expiresAt": p.Expires.UTC().Format(time.RFC3339Nano)}
		wait := e.Issue(manifest.Rule{Tool: read, Action: "WAIT_FOR_HUMAN", Decision: "REVIEW_ANALYSIS_RENEWAL", Reason: p.Reason}, args, obs)
		if wait["action"] == "WAIT_FOR_HUMAN" {
			delete(wait, "contract")
			delete(wait, "deadline")
			blocked["alternatives"] = append([]Object{wait}, blocked["alternatives"].([]Object)...)
		} else {
			return wait
		}
	} else {
		blocked["userMessage"] = "Consulta lo stato del piano e la disponibilità della revisione."
	}
	return blocked
}
