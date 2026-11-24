package nextstep

import (
	"time"

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
