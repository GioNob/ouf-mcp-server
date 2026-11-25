package nextstep

import (
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/google/uuid"
)

func (e Engine) SummaryNext(id, view, cursor string, s analysis.Summary) Object {
	read := Object{"analysisId": id, "view": "SUMMARY"}
	if cursor != "" {
		read["cursor"] = cursor
		return call("CALL_TOOL", "ANALYSIS_NEXT_PAGE", "source.analysis.read", read)
	}
	state := s.State()
	if view == "STATUS" && state != "RUNNING" && !analysis.PermissionHold(s.Hint.Reason) {
		return call("CALL_TOOL", "ANALYSIS_SUMMARY", "source.analysis.read", read)
	}
	h := s.Hint
	if state == "PARTIAL" && h.Reason != "" {
		state = "BLOCKED"
	}
	switch state {
	case "RUNNING":
		var args Object
		switch h.Tool {
		case "source.analysis.step":
			args = Object{"analysisId": id, "taskId": h.TaskID}
		case "source.analysis.plan":
			args = Object{"assetId": h.AssetID, "profileId": h.ProfileID, "expectedRevision": h.Revision,
				"work": []Object{{"tool": "semantic.discovery.request", "columns": []int{h.Ordinal},
					"arguments": Object{"intent": h.Header, "preferredLanguages": []string{"it"}, "requestedArtifactType": "PROPERTY"}}}}
		case "semantic.discovery.status", "semantic.discovery.candidates":
			args = Object{"analysisId": id, "requestId": h.RequestID}
			if h.Tool == "semantic.discovery.candidates" {
				args["includeState"] = true
			}
		}
		if (h.Tool == "source.analysis.plan" || h.Tool == "source.analysis.step") && e.CommandKey != nil {
			args["commandKey"] = e.CommandKey(h.Tool, args, nil, nil)
		}
		if e.Valid(h.Tool, args) {
			return call("CALL_TOOL", "ANALYSIS_CONTINUE", h.Tool, args)
		}
		return ManualBlock("ANALYSIS_HELD", "Verifica la prosecuzione autorizzata dell’analisi.", "OPERATOR")
	case "COMPLETE":
		if !s.Onboarding {
			next := step("DONE", "ANALYSIS_COMPLETE", "Ogni colonna ha un esito verificato.")
			next["outcome"] = "SUCCEEDED"
			return next
		}
		next := Block("MAPPING_DECISION_REQUIRED", "", "Scegli il mapping usando gli esiti delle colonne e i riferimenti verificati.")
		alts := []Object{}
		for _, option := range []struct {
			tool string
			args Object
		}{
			{"semantic.discovery.candidates", Object{"analysisId": id, "requestId": h.RequestID, "includeState": true}},
			{"source.file.preview", Object{"assetId": h.AssetID, "profileId": h.ProfileID}},
		} {
			if e.Valid(option.tool, option.args) {
				alts = append(alts, Object{"action": "CALL_TOOL", "tool": option.tool, "arguments": option.args, "reasonCode": "MAPPING_DECISION_REQUIRED", "userMessage": "Consulta l’evidenza per scegliere il mapping."})
			}
		}
		next["alternatives"] = alts
		return next
	case "PARTIAL":
		return ManualBlock("ANALYSIS_PARTIAL", "Verifica i motivi delle colonne bloccate nel riepilogo.", "OPERATOR")
	default:
		reason := h.Reason
		if reason == "" {
			reason = "ANALYSIS_HELD"
		}
		if analysis.PermissionHold(reason) || reason == analysis.ErrEpochChanged.Error() {
			return e.ReplanNext(id, reason)
		}
		if reason == "AUTHORIZATION_DENIED" {
			return permissionHold(reason, "Verifica i permessi del ruolo nella THS.")
		}
		switch reason {
		case "SEM_READ_RECEIPT_INVALID", "SEM_DISCOVERY_RECEIPT_INVALID":
			return ManualBlock(reason, "La ricevuta di lettura è invalida. Chiedi all’operatore di verificare la piattaforma.", "OPERATOR")
		}
		return ManualBlock(reason, "Verifica il motivo che impedisce di proseguire.", "OPERATOR")
	}
}

func (e Engine) ReplanNext(id, reason string) Object {
	const message = "Dopo la conferma della correzione dei permessi nella THS, ripianifica l'analisi da qui"
	next := permissionHold(reason, message)
	args := Object{"replanOf": id, "commandKey": uuid.NewString()}
	if analysis.ValidateID(id) == nil && (e.Valid == nil || e.Valid("source.analysis.plan", args)) {
		next["alternatives"] = append(next["alternatives"].([]Object), alternative("source.analysis.plan", args, reason, message))
	}
	return next
}

func permissionHold(reason, message string) Object {
	next := ManualBlock(reason, message, "HUMAN_THS")
	next["alternatives"] = append(next["alternatives"].([]Object), alternative("authorization.permissions.read", Object{"view": "ROLES"}, reason, "Rileggi il catalogo dei ruoli."))
	return next
}
