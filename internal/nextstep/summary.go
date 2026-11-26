package nextstep

import (
	"fmt"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/google/uuid"
)

func (e Engine) ExpiredSummary(id string) Object {
	next := e.ownerCall(analysis.ErrExpired.Error(), "source.analysis.read", Object{"analysisId": id, "view": "SUMMARY"}, nil)
	if next["action"] == "CALL_TOOL" {
		next["userMessage"] = "Il cursore è scaduto. Leggi il riepilogo corrente dalla prima pagina."
	}
	return next
}

func (e Engine) PreviewColumn(h analysis.Hint) Object {
	args := Object{"assetId": h.AssetID, "profileId": h.ProfileID, "view": "COLUMNS", "offset": h.Ordinal, "limit": 1}
	if !e.Valid("source.file.preview", args) {
		return nil
	}
	next := e.ownerCall("ANALYSIS_COLUMN_PREVIEW", "source.file.preview", args, h.Timing())
	if next["action"] == "CALL_TOOL" {
		next["userMessage"] = "Leggi i metadati della colonna da pianificare."
	}
	return next
}

func (e Engine) SummaryNext(id, view, cursor string, s analysis.Summary) Object {
	read := Object{"analysisId": id, "view": "SUMMARY"}
	if cursor != "" {
		read["cursor"] = cursor
		return e.ownerCall("ANALYSIS_NEXT_PAGE", "source.analysis.read", read, nil)
	}
	state := s.State()
	if view == "STATUS" && state != "RUNNING" && !analysis.PermissionHold(s.Hint.Reason) {
		return e.ownerCall("ANALYSIS_SUMMARY", "source.analysis.read", read, nil)
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
					"intentHint": Object{"originalName": h.Header},
					"arguments":  Object{"intent": analysis.ColumnIntent(h.Header), "preferredLanguages": []string{"it"}, "requestedArtifactType": "PROPERTY"}}}}
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
			reason := "ANALYSIS_CONTINUE"
			if h.DecisionRequired {
				reason = analysis.DiscoveryDecisionRequired
			}
			next := e.ownerCall(reason, h.Tool, args, h.Timing())
			if h.DecisionRequired && next["action"] == "CALL_TOOL" {
				next["userMessage"] = fmt.Sprintf("Decisione richiesta per la colonna di ordinale %d. Leggi la pagina dei candidati per scegliere come proseguire.", h.Ordinal)
			}
			if h.Tool == "source.analysis.plan" && next["action"] == "CALL_TOOL" {
				next["userMessage"] = planIntentMessage
			}
			return next
		}
		return ManualBlock("ANALYSIS_HELD", "Verifica la prosecuzione autorizzata dell’analisi.", "OPERATOR")
	case "COMPLETE":
		if !s.Onboarding {
			next := step("DONE", "ANALYSIS_COMPLETE", "Ogni colonna ha un esito verificato.")
			next["outcome"] = "SUCCEEDED"
			return next
		}
		const reason = "MAPPING_DECISION_REQUIRED"
		message := fmt.Sprintf("Con termine pubblicato: %d. Con candidati: %d. Senza candidati: %d. ", s.Counts.Matched, s.Counts.Ready, s.Counts.Zero)
		if s.Counts.Ready > 0 {
			message += "Confronta ogni colonna informativa con i candidati del suo requestId. Se uno la rappresenta usa semantic.discovery.adopt con la coppia letta. "
		} else {
			message += "Scegli i termini pubblicati che rappresentano le colonne informative. "
		}
		message += "Se manca un termine valuta semantic.propose. Escludi solo per provenienza o assenza di termini adatti e motiva la scelta all’utente. Prepara la fonte solo a termini pubblicati."
		next := Block(reason, "", message)
		alts := []Object{}
		add := func(tool string, args Object, label string) {
			if e.Valid(tool, args) {
				alts = append(alts, alternative(tool, args, reason, label))
			}
		}
		add("source.analysis.read", Object{"analysisId": id}, "Scegli il mapping con le evidenze già lette")
		if h.RequestID != "" {
			add("semantic.discovery.candidates", Object{"analysisId": id, "requestId": h.RequestID, "includeState": true}, "Consulta i candidati della richiesta indicata")
		}
		add("source.file.preview", Object{"assetId": h.AssetID, "profileId": h.ProfileID}, "Verifica il significato delle colonne sui valori del file")
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
