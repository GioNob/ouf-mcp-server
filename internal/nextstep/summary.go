package nextstep

import (
	"fmt"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
)

func (e Engine) SummaryNext(id, view, cursor string, s analysis.Summary) Object {
	read := Object{"analysisId": id, "view": "SUMMARY"}
	if cursor != "" {
		read["cursor"] = cursor
		return call("CALL_TOOL", "ANALYSIS_NEXT_PAGE", "source.analysis.read", read)
	}
	state := s.State()
	if view == "STATUS" && state != "RUNNING" {
		return call("CALL_TOOL", "ANALYSIS_SUMMARY", "source.analysis.read", read)
	}
	h := s.Hint
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
		add(proposalRead, Object{"analysisId": id, "reviewContextRef": id}, "Controlla le proposte semantiche già preparate per questo file")
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
		if reason == "OWNER_AUTHORIZATION_DENIED" || reason == "AUTHORIZATION_DENIED" {
			return ManualBlock(reason, "Verifica il permesso per riprendere l’analisi.", "HUMAN_THS")
		}
		return ManualBlock(reason, "Verifica il motivo che impedisce di proseguire.", "OPERATOR")
	}
}
