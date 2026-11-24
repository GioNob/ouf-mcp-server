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
			if h.UnitProfileRef != nil {
				delete(args, "assetId")
				delete(args, "profileId")
				ref := h.UnitProfileRef
				args["unitProfileRef"] = Object{"profileId": ref.ProfileID, "profileVersion": ref.Version, "unitRef": ref.UnitRef, "schemaHash": ref.SchemaHash}
			}
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
			if option.tool == "source.file.preview" && h.UnitProfileRef != nil {
				option.args = Object{"view": "UNIT", "profileId": h.ProfileID, "unitRef": h.UnitProfileRef.UnitRef}
			}
			if e.Valid(option.tool, option.args) {
				alts = append(alts, Object{"action": "CALL_TOOL", "tool": option.tool, "arguments": option.args, "reasonCode": "MAPPING_DECISION_REQUIRED", "userMessage": "Consulta l’evidenza per scegliere il mapping."})
			}
		}
		if h.UnitProfileRef != nil {
			create := Object{"analysisId": id, "unitProfileRef": h.UnitProfileRef, "commandKey": uuid.NewString()}
			alts = append(alts, alternative("source.onboarding.create", create, "MAPPING_DECISION_REQUIRED", "Completa il mapping del foglio con i riferimenti verificati."))
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
		if reason == "OWNER_AUTHORIZATION_DENIED" || reason == "AUTHORIZATION_DENIED" {
			return ManualBlock(reason, "Verifica il permesso per riprendere l’analisi.", "HUMAN_THS")
		}
		return ManualBlock(reason, "Verifica il motivo che impedisce di proseguire.", "OPERATOR")
	}
}
