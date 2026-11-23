package nextstep

import "github.com/google/uuid"

const (
	onboardingCreate  = "source.onboarding.create"
	onboardingPrepare = "source.onboarding.review.prepare"
	onboardingRead    = "source.onboarding.read"
	readerCorrection  = "Ripeti gli stessi argomenti della create precedente con la nuova commandKey. Per i soli campi elencati nel corpo dell’errore, sostituisci transform con uno dei rispettivi fields[].allowedTransforms."
)

func alternative(tool string, args Object, reason, message string) Object {
	return Object{"action": "CALL_TOOL", "tool": tool, "arguments": args, "reasonCode": reason, "userMessage": message}
}

func draftDecision(reason, message string, options []Object) Object {
	if len(options) == 0 {
		return ManualBlock(reason, "Chiedi all’operatore di verificare la bozza e le azioni consentite dall’Onboarding.", "OPERATOR")
	}
	next := Block(reason, "", message)
	next["alternatives"] = options
	return next
}

// Keep only the immutable file references for the next decision. Some owner
// reads and standalone preparations do not carry the original create arguments.
func draftAssetRef(input Object) Object {
	if ref, ok := input["draftAssetRef"].(Object); ok {
		input = ref
	} else if ref, ok := input["assetRef"].(Object); ok {
		input = ref
	}
	asset, _ := input["assetId"].(string)
	profile, _ := input["profileId"].(string)
	if len(asset) != 36 || len(profile) != 36 || uuid.Validate(asset) != nil || uuid.Validate(profile) != nil {
		return nil
	}
	return Object{"assetId": asset, "profileId": profile}
}

// As in MAPPING_DECISION_REQUIRED, an AGENT completes the create from the
// original mapping before calling the tool. Never embed that mapping in next.
func draftCreateDecision(reason, message string, input Object) Object {
	args := draftAssetRef(input)
	if args == nil {
		args = Object{}
	}
	args["sourceId"], args["commandKey"] = input["sourceId"], uuid.NewString()
	if id := input["analysisId"]; id != nil {
		args["analysisId"] = id
	}
	return draftDecision(reason, message, []Object{alternative(onboardingCreate, args, reason, message)})
}

func correctableReaders(owner Object) bool {
	fields, _ := owner["fields"].([]any)
	for _, item := range fields {
		field, _ := item.(Object)
		transform, _ := field["transform"].(string)
		name, _ := field["fieldName"].(string)
		allowed, _ := field["allowedTransforms"].([]any)
		correctable := false
		for _, value := range allowed {
			if reader, ok := value.(string); ok && reader != "" && reader != transform {
				correctable = true
				break
			}
		}
		if name == "" || transform == "" || !correctable {
			return false
		}
	}
	return len(fields) > 0
}

func (e Engine) draftFailure(tool, code, status string, input, owner Object) Object {
	var options []Object
	switch {
	case tool == onboardingCreate && status == "422" && code == "ONB_READER_UNSUPPORTED":
		if _, reported := owner["fields"]; !reported {
			return nil // Predecessor reports only fieldName.
		}
		if correctableReaders(owner) {
			return draftCreateDecision(code, readerCorrection, input)
		}
		return draftDecision(code, readerCorrection, nil)
	case tool == onboardingCreate && status == "412" && code == "ONB_REVIEW_STALE_VERSION":
		message := "Rileggi la versione corrente della fonte prima di riproporre la sostituzione."
		read := latestSource(input["sourceId"], input)
		read["draftAssetRef"] = draftAssetRef(input)
		if e.Valid(onboardingRead, read) {
			options = append(options, alternative(onboardingRead, read, code, message))
		}
		return draftDecision(code, message, options)
	case tool == onboardingPrepare && status == "422" && code == "ONB_DATATYPE_UNRESOLVED":
		if matches(map[string]string{"resolutions": "REPLACE_DRAFT"}, owner, input) && correctableReaders(owner) {
			return draftCreateDecision(code, readerCorrection, input)
		}
	case tool == onboardingCreate && status == "409" && code == "ONB_REVISION_ASSET_SOURCE_CONFLICT":
	default:
		return nil
	}

	message := "Scegli una delle azioni consentite dall’Onboarding."
	if matches(map[string]string{"resolutions": "CREATE_REVISION"}, owner, input) {
		source := owner["boundSourceId"]
		if source == nil && tool == onboardingPrepare {
			source = input["sourceId"]
		}
		read := latestSource(source, input)
		instruction := "Leggi la fonte legata per ottenere il padre, la versione e l’hash necessari a source.onboarding.revision.prepare."
		if ref := draftAssetRef(input); ref != nil {
			read["revisionAssetRef"] = ref
		} else {
			instruction = "Completa revisionAssetRef con assetId e profileId della create precedente e leggi la fonte per preparare la revisione."
		}
		if e.Valid(onboardingRead, read) {
			options = append(options, alternative(onboardingRead, read, code, instruction))
		}
	}
	if matches(map[string]string{"resolutions": "UPLOAD_NEW_FILE"}, owner, input) && e.Valid("source.file.upload", Object{}) {
		options = append(options, alternative("source.file.upload", Object{}, code, "Apri un nuovo selettore e scegli il file da caricare."))
	}
	return draftDecision(code, message, options)
}

func latestSource(source any, input Object) Object {
	read := Object{"view": "LATEST", "sourceId": source}
	if id := input["analysisId"]; id != nil {
		read["analysisId"] = id
	}
	return read
}

func (e Engine) afterDraftRecoveryRead(input, owner Object) Object {
	if input["draftAssetRef"] == nil && input["revisionAssetRef"] == nil {
		return nil
	}
	var options []Object
	const reason = "CURRENT_REVIEW_REQUIRED"
	message := "Verifica la versione riletta prima di confermare la modifica."
	if owner["sourceId"] != input["sourceId"] || owner["replay"] == true || owner["unchanged"] == true {
		return draftDecision(reason, message, nil)
	}
	switch get(owner, nil, "workflowState|state") {
	case "DRAFT":
		draft, _ := input["draftAssetRef"].(Object)
		version, _ := owner["onboardingVersionId"].(string)
		asset := get(owner, nil, "assetRef.assetId|assetId")
		profile := get(owner, nil, "assetRef.profileId|profileId")
		if draft != nil && uuid.Validate(version) == nil &&
			(asset == nil || asset == draft["assetId"]) && (profile == nil || profile == draft["profileId"]) &&
			matches(map[string]string{"allowedActions": "REPLACE_DRAFT|PREPARE_REVIEW|-"}, owner, input) {
			return draftCreateDecision(reason, "Confronta la bozza riletta con la correzione. Se la sostituzione è ancora appropriata, ripeti gli argomenti della create di sostituzione con la nuova commandKey.", input)
		}
	case "ACTIVE":
		if asset := input["revisionAssetRef"]; asset != nil {
			const revision = "source.onboarding.revision.prepare"
			args := Object{"sourceId": owner["sourceId"], "parentVersionId": owner["onboardingVersionId"], "expectedVersion": owner["lockVersion"],
				"configurationHash": owner["configurationHash"], "publicationEffect": "NEW_PUBLICATION_MAY_START_ACQUISITION", "commandKey": uuid.NewString(), "assetRef": asset}
			if e.Valid(revision, args) {
				options = append(options, alternative(revision, args, reason, "Verifica l’effetto di pubblicazione e prepara la revisione della fonte legata con il file indicato."))
			}
		}
	}
	return draftDecision(reason, message, options)
}
