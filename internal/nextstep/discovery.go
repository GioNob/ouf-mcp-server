package nextstep

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

const planIntentMessage = "Prima di chiamare source.analysis.plan, usa intentHint per riformulare ogni intent in italiano. Descrivi che cosa rappresenta la colonna senza copiare i valori di esempio."

func permitsExamples(metadata Object) bool {
	for _, key := range []string{"redacted", "valuesRedacted", "distinctValuesRedacted"} {
		if flag, present := metadata[key]; present && flag != false {
			return false
		}
	}
	for _, key := range []string{"dataAccessLabel", "accessLabel"} {
		if label, present := metadata[key]; present && label != "OPEN" && label != "PUBLIC" {
			return false
		}
	}
	return true
}

// Copy only scalar examples from the owner's filtered distinct list. Never
// unwrap redaction objects, rows or cursor data.
func intentHint(page, column Object, name string) Object {
	hint := Object{"originalName": name}
	if typ, ok := get(column, nil, "dataType|inferredType|inferredDataType|type").(string); ok && utf8.RuneCountInString(typ) <= 80 {
		hint["dataType"] = typ
	}
	count := get(column, nil, "distinctCount|distinctValueCount|distinctValuesCount")
	if count == nil {
		// Earlier profiles used a numeric distinctValues for the count alone.
		switch v := column["distinctValues"].(type) {
		case json.Number, float64, int:
			count = v
		case []any:
			if permitsExamples(page) && permitsExamples(column) && column["distinctValuesComplete"] == true && page["offset"] == nil && page["view"] != "COLUMNS" {
				count = len(v)
			}
		}
	}
	if n, err := strconv.Atoi(fmt.Sprint(count)); err == nil && n >= 0 {
		hint["distinctCount"] = n
	}
	if !permitsExamples(page) || !permitsExamples(column) {
		return hint
	}
	var examples []any
	values, _ := column["distinctValues"].([]any)
	for _, value := range values {
		switch v := value.(type) {
		case string:
			marker := strings.ToUpper(strings.Trim(v, " []<>*"))
			if marker == "" || marker == "REDACTED" || marker == "MASKED" || utf8.RuneCountInString(v) > 256 {
				continue
			}
		case bool, json.Number, float64, int:
		default:
			continue
		}
		if !slices.ContainsFunc(examples, func(sample any) bool { return sameJSON(sample, value) }) {
			examples = append(examples, value)
			if len(examples) == 5 {
				break
			}
		}
	}
	if len(examples) > 0 {
		hint["distinctValues"] = examples
	}
	return hint
}

func (e Engine) emptyCandidates(input Object) Object {
	const reason = "DISCOVERY_NO_CANDIDATES"
	id, _ := input["analysisId"].(string)
	request, _ := input["requestId"].(string)
	options := []Object{}
	message := "La ricerca si è conclusa senza candidati disponibili."
	if id == "" {
		options = append(options, alternative("semantic.discovery.request", Object{"idempotencyKey": uuid.NewString()}, reason, "Riformula l’intento e completa tipo e lingue della nuova discovery."))
		message += " Puoi riformulare l’intento e avviare una nuova discovery."
	} else if e.ReformulationColumn != nil {
		if column, ok := e.ReformulationColumn(id, request); ok {
			args := Object{"replanOf": id, "requestId": request, "column": column, "commandKey": uuid.NewString()}
			options = append(options, alternative("source.analysis.plan", args, reason, "Completa intent con una nuova descrizione in italiano del significato della colonna indicata."))
			message += " Puoi riformulare l’intento e ripetere la ricerca per la stessa colonna."
		}
	}
	proposal, exclusion := uuid.NewString(), uuid.NewString()
	if id != "" && e.DecisionKeys != nil {
		var err error
		proposal, exclusion, err = e.DecisionKeys(id, request)
		if err != nil {
			return ManualBlock("ANALYSIS_STORE_UNAVAILABLE", "Verifica le decisioni registrate per la richiesta.", "OPERATOR")
		}
	}
	key := func(command string) Object {
		args := Object{"commandKey": command}
		if id != "" {
			args["analysisId"] = id
		}
		return args
	}
	if e.CanPropose != nil && e.CanPropose() {
		options = append(options, alternative("semantic.propose", key(proposal), reason, "Prepara una proposta semantica per la colonna."))
		message += " Puoi proporre un termine con semantic.propose."
	}
	if id != "" {
		message += " Puoi escludere la colonna dal mapping motivando la scelta."
		options = append(options, alternative(onboardingCreate, key(exclusion), reason, "Imposta extractionDecision a EXCLUDE nel mapping della colonna e spiega la motivazione all’utente."))
	}
	return draftDecision(reason, message, options)
}
