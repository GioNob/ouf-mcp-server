package nextstep

import (
	"fmt"
	"strconv"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
)

// Rejection codes are owner data. Only these fixed messages enter the rail.
func UnitRejectionMessage(code string) string {
	switch code {
	case "XLSX_HEADER_INVALID":
		return "Correggi le intestazioni del foglio e ricarica il file."
	case "XLSX_MERGED_CELLS":
		return "Il foglio contiene celle unite nell’area dei dati. Separale e ricarica il file."
	case "XLSX_FORMULA_WITHOUT_CACHED_VALUE":
		return "Ricalcola le formule, salva il foglio e ricarica il file."
	case "XLSX_CELL_ERROR":
		return "Correggi le celle in errore e ricarica il file."
	case "XLSX_DATA_OUTSIDE_HEADER":
		return "Allinea i dati alle colonne con intestazione e ricarica il file."
	case "XLSX_DATE_OUT_OF_RANGE":
		return "Correggi le date fuori intervallo e ricarica il file."
	case "XLSX_TIME_ZONE_UNDECLARED":
		return "Chiedi all’operatore di configurare il fuso orario prima di riprofilare il foglio."
	case "XLSX_ROW_LIMIT":
		return "Riduci il numero di righe del foglio e ricarica il file."
	case "XLSX_COLUMN_LIMIT":
		return "Riduci il numero di colonne del foglio e ricarica il file."
	case "XLSX_EMPTY_SHEET":
		return "Scegli un foglio che contenga una tabella."
	}
	return ""
}

func profileRef(profile Object) Object {
	return Object{"profileId": profile["profileId"], "profileVersion": profile["profileVersion"], "unitRef": profile["unitRef"], "schemaHash": profile["schemaHash"]}
}

// Continue at the first unoffered inventory row, including after a mixture
// of rejected and ready sheets. An owner page may contain the entire workbook.
func (e Engine) unitSelection(input, obs Object, rule manifest.Rule) Object {
	const preview = "source.file.preview"
	profile := obs["profileId"]
	offset := 0
	if input["offset"] != nil {
		var err error
		offset, err = strconv.Atoi(fmt.Sprint(input["offset"]))
		if err != nil {
			return ManualBlock("OWNER_RESULT_INVALID", "Verifica la pagina delle unità.", "OPERATOR")
		}
	}
	rows, _ := obs["units"].([]any)
	alts := []Object{}
	positions := []int{}
	for i, item := range rows {
		row, _ := item.(Object)
		if row["status"] == "READY" {
			args := Object{"view": "UNIT", "profileId": profile, "unitRef": row["unitRef"]}
			if !e.Valid(preview, args) {
				return ManualBlock("OWNER_RESULT_INVALID", "Verifica il riferimento dell’unità.", "OPERATOR")
			}
			alts = append(alts, call("CALL_TOOL", rule.Reason, preview, args))
			positions = append(positions, offset+i)
		}
	}
	more := obs["hasMore"] == true
	if len(alts) > 4 || more {
		var next any = obs["nextOffset"]
		if len(alts) > 3 {
			next, alts = positions[3], alts[:3]
		}
		args := Object{"view": "UNITS", "profileId": profile, "offset": next}
		if !e.Valid(preview, args) {
			return ManualBlock("OWNER_RESULT_INVALID", "Verifica la continuazione dell’inventario.", "OPERATOR")
		}
		alts = append(alts, call("CALL_TOOL", "SOURCE_UNIT_INVENTORY", preview, args))
	}
	switch len(alts) {
	case 0:
		// The last page may contain only rejected units. Earlier pages
		// still contain the ready units counted by the owner.
		if offset > 0 && fmt.Sprint(lookup(obs, "unitSummary.readyUnits")) != "0" {
			args := Object{"view": "UNITS", "profileId": profile}
			if e.Valid(preview, args) {
				return call("CALL_TOOL", "SOURCE_UNIT_INVENTORY", preview, args)
			}
		}
		message := "Nessuna unità pronta. Scegli un nuovo file da caricare."
		if len(rows) > 0 {
			row, _ := rows[0].(Object)
			code, _ := lookup(row, "rejection.code").(string)
			if detail := UnitRejectionMessage(code); detail != "" {
				message = "Nessuna unità pronta. " + detail
			}
		}
		return Block("SOURCE_UNITS_NONE_READY", "source.file.upload", message)
	case 1:
		return alts[0]
	}
	for _, alt := range alts {
		delete(alt, "contract")
	}
	next := Block(rule.Reason, "", rule.Message)
	next["alternatives"] = alts
	return next
}
