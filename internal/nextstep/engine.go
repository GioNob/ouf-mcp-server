// Package nextstep projects owner observations into the versioned workflow contract.
package nextstep

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

type Object = map[string]any

const Contract = "ouf.next-step.v1"

const DefaultWaitMaxSeconds = 3600

type Engine struct {
	PublishedLanguages  []string
	WaitMaxSeconds      int
	PollIntervalSeconds int
	Errors              []manifest.Rule
	Valid               func(string, Object) bool
	Issue               func(manifest.Rule, Object, Object) Object
	Now                 func() time.Time
	AnalysisID          func(string, string) string
	CommandKey          func(string, Object, Object, Object) string
}

func step(action, reason, message string) Object {
	return Object{"contract": Contract, "action": action, "reasonCode": reason, "userMessage": message}
}

func Block(reason, tool, message string) Object {
	s := step("BLOCKED", reason, message)
	alts := []Object{}
	if tool != "" {
		alts = append(alts, Object{"action": "CALL_TOOL", "reasonCode": reason, "tool": tool, "userMessage": message})
	}
	s["actor"], s["alternatives"] = "AGENT", alts
	return s
}

func lookup(obj Object, path string) any {
	var value any = obj
	for _, k := range strings.Split(path, ".") {
		switch v := value.(type) {
		case Object:
			value = v[k]
		case []any:
			i, err := strconv.Atoi(k)
			if err != nil || i < 0 || i >= len(v) {
				return nil
			}
			value = v[i]
		default:
			return nil
		}
	}
	return value
}

func incompleteColumn(obs Object) Object {
	find := func(container Object) Object {
		columns, _ := container["columns"].([]any)
		for _, item := range columns {
			column, _ := item.(Object)
			if column["distinctValuesComplete"] == false {
				return column
			}
		}
		return nil
	}
	if column := find(obs); column != nil {
		return column
	}
	layers, _ := lookup(obs, "metadata.layers").([]any)
	for _, item := range layers {
		layer, _ := item.(Object)
		if column := find(layer); column != nil {
			return column
		}
	}
	return nil
}

func get(obs, input Object, paths string) any {
	switch paths {
	case "executionProposalRequired":
		if required, ok := obs[paths].(bool); ok {
			return required
		}
		return nil
	case "#incompleteColumn":
		return incompleteColumn(obs) != nil
	case "#distinctCursor":
		// An absent continuation must fail the cursor branch even though
		// a fresh COLUMNS request would otherwise validate without it.
		cursor, _ := incompleteColumn(obs)["distinctValuesCursor"].(string)
		return cursor
	case "#afterCursorColumn":
		// A distinct-values page always reports hasMore=false, even when
		// subsequent columns remain in the immutable flat profile.
		offset, e1 := strconv.Atoi(fmt.Sprint(obs["offset"]))
		count, e2 := strconv.Atoi(fmt.Sprint(obs["columnCount"]))
		if e1 == nil && e2 == nil && offset >= 0 && count > 0 && count <= 1024 && offset < count-1 {
			return offset + 1
		}
		return nil
	case "#readyTask":
		tasks, _ := obs["tasks"].([]any)
		for _, item := range tasks {
			task, _ := item.(Object)
			switch task["state"] {
			case "PLANNED", "READY", "PENDING":
				return task["taskId"]
			}
		}
		return nil
	case "#resumeTask", "#allTerminal", "#nextTaskOffset":
		tasks, ok := obs["tasks"].([]any)
		if !ok || len(tasks) == 0 {
			return nil
		}
		if paths == "#nextTaskOffset" {
			offset, err := strconv.Atoi(fmt.Sprint(obs["offset"]))
			if obs["hasMore"] == true && err == nil && offset >= 0 {
				return offset + len(tasks)
			}
			return nil
		}
		terminal := true
		for _, item := range tasks {
			task, _ := item.(Object)
			switch task["state"] {
			case "COMPLETED", "SUCCEEDED":
				continue
			case "WAITING", "RUNNING":
				if paths == "#resumeTask" {
					return task["taskId"]
				}
			case "HELD", "FAILED":
				if paths == "#resumeTask" && task["retryable"] == true {
					return task["taskId"]
				}
			}
			terminal = false
		}
		if paths == "#allTerminal" {
			// A caller can resume on a later page. The owner's totals cover
			// the whole plan, including tasks that are not in this page.
			if planned := lookup(obs, "budget.plannedCalls"); planned != nil &&
				fmt.Sprint(planned) != fmt.Sprint(lookup(obs, "budget.completedTasks")) {
				return false
			}
			return terminal && obs["hasMore"] != true
		}
		return nil
	}
	for _, path := range strings.Split(paths, "|") {
		if strings.HasPrefix(path, "=") {
			return path[1:]
		}
		source := obs
		if strings.HasPrefix(path, "@") {
			source, path = input, path[1:]
		}
		if value := lookup(source, path); value != nil && value != "" {
			return value
		}
	}
	return nil
}

func matches(when map[string]string, obs, input Object) bool {
	for path, alternatives := range when {
		value := get(obs, input, path)
		allowed := strings.Split(alternatives, "|")
		found := value != nil && slices.Contains(allowed, "?")
		switch v := value.(type) {
		case nil:
			found = slices.Contains(allowed, "-")
		case []any:
			for _, item := range v {
				if s, ok := item.(string); ok && slices.Contains(allowed, s) {
					found = true
				}
			}
		case string, bool:
			found = found || slices.Contains(allowed, fmt.Sprint(v))
		case json.Number, float64, float32, int, int64, int32, int16, int8, uint, uint64, uint32, uint16, uint8:
			if found {
				continue
			}
			number, ok := new(big.Rat).SetString(fmt.Sprint(v))
			for _, literal := range allowed {
				want, valid := new(big.Rat).SetString(literal)
				if ok && valid && number.Cmp(want) == 0 {
					found = true
					break
				}
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Plan only from column metadata. The coordinator generates the durable job
// keys. Wider profiles require the existing paged analysis workflow.
func (e Engine) discoveryWork(obs Object) any {
	langs := e.PublishedLanguages
	if len(langs) == 0 {
		langs = []string{"it"}
	}
	columns, ok := obs["columns"].([]any)
	if !ok || len(columns) == 0 || len(columns) > analysis.MaxPlanBatch {
		return nil
	}
	work := make([]Object, 0, len(columns))
	offset := 0
	if raw := obs["offset"]; raw != nil {
		var err error
		offset, err = strconv.Atoi(fmt.Sprint(raw))
		if err != nil || offset < 0 || offset > 1024-len(columns) {
			return nil
		}
	}
	for i, col := range columns {
		column, _ := col.(Object)
		name, ok := column["name"].(string)
		if !ok || !analysis.UsableColumnName(name) {
			continue
		}
		work = append(work, Object{"tool": "semantic.discovery.request", "columns": []int{offset + i}, "arguments": Object{
			"intent": name, "preferredLanguages": langs, "requestedArtifactType": "PROPERTY",
		}})
	}
	return work
}

func (e Engine) project(r manifest.Rule, obs, input Object) Object {
	args := Object{}
	if r.CopyInput {
		maps.Copy(args, input)
	}
	for k, selector := range r.Args {
		value := get(obs, input, selector)
		switch selector {
		case "#deferredAssetRef":
			value = nil
			if lookup(obs, "deferredReaderChecks.0.fieldName") != nil && (obs["sourceId"] == nil || obs["sourceId"] == input["sourceId"]) {
				ref := draftAssetRef(input)
				if ref == nil {
					ref = draftAssetRef(obs)
				}
				if ref != nil {
					value = ref
				}
			}
		case "#true":
			value = true
		case "#uuid":
			continue
		case "#decisionKey":
			// The agent has not authored the intent yet. Allocate a key for
			// this offered command; retries use the completed arguments.
			value = uuid.NewString()
		case "#searchQuery":
			// Discovery intents can exceed the search schema's query limit.
			// Only bind the original wording when it is executable as a query.
			value = nil
			if intent := get(obs, input, "intent|@intent"); intent != nil && e.Valid(r.Tool, Object{"q": intent}) {
				value = intent
			}
		case "#zero":
			value = 0
		case "#discovery":
			value = e.discoveryWork(obs)
		}
		if value != nil {
			args[k] = value
		}
	}
	if r.Args["commandKey"] == "#uuid" {
		if e.CommandKey != nil {
			args["commandKey"] = e.CommandKey(r.Tool, args, obs, input)
		} else {
			args["commandKey"] = uuid.NewString()
		}
	}
	return args
}

func call(action, reason, tool string, args Object) Object {
	s := step(action, reason, "Prosegui con la capability indicata.")
	s["tool"], s["arguments"] = tool, args
	return s
}

func (e Engine) Evaluate(c manifest.Capability, policy manifest.Policy, input, owner Object, failed bool) Object {
	return ensureProgress(c.ToolName, input, e.evaluate(c, policy, input, owner, failed))
}

// Compare decoded JSON numbers by value, including 1, 1.0 and 1e0.
// Float64 would also merge distinct integer versions above 2^53.
func sameJSON(a, b any) bool {
	switch x := a.(type) {
	case Object:
		y, ok := b.(Object)
		return ok && maps.EqualFunc(x, y, sameJSON)
	case []any:
		y, ok := b.([]any)
		return ok && slices.EqualFunc(x, y, sameJSON)
	case json.Number:
		y, ok := b.(json.Number)
		if !ok || x == y {
			return ok
		}
		left, ok := new(big.Rat).SetString(x.String())
		right, valid := new(big.Rat).SetString(y.String())
		return ok && valid && left.Cmp(right) == 0
	default:
		return a == b
	}
}

func wireArgumentsEqual(input Object, args any) bool {
	decode := func(v any) (Object, error) {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		var obj Object
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		err = decoder.Decode(&obj)
		return obj, err
	}
	a, err := decode(input)
	b, otherErr := decode(args)
	return err != nil || otherErr != nil || sameJSON(a, b)
}

// A missing argument list on a same-tool hint cannot identify a different
// request. Compare wire values so typed maps, integers and decoded JSON agree.
func ensureProgress(tool string, input, next Object) Object {
	same := func(target Object) bool {
		if target["tool"] != tool {
			return false
		}
		delay, _ := target["pollAfterSeconds"].(int)
		states, _ := target["terminalStates"].([]string)
		if target["action"] == "POLL" && delay > 0 && len(states) > 0 {
			return false
		}
		if target["arguments"] == nil {
			return true
		}
		return wireArgumentsEqual(input, target["arguments"])
	}
	reason := next["reasonCode"].(string)
	if same(next) {
		return ManualBlock(reason, "Il passo indicato ripete la richiesta. Chiedi all’operatore di verificare lo stato.", "OPERATOR")
	}
	if alts, ok := next["alternatives"].([]Object); ok {
		remaining := slices.DeleteFunc(alts, same)
		if len(alts) > 0 && len(remaining) == 0 {
			actor, _ := next["actor"].(string)
			if actor == "AGENT" || reason == "WAIT_DEADLINE_EXPIRED" {
				actor = "OPERATOR"
			}
			message := next["userMessage"].(string)
			return ManualBlock(reason, message, actor)
		}
		next["alternatives"] = remaining
	}
	return next
}

func ManualBlock(reason, message, actor string) Object {
	s := Block(reason, "", message)
	s["actor"] = actor
	s["alternatives"] = []Object{{"action": "WAIT_FOR_HUMAN", "reasonCode": reason, "userMessage": message}}
	return s
}

// A fallback read is a concrete call. Keep only the reference used by that
// read, even when the failed command carried a much larger input.
func readReference(tool string, obs, input Object) Object {
	args := Object{}
	var fields []string
	switch tool {
	case "source.analysis.read":
		fields = []string{"analysisId"}
	case "semantic.proposal.read":
		fields = []string{"commandKey", "analysisId"}
	case "authorization.proposal.read":
		fields = []string{"proposalId"}
	}
	for _, field := range fields {
		if v := get(obs, input, field+"|@"+field); v != nil {
			args[field] = v
		}
	}
	return args
}

func (e Engine) blockTarget(reason, tool string, args Object, message, actor string) Object {
	if tool == "" || e.Valid == nil || !e.Valid(tool, args) {
		if actor == "" || actor == "AGENT" {
			actor = "OPERATOR"
			if tool == "authorization.proposal.read" {
				actor = "HUMAN_THS"
			}
		}
		return ManualBlock(reason, message, actor)
	}
	s := Block(reason, tool, message)
	if len(args) > 0 {
		s["alternatives"].([]Object)[0]["arguments"] = args
	}
	if actor != "" {
		s["actor"] = actor
	}
	return s
}

// Neither a denial nor a catalogue entry supplies a user's grant interval,
// grant ID or intended role changes. Read the complete role catalogue before
// preparing a role proposal, preserving unrelated entries and assignments.
func PermissionDenied(capability, reason string) Object {
	message := "Manca il permesso per " + capability + ". Verifica la configurazione dei permessi nella THS."
	s := Block(reason, "", message)
	s["actor"] = "HUMAN_THS"
	read := func(view, instruction string) Object {
		return Object{"action": "CALL_TOOL", "tool": "authorization.permissions.read", "arguments": Object{"view": view},
			"reasonCode": reason, "userMessage": instruction}
	}
	s["alternatives"] = []Object{
		read("CAPABILITIES", "Verifica se il catalogo include il permesso "+capability+"."),
		read("ROLES", "Leggi ruoli e assegnazioni per preparare la modifica con authorization.permissions.propose."),
	}
	return s
}

// ProposalSections projects only contract section names into the receipt and message.
func ProposalSections(values []any) []string {
	sections := []string{}
	for _, value := range values {
		name, _ := value.(string)
		switch name {
		case "semanticBindings", "semanticPublicationSetRef", "udp.resolution", "udp.materialization", "limits":
			if !slices.Contains(sections, name) {
				sections = append(sections, name)
			}
		}
	}
	return sections
}

func (e Engine) blockedRule(r manifest.Rule, obs, input Object) Object {
	if strings.Contains(r.Message, "{missingSections}") {
		sections, _ := obs["missingSections"].([]any)
		label := strings.Join(ProposalSections(sections), ", ")
		if label == "" {
			label = "le sezioni richieste della proposta esecutiva"
		}
		r.Message = strings.ReplaceAll(r.Message, "{missingSections}", label)
	}
	if len(r.Alternatives) == 0 {
		args := readReference(r.Tool, obs, input)
		if len(r.Args) > 0 || r.CopyInput {
			args = e.project(r, obs, input)
		}
		if r.Decision != "" && e.Issue != nil && e.Valid(r.Tool, args) {
			return e.Issue(r, args, obs)
		}
		return e.blockTarget(r.Reason, r.Tool, args, r.Message, r.Actor)
	}
	alts := make([]Object, 0, len(r.Alternatives))
	for _, option := range r.Alternatives {
		alt := Object{"action": "CALL_TOOL", "tool": option.Tool, "reasonCode": r.Reason, "userMessage": option.Message}
		args := e.project(option, obs, input)
		if option.CopyInput && !e.Valid(option.Tool, args) {
			continue
		}
		if len(args) > 0 {
			alt["arguments"] = args
		}
		alts = append(alts, alt)
	}
	s := Block(r.Reason, "", r.Message)
	s["alternatives"] = alts
	if r.Actor != "" {
		s["actor"] = r.Actor
	}
	return s
}

func (e Engine) evaluate(c manifest.Capability, policy manifest.Policy, input, owner Object, failed bool) Object {
	if failed {
		return e.failure(c, policy, input, owner)
	}
	obs := Observation(c.ToolName, input, owner)
	if c.ToolName == onboardingRead && input["view"] == "LATEST" {
		if next := e.afterDraftRecoveryRead(input, obs); next != nil {
			return next
		}
	}
	if c.ToolName == "source.file.upload.status" || c.ToolName == "source.file.upload" {
		if obs["legacy"] == true {
			return (WaitRef{Read: "source.file.upload", Args: Object{}}).Block("HANDOFF_NOT_ISSUED", "Apri un nuovo selettore per scegliere il file.")
		}
		if obs["state"] == "EXPIRED" || obs["status"] == "EXPIRED" {
			args := Object{"handoffId": get(obs, input, "@handoffId|handoffId")}
			return (WaitRef{Read: "source.file.upload.status", Args: args}).Block("WAIT_DEADLINE_EXPIRED", "Rileggi l’esito registrato.")
		}
		waitUntil, _ := obs["waitUntil"].(string)
		if waitUntil == "" && (obs["handoffId"] != nil || c.ToolName == "source.file.upload.status" || obs["status"] == "AWAITING_FILE_SELECTION") {
			return ManualBlock("HANDOFF_DEADLINE_REQUIRED", "Aggiorna l’owner Onboarding per ottenere la scadenza del selettore.", "OPERATOR")
		}
		if c.ToolName == "source.file.upload.status" && obs["state"] == "PENDING" && obs["pickerUrl"] == nil {
			if end, err := time.Parse(time.RFC3339Nano, waitUntil); err == nil && !e.Now().Before(end) {
				return (WaitRef{Read: c.ToolName, Args: input}).Block("WAIT_DEADLINE_EXPIRED", "L’attesa è scaduta.")
			}
			return Block("HANDOFF_REFERENCE_REQUIRED", "ouf.wait", "Riprendi l’attesa con il waitRef ricevuto all’apertura del selettore.")
		}
	}
	if c.ToolName == "ouf.ingestion.status" {
		obs = recoveryObservation(obs)
	}
	if obs["replay"] == true {
		tool, _ := obs["readTool"].(string)
		args, _ := obs["readArguments"].(Object)
		pickerReceipt := c.ToolName == "source.file.upload" && tool == "source.file.upload.status"
		if (tool == policy.Read || pickerReceipt) && args != nil && e.Valid(tool, args) {
			if id := input["analysisId"]; id != nil {
				contextual := maps.Clone(args)
				contextual["analysisId"] = id
				if e.Valid(tool, contextual) {
					args = contextual
				}
			}
			return call("CALL_TOOL", "READ_ORIGINAL_RECEIPT", tool, args)
		}
		return ManualBlock("READ_ORIGINAL_RECEIPT", "Verifica la ricevuta del comando originale.", "OPERATOR")
	}
	for path, allowed := range policy.Vocabulary {
		if path == "activationProgress.readiness_phase" && obs["allowedActions"] != nil {
			continue
		}
		for _, field := range strings.Split(path, "|") {
			if v := lookup(obs, field); v != nil {
				state, ok := v.(string)
				if !ok || !slices.Contains(allowed, state) {
					return e.blockTarget("OWNER_STATE_UNKNOWN", policy.Read, readReference(policy.Read, obs, input), "Lo stato ricevuto richiede una nuova lettura.", "")
				}
				break
			}
		}
	}
	for _, r := range policy.Rules {
		if !matches(r.When, obs, input) {
			continue
		}
		switch r.Action {
		case "BLOCKED":
			if r.Reason == "EXECUTION_PROPOSAL_REQUIRED" && lookup(obs, "deferredReaderChecks.0.fieldName") != nil {
				r.Message = "Completa executionProposal ({missingSections}) con semanticId, semanticVersion, revisionId e publicationSetId da semantic.get. Usa quel publicationSetId per semanticPublicationSetRef e i limiti della preview. I campi in deferredReaderChecks saranno verificati in preparazione. Per un CSV, IDENTITY è sempre preparabile."
			}
			return e.blockedRule(r, obs, input)
		case "DONE":
			msg := "La lettura degli oggetti è completata."
			if r.Outcome != "SUCCEEDED" {
				msg = "Il flusso si è concluso senza pubblicare altri dati."
			}
			if r.Message != "" {
				msg = r.Message
			}
			s := step("DONE", r.Reason, msg)
			s["outcome"] = r.Outcome
			return s
		}
		args := e.project(r, obs, input)
		if r.Tool == "source.analysis.read" && args["analysisId"] == nil && e.AnalysisID != nil {
			for _, pair := range []any{obs["nextAction"], obs["assetRef"], obs, input["assetRef"], input} {
				source, _ := pair.(Object)
				asset, _ := source["assetId"].(string)
				profile, _ := source["profileId"].(string)
				if uuid.Validate(asset) == nil && uuid.Validate(profile) == nil {
					args["analysisId"] = e.AnalysisID(asset, profile)
					break
				}
			}
		}
		if !e.Valid(r.Tool, args) {
			actor := "OPERATOR"
			if r.Action == "WAIT_FOR_HUMAN" {
				actor = "HUMAN_THS"
			}
			return ManualBlock(r.Reason, "Verifica i riferimenti mancanti per il passo indicato.", actor)
		}
		if r.Action == "WAIT_FOR_HUMAN" {
			if e.Issue != nil {
				next := e.Issue(r, args, obs)
				// Both semantic review commands bind admission and owner
				// deduplication to commandKey. Poll the original invocation;
				// a replay receipt continues with its authorized read.
				if next["action"] == "POLL" && next["reasonCode"] == "OWNER_REVIEW_LINK_UNAVAILABLE" &&
					(c.ToolName == "semantic.review.prepare" || c.ToolName == "semantic.approval.request") &&
					input["commandKey"] != nil && e.Valid(c.ToolName, input) {
					next["tool"], next["arguments"] = c.ToolName, input
				}
				return next
			}
			return e.blockTarget("WAIT_UNAVAILABLE", r.Tool, args, "Consulta lo stato della decisione.", "HUMAN_THS")
		}
		s := call(r.Action, r.Reason, r.Tool, args)
		if r.Message != "" {
			s["userMessage"] = r.Message
		}
		if r.Action == "POLL" {
			return e.machinePoll(s, policy.Read, obs, args, r.Terminal)
		}
		return s
	}
	return e.blockTarget("OWNER_STATE_UNKNOWN", policy.Read, readReference(policy.Read, obs, input), "Lo stato ricevuto richiede una nuova lettura.", "")
}

// A deadline is checked before proposing another delay, including an owner error.
func (e Engine) machinePoll(next Object, read string, obs, args Object, terminal []string) Object {
	now := e.Now()
	end, valid := e.machineDeadline(obs)
	if !valid {
		return ManualBlock("RETRY_WINDOW_UNAVAILABLE", "Verifica la scadenza del lavoro con l’owner.", "OPERATOR")
	}
	if !end.IsZero() {
		if !now.Before(end) {
			if next["tool"] == read {
				return (WaitRef{Read: read, Args: args}).Block("WAIT_DEADLINE_EXPIRED", "L’attesa è scaduta. Rileggi l’esito registrato su richiesta dell’utente.")
			}
			return ManualBlock("WAIT_DEADLINE_EXPIRED", "L’attesa è scaduta. Verifica l’esito del lavoro con l’operatore.", "OPERATOR")
		}
		next["deadline"] = end.UTC().Format(time.RFC3339Nano)
	}
	delay, ok := retryDelay(obs, now)
	if !ok {
		return e.blockTarget("RETRY_WINDOW_UNAVAILABLE", read, readReference(read, obs, args), "Verifica la finestra di ripresa.", "")
	}
	next["pollAfterSeconds"], next["terminalStates"] = max(delay, e.PollIntervalSeconds), terminal
	return next
}

func (e Engine) machineDeadline(obs Object) (time.Time, bool) {
	maximum := e.WaitMaxSeconds
	if maximum == 0 {
		maximum = DefaultWaitMaxSeconds
	}
	var earliest time.Time
	for _, field := range []string{"deadline", "waitUntil", "expiresAt", "decisionDeadline", "budgetDeadline", "activationProgress.budgetExpiresAt", "createdAt", "startedAt"} {
		raw := lookup(obs, field)
		if raw == nil {
			continue
		}
		end, err := time.Parse(time.RFC3339Nano, fmt.Sprint(raw))
		if err != nil {
			return time.Time{}, false
		}
		if field == "createdAt" || field == "startedAt" {
			end = end.Add(time.Duration(maximum) * time.Second)
		}
		if earliest.IsZero() || end.Before(earliest) {
			earliest = end
		}
	}
	return earliest, true
}

// Only a single visible source in a complete page can select a run. Preserve
// any source filter the caller supplied.
func Observation(tool string, args, body Object) Object {
	if tool == "ouf.ingestion.status" && body["items"] != nil {
		items, _ := body["items"].([]any)
		if len(items) == 1 && body["partial"] != true {
			row, _ := items[0].(Object)
			source, _ := get(row, nil, "sourceId|source_ref").(string)
			selected, _ := args["sourceId"].(string)
			if source != "" && (selected == "" || source == selected) {
				return row
			}
		}
		return nil
	}
	return body
}

var reasonPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{2,79}$`)

// A status row can describe a run and a schedule. A run recovery takes
// precedence. A terminal schedule has no recovery work, even while the run is active.
func recoveryObservation(obs Object) Object {
	schedule, action := obs["scheduleRecoveryAction"], obs["recoveryAction"]
	if schedule == nil || schedule == "NONE" || schedule == "TERMINAL" || (action != nil && action != "NONE") {
		return obs
	}
	row := maps.Clone(obs)
	row["recoveryAction"] = schedule
	row["decisionDeadline"] = obs["scheduleDecisionDeadline"]
	row["retryAt"] = get(obs, nil, "scheduleRetryAt|retryAt")
	return row
}

func (e Engine) failure(c manifest.Capability, p manifest.Policy, args, obs Object) Object {
	code, _ := get(obs, nil, "reasonCode|code|Code").(string)
	if !reasonPattern.MatchString(code) {
		code = "OWNER_REQUEST_FAILED"
	}
	status := fmt.Sprint(get(obs, nil, "Status|status"))
	if c.ToolName == "source.analysis.plan" && code == analysis.ErrEpochChanged.Error() {
		if id, ok := obs["analysisId"].(string); ok && analysis.ValidateID(id) == nil {
			return e.ReplanNext(id, code)
		}
	}
	if id, ok := args["analysisId"].(string); ok && (c.ToolName == "source.analysis.step" || c.ToolName == "source.analysis.read") {
		if code == analysis.ErrEpochChanged.Error() || (obs["authorizationPhase"] == "OWNER" && analysis.PermissionHold(code)) {
			return e.ReplanNext(id, code)
		}
	}
	if c.ToolName == "source.analysis.plan" && code == analysis.ErrPolicyUnchanged.Error() {
		return permissionHold(code, "Attendi la conferma della correzione dei permessi nella THS.")
	}
	if obs["authorizationPhase"] == "OWNER" {
		switch code {
		case "SEM_READ_RECEIPT_INVALID", "SEM_DISCOVERY_RECEIPT_INVALID":
			return ManualBlock(code, "La ricevuta di lettura è invalida. Chiedi all’operatore di verificare la piattaforma.", "OPERATOR")
		case "CAPABILITY_NOT_DECLARED", "SCOPE_MISSING", "ACTOR_NOT_ALLOWED", "EXPLICIT_DENY":
			return PermissionDenied(c.RequiredAuthorization, code)
		}
	}
	if next := e.draftFailure(c.ToolName, code, status, args, obs); next != nil {
		return next
	}
	if c.ToolName == "source.file.upload" && code == "HANDOFF_DEADLINE_REQUIRED" {
		return ManualBlock(code, "Aggiorna l’owner Onboarding per ottenere la scadenza del selettore.", "OPERATOR")
	}
	if c.ToolName == "source.file.upload.status" && code == "ONB_HANDOFF_UNKNOWN" {
		return (WaitRef{Read: "source.file.upload", Args: Object{}}).Block(code, "Apri un nuovo selettore per scegliere il file.")
	}
	if AdmissionWindowCode(code) {
		// The admission store refused before dispatch. It knows the exact
		// remaining window even for a command with no owner commandKey.
		delay, err := strconv.Atoi(fmt.Sprint(obs["retryAfterSeconds"]))
		end, _ := obs["windowEndsAt"].(string)
		deadline, dateErr := time.Parse(time.RFC3339Nano, end)
		if obs["retryable"] == true && err == nil && delay > 0 && delay <= 3600 && dateErr == nil && !deadline.IsZero() && e.Valid(c.ToolName, args) {
			s := call("POLL", code, c.ToolName, args)
			s["pollAfterSeconds"], s["deadline"], s["terminalStates"] = delay, end, []string{"ADMITTED", "BLOCKED"}
			return s
		}
		return ManualBlock(code, "Verifica il budget di ammissione e la disponibilità di una finestra di ripresa.", "OPERATOR")
	}
	retryable := obs["retryable"] == true || (obs["retryable"] == nil && (status == "503" || status == "429"))
	safe := c.OperationClass == "READ" || c.OperationClass == "SEARCH" || (args["commandKey"] != nil && args["commandKey"] != "")
	flags := Object{}
	maps.Copy(flags, obs)
	flags["code"], flags["tool"], flags["retryable"], flags["safeToRepeat"] = code, c.ToolName, retryable, safe
	if c.ToolName == "authorization.permissions.propose" {
		if id := obs["proposalId"]; id != nil {
			read := Object{"proposalId": id}
			if e.Valid(p.Read, read) {
				if state, _ := obs["state"].(string); state != "" {
					return e.evaluate(c, p, args, obs, false)
				}
				return call("CALL_TOOL", code, p.Read, read)
			}
		}
		flags["applicationRejection"] = obs["applicationRejection"] == true || status == "400" || status == "422" || code == "ONB_BAD_REQUEST" || code == "AUTH_ROLE_CAPABILITY_INVALID" || code == "AUTH_GRANT_CONSTRAINTS_INAPPLICABLE"
	}
	for _, r := range e.Errors {
		if !matches(r.When, flags, args) {
			continue
		}
		target := r.Tool
		switch target {
		case "$read":
			target = p.Read
		case "$self":
			target = c.ToolName
		}
		if r.Action == "BLOCKED" {
			if c.ToolName == "authorization.permissions.propose" && target == "authorization.proposal.read" {
				return ManualBlock(code, "Verifica nella THS l’esito della proposta prima di inviare altri comandi.", "HUMAN_THS")
			}
			if code == "NO_APPLICABLE_GRANT" && target == "authorization.permissions.propose" {
				capability := c.RequiredAuthorization
				if required, ok := obs["requiredCapability"].(string); ok && (obs["authorizationPhase"] == "MCP_PRE_ADMISSION" || obs["authorizationPhase"] == "OWNER") {
					capability = required
				}
				if c.ToolName == "authorization.permissions.read" {
					return ManualBlock(code, "Manca il permesso per "+capability+". Chiedi a un responsabile autorizzato di preparare la modifica con authorization.permissions.propose.", "HUMAN_THS")
				}
				return PermissionDenied(capability, code)
			}
			r.Tool, r.Reason = target, code
			return e.blockedRule(r, obs, args)
		}
		projected := e.project(r, obs, args)
		if r.Action == "POLL" {
			projected = args
		}
		if !e.Valid(target, projected) {
			if code == "INVALID_TOOL_ARGUMENTS" {
				continue
			}
			return ManualBlock(code, "Verifica i riferimenti necessari per proseguire.", "OPERATOR")
		}
		s := call(r.Action, code, target, projected)
		if r.Action == "POLL" {
			return e.machinePoll(s, p.Read, obs, projected, r.Terminal)
		}
		return s
	}
	return e.blockTarget(code, p.Read, readReference(p.Read, obs, args), "Verifica il motivo del rifiuto.", "")
}

func AdmissionWindowCode(code string) bool {
	return code == "TOOL_SELECTION_STALL" || strings.HasSuffix(code, "_WINDOW_BUDGET_EXHAUSTED")
}

func retryDelay(obs Object, now time.Time) (int, bool) {
	delay := orchestration.PollIntervalSeconds
	for _, field := range []string{"RetryAfter", "retryAfterSeconds", "pollAfterSeconds"} {
		raw := get(obs, nil, field)
		if raw == nil {
			continue
		}
		seconds, err := strconv.Atoi(fmt.Sprint(raw))
		if err != nil {
			date, err := http.ParseTime(fmt.Sprint(raw))
			if err != nil {
				return 0, false
			}
			if date.After(now.Add(time.Hour)) {
				return 0, false
			}
			seconds = max(0, int((date.Sub(now)+time.Second-1)/time.Second))
		}
		if seconds < 0 || seconds > 3600 {
			return 0, false
		}
		delay = max(delay, seconds)
	}
	if raw := get(obs, nil, "retryAt|next_retry_at|nextAttemptAt|notBefore"); raw != nil {
		at, err := time.Parse(time.RFC3339Nano, fmt.Sprint(raw))
		if err == nil && at.After(now.Add(time.Hour)) && obs["tool"] == analysis.BatchTool && obs["code"] == "TRANSIENT_OWNER_WAIT" {
			// A public poll may inspect the durable wait hourly. The plan
			// still prohibits owner dispatch before the full Retry-After.
			return 3600, true
		}
		if err != nil || at.After(now.Add(time.Hour)) {
			return 0, false
		}
		if at.After(now) {
			delay = max(delay, int((at.Sub(now)+time.Second-1)/time.Second))
		}
	}
	return delay, true
}
