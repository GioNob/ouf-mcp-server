// Package nextstep projects owner observations into the versioned workflow contract.
package nextstep

import (
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/google/uuid"
)

type Object = map[string]any

const Contract = "ouf.next-step.v1"

type Engine struct {
	Errors     []manifest.Rule
	Valid      func(string, Object) bool
	Issue      func(manifest.Rule, Object, Object) Object
	Now        func() time.Time
	AnalysisID func(string, string) string
	CommandKey func(string, Object, Object, Object) string
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
		}
		if !found {
			return false
		}
	}
	return true
}

// Plan only from column metadata. The coordinator generates the durable job
// keys. Wider profiles require the existing paged analysis workflow.
func discoveryWork(obs Object) any {
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
		if !ok || strings.TrimSpace(name) == "" || len(name) > 2000 {
			return nil
		}
		work = append(work, Object{"tool": "semantic.discovery.request", "columns": []int{offset + i}, "arguments": Object{
			"intent": name, "preferredLanguages": []string{"it"}, "requestedArtifactType": "PROPERTY",
		}})
	}
	return work
}

func (e Engine) project(r manifest.Rule, obs, input Object) Object {
	args := Object{}
	for k, selector := range r.Args {
		value := get(obs, input, selector)
		switch selector {
		case "#true":
			value = true
		case "#uuid":
			continue
		case "#zero":
			value = 0
		case "#discovery":
			value = discoveryWork(obs)
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
	obs := Observation(c.ToolName, input, owner)
	if failed {
		return e.failure(c, policy, input, obs)
	}
	if obs["replay"] == true {
		tool, _ := obs["readTool"].(string)
		args, _ := obs["readArguments"].(Object)
		if tool == policy.Read && args != nil && e.Valid(tool, args) {
			return call("CALL_TOOL", "READ_ORIGINAL_RECEIPT", tool, args)
		}
		return Block("READ_ORIGINAL_RECEIPT", policy.Read, "Consulta la ricevuta del comando originale.")
	}
	for path, allowed := range policy.Vocabulary {
		if path == "activationProgress.readiness_phase" && obs["allowedActions"] != nil {
			continue
		}
		for _, field := range strings.Split(path, "|") {
			if v := lookup(obs, field); v != nil {
				state, ok := v.(string)
				if !ok || !slices.Contains(allowed, state) {
					return Block("OWNER_STATE_UNKNOWN", policy.Read, "Lo stato ricevuto richiede una nuova lettura.")
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
			if r.Decision != "" && e.Issue != nil {
				args := e.project(r, obs, input)
				if e.Valid(r.Tool, args) {
					return e.Issue(r, args, obs)
				}
			}
			s := Block(r.Reason, r.Tool, r.Message)
			if r.Actor != "" {
				s["actor"] = r.Actor
			}
			return s
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
			return Block(r.Reason, r.Tool, "Recupera gli argomenti mancanti per il passo indicato.")
		}
		if r.Action == "WAIT_FOR_HUMAN" {
			if e.Issue != nil {
				return e.Issue(r, args, obs)
			}
			return Block("WAIT_UNAVAILABLE", r.Tool, "Consulta lo stato della decisione.")
		}
		s := call(r.Action, r.Reason, r.Tool, args)
		if r.Action == "POLL" {
			delay, ok := retryDelay(obs, e.Now())
			if !ok {
				return Block("RETRY_WINDOW_UNAVAILABLE", policy.Read, "Verifica la finestra di ripresa.")
			}
			s["pollAfterSeconds"], s["terminalStates"] = delay, r.Terminal
			if raw := get(obs, nil, "budgetDeadline|activationProgress.budgetExpiresAt"); raw != nil {
				end, err := time.Parse(time.RFC3339Nano, fmt.Sprint(raw))
				if err != nil || !e.Now().Add(time.Duration(delay)*time.Second).Before(end) {
					return Block("RETRY_WINDOW_EXHAUSTED", policy.Read, "Verifica il budget prima della prossima lettura.")
				}
				s["deadline"] = end.UTC().Format(time.RFC3339Nano)
			}
		}
		return s
	}
	return Block("OWNER_STATE_UNKNOWN", policy.Read, "Lo stato ricevuto richiede una nuova lettura.")
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

func (e Engine) failure(c manifest.Capability, p manifest.Policy, args, obs Object) Object {
	code, _ := get(obs, nil, "reasonCode|code|Code").(string)
	if !reasonPattern.MatchString(code) {
		code = "OWNER_REQUEST_FAILED"
	}
	status := fmt.Sprint(get(obs, nil, "Status|status"))
	retryable := obs["retryable"] == true || (obs["retryable"] == nil && (status == "503" || status == "429"))
	safe := c.OperationClass == "READ" || c.OperationClass == "SEARCH" || (args["commandKey"] != nil && args["commandKey"] != "")
	flags := Object{"code": code, "tool": c.ToolName, "retryable": retryable, "safeToRepeat": safe}
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
			return Block(code, target, r.Message)
		}
		projected := e.project(r, obs, args)
		if r.Action == "POLL" {
			projected = args
		}
		if !e.Valid(target, projected) {
			if code == "INVALID_TOOL_ARGUMENTS" {
				continue
			}
			return Block(code, target, "Recupera gli argomenti necessari per proseguire.")
		}
		s := call(r.Action, code, target, projected)
		if r.Action == "POLL" {
			delay, ok := retryDelay(obs, e.Now())
			if !ok {
				return Block(code, p.Read, "Verifica la finestra di ripresa.")
			}
			s["pollAfterSeconds"], s["terminalStates"] = delay, r.Terminal
			if t, err := time.Parse(time.RFC3339Nano, fmt.Sprint(obs["windowEndsAt"])); err == nil {
				s["deadline"] = t.UTC().Format(time.RFC3339Nano)
			}
		}
		return s
	}
	return Block(code, p.Read, "Verifica il motivo del rifiuto.")
}

func retryDelay(obs Object, now time.Time) (int, bool) {
	raw := get(obs, nil, "RetryAfter|retryAfterSeconds|pollAfterSeconds")
	if obs["recoveryAction"] != nil && obs["retryAt"] != nil && obs["RetryAfter"] == nil {
		raw = nil
	}
	if raw == nil {
		if at := get(obs, nil, "retryAt|next_retry_at|nextAttemptAt"); at != nil {
			t, err := time.Parse(time.RFC3339Nano, fmt.Sprint(at))
			if err != nil {
				return 0, false
			}
			delay := max(5, int((t.Sub(now)+time.Second-1)/time.Second))
			return delay, delay <= 3600
		}
		return 5, true
	}
	s := fmt.Sprint(raw)
	n, err := strconv.Atoi(s)
	if err != nil {
		date, err := http.ParseTime(s)
		if err != nil {
			return 0, false
		}
		n = int((date.Sub(now) + time.Second - 1) / time.Second)
		n = max(n, 0)
	}
	return max(n, 5), n >= 0 && n <= 3600
}
