package nextstep

import (
	"encoding/json"

	"github.com/google/jsonschema-go/jsonschema"
)

// stateKeys name values a decision alternative can never leave to the agent:
// the MCP issues command keys and replan sources, and proposal, analysis and
// task identifiers exist only once the owner has returned them. Other
// identifiers (sourceId, requestId, candidateId, semanticId, revisionId,
// grantId, ...) may be completed from the indicated evidence.
var stateKeys = map[string]bool{
	"commandKey": true, "replanOf": true, "proposalId": true, "analysisId": true, "taskId": true,
}

// A decision alternative may leave evidence-based choices to the agent.
// Keep the supplied fields, types and discriminators subject to the target
// schema, and retain every required state identifier in each possible form.
func DecisionSchema(raw []byte) (*jsonschema.Resolved, error) {
	var root Object
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, err
	}
	var relax func(Object)
	relax = func(schema Object) {
		if required, ok := schema["required"].([]any); ok {
			ids := make([]string, 0, len(required))
			for _, field := range required {
				key, _ := field.(string)
				if stateKeys[key] {
					ids = append(ids, key)
				}
			}
			schema["required"] = ids
		}
		for _, key := range []string{"oneOf", "anyOf", "allOf"} {
			forms, _ := schema[key].([]any)
			for _, form := range forms {
				if obj, ok := form.(Object); ok {
					relax(obj)
				}
			}
			if key == "oneOf" && len(forms) > 0 {
				// More than one completion may remain possible.
				delete(schema, key)
				schema["anyOf"] = forms
			}
		}
		for _, key := range []string{"properties", "$defs"} {
			children, _ := schema[key].(Object)
			for name, child := range children {
				if obj, ok := child.(Object); ok {
					if obj["type"] == "string" && stateKeys[name] && obj["minLength"] == nil {
						obj["minLength"] = 1
					}
					relax(obj)
				}
			}
		}
		if items, ok := schema["items"].(Object); ok {
			relax(items)
		}
		// Conditional requirements (if/then/else) are decision fields too.
		// "if" and "not" keep their requirements: relaxing them would change
		// which branch applies or invert an exclusion.
		for _, key := range []string{"then", "else"} {
			if branch, ok := schema[key].(Object); ok {
				relax(branch)
			}
		}
	}
	relax(root)
	raw, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		return nil, err
	}
	return schema.Resolve(nil)
}

// Validate the action the agent will actually see, after dynamic rule
// selection, wait issuance and fallback projection.
func CheckTargets(next Object, valid func(string, Object, bool) bool) Object {
	check := func(action Object, decision bool) bool {
		name, _ := action["tool"].(string)
		if name == "" {
			return action["action"] == "WAIT_FOR_HUMAN" || action["action"] == "DONE"
		}
		args, _ := action["arguments"].(Object)
		if args == nil {
			args = Object{}
		}
		return valid(name, args, decision)
	}
	reason, _ := next["reasonCode"].(string)
	if next["action"] != "BLOCKED" {
		if check(next, false) {
			return next
		}
		return ManualBlock(reason, "Verifica i riferimenti necessari per proseguire.", "OPERATOR")
	}
	options, ok := next["alternatives"].([]Object)
	if !ok {
		items, _ := next["alternatives"].([]any)
		for _, item := range items {
			if option, ok := item.(Object); ok {
				options = append(options, option)
			}
		}
	}
	kept := make([]Object, 0, len(options))
	for _, option := range options {
		if check(option, next["actor"] == "AGENT") {
			kept = append(kept, option)
		}
	}
	if len(kept) == 0 {
		return ManualBlock(reason, "Verifica i riferimenti necessari per proseguire.", "OPERATOR")
	}
	next["alternatives"] = kept
	return next
}
