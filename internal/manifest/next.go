package manifest

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
)

const FollowNext = "After this tool, follow the `next` block of the result exactly; never infer the next step. For a new command MCP supplies a UUIDv4 commandKey in next.arguments. Retry that step with the same arguments and key."

const PickerStatusInputSchema = `{"type":"object","additionalProperties":false,"required":["handoffId"],"properties":{"handoffId":{"type":"string","format":"uuid"}}}`

type Policy struct {
	Read       string              `json:"read"`
	Vocabulary map[string][]string `json:"vocabulary,omitempty"`
	Rules      []Rule              `json:"rules"`
}

// Rules run in order. Selectors address owner fields. An @ prefix explicitly
// refers to the current request. Constants are prefixed with =.
type Rule struct {
	When     map[string]string `json:"when"`
	Action   string            `json:"action"`
	Reason   string            `json:"reason"`
	Tool     string            `json:"tool,omitempty"`
	Args     map[string]string `json:"args,omitempty"`
	Decision string            `json:"decision,omitempty"`
	Terminal []string          `json:"terminal,omitempty"`
	Actor    string            `json:"actor,omitempty"`
	Outcome  string            `json:"outcome,omitempty"`
	Message  string            `json:"message,omitempty"`
}

type branch struct {
	Required   []string          `json:"required"`
	Properties map[string]branch `json:"properties"`
	Const      any               `json:"const"`
	OneOf      []branch          `json:"oneOf"`
}

// Include inherited requirements in nested unions and const discriminators
// that distinguish otherwise identical selection forms.
func InputForms(raw json.RawMessage) []string {
	var root branch
	if json.Unmarshal(raw, &root) != nil {
		return nil
	}
	var forms []string
	var walk func(branch, []string, map[string]string)
	walk = func(b branch, parent []string, inherited map[string]string) {
		keys := append(slices.Clone(parent), b.Required...)
		values := map[string]string{}
		for k, v := range inherited {
			values[k] = v
		}
		for k, prop := range b.Properties {
			if prop.Const != nil {
				values[k] = fmt.Sprint(prop.Const)
			}
		}
		for _, arm := range b.OneOf {
			walk(arm, keys, values)
		}
		if len(b.OneOf) == 0 {
			sort.Strings(keys)
			keys = slices.Compact(keys)
			for i, k := range keys {
				if v := values[k]; v != "" {
					keys[i] = k + "=" + v
				}
			}
			forms = append(forms, "("+strings.Join(keys, ", ")+")")
		}
	}
	walk(root, nil, nil)
	return forms
}
