// Package statusview defines the fixed public view of MCP-owned system status.
// It never carries counters, incident references or arbitrary producer fields.
package statusview

import (
	"encoding/json"
	"errors"
)

const Capability = "ouf.system.status"
const Public = "PUBLIC_OPERATIONAL"

var ErrInvalid = errors.New("invalid public operational status")

// Project uses an allowlist rather than removing known sensitive keys. Missing
// or invalid required fields cannot silently become a healthy/empty snapshot.
func Project(raw []byte) ([]byte, error) {
	var source struct {
		Module         string `json:"module"`
		Status         string `json:"status"`
		ActionRequired *bool  `json:"actionRequired"`
		Partial        *bool  `json:"partial"`
	}
	if json.Unmarshal(raw, &source) != nil || source.Module != "MCP" || source.ActionRequired == nil || source.Partial == nil {
		return nil, ErrInvalid
	}
	switch source.Status {
	case "HEALTHY", "RECOVERING", "DEGRADED":
	default:
		return nil, ErrInvalid
	}
	return json.Marshal(struct {
		Module          string `json:"module"`
		Status          string `json:"status"`
		ActionRequired  bool   `json:"actionRequired"`
		Partial         bool   `json:"partial"`
		VisibilityClass string `json:"visibilityClass"`
		Redacted        bool   `json:"redacted"`
	}{source.Module, source.Status, *source.ActionRequired, *source.Partial, Public, true})
}
