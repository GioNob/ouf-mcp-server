package authorization

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"time"
)

type GrantConstraints struct {
	Effect                      string            `json:"effect"`
	ExternalRoleRef             string            `json:"externalRoleRef"`
	ResourceType                string            `json:"resourceType"`
	ResourceID                  string            `json:"resourceId"`
	ResourceAttributes          map[string]string `json:"resourceAttributes"`
	AllowedDataLabels           []string          `json:"allowedDataLabels"`
	AllowedDetailLevels         []string          `json:"allowedDetailLevels"`
	RequiredAcr                 string            `json:"requiredAcr"`
	RequiredAmr                 []string          `json:"requiredAmr"`
	MaxAuthenticationAgeSeconds *int64            `json:"maxAuthenticationAgeSeconds"`
}

func (c GrantConstraints) validate() error {
	if c.Effect != "" && c.Effect != "ALLOW" && c.Effect != "DENY" {
		return errors.New("invalid grant effect")
	}
	if len(c.ResourceAttributes) > 64 || len(c.AllowedDataLabels) > 64 || len(c.AllowedDetailLevels) > 16 || len(c.RequiredAmr) > 32 {
		return errors.New("policy constraints limit")
	}
	if c.MaxAuthenticationAgeSeconds != nil && (*c.MaxAuthenticationAgeSeconds < 1 || *c.MaxAuthenticationAgeSeconds > 86400) {
		return errors.New("authentication freshness limit")
	}
	return nil
}
func (c GrantConstraints) matches(p orchestration.Identity, r orchestration.ResourceContext, now time.Time) bool {
	claims := p.Claims
	if c.ExternalRoleRef != "" && (claims == nil || !contains(claims.ExternalRoleRefs, c.ExternalRoleRef)) {
		return false
	}
	if c.ResourceType != "" && c.ResourceType != r.ResourceType {
		return false
	}
	if c.ResourceID != "" && c.ResourceID != r.ResourceID {
		return false
	}
	for k, v := range c.ResourceAttributes {
		actual, ok := r.Attributes[k]
		if !ok || actual != v {
			return false
		}
	}
	if len(c.AllowedDataLabels) > 0 && !contains(c.AllowedDataLabels, r.Attributes["dataAccessLabel"]) {
		return false
	}
	if len(c.AllowedDetailLevels) > 0 && !contains(c.AllowedDetailLevels, r.Attributes["detailLevel"]) {
		return false
	}
	if c.RequiredAcr != "" && (claims == nil || claims.Acr != c.RequiredAcr) {
		return false
	}
	for _, amr := range c.RequiredAmr {
		if claims == nil || !contains(claims.Amr, amr) {
			return false
		}
	}
	if c.MaxAuthenticationAgeSeconds != nil && (claims == nil || claims.AuthenticatedAt.IsZero() || now.Before(claims.AuthenticatedAt) || !now.Before(claims.AuthenticatedAt.Add(time.Duration(*c.MaxAuthenticationAgeSeconds)*time.Second))) {
		return false
	}
	return true
}

// Preserve strict field validation even when decoding optional conditions.
func (c *GrantConstraints) UnmarshalJSON(raw []byte) error {
	type plain GrantConstraints
	var decoded plain
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, key := range []string{"effect", "externalRoleRef", "resourceType", "resourceId", "requiredAcr"} {
		value, exists := fields[key]
		if exists && string(value) != "null" {
			var text string
			if err := json.Unmarshal(value, &text); err != nil || strings.TrimSpace(text) == "" {
				return errors.New("blank or invalid grant constraint")
			}
		}
	}
	*c = GrantConstraints(decoded)
	return c.validate()
}
