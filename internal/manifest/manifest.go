package manifest

import (
	"crypto/sha256"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

//go:embed capabilities.json
var files embed.FS

type Snapshot struct {
	Version      string       `json:"version"`
	Capabilities []Capability `json:"capabilities"`
}

type Capability struct {
	ToolName              string          `json:"toolName"`
	CapabilityID          string          `json:"capabilityId"`
	Purpose               string          `json:"purpose"`
	UseWhen               []string        `json:"useWhen"`
	DoNotUseWhen          []string        `json:"doNotUseWhen"`
	PreferredAlternatives []string        `json:"preferredAlternatives"`
	OperationalLimits     json.RawMessage `json:"operationalLimits"`
	ResultSemantics       string          `json:"resultSemantics"`
	SecurityNotes         []string        `json:"securityNotes"`
	MCPClass              string          `json:"mcpClass"`
	Owner                 string          `json:"owner"`
	OperationClass        string          `json:"operationClass"`
	PublicationState      string          `json:"publicationState"`
	RequiredAuthorization string          `json:"requiredAuthorizationCapability"`
	GatewayBindingRef     string          `json:"gatewayBindingRef"`
	BaseCostEnvelope      json.RawMessage `json:"baseCostEnvelope"`
	ConditionalCost       json.RawMessage `json:"conditionalOutcomeCost"`
	ToolEligible          bool            `json:"toolEligible"`
	InputSchema           json.RawMessage `json:"inputSchema"`
}

func Load() (*Snapshot, error) {
	b, err := files.ReadFile("capabilities.json")
	if err != nil {
		return nil, err
	}
	var s Snapshot
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

func (s *Snapshot) Validate() error {
	if strings.TrimSpace(s.Version) == "" {
		return errors.New("manifest version is required")
	}
	seen := map[string]bool{}
	for i, c := range s.Capabilities {
		prefix := fmt.Sprintf("capabilities[%d]", i)
		if c.CapabilityID == "" || c.Purpose == "" || len(c.UseWhen) == 0 || len(c.DoNotUseWhen) == 0 || len(c.PreferredAlternatives) == 0 || len(c.OperationalLimits) == 0 || c.ResultSemantics == "" || len(c.SecurityNotes) == 0 || len(c.InputSchema) == 0 || c.Owner == "" || c.OperationClass == "" || c.PublicationState == "" || c.RequiredAuthorization == "" || c.GatewayBindingRef == "" || len(c.BaseCostEnvelope) == 0 || len(c.ConditionalCost) == 0 {
			return fmt.Errorf("%s: incomplete mandatory manifest fields", prefix)
		}
		if c.ToolEligible && c.ToolName == "" {
			return fmt.Errorf("%s: tool-eligible capability requires toolName", prefix)
		}
		if c.ToolName != "" && seen[c.ToolName] {
			return fmt.Errorf("%s: duplicate toolName %q", prefix, c.ToolName)
		}
		seen[c.ToolName] = c.ToolName != ""
		if c.MCPClass == "TRUSTED_HUMAN_ONLY" && c.ToolEligible {
			return fmt.Errorf("%s: trusted-human-only capability cannot be tool eligible", prefix)
		}
		if c.PublicationState != "ACTIVE" && c.PublicationState != "INACTIVE" && c.PublicationState != "REVOKED" {
			return fmt.Errorf("%s: invalid publicationState %q", prefix, c.PublicationState)
		}
		if c.OperationClass != "READ" && c.OperationClass != "SEARCH" && c.OperationClass != "COMMAND" {
			return fmt.Errorf("%s: invalid operationClass %q", prefix, c.OperationClass)
		}
		if !strings.HasPrefix(c.GatewayBindingRef, "capability://") {
			return fmt.Errorf("%s: gatewayBindingRef must be a capability reference", prefix)
		}
		var schema map[string]any
		if err := json.Unmarshal(c.InputSchema, &schema); err != nil || schema["type"] != "object" || schema["additionalProperties"] != false {
			return fmt.Errorf("%s: inputSchema must be a closed object schema", prefix)
		}
		lower := strings.ToLower(string(c.InputSchema))
		for _, forbidden := range []string{"sql", "jpql", "cypher", "sparql", "querylanguage", "query_language", "hostname", "baseurl"} {
			if strings.Contains(lower, forbidden) {
				return fmt.Errorf("%s: forbidden input surface %q", prefix, forbidden)
			}
		}
	}
	return nil
}

func (s *Snapshot) CanonicalPayload() ([]byte, error) { return json.Marshal(s) }
func (s *Snapshot) Checksum() (string, error) {
	payload, err := s.CanonicalPayload()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%x", sum), nil
}

func (s *Snapshot) ToolEligible() []Capability {
	out := make([]Capability, 0, len(s.Capabilities))
	for _, c := range s.Capabilities {
		if c.ToolEligible && c.MCPClass == "MCP_TOOL" && c.PublicationState == "ACTIVE" {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ToolName < out[j].ToolName })
	return out
}

func (c Capability) Description() string {
	return fmt.Sprintf("%s Use when: %s. Do not use when: %s. Preferred alternatives: %s. Result: %s. Security: %s.", c.Purpose, strings.Join(c.UseWhen, "; "), strings.Join(c.DoNotUseWhen, "; "), strings.Join(c.PreferredAlternatives, ", "), c.ResultSemantics, strings.Join(c.SecurityNotes, "; "))
}
