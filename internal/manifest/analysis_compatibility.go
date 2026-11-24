package manifest

import (
	"encoding/json"
	"sync"
)

const publishedVersion = "1.17.0-published-batch-v1"
const predecessorVersion = "1.16.0-analysis-summary-v2"

// Reconstruct only the supported additive catalog changes. Installation
// edits outside those changes never inherit a predecessor's authority.
var compatibleAnalysis = sync.OnceValue(func() map[string][]string {
	result := make(map[string][]string)
	for _, state := range []string{"ACTIVE", "INACTIVE", "ABSENT"} {
		m, err := Load()
		if err != nil || m.Version != "1.18.0-analysis-run-v2" {
			return result
		}
		for index, c := range m.Capabilities {
			if c.CapabilityID == "ouf.semantic.search.batch" {
				if state == "ABSENT" {
					m.Capabilities = append(m.Capabilities[:index], m.Capabilities[index+1:]...)
				} else {
					m.Capabilities[index].PublicationState = state
				}
				break
			}
		}
		current, _ := m.Checksum()
		for index, c := range m.Capabilities {
			if c.ToolName == "source.analysis.plan" {
				var schema struct{ OneOf []json.RawMessage }
				if json.Unmarshal(c.InputSchema, &schema) != nil || len(schema.OneOf) != 2 {
					return result
				}
				m.Capabilities[index].InputSchema = schema.OneOf[0]
				m.Capabilities[index].OutputSchema = nil
			}
		}
		m.Version = publishedVersion
		t1, _ := m.Checksum()
		for index, c := range m.Capabilities {
			if c.CapabilityID == "ouf.semantic.search.batch" {
				m.Capabilities = append(m.Capabilities[:index], m.Capabilities[index+1:]...)
				break
			}
		}
		m.Version = predecessorVersion
		t0, _ := m.Checksum()
		result[current] = []string{t1, t0}
	}
	return result
})

func AnalysisPredecessors(current string) []string {
	return append([]string{current}, compatibleAnalysis()[current]...)
}
