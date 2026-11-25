package manifest

import (
	"encoding/json"
	"sync"
)

const publishedVersion = "1.17.0-published-batch-v1"

// ea4033eManifest is the exact checksum of the 1.16.0-analysis-summary-v2
// manifest of ea4033e (50r). The T1 release (4b39568) accepts its analyses in
// all three batch states; the T2 keeps that acceptance verbatim (D-080: the
// candidate resumes everything its predecessor resumes). It cannot be rebuilt
// from the current manifest, which also carries the 50s next exits.
const ea4033eManifest = "01d00a53be5614115924b4775e0d098938ebae2e8e713ef8dd5b4c772e520c24"

// Reconstruct only the supported additive catalog changes. Installation
// edits outside those changes never inherit a predecessor's authority.
var compatibleAnalysis = sync.OnceValue(func() map[string][]string {
	result := make(map[string][]string)
	for _, state := range []string{"ACTIVE", "INACTIVE", "ABSENT"} {
		m, err := Load()
		if err != nil || m.Version != "1.19.0-analysis-renewal-v1" {
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
			if c.ToolName == "source.analysis.renewal.propose" {
				m.Capabilities = append(m.Capabilities[:index], m.Capabilities[index+1:]...)
				break
			}
		}
		m.Version = "1.18.0-analysis-run-v2"
		run, _ := m.Checksum()
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
		result[current] = []string{run, t1, ea4033eManifest}
	}
	return result
})

func AnalysisPredecessors(current string) []string {
	return append([]string{current}, compatibleAnalysis()[current]...)
}
