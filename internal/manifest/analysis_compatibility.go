package manifest

import "sync"

const publishedVersion = "1.17.0-published-batch-v1"
const predecessorVersion = "1.16.0-analysis-summary-v2"

// Only these additive installation states may resume an ea4033e plan.
// Reconstruct its exact canonical checksum without changing the stored plan.
var compatibleAnalysis = sync.OnceValue(func() map[string]string {
	result := make(map[string]string)
	m, err := Load()
	if err != nil || m.Version != publishedVersion {
		return result
	}
	for index, c := range m.Capabilities {
		if c.CapabilityID != "ouf.semantic.search.batch" {
			continue
		}
		active, _ := m.Checksum()
		m.Capabilities[index].PublicationState = "INACTIVE"
		inactive, _ := m.Checksum()
		m.Capabilities = append(m.Capabilities[:index], m.Capabilities[index+1:]...)
		absent, _ := m.Checksum()
		m.Version = predecessorVersion
		old, _ := m.Checksum()
		for _, current := range []string{active, inactive, absent} {
			result[current] = old
		}
		break
	}
	return result
})

func AnalysisPredecessor(current string) string {
	return compatibleAnalysis()[current]
}
