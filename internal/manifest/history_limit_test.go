package manifest

import (
	"encoding/json"
	"testing"
)

// DN-L3-3: ouf.ingestion.history promises exactly the owner limit. The Ingestion
// decoder shared by every operational query rejects limit > 100, so a larger
// manifest maximum would only turn a valid-looking call into a 400.
func TestIngestionHistoryLimitMatchesOwner(t *testing.T) {
	m, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range m.Capabilities {
		if c.CapabilityID != "ouf.ingestion.history" {
			continue
		}
		var schema struct {
			Properties struct {
				Limit struct {
					Minimum int `json:"minimum"`
					Maximum int `json:"maximum"`
				} `json:"limit"`
			} `json:"properties"`
		}
		var limits struct {
			MaxLimit int `json:"maxLimit"`
		}
		var cost struct {
			Objects int `json:"hardMaxDistinctObjects"`
		}
		if json.Unmarshal(c.InputSchema, &schema) != nil || json.Unmarshal(c.OperationalLimits, &limits) != nil ||
			json.Unmarshal(c.BaseCostEnvelope, &cost) != nil {
			t.Fatal("unreadable ouf.ingestion.history declaration")
		}
		if schema.Properties.Limit.Minimum != 1 || schema.Properties.Limit.Maximum != 100 || limits.MaxLimit != 100 || cost.Objects != 100 {
			t.Fatalf("history limit %d..%d, maxLimit %d, objects %d: want 1..100 everywhere",
				schema.Properties.Limit.Minimum, schema.Properties.Limit.Maximum, limits.MaxLimit, cost.Objects)
		}
		return
	}
	t.Fatal("missing ouf.ingestion.history")
}
