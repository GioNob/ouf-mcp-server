package orchestration

import (
	"encoding/json"
	"errors"
	"time"
)

const ManagedSchemaBudgetProfile = "ouf.managed-schema-pages.v1"

// Page reads have independent finite capacity: three complete reads of the
// largest supported profile per minute. Identical-query retry guards remain 3.
func isManagedSchemaPage(in Invocation) bool {
	if in.CapabilityID != "ouf.managed-source.preview" || in.OperationClass != "READ" {
		return false
	}
	var args struct {
		View string `json:"view"`
	}
	return json.Unmarshal(in.Arguments, &args) == nil && args.View == "COLUMNS"
}

func managedSchemaWindowBudget(in AdmissionRequest) (string, Cost, error) {
	if in.CapabilityID != "ouf.managed-source.preview" || in.OperationClass != "READ" ||
		in.ManifestChecksum == "" || in.Window != time.Minute || in.RetryThreshold != 3 ||
		in.Maximum.ToolCalls != 1 || in.Maximum.ResultBytes < 1 || in.Maximum.ResultBytes > 1<<20 ||
		in.Maximum.DistinctObjects < 0 || in.Maximum.DistinctObjects > 200 {
		return "", Cost{}, errors.New("unsupported managed schema budget")
	}
	return in.ManifestChecksum + ":" + ManagedSchemaBudgetProfile, Cost{ToolCalls: 24, ResultBytes: 24 << 20, DistinctObjects: 4800}, nil
}
