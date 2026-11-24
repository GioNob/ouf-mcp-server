package analysis

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
)

type RunInstallation struct {
	Enabled            bool
	Manifest, Workload string
	Limits             RunLimits
}

// Read once at process startup. The same immutable UTF-8 file is deployed
// to both services. Pin its actual bytes, including whitespace and final LF.
func RunConfiguration(m *manifest.Snapshot) (RunInstallation, error) {
	cfg := RunInstallation{Limits: RunLimits{ProofSeconds: 5, WaitSeconds: 3600, LifetimeSeconds: 86400, ActivePlans: 1, ProviderSlots: 1, PreviewPageSize: 64,
		Shared:  RunSharedLimits{TenantBytes: 1 << 30, SourceBytes: 128 << 20, TenantQueue: 32, SourceQueue: 4, TenantSlots: 4, SourceSlots: 1, DependencySlots: 4, TenantPerMinute: 120, SourcePerMinute: 30, DependencyPerMinute: 120},
		Maximum: RunCost{Calls: 4096, Attempts: 4096, Objects: 409600, Bytes: 64 << 20, Millis: 3600000}}}
	switch os.Getenv("OUF_ANALYSIS_RUN_ENABLED") {
	case "", "false":
		return cfg, nil
	case "true":
		cfg.Enabled = true
	default:
		return cfg, fmt.Errorf("invalid OUF_ANALYSIS_RUN_ENABLED")
	}
	cfg.Workload = os.Getenv("OUF_ANALYSIS_WORKLOAD")
	if cfg.Workload == "" {
		return cfg, fmt.Errorf("analysis workload required")
	}
	if limits, ok := os.LookupEnv("OUF_ANALYSIS_RUN_LIMITS"); ok {
		var fields map[string]json.RawMessage
		if !ValidEvidenceJSON([]byte(limits)) || json.Unmarshal([]byte(limits), &fields) != nil || len(fields) != 9 {
			return cfg, ErrInvalid
		}
		for _, k := range []string{"Maximum", "OwnerDedupQualified", "Shared", "ProofSeconds", "WaitSeconds", "LifetimeSeconds", "ActivePlans", "ProviderSlots", "PreviewPageSize"} {
			if fields[k] == nil || string(fields[k]) == "null" {
				return cfg, ErrInvalid
			}
		}
		if _, ok := closedFields(fields["Maximum"], "Calls", "Attempts", "Objects", "Bytes", "Millis"); !ok {
			return cfg, ErrInvalid
		}
		if _, ok := closedFields(fields["Shared"], "TenantBytes", "SourceBytes", "TenantQueue", "SourceQueue", "TenantSlots", "SourceSlots", "DependencySlots", "TenantPerMinute", "SourcePerMinute", "DependencyPerMinute"); !ok {
			return cfg, ErrInvalid
		}
		var l RunLimits
		if json.Unmarshal([]byte(limits), &l) != nil || !l.Valid() {
			return cfg, ErrInvalid
		}
		cfg.Limits = l
	}
	raw, err := os.ReadFile(os.Getenv("OUF_ANALYSIS_MANIFEST_FILE"))
	if err != nil {
		return cfg, fmt.Errorf("analysis manifest file unavailable")
	}
	cfg.Manifest, err = FileManifestChecksum(m, raw)
	return cfg, err
}

func FileManifestChecksum(m *manifest.Snapshot, raw []byte) (string, error) {
	if !ValidEvidenceJSON(raw) {
		return "", ErrRunDrift
	}
	// Compare all JSON fields, including fields unknown to Snapshot.
	catalog, err := manifest.FilePayload()
	if err != nil {
		return "", err
	}
	pristine, err := manifest.Load()
	if err != nil {
		return "", err
	}
	actual, err := m.Checksum()
	if err != nil {
		return "", err
	}
	want, err := pristine.Checksum()
	if err != nil || want != actual {
		return "", ErrRunDrift
	}
	decode := func(b []byte) (any, error) {
		d := json.NewDecoder(bytes.NewReader(b))
		d.UseNumber()
		var v any
		err := d.Decode(&v)
		return v, err
	}
	file, err := decode(raw)
	if err != nil {
		return "", err
	}
	active, err := decode(catalog)
	if err != nil || !reflect.DeepEqual(file, active) {
		return "", ErrRunDrift
	}
	return BytesDigest(raw), nil
}
