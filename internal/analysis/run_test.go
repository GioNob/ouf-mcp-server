package analysis

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
)

func TestRunGatewayFileAndArguments(t *testing.T) { // MCP-50U-01
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := manifest.FilePayload()
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range [][]byte{raw, append(bytes.Clone(raw), '\n')} {
		sum, err := FileManifestChecksum(m, file)
		if err != nil || sum != fmt.Sprintf("%x", sha256.Sum256(file)) {
			t.Fatal("gateway file digest", sum, err)
		}
	}
	// Independent of the current manifest version string.
	modified := bytes.Replace(raw, []byte(`"version": "`), []byte(`"version": "drift-`), 1)
	if bytes.Equal(modified, raw) {
		t.Fatal("fixture did not modify the manifest")
	}
	if _, err = FileManifestChecksum(m, modified); err == nil {
		t.Fatal("accepted drift")
	}
	// Python json.dumps(sort_keys=True, ensure_ascii=False, separators=(',',':')).
	const data = `{"z":"<>&\u2028è","a":[9007199254740993,true,"e\u0301"]}`
	got, err := MandateHash([]byte(data))
	if err != nil || got != "42ea929c078d29c596965ab5938e579b828aeb8d1b9d0dd60eb33861ddd33ee4" {
		t.Fatal(got, err)
	}
	for _, bad := range []string{`{"a":1,"a":1}`, `{"a":1.1}`, `{"a":null}`, `{"a":"\ud800"}`} {
		if _, err = MandateHash([]byte(bad)); err == nil {
			t.Fatal("accepted", bad)
		}
	}
}

func TestRunDisabledAndClosedConfiguration(t *testing.T) { // MCP-50U-02
	t.Setenv("OUF_ANALYSIS_RUN_ENABLED", "false")
	cfg, err := RunConfiguration(nil)
	if err != nil || cfg.Enabled || !cfg.Limits.Valid() || cfg.Limits.ActivePlans != 1 || cfg.Limits.ProviderSlots != 1 || cfg.Limits.OwnerDedupQualified {
		t.Fatal(cfg, err)
	}
	t.Setenv("OUF_ANALYSIS_RUN_ENABLED", "true")
	t.Setenv("OUF_ANALYSIS_WORKLOAD", "fixture")
	raw, _ := json.Marshal(cfg.Limits)
	var args map[string]any
	_ = json.Unmarshal(raw, &args)
	args["Maximum"].(map[string]any)["Calls"] = 0
	bad, _ := json.Marshal(args)
	t.Setenv("OUF_ANALYSIS_RUN_LIMITS", string(bad))
	if _, err = RunConfiguration(nil); err != ErrInvalid {
		t.Fatal(err)
	}
}

func TestRenewalInstallationLimits(t *testing.T) { // MCP-55A-03
	t.Setenv("OUF_ANALYSIS_RUN_ENABLED", "false")
	base, err := RunConfiguration(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("OUF_ANALYSIS_RUN_ENABLED", "true")
	t.Setenv("OUF_ANALYSIS_WORKLOAD", "fixture")
	t.Setenv("OUF_ANALYSIS_MANIFEST_FILE", "../manifest/capabilities.json")
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(base.Limits)
	var fields map[string]any
	if err = json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	configure := func() (RunInstallation, error) {
		raw, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		t.Setenv("OUF_ANALYSIS_RUN_LIMITS", string(raw))
		return RunConfiguration(m)
	}
	for _, field := range []string{"RenewalPercent", "RenewalSeconds", "InstallationActivePlans"} {
		for _, bad := range []any{nil, -1, 0, 1.5, "1", 3601} {
			fields[field] = bad
			if _, err = configure(); err != ErrInvalid {
				t.Fatal(field, bad, err)
			}
		}
		delete(fields, field)
	}
	for _, bad := range []any{nil, 1, "", " ", "a/b", strings.Repeat("x", 129)} {
		fields["InstallationRef"] = bad
		if _, err = configure(); err != ErrInvalid {
			t.Fatal("InstallationRef", bad, err)
		}
	}
	fields["InstallationRef"] = "fixture"
	fields["InstallationActivePlans"] = 129
	if _, err = configure(); err != ErrInvalid {
		t.Fatal("installation plan maximum", err)
	}
	fields["InstallationActivePlans"] = 4
	cfg, err := configure()
	if err != nil || cfg.Limits != base.Limits || cfg.Limits.InstallationPlanLimit() != 4 {
		t.Fatal("pre-55e profile with explicit defaults", err)
	}
	fields["InstallationRef"], fields["InstallationActivePlans"] = "shared-fixture", 128
	cfg, err = configure()
	if err != nil || cfg.Limits.InstallationPlanLimit() != 128 || cfg.Limits.InstallationRef != "shared-fixture" {
		t.Fatal("installation overrides", err)
	}
	delete(fields, "InstallationRef")
	delete(fields, "InstallationActivePlans")
	fields["RenewalPercent"] = 26
	if _, err = configure(); err != ErrInvalid {
		t.Fatal("percentage above D-091", err)
	}
	fields["RenewalPercent"], fields["RenewalSeconds"] = 25, 3600
	cfg, err = configure()
	if err != nil || cfg.Limits != base.Limits || cfg.Limits.RenewalDuration() != 3600 {
		t.Fatal("pre-020 profile compatibility", err)
	}
	fields["RenewalPercent"], fields["RenewalSeconds"] = 10, 90
	cfg, err = configure()
	if err != nil || cfg.Limits.RenewalDuration() != 90 || cfg.Limits.RenewalCeiling().Calls != base.Limits.Maximum.Calls/10 {
		t.Fatal("installation ceiling", err)
	}
}
