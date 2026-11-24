package analysis

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
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
	modified := bytes.Replace(raw, []byte("analysis-run-v2"), []byte("analysis-run-v3"), 1)
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
