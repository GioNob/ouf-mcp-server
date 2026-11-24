package main

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

// MCP-55I-01. Rejected invocations produce one closed record before connecting.
func TestRetentionCommandsRejectInvalidInput(t *testing.T) {
	t.Setenv("OUF_ANALYSIS_WORKLOAD", "fixture")
	for _, command := range []string{"migrate-analysis-retention-019", "analysis-retention-once"} {
		for _, extra := range []string{"--help", "private-fixture"} {
			assertRetentionRejected(t, []string{command, extra}, "private-dsn")
		}
		assertRetentionRejected(t, []string{command}, "")
	}
	for _, invalid := range []string{"", "0", "-1", "257", "private-fixture"} {
		t.Setenv("OUF_ANALYSIS_RETENTION_BATCH", invalid)
		assertRetentionRejected(t, []string{"analysis-retention-once"}, "private-dsn")
	}
}

func assertRetentionRejected(t *testing.T, args []string, dsn string) {
	t.Helper()
	var line bytes.Buffer
	if got := runRetentionCommand(context.Background(), &line, args, dsn); got != 2 {
		t.Fatalf("expected usage exit, got %d", got)
	}
	var fields map[string]json.RawMessage
	if strings.Count(line.String(), "\n") != 1 || json.Unmarshal(line.Bytes(), &fields) != nil || len(fields) != 4 ||
		string(fields["status"]) != `"UNVERIFIED"` || string(fields["counts"]) != "null" ||
		string(fields["ownerAclSha256"]) != "null" || !regexp.MustCompile(`^"[0-9a-f]{64}"$`).Match(fields["sqlSha256"]) {
		t.Fatal("unexpected retention failure record")
	}
}
