package statusview

import (
	"encoding/json"
	"testing"
)

func TestPublicStatusAllowlistAndIdempotence(t *testing.T) {
	raw := []byte(`{"module":"MCP","status":"DEGRADED","actionRequired":true,"partial":true,"securityIncidentCount":9,"incidents":[{"incidentRef":"private"}],"futureSecret":"sensitive"}`)
	got, err := Project(raw)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(got, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 6 || fields["status"] != "DEGRADED" || fields["actionRequired"] != true || fields["partial"] != true || fields["visibilityClass"] != Public || fields["redacted"] != true {
		t.Fatalf("unexpected public status: %s", got)
	}
	for _, key := range []string{"module", "status", "actionRequired", "partial", "visibilityClass", "redacted"} {
		if _, ok := fields[key]; !ok {
			t.Fatal("missing", key)
		}
	}
	again, err := Project(got)
	if err != nil || string(again) != string(got) {
		t.Fatalf("projection is not idempotent: %s %v", again, err)
	}
}

func TestPublicStatusNeverInventsHealthySnapshot(t *testing.T) {
	for _, raw := range []string{`null`, `{}`, `[]`, `{"module":"MCP","status":"HEALTHY","partial":false}`, `{"module":"MCP","status":"BOGUS","partial":false,"actionRequired":false}`, `{"module":"OTHER","status":"HEALTHY","partial":false,"actionRequired":false}`, `{"module":"MCP","status":"HEALTHY","partial":null,"actionRequired":false}`} {
		if body, err := Project([]byte(raw)); err == nil || len(body) != 0 {
			t.Fatalf("invalid snapshot accepted: %s -> %s %v", raw, body, err)
		}
	}
}
