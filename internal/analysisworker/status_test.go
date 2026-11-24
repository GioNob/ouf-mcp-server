package analysisworker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type statusGateway struct {
	t       *testing.T
	mandate analysis.Mandate
	reply   orchestration.MandateResponse
	err     error
}

func (g statusGateway) MandateCall(_ context.Context, op string, body json.RawMessage, delegation string, limit int64, timeout time.Duration) (orchestration.MandateResponse, error) {
	g.t.Helper()
	var req map[string]string
	if op != "status" || delegation != "" || limit != 4096 || timeout != 3*time.Second ||
		json.Unmarshal(body, &req) != nil || len(req) != 2 || req["mandateId"] != g.mandate.ID || req["generation"] != g.mandate.Generation {
		g.t.Fatal("unexpected control call")
	}
	return g.reply, g.err
}

func TestMandateStatusBinding(t *testing.T) { // MCP-50V-01
	m := analysis.Mandate{ID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", Generation: "1", Expires: time.Unix(2000000000, 0).UTC()}
	for _, state := range []string{"ACTIVE", "EXPIRED", "REVOKED"} {
		body, _ := json.Marshal(map[string]any{"mandateId": m.ID, "generation": m.Generation, "expiresAt": m.Expires, "decisionRef": "fixture", "state": state})
		g := statusGateway{t: t, mandate: m, reply: orchestration.MandateResponse{Status: 200, Body: body}}
		got, ref, _ := observeMandate(context.Background(), g, m)
		if got != state || ref != "fixture" {
			t.Fatal(got, ref)
		}
	}
	for _, tc := range []struct {
		status     int
		body, want string
	}{
		{404, `{"code":"ANALYSIS_MANDATE_UNKNOWN"}`, "REVOKED"},
		{409, `{"code":"ANALYSIS_STALE_EXECUTION"}`, "DRIFT"},
		{503, `{"code":"ANALYSIS_AUTHORITY_UNAVAILABLE"}`, "UNAVAILABLE"},
		{200, `{"mandateId":"other","generation":"1","expiresAt":"2033-05-18T03:33:20Z","state":"ACTIVE","decisionRef":"fixture"}`, "DRIFT"},
		{200, `{"state":"REVOKED","state":"ACTIVE"}`, "DRIFT"},
	} {
		g := statusGateway{t: t, mandate: m, reply: orchestration.MandateResponse{Status: tc.status, Body: []byte(tc.body)}}
		if got, _, _ := observeMandate(context.Background(), g, m); got != tc.want {
			t.Fatal(tc.status, got)
		}
	}
	g := statusGateway{t: t, mandate: m, err: orchestration.ErrMandateTransportUncertain}
	if got, _, _ := observeMandate(context.Background(), g, m); got != "UNAVAILABLE" {
		t.Fatal(got)
	}
}

func TestInstallationCadenceAndRetentionBounds(t *testing.T) { // MCP-50V-09
	for _, setting := range []string{"OUF_ANALYSIS_STATUS_SECONDS", "OUF_ANALYSIS_RESERVATION_DAYS", "OUF_ANALYSIS_ATTEMPT_DAYS", "OUF_ANALYSIS_SNAPSHOT_DAYS", "OUF_ANALYSIS_READ_DAYS"} {
		t.Run(setting, func(t *testing.T) {
			t.Setenv(setting, "0")
			if _, _, err := analysis.RunMaintenanceConfiguration(); err == nil {
				t.Fatal("accepted zero")
			}
		})
	}
	t.Setenv("OUF_ANALYSIS_STATUS_SECONDS", "4")
	if _, _, err := analysis.RunMaintenanceConfiguration(); err == nil {
		t.Fatal("cadence below minimum")
	}
	t.Setenv("OUF_ANALYSIS_STATUS_SECONDS", "5")
	if d, _, err := analysis.RunMaintenanceConfiguration(); err != nil || d != 5*time.Second {
		t.Fatal(d, err)
	}
}
