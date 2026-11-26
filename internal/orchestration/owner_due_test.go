package orchestration

import (
	"net/http"
	"testing"
	"time"
)

func TestOwnerDueLatestBoundSurvivesReceiptReplay(t *testing.T) {
	start := time.Unix(1800000000, 0).UTC()
	now := start.Add(10 * time.Second)
	at := func(seconds int) time.Time { return start.Add(time.Duration(seconds) * time.Second) }
	body := map[string]any{
		"completedAt": start.Format(time.RFC3339Nano), "pollAfterSeconds": 40,
		"nextAttemptAt": at(55).Format(time.RFC3339Nano),
		"result":        map[string]any{"retryAfterSeconds": 80, "notBefore": at(60).Format(time.RFC3339Nano)},
	}
	for _, tc := range []struct {
		header string
		floor  time.Time
		want   time.Time
	}{
		{"20", at(70), at(80)}, // Stored relative bound.
		{"85", at(70), at(95)}, // HTTP delta from this response.
		{at(100).Format(http.TimeFormat), at(70), at(100)},
		{"20", at(101), at(101)}, // Local backoff.
	} {
		got, valid := OwnerDue(body, now, tc.floor, tc.header)
		if !valid || !got.Equal(tc.want) {
			t.Fatal(tc, got, valid)
		}
	}
	body["result"] = map[string]any{"recoveryState": "RETRY_WAIT"}
	delete(body, "nextAttemptAt")
	delete(body, "pollAfterSeconds")
	if got, ok := OwnerDue(body, now, time.Time{}, ""); !ok || !got.Equal(at(5)) {
		t.Fatal("receipt fallback restarted on reread", got, ok)
	}
	body["result"] = map[string]any{"next_retry_at": "invalid"}
	if _, ok := OwnerDue(body, now, at(70), "100"); ok {
		t.Fatal("invalid receipt shortened to a known bound")
	}
}
