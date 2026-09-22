package operational

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

// One owner per page avoids dropping overflow or embedding owner data in a client cursor.
// Owner cursors are passed back only to their original producer; every page is reauthorized.
type incidentQuery struct {
	Limit    *int       `json:"limit,omitempty"`
	State    *string    `json:"state,omitempty"`
	Since    *time.Time `json:"since,omitempty"`
	Until    *time.Time `json:"until,omitempty"`
	SourceID *string    `json:"sourceId,omitempty"`
	JobID    *string    `json:"jobId,omitempty"`
	Severity *string    `json:"severity,omitempty"`
	Cursor   *string    `json:"cursor,omitempty"`
}
type incidentCursor struct {
	Version     int       `json:"v"`
	Producer    int       `json:"producer"`
	OwnerCursor string    `json:"ownerCursor,omitempty"`
	Since       time.Time `json:"since"`
	Until       time.Time `json:"until"`
	Expires     time.Time `json:"expires"`
	Binding     string    `json:"binding"`
}

func incidentBinding(q incidentQuery, id orchestration.Identity) string {
	q.Cursor = nil
	q.Since = nil
	q.Until = nil
	raw, _ := json.Marshal(struct {
		Query             incidentQuery
		Tenant, Principal string
	}{q, id.TenantID, id.PrincipalID})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func prepareIncidentQuery(q *incidentQuery, id orchestration.Identity) (incidentCursor, error) {
	now := time.Now().UTC()
	limit := 50
	if q.Limit != nil {
		limit = *q.Limit
	}
	q.Limit = &limit
	invalid := fmt.Errorf("invalid incident query")
	if limit < 1 || limit > 100 {
		return incidentCursor{}, invalid
	}
	if q.State != nil && *q.State != "OPEN" && *q.State != "RECOVERING" && *q.State != "RESOLVED" {
		return incidentCursor{}, invalid
	}
	if q.Severity != nil && *q.Severity != "WARNING" && *q.Severity != "ERROR" {
		return incidentCursor{}, invalid
	}
	if q.SourceID != nil && (len(*q.SourceID) < 1 || len(*q.SourceID) > 200) {
		return incidentCursor{}, invalid
	}
	if q.JobID != nil && (!regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`).MatchString(*q.JobID)) {
		return incidentCursor{}, invalid
	}
	c := incidentCursor{Version: 1, Expires: now.Add(15 * time.Minute), Binding: incidentBinding(*q, id)}
	if q.Cursor != nil {
		if len(*q.Cursor) > 8192 {
			return c, invalid
		}
		raw, err := base64.RawURLEncoding.DecodeString(*q.Cursor)
		if err != nil || json.Unmarshal(raw, &c) != nil || c.Version != 1 || c.Producer < 0 || c.Producer >= len(incidentProducers) ||
			c.Binding != incidentBinding(*q, id) || !c.Expires.After(now) || c.Expires.After(now.Add(16*time.Minute)) || len(c.OwnerCursor) > 4096 {
			return c, invalid
		}
		if (q.Since != nil && !q.Since.Equal(c.Since)) || (q.Until != nil && !q.Until.Equal(c.Until)) {
			return c, invalid
		}
	} else {
		c.Until = now
		if q.Until != nil {
			c.Until = *q.Until
		}
		c.Since = c.Until.Add(-24 * time.Hour)
		if q.Since != nil {
			c.Since = *q.Since
		}
	}
	if c.Since.After(c.Until) || c.Until.After(now.Add(time.Second)) || c.Until.Sub(c.Since) > 30*24*time.Hour {
		return c, invalid
	}
	q.Since = &c.Since
	q.Until = &c.Until
	return c, nil
}
func encodeIncidentCursor(c incidentCursor) string {
	raw, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func (a Aggregator) Incidents(ctx context.Context, in aggregateRequest) ([]byte, error) {
	if a.Caller == nil || a.ManifestChecksum == "" {
		return nil, fmt.Errorf("operational aggregator is not configured")
	}
	var q incidentQuery
	if json.Unmarshal(in.Arguments, &q) != nil {
		return nil, fmt.Errorf("invalid incident query")
	}
	c, err := prepareIncidentQuery(&q, in.Identity)
	if err != nil {
		return nil, err
	}
	producer := incidentProducers[c.Producer]
	q.Cursor = nil
	if c.OwnerCursor != "" {
		q.Cursor = &c.OwnerCursor
	}
	in.Arguments, _ = json.Marshal(q)
	body, ok, denied := a.callProducer(ctx, in, producer)
	if denied {
		return nil, orchestration.ErrUnauthorized
	}
	unavailable := func() ([]byte, error) {
		return json.Marshal(map[string]any{"items": []any{}, "partial": true, "hasMore": true, "nextCursor": encodeIncidentCursor(c), "unavailableProducers": []string{producer.Name}})
	}
	if !ok || len(body) > 512<<10 {
		return unavailable()
	}
	var page struct {
		Items         []map[string]any `json:"items"`
		Partial       *bool            `json:"partial"`
		HasMore       *bool            `json:"hasMore"`
		NextCursor    string           `json:"nextCursor"`
		Authorization string           `json:"authorization"`
	}
	if json.Unmarshal(body, &page) != nil || page.Partial == nil || page.HasMore == nil || len(page.Items) > *q.Limit || len(page.NextCursor) > 4096 || (*page.HasMore && page.NextCursor == "") {
		return unavailable()
	}
	items := make([]map[string]any, 0, len(page.Items))
	partial := *page.Partial || page.Authorization == "REDACTED"
	for _, item := range page.Items {
		safe, err := safeIncident(item, producer.Name)
		if err != nil {
			partial = true
			continue
		}
		items = append(items, safe)
	}
	if *page.HasMore {
		c.OwnerCursor = page.NextCursor
	} else {
		c.Producer++
		c.OwnerCursor = ""
	}
	more := c.Producer < len(incidentProducers)
	out := map[string]any{"items": items, "partial": partial, "hasMore": more, "unavailableProducers": []string{}, "since": c.Since, "until": c.Until, "visibilityClass": "TENANT_OPERATIONAL", "redacted": true}
	if more {
		out["nextCursor"] = encodeIncidentCursor(c)
	}
	return json.Marshal(out)
}

func safeIncident(in map[string]any, module string) (map[string]any, error) {
	if in["module"] != module || (in["visibility_class"] != "TENANT_OPERATIONAL" && in["visibility_class"] != "PUBLIC_OPERATIONAL") {
		return nil, fmt.Errorf("invalid projection")
	}
	state := in["lifecycle_state"]
	if state != "OPEN" && state != "RECOVERING" && state != "RESOLVED" {
		return nil, fmt.Errorf("invalid state")
	}
	if id, ok := in["incident_id"].(string); !ok || id == "" {
		return nil, fmt.Errorf("missing incident")
	}
	out := map[string]any{}
	for _, key := range []string{"event_id", "incident_id", "module", "event_type", "lifecycle_state", "severity", "first_seen_at", "last_seen_at", "resolved_at", "source_ref", "job_ref", "endpoint_ref", "error_code", "retry_state", "next_retry_at", "impact_summary", "visibility_class", "safe_outcome", "correlation_id"} {
		if value, exists := in[key]; exists && value != nil {
			s, ok := value.(string)
			if !ok || len(s) > 2048 {
				return nil, fmt.Errorf("invalid field")
			}
			out[key] = s
		}
	}
	if v, ok := in["action_required"].(bool); ok {
		out["action_required"] = v
	}
	for _, key := range []string{"attempt_count", "duration_ms", "occurrence_count"} {
		if v, ok := in[key].(float64); ok && v >= 0 && v == float64(int64(v)) {
			out[key] = v
		}
	}
	return out, nil
}
