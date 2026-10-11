package operational

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

var ErrCursorOwnerUnavailable = errors.New("operational owner snapshot unavailable")

// CursorComposer is an owner-vector read primitive, not a delivery receipt or
// a public handler. The adapter must freshly authorize the root collection
// before calling it. Every owner read still uses Aggregator's governed caller.
type CursorComposer struct {
	Aggregator *Aggregator
	Store CursorStore
}

type cursorOwnerPage struct {
	Items []map[string]any `json:"items"`
	Partial *bool `json:"partial"`
	Truncated bool `json:"truncated"`
	Cursor string `json:"cursor"`
	NextCursor string `json:"nextCursor"`
	SnapshotRef string `json:"snapshotRef"`
	ItemsDigest string `json:"itemsDigest"`
	ExpiresAt time.Time `json:"expiresAt"`
	SnapshotExpiresAt time.Time `json:"snapshotExpiresAt"`
	PriorityProfile string `json:"priorityProfile"`
	ConsultationRecorded bool `json:"consultationRecorded"`
}

// Start captures two independent owner snapshots. No incident payload is
// written to MCP storage; a subsequent call re-fetches and verifies each page.
func (c CursorComposer) Start(ctx context.Context, in aggregateRequest, policyRef string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
	defer cancel()
	if err := c.configured(); err != nil { return nil, err }
	limit, valid := operationalQuery(in.Arguments, true)
	if !valid { return nil, ErrCursorConflict }
	binding, err := BindIncidentCursor(in.Identity, policyRef, c.Aggregator.ManifestChecksum)
	if err != nil { return nil, err }
	var filters map[string]json.RawMessage
	if json.Unmarshal(in.Arguments, &filters) != nil { return nil, ErrCursorConflict }
	delete(filters, "limit")
	canonicalFilters, err := json.Marshal(filters)
	if err != nil { return nil, err }
	filters["limit"] = json.RawMessage(`100`)
	filters["snapshot"] = json.RawMessage(`true`)
	in.Arguments, err = json.Marshal(filters)
	if err != nil { return nil, err }
	pages := map[string]cursorOwnerPage{}
	for result := range c.Aggregator.callProducers(ctx, in, incidentProducers) {
		if !result.OK { return nil, ErrCursorOwnerUnavailable }
		page, err := decodeCursorOwnerPage(result.Body, result.Producer.Name)
		if err != nil { return nil, err }
		pages[result.Producer.Name] = page
	}
	selfStatus, selfAction, err := c.currentSelf(ctx, in)
	if err != nil { return nil, err }
	frame := CursorFrame{Profile:CursorProfile, Filters:canonicalFilters, PageLimit:limit, SelfStatus:selfStatus, SelfActionRequired:selfAction}
	for _, name := range []string{"GATEWAY", "INGESTION"} {
		page, exists := pages[name]
		if !exists { return nil, ErrCursorOwnerUnavailable }
		frame.Owners = append(frame.Owners, page.position(name, 0))
	}
	if err := ValidateCursorFrame(frame); err != nil { return nil, err }
	node, err := c.Store.CreateIncidentCursor(ctx, binding, frame)
	if err != nil { return nil, err }
	return c.compose(ctx, in, binding, node, pages, 2)
}

func (c CursorComposer) Page(ctx context.Context, in aggregateRequest, policyRef string, token uuid.UUID) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
	defer cancel()
	if err := c.configured(); err != nil { return nil, err }
	binding, err := BindIncidentCursor(in.Identity, policyRef, c.Aggregator.ManifestChecksum)
	if err != nil { return nil, err }
	node, err := c.Store.LoadIncidentCursor(ctx, token, binding)
	if err != nil { return nil, err }
	if err := ValidateCursorFrame(node.Frame); err != nil { return nil, err }
	if time.Now().After(node.ExpiresAt) { return nil, ErrCursorExpired }
	// Revalidate even completed owners: a cached grant or payload must never
	// substitute for current owner authorization on a replay or later page.
	pages := map[string]cursorOwnerPage{}
	for _, owner := range node.Frame.Owners {
		page, err := c.readPage(ctx, in, node.Frame.Filters, owner.Name, owner.Cursor)
		if err != nil { return nil, err }
		if !sameOwnerPage(owner, page) { return nil, ErrCursorConflict }
		pages[owner.Name] = page
	}
	status, action, err := c.currentSelf(ctx, in)
	if err != nil { return nil, err }
	if status != node.Frame.SelfStatus || action != node.Frame.SelfActionRequired { return nil, ErrCursorConflict }
	return c.compose(ctx, in, binding, node, pages, 2)
}

func (c CursorComposer) configured() error {
	if c.Store == nil || c.Aggregator == nil || c.Aggregator.Caller == nil || c.Aggregator.Self == nil { return ErrCursorOwnerUnavailable }
	return nil
}

func (c CursorComposer) currentSelf(ctx context.Context, in aggregateRequest) (string, bool, error) {
	body, err := c.Aggregator.selfStatus(ctx, in.Identity)
	if err != nil { return "", false, ErrCursorOwnerUnavailable }
	var status struct { Status string `json:"status"`; Partial bool `json:"partial"`; Action bool `json:"actionRequired"` }
	if json.Unmarshal(body, &status) != nil || !validStatus(status.Status) || status.Partial { return "", false, ErrCursorOwnerUnavailable }
	return status.Status, status.Action, nil
}

func (p cursorOwnerPage) position(name string, index int) OwnerPosition {
	canonical, _ := json.Marshal(p.Items)
	return OwnerPosition{Name:name, Cursor:p.Cursor, PageIndex:index, ItemsDigest:digestBytes(canonical), OwnerDigest:p.ItemsDigest, SnapshotRef:p.SnapshotRef}
}

func sameOwnerPage(position OwnerPosition, page cursorOwnerPage) bool {
	observed := page.position(position.Name, position.PageIndex)
	return position.Cursor == observed.Cursor && position.SnapshotRef == observed.SnapshotRef && position.ItemsDigest == observed.ItemsDigest && position.OwnerDigest == observed.OwnerDigest && position.Offset <= len(page.Items)
}

func (c CursorComposer) readPage(ctx context.Context, in aggregateRequest, filters json.RawMessage, name, token string) (cursorOwnerPage, error) {
	arguments, _ := json.Marshal(map[string]string{"cursor":token})
	in.Arguments = arguments
	// Preserve server-saved source scope while the wire query remains cursor-only.
	var scope map[string]json.RawMessage
	if json.Unmarshal(filters, &scope) != nil { return cursorOwnerPage{}, ErrCursorConflict }
	if value, exists := scope["sourceId"]; exists { if json.Unmarshal(value, &in.ScopeSource) != nil { return cursorOwnerPage{}, ErrCursorConflict } }
	for _, producer := range incidentProducers {
		if producer.Name == name {
			body, ok := c.Aggregator.callProducer(ctx, in, producer)
			if !ok { return cursorOwnerPage{}, ErrCursorOwnerUnavailable }
			return decodeCursorOwnerPage(body, name)
		}
	}
	return cursorOwnerPage{}, ErrCursorConflict
}

// Each page uses at most three governed owner reads: two fresh readbacks and
// one refill. Ending early at an unknown head preserves global ordering. This
// does not create another retry loop or reset the caller's durable budgets.
func (c CursorComposer) compose(ctx context.Context, in aggregateRequest, binding CursorBinding, node CursorNode, pages map[string]cursorOwnerPage, calls int) ([]byte, error) {
	frame := node.Frame
	frame.Owners = append([]OwnerPosition{}, frame.Owners...)
	items := make([]map[string]any, 0, frame.PageLimit)
	scanPending := false
	for len(items) < frame.PageLimit {
		unknownHead := false
		for index := range frame.Owners {
			position := &frame.Owners[index]
			if position.Done { continue }
			page := pages[position.Name]
			if position.Offset < len(page.Items) { continue }
			if page.NextCursor == "" { position.Done = true; continue }
			if calls >= 3 { unknownHead = true; break }
			if position.PageIndex >= 200 { return nil, ErrCursorCapacity }
			next, err := c.readPage(ctx, in, frame.Filters, position.Name, page.NextCursor)
			calls++
			if err != nil { return nil, err }
			if next.SnapshotRef != position.SnapshotRef || next.Cursor != page.NextCursor { return nil, ErrCursorConflict }
			// Owners must keep priority order across page boundaries as well.
			if len(page.Items) > 0 && len(next.Items) > 0 && incidentBefore(next.Items[0], page.Items[len(page.Items)-1]) { return nil, ErrCursorConflict }
			*position = next.position(position.Name, position.PageIndex+1)
			pages[position.Name] = next
			if len(next.Items) == 0 { position.Done = true }
		}
		if unknownHead { scanPending = true; break }
		winner := -1
		var candidate map[string]any
		for index, position := range frame.Owners {
			if position.Done { continue }
			page := pages[position.Name]
			if position.Offset >= len(page.Items) { return nil, ErrCursorConflict }
			item := page.Items[position.Offset]
			if candidate == nil || incidentBefore(item, candidate) { candidate = item; winner = index }
		}
		self := selfCursorIncident(frame)
		if self != nil && (candidate == nil || incidentBefore(self, candidate)) {
			items = append(items, self); frame.SelfConsumed = true; continue
		}
		if winner < 0 { break }
		items = append(items, candidate)
		frame.Owners[winner].Offset++
	}
	more := !frame.SelfConsumed && frame.SelfStatus != "HEALTHY"
	for index := range frame.Owners {
		position := &frame.Owners[index]
		page := pages[position.Name]
		if position.Offset == len(page.Items) && page.NextCursor == "" { position.Done = true }
		more = more || !position.Done
	}
	result := map[string]any{"items":items,"partial":false,"truncated":more,"cursor":node.Token.String(),"snapshotRef":node.Session.String(),"expiresAt":node.ExpiresAt.UTC(),"priorityProfile":"ouf.incident-priority.v1","cursorProfile":CursorProfile,"consultationRecorded":false,
		"coverage":map[string]any{"kind":"INDEPENDENT_OWNER_SNAPSHOT_VECTOR","scanPending":scanPending,"pageComplete":!scanPending,"ownerCalls":calls,"atomicAcrossOwners":false}}
	canonical, err := json.Marshal(result)
	if err != nil { return nil, err }
	if len(canonical) > (512<<10)-1024 { return nil, ErrCursorCapacity }
	var next *CursorFrame
	if more {
		if frame.PageNumber >= 200 { return nil, ErrCursorCapacity }
		frame.PageNumber++
		next = &frame
	}
	token, err := c.Store.CommitIncidentCursorPage(ctx, node.Token, binding, digestBytes(canonical), next)
	if err != nil { return nil, err }
	if token != nil { result["nextCursor"] = token.String() }
	return json.Marshal(result)
}

func selfCursorIncident(frame CursorFrame) map[string]any {
	if frame.SelfConsumed || frame.SelfStatus == "HEALTHY" { return nil }
	return map[string]any{"incident_id":"mcp-operational-state","module":"MCP","event_type":"MCP_OPERATIONAL_"+frame.SelfStatus,"lifecycle_state":mapStatusLifecycle(frame.SelfStatus),"severity":mapStatusSeverity(frame.SelfStatus),"action_required":frame.SelfActionRequired,"impact_summary":"MCP has governed recovery, debt, evidence or unresolved orchestration state requiring attention.","visibility_class":"TENANT_OPERATIONAL"}
}

func decodeCursorOwnerPage(body []byte, name string) (cursorOwnerPage, error) {
	var page cursorOwnerPage
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if len(body) > 512<<10 || decoder.Decode(&page) != nil { return page, ErrCursorOwnerUnavailable }
	if name == "INGESTION" { page.ExpiresAt = page.SnapshotExpiresAt }
	if page.Items == nil || page.Partial == nil || *page.Partial || page.ConsultationRecorded || len(page.Items) > 100 || page.PriorityProfile != "ouf.incident-priority.v1" || !validDigest(page.ItemsDigest) || time.Now().After(page.ExpiresAt) { return page, ErrCursorOwnerUnavailable }
	for _, token := range []string{page.Cursor, page.SnapshotRef} { if parsed, err := uuid.Parse(token); err != nil || parsed == uuid.Nil { return page, ErrCursorConflict } }
	if page.Truncated != (page.NextCursor != "") { return page, ErrCursorConflict }
	if page.NextCursor != "" { if token, err := uuid.Parse(page.NextCursor); err != nil || token == uuid.Nil || page.NextCursor == page.Cursor || len(page.Items) != 100 { return page, ErrCursorConflict } }
	seen := map[string]bool{}
	for _, item := range page.Items {
		if item == nil { return page, ErrCursorConflict }
		for field := range item { if !cursorIncidentField(field) { return page, ErrCursorConflict } }
		if module, exists := item["module"]; exists && module != name { return page, ErrCursorConflict }
		item["module"] = name
		id := incidentField(item,"incident_id","incidentId")
		state := incidentField(item,"lifecycle_state","lifecycleState")
		if id == "" || seen[id] || severityRank(stringValue(item["severity"])) == 0 || (state != "OPEN" && state != "RECOVERING" && state != "RESOLVED") { return page, ErrCursorConflict }
		if _, ok := incidentValue(item,"action_required","actionRequired").(bool); !ok { return page, ErrCursorConflict }
		seen[id] = true
	}
	if !sort.SliceIsSorted(page.Items,func(i,j int)bool{return incidentBefore(page.Items[i],page.Items[j])}) { return page, fmt.Errorf("%w: owner priority order", ErrCursorConflict) }
	return page, nil
}

func cursorIncidentField(field string) bool {
	switch field {
	case "incident_id", "module", "event_type", "lifecycle_state", "severity", "first_seen_at", "last_seen_at", "resolved_at", "correlation_id", "endpoint_ref", "error_code", "impact_summary", "action_required", "dedup_key", "occurrence_count", "visibility_class", "job_ref", "source_ref", "authorization_decision_ref":
		return true
	default:
		return false
	}
}
