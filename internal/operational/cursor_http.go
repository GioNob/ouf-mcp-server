package operational

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"unicode/utf8"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

// Legacy reads retain their explicit partial semantics. Snapshot/continuation
// is opt-in and requires a fresh root decision plus verified delegated context.
func (h *ownerAPI) incidentCursor(w http.ResponseWriter, r *http.Request, identity orchestration.Identity, raw []byte) bool {
	if h.incidentPublication(w, r, identity, raw) { return true }
	query, token, snapshot, valid := incidentCursorQuery(raw)
	if !valid {
		http.Error(w, "invalid operational request", http.StatusBadRequest)
		return true
	}
	if !snapshot && token == uuid.Nil {
		return false
	}
	if identity.Delegation == "" {
		http.Error(w, "verified aggregate delegated context required", http.StatusForbidden)
		return true
	}
	if h.cursorStore == nil {
		http.Error(w, "operational cursor unavailable", http.StatusServiceUnavailable)
		return true
	}
	in := aggregateRequest{Identity: identity, Arguments: query, AttemptID: r.Header.Get("X-Tool-Attempt-ID"), CorrelationID: r.Header.Get("X-Correlation-ID")}
	composer := CursorComposer{Aggregator: h.aggregator, Store: h.cursorStore}
	// authorize already verified this reference against the current owner decision.
	policy := r.Header.Get("X-OUF-Authorization-Decision-Ref")
	var body []byte
	var err error
	if snapshot {
		body, err = composer.Start(r.Context(), in, policy)
	} else {
		body, err = composer.Page(r.Context(), in, policy, token)
	}
	if err != nil {
		status := http.StatusServiceUnavailable
		message := "operational cursor unavailable"
		switch {
		case errors.Is(err, ErrCursorNotFound):
			status = http.StatusNotFound
			message = "operational cursor not found"
		case errors.Is(err, ErrCursorExpired):
			status = http.StatusGone
			message = "operational cursor expired"
		case errors.Is(err, ErrCursorConflict):
			status = http.StatusConflict
			message = "operational snapshot invalidated"
		case errors.Is(err, ErrCursorCapacity):
			status = http.StatusTooManyRequests
			message = "operational cursor capacity unavailable"
		}
		http.Error(w, message, status)
		return true
	}
	if len(body) > 512<<10 || !json.Valid(body) {
		http.Error(w, "operational result unavailable", http.StatusServiceUnavailable)
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
	return true
}

// Three disjoint modes: legacy filters, snapshot=true plus filters, or one
// canonical opaque cursor. Duplicate/null/mixed/unknown fields fail before I/O.
func incidentCursorQuery(raw []byte) (json.RawMessage, uuid.UUID, bool, bool) {
	if !utf8.Valid(raw) {
		return nil, uuid.Nil, false, false
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	opening, err := d.Token()
	if err != nil || opening != json.Delim('{') {
		return nil, uuid.Nil, false, false
	}
	fields := map[string]json.RawMessage{}
	for d.More() {
		key, err := d.Token()
		name, ok := key.(string)
		if err != nil || !ok {
			return nil, uuid.Nil, false, false
		}
		if _, exists := fields[name]; exists {
			return nil, uuid.Nil, false, false
		}
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return nil, uuid.Nil, false, false
		}
		fields[name] = value
	}
	if close, err := d.Token(); err != nil || close != json.Delim('}') {
		return nil, uuid.Nil, false, false
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, uuid.Nil, false, false
	}
	if value, exists := fields["cursor"]; exists {
		var text string
		if len(fields) != 1 || json.Unmarshal(value, &text) != nil {
			return nil, uuid.Nil, false, false
		}
		token, err := uuid.Parse(text)
		if err != nil || token == uuid.Nil || token.String() != text {
			return nil, uuid.Nil, false, false
		}
		return json.RawMessage("{}"), token, false, true
	}
	snapshot := false
	if value, exists := fields["snapshot"]; exists {
		var flag *bool
		if json.Unmarshal(value, &flag) != nil || flag == nil || !*flag {
			return nil, uuid.Nil, false, false
		}
		snapshot = true
		delete(fields, "snapshot")
	}
	query, err := json.Marshal(fields)
	if err != nil {
		return nil, uuid.Nil, false, false
	}
	if _, valid := operationalQuery(query, true); !valid {
		return nil, uuid.Nil, false, false
	}
	return query, uuid.Nil, snapshot, true
}

