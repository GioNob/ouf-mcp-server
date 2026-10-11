package operational

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

const PublicationVectorProfile = "ouf.operational-publication-vector.v1"

// This profile covers the two publication owners only. It is not full MCP
// event history, a client delivery receipt, a checkpoint, or cached authority.
type PublicationPosition struct {
	Name           string `json:"name"`
	Bookmark       string `json:"bookmark"`
	ParentBookmark string `json:"parentBookmark,omitempty"`
	SnapshotRef    string `json:"snapshotRef"`
	ItemsDigest    string `json:"itemsDigest"`
}
type PublicationVector struct {
	Profile string                `json:"profile"`
	Filters json.RawMessage       `json:"filters"`
	Owners  []PublicationPosition `json:"owners"`
}
type PublicationBookmark struct {
	Token     uuid.UUID
	Parent    *uuid.UUID
	Vector    PublicationVector
	ExpiresAt time.Time
}
type PublicationBookmarkStore interface {
	PutPublicationBookmark(context.Context, CursorBinding, PublicationVector, *uuid.UUID) (PublicationBookmark, error)
	LoadPublicationBookmark(context.Context, uuid.UUID, CursorBinding) (PublicationBookmark, error)
}

// Stable verified identity, independent of login/conversation. Current root
// and owner authorization are mandatory before using this private store.
func BindPublicationBookmark(identity orchestration.Identity, policy, manifest string) (CursorBinding, error) {
	if identity.PrincipalID == "" || identity.TenantID == "" || !validDigest(manifest) || policy == "" ||
		(identity.ActorType != "HUMAN" && identity.ActorType != "SERVICE" && identity.ActorType != "AI_AGENT") {
		return CursorBinding{}, errors.New("publication bookmark identity incomplete")
	}
	raw, err := json.Marshal([]string{identity.PrincipalID, identity.TenantID, identity.ActorType, identity.ServicePrincipalID, identity.Issuer, identity.Audience})
	if err != nil {
		return CursorBinding{}, err
	}
	if len(raw) > 16384 {
		return CursorBinding{}, ErrCursorCapacity
	}
	return CursorBinding{identity.PrincipalID, identity.TenantID, digestBytes(raw), policy, manifest}, nil
}

func CanonicalPublicationVector(vector PublicationVector) ([]byte, error) {
	if vector.Profile != PublicationVectorProfile || len(vector.Owners) != 2 ||
		vector.Owners[0].Name != "GATEWAY" || vector.Owners[1].Name != "INGESTION" {
		return nil, ErrCursorConflict
	}
	canonical, err := canonicalPublicationFilters(vector.Filters)
	if err != nil {
		return nil, err
	}
	for _, position := range vector.Owners {
		for _, value := range []string{position.Bookmark, position.SnapshotRef} {
			if !canonicalPublicationToken(value) {
				return nil, ErrCursorConflict
			}
		}
		if position.ParentBookmark != "" && !canonicalPublicationToken(position.ParentBookmark) {
			return nil, ErrCursorConflict
		}
		if !validDigest(position.ItemsDigest) {
			return nil, ErrCursorConflict
		}
	}
	vector.Filters = canonical
	raw, err := json.Marshal(vector)
	if err != nil {
		return nil, err
	}
	if len(raw) > 4096 {
		return nil, ErrCursorCapacity
	}
	return raw, nil
}
func DecodePublicationVector(raw []byte) (PublicationVector, error) {
	var vector PublicationVector
	if len(raw) > 4096 {
		return vector, ErrCursorCapacity
	}
	if _, err := uniquePublicationJSON(raw); err != nil {
		return vector, ErrCursorConflict
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&vector) != nil {
		return vector, ErrCursorConflict
	}
	canonical, err := CanonicalPublicationVector(vector)
	if err != nil {
		return vector, err
	}
	// Canonical encoding also rules out duplicate keys and trailing documents.
	var original, normalized any
	if json.Unmarshal(raw, &original) != nil || json.Unmarshal(canonical, &normalized) != nil {
		return vector, ErrCursorConflict
	}
	a, _ := json.Marshal(original)
	b, _ := json.Marshal(normalized)
	if !bytes.Equal(a, b) {
		return vector, ErrCursorConflict
	}
	return vector, nil
}
func canonicalPublicationToken(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}

// A lineage proof uses exact owner tokens, never UUID order or local clocks.
// The composer must obtain the complete, non-redacted owner coverage first.
func PublicationVectorLineage(previous, next PublicationVector) bool {
	a, errA := CanonicalPublicationVector(previous)
	b, errB := CanonicalPublicationVector(next)
	if errA != nil || errB != nil {
		return false
	}
	old, _ := DecodePublicationVector(a)
	current, _ := DecodePublicationVector(b)
	if !bytes.Equal(old.Filters, current.Filters) {
		return false
	}
	for i := range old.Owners {
		if current.Owners[i].ParentBookmark != old.Owners[i].Bookmark {
			return false
		}
	}
	return true
}
func SamePublicationBoundaries(previous, next PublicationVector) bool {
	if !PublicationVectorLineage(previous, next) {
		return false
	}
	for i := range previous.Owners {
		if previous.Owners[i].Bookmark != next.Owners[i].Bookmark {
			return false
		}
	}
	return true
}

func uniquePublicationJSON(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := uniquePublicationValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, ErrCursorConflict
	}
	return value, nil
}
func uniquePublicationValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, container := token.(json.Delim)
	if !container {
		return token, nil
	}
	switch delimiter {
	case '{':
		values := map[string]any{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, ErrCursorConflict
			}
			if _, exists := values[key]; exists {
				return nil, ErrCursorConflict
			}
			value, err := uniquePublicationValue(decoder)
			if err != nil {
				return nil, err
			}
			values[key] = value
		}
		closeToken, err := decoder.Token()
		if err != nil || closeToken != json.Delim('}') {
			return nil, ErrCursorConflict
		}
		return values, nil
	case '[':
		values := []any{}
		for decoder.More() {
			value, err := uniquePublicationValue(decoder)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		closeToken, err := decoder.Token()
		if err != nil || closeToken != json.Delim(']') {
			return nil, ErrCursorConflict
		}
		return values, nil
	default:
		return nil, ErrCursorConflict
	}
}
