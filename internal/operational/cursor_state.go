package operational

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/hex"
	"errors"
	"sort"
	"slices"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

const CursorProfile = "ouf.operational-owner-vector.v1"

var (
	ErrCursorNotFound = errors.New("operational cursor not found")
	ErrCursorExpired = errors.New("operational cursor expired")
	ErrCursorConflict = errors.New("operational cursor result changed")
	ErrCursorCapacity = errors.New("operational cursor capacity unavailable")
)

// Only owner positions/coverage and MCP's own public self-state are persisted.
// Producer incident payloads and previously granted authority are never cached.
type OwnerPosition struct {
	Name string `json:"name"`
	Cursor string `json:"cursor"`
	Offset int `json:"offset"`
	PageIndex int `json:"pageIndex"`
	ItemsDigest string `json:"itemsDigest"`
	OwnerDigest string `json:"ownerDigest"`
	SnapshotRef string `json:"snapshotRef"`
	Done bool `json:"done"`
}

type CursorFrame struct {
	Profile string `json:"profile"`
	Filters json.RawMessage `json:"filters"`
	Owners []OwnerPosition `json:"owners"`
	PageLimit int `json:"pageLimit"`
	PageNumber int `json:"pageNumber"`
	SelfStatus string `json:"selfStatus"`
	SelfActionRequired bool `json:"selfActionRequired"`
	SelfConsumed bool `json:"selfConsumed"`
	Partial bool `json:"partial"`
}

type CursorBinding struct {
	PrincipalID, TenantID, Fingerprint, PolicyRef, ManifestChecksum string
}

type CursorNode struct {
	Token, Session uuid.UUID
	Frame CursorFrame
	ExpiresAt time.Time
	NextToken *uuid.UUID
	PageDigest string
}

type CursorStore interface {
	CreateIncidentCursor(context.Context, CursorBinding, CursorFrame) (CursorNode, error)
	LoadIncidentCursor(context.Context, uuid.UUID, CursorBinding) (CursorNode, error)
	CommitIncidentCursorPage(context.Context, uuid.UUID, CursorBinding, string, *CursorFrame) (*uuid.UUID, error)
}

// Bind all verified identity evidence, independently from conversation/login.
// No delegation token or secret is serialized. Reauthorization remains mandatory.
func BindIncidentCursor(identity orchestration.Identity, policyRef, manifest string) (CursorBinding, error) {
	if identity.PrincipalID == "" || identity.TenantID == "" || identity.ActorType == "" || policyRef == "" || !validDigest(manifest) {
		return CursorBinding{}, errors.New("operational cursor identity incomplete")
	}
	scopes := append([]string{}, identity.Scopes...)
	sort.Strings(scopes)
	scopes = slices.Compact(scopes)
	var claims *orchestration.IdentityClaims
	if identity.Claims != nil {
		copyClaims := *identity.Claims
		copyClaims.ExternalRoleRefs = append([]string{}, identity.Claims.ExternalRoleRefs...)
		copyClaims.Amr = append([]string{}, identity.Claims.Amr...)
		sort.Strings(copyClaims.ExternalRoleRefs)
		sort.Strings(copyClaims.Amr)
		copyClaims.ExternalRoleRefs = slices.Compact(copyClaims.ExternalRoleRefs)
		copyClaims.Amr = slices.Compact(copyClaims.Amr)
		copyClaims.AuthenticatedAt = copyClaims.AuthenticatedAt.UTC()
		claims = &copyClaims
	}
	payload, err := json.Marshal(struct {
		Principal, Tenant, Service, Actor, Authentication, Issuer, Audience string
		Scopes []string
		Claims *orchestration.IdentityClaims
	}{identity.PrincipalID, identity.TenantID, identity.ServicePrincipalID, identity.ActorType,
		identity.AuthenticationContextRef, identity.Issuer, identity.Audience, scopes, claims})
	if err != nil {
		return CursorBinding{}, err
	}
	if len(payload) > 16<<10 {
		return CursorBinding{}, ErrCursorCapacity
	}
	return CursorBinding{identity.PrincipalID, identity.TenantID, digestBytes(payload), policyRef, manifest}, nil
}

func digestBytes(raw []byte) string {
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}

func ValidateCursorFrame(frame CursorFrame) error {
	if frame.Profile != CursorProfile || frame.PageLimit < 1 || frame.PageLimit > 100 || frame.PageNumber < 0 || frame.PageNumber > 200 || !validStatus(frame.SelfStatus) || len(frame.Owners) != 2 {
		return ErrCursorConflict
	}
	if _, ok := operationalQuery(frame.Filters, true); !ok {
		return ErrCursorConflict
	}
	seen := map[string]bool{}
	for _, owner := range frame.Owners {
		if (owner.Name != "GATEWAY" && owner.Name != "INGESTION") || seen[owner.Name] || owner.Offset < 0 || owner.Offset > 100 || owner.PageIndex < 0 || owner.PageIndex > 200 || !validDigest(owner.ItemsDigest) || !validDigest(owner.OwnerDigest) {
			return ErrCursorConflict
		}
		if token, err := uuid.Parse(owner.Cursor); err != nil || token == uuid.Nil {
			return ErrCursorConflict
		}
		if token, err := uuid.Parse(owner.SnapshotRef); err != nil || token == uuid.Nil {
			return ErrCursorConflict
		}
		seen[owner.Name] = true
	}
	encoded, err := json.Marshal(frame)
	if err != nil || len(encoded) > 16<<10 {
		return ErrCursorCapacity
	}
	return nil
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}
