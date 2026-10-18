// Package reprocessing verifies an installation-bound, dual-signed, one-call
// admission session. It never authorizes a semantic approval or owner decision.
package reprocessing

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
)

var ErrPermit = errors.New("SEMANTIC_REPROCESSING_PERMIT_INVALID")
var ErrExpired = errors.New("SEMANTIC_REPROCESSING_SESSION_EXPIRED")

// KeyProfile is trusted deployment configuration, hash-bound separately from
// the untrusted permit. Keys must be the existing installation role keys.
type KeyProfile struct {
	Schema               string `json:"schema"`
	MaxSessionsPerTenant int64  `json:"maxSessionsPerTenant"`
	InstallationRef      string `json:"installationRef"`
	EntityRef            string `json:"entityRef"`
	InstallerKeyRef      string `json:"installerKeyRef"`
	IntentIssuerRef      string `json:"intentIssuerRef"`
	ApprovalIssuerRef    string `json:"approvalIssuerRef"`
	TrustPolicyHash      string `json:"trustPolicyHash"`
}

// These are the existing installation trust-policy and detached-signature
// schemas. This consumer neither creates keys nor widens an issuer's roles.
type TrustKey struct {
	KeyRef    string   `json:"keyRef"`
	IssuerRef string   `json:"issuerRef"`
	Roles     []string `json:"roles"`
	PublicKey string   `json:"publicKey"`
	NotBefore int64    `json:"notBefore"`
	ExpiresAt int64    `json:"expiresAt"`
	State     string   `json:"state"`
}
type TrustPolicy struct {
	Schema          string     `json:"schema"`
	InstallationRef string     `json:"installationRef"`
	EntityRef       string     `json:"entityRef"`
	Keys            []TrustKey `json:"keys"`
}
type DetachedSignature struct {
	Schema          string `json:"schema"`
	Algorithm       string `json:"algorithm"`
	KeyRef          string `json:"keyRef"`
	Role            string `json:"role"`
	IssuerRef       string `json:"issuerRef"`
	InstallationRef string `json:"installationRef"`
	EntityRef       string `json:"entityRef"`
	PayloadHash     string `json:"payloadHash"`
	Signature       string `json:"signature"`
}

type Permit struct {
	Schema                     string `json:"schema"`
	SessionID                  string `json:"sessionId"`
	DecisionRef                string `json:"decisionRef"`
	Reason                     string `json:"reason"`
	InstallationRef            string `json:"installationRef"`
	KeyProfileHash             string `json:"keyProfileHash"`
	ManifestChecksum           string `json:"manifestChecksum"`
	ServicePrincipalID         string `json:"servicePrincipalId"`
	HumanSubject               string `json:"humanSubject"`
	TenantID                   string `json:"tenantId"`
	Owner                      string `json:"owner"`
	CapabilityID               string `json:"capabilityId"`
	OperationClass             string `json:"operationClass"`
	CommandKey                 string `json:"commandKey"`
	RequestHash                string `json:"requestHash"`
	PredecessorAttemptID       string `json:"predecessorAttemptId"`
	PredecessorLockVersion     int64  `json:"predecessorLockVersion"`
	PredecessorOutcomeCode     string `json:"predecessorOutcomeCode"`
	PriorToolCalls             int64  `json:"priorToolCalls"`
	MaximumNewToolCalls        int64  `json:"maximumNewToolCalls"`
	MaximumCumulativeToolCalls int64  `json:"maximumCumulativeToolCalls"`
	OwnerReceiptAbsentAt       int64  `json:"ownerReceiptAbsentAt"`
	IssuedAt                   int64  `json:"issuedAt"`
	ExpiresAt                  int64  `json:"expiresAt"`
}

type Envelope struct {
	Payload  []byte            `json:"payload"`
	Intent   DetachedSignature `json:"intent"`
	Approval DetachedSignature `json:"approval"`
}

type VerifiedPermit struct {
	maxSessions int64
	value       Permit
	hash        string
}

func (v *VerifiedPermit) Capacity() int64 { return v.maxSessions }
func (v *VerifiedPermit) Value() Permit   { return v.value }
func (v *VerifiedPermit) Hash() string    { return v.hash }
func Hash(raw []byte) string              { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func hash(s string) bool {
	_, e := hex.DecodeString(s)
	return len(s) == 64 && e == nil && strings.ToLower(s) == s
}
func bounded(s string, n int) bool {
	return len(s) > 0 && len(s) <= n && !strings.ContainsAny(s, "\x00\r\n")
}

func decode(raw []byte, v any) error {
	if len(raw) == 0 || len(raw) > 65536 {
		return ErrPermit
	}
	// Duplicate keys are rejected at every nesting level before typed decoding.
	d := json.NewDecoder(bytes.NewReader(raw))
	if err := uniqueValue(d); err != nil {
		return ErrPermit
	}
	if _, err := d.Token(); err != io.EOF {
		return ErrPermit
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		return ErrPermit
	}
	return nil
}
func uniqueValue(d *json.Decoder) error {
	t, e := d.Token()
	if e != nil {
		return e
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	if delim == '{' {
		keys := map[string]bool{}
		for d.More() {
			k, e := d.Token()
			if e != nil {
				return e
			}
			s, ok := k.(string)
			if !ok || keys[s] {
				return ErrPermit
			}
			keys[s] = true
			if e = uniqueValue(d); e != nil {
				return e
			}
		}
	} else if delim == '[' {
		for d.More() {
			if e = uniqueValue(d); e != nil {
				return e
			}
		}
	} else {
		return ErrPermit
	}
	_, e = d.Token()
	return e
}

func identity(s string) bool {
	return regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`).MatchString(s)
}

// SigningBytes exactly implements the existing public Python producer frame:
// DOMAIN + big-endian header length + ASCII canonical header + payload length + exact payload.
func SigningBytes(payload []byte, h DetachedSignature) ([]byte, error) {
	if h.Schema != "ouf.semantic-deployment-detached-signature.v1" || h.Algorithm != "Ed25519" || !identity(h.KeyRef) || !identity(h.IssuerRef) || !identity(h.InstallationRef) || !identity(h.EntityRef) || h.PayloadHash != Hash(payload) || (h.Role != "DEPLOYMENT_INTENT" && h.Role != "FINAL_DEPLOYMENT_APPROVAL") {
		return nil, ErrPermit
	}
	header, _ := json.Marshal(map[string]string{"schema": h.Schema, "algorithm": h.Algorithm, "keyRef": h.KeyRef, "role": h.Role, "issuerRef": h.IssuerRef, "installationRef": h.InstallationRef, "entityRef": h.EntityRef, "payloadHash": h.PayloadHash})
	var frame bytes.Buffer
	frame.WriteString("OUF-DEPLOYMENT-EVIDENCE\x00V1\x00")
	_ = binary.Write(&frame, binary.BigEndian, uint32(len(header)))
	frame.Write(header)
	_ = binary.Write(&frame, binary.BigEndian, uint32(len(payload)))
	frame.Write(payload)
	return frame.Bytes(), nil
}
func SignatureHeader(profile KeyProfile, payload []byte, role string) DetachedSignature {
	issuer := profile.IntentIssuerRef
	if role == "FINAL_DEPLOYMENT_APPROVAL" {
		issuer = profile.ApprovalIssuerRef
	}
	return DetachedSignature{Schema: "ouf.semantic-deployment-detached-signature.v1", Algorithm: "Ed25519", KeyRef: profile.InstallerKeyRef, Role: role, IssuerRef: issuer, InstallationRef: profile.InstallationRef, EntityRef: profile.EntityRef, PayloadHash: Hash(payload)}
}
func Verify(profileRaw, policyRaw, envelopeRaw []byte, expectedProfileHash, manifest string, now time.Time) (*VerifiedPermit, error) {
	if !hash(expectedProfileHash) || Hash(profileRaw) != expectedProfileHash || !hash(manifest) {
		return nil, ErrPermit
	}
	var profile KeyProfile
	var policy TrustPolicy
	var env Envelope
	var p Permit
	if decode(profileRaw, &profile) != nil || decode(policyRaw, &policy) != nil || decode(envelopeRaw, &env) != nil || decode(env.Payload, &p) != nil {
		return nil, ErrPermit
	}
	if profile.Schema != "ouf.mcp-reprocessing-key-profile.v1" || profile.MaxSessionsPerTenant < 1 || profile.MaxSessionsPerTenant > 10000 || !identity(profile.InstallationRef) || !identity(profile.EntityRef) || !identity(profile.InstallerKeyRef) || !identity(profile.IntentIssuerRef) || !identity(profile.ApprovalIssuerRef) || profile.TrustPolicyHash != Hash(policyRaw) || policy.Schema != "ouf.semantic-deployment-trust-policy.v1" || policy.InstallationRef != profile.InstallationRef || policy.EntityRef != profile.EntityRef || len(policy.Keys) < 1 || len(policy.Keys) > 16 {
		return nil, ErrPermit
	}
	var key TrustKey
	found := 0
	seen := map[string]bool{}
	for _, k := range policy.Keys {
		if seen[k.KeyRef] {
			return nil, ErrPermit
		}
		seen[k.KeyRef] = true
		if k.KeyRef == profile.InstallerKeyRef {
			key = k
			found++
		}
	}
	if found != 1 || key.State != "ACTIVE" || key.IssuerRef != profile.IntentIssuerRef || key.IssuerRef != profile.ApprovalIssuerRef || len(key.Roles) != 2 || key.Roles[0] == key.Roles[1] {
		return nil, ErrPermit
	}
	roles := map[string]bool{}
	for _, r := range key.Roles {
		roles[r] = true
	}
	if !roles["DEPLOYMENT_INTENT"] || !roles["FINAL_DEPLOYMENT_APPROVAL"] {
		return nil, ErrPermit
	}
	public, err := hex.DecodeString(key.PublicKey)
	if err != nil || len(public) != ed25519.PublicKeySize {
		return nil, ErrPermit
	}
	for _, item := range []struct {
		role      string
		signature DetachedSignature
	}{{"DEPLOYMENT_INTENT", env.Intent}, {"FINAL_DEPLOYMENT_APPROVAL", env.Approval}} {
		want := SignatureHeader(profile, env.Payload, item.role)
		actual := item.signature
		sig, e := hex.DecodeString(actual.Signature)
		actual.Signature = ""
		frame, e2 := SigningBytes(env.Payload, actual)
		if e != nil || e2 != nil || actual != want || len(sig) != ed25519.SignatureSize || !ed25519.Verify(public, frame, sig) {
			return nil, ErrPermit
		}
	}
	id, err := uuid.Parse(p.SessionID)
	prior, e := uuid.Parse(p.PredecessorAttemptID)
	if err != nil || e != nil || id == uuid.Nil || prior == uuid.Nil || p.Schema != "ouf.mcp-semantic-reprocessing-session.v1" || !bounded(p.DecisionRef, 256) || !bounded(p.Reason, 512) || p.InstallationRef != profile.InstallationRef || p.KeyProfileHash != expectedProfileHash || p.ManifestChecksum != manifest || !bounded(p.ServicePrincipalID, 128) || !bounded(p.HumanSubject, 128) || !bounded(p.TenantID, 128) || p.Owner != "semantic" || p.CapabilityID != "ouf.semantic.propose" || p.OperationClass != "COMMAND" || !bounded(p.CommandKey, 128) || !hash(p.RequestHash) || p.PredecessorLockVersion < 0 || p.PredecessorOutcomeCode != "SEM_CAPABILITY_REQUIRED:ouf.semantic.propose" || p.PriorToolCalls != 1 || p.MaximumNewToolCalls != 1 || p.MaximumCumulativeToolCalls != 2 || p.ExpiresAt-p.IssuedAt <= 0 || p.ExpiresAt-p.IssuedAt > 900 || p.OwnerReceiptAbsentAt > p.IssuedAt || p.OwnerReceiptAbsentAt < p.IssuedAt-300 || p.IssuedAt < key.NotBefore || p.ExpiresAt > key.ExpiresAt {
		return nil, ErrPermit
	}
	verified := &VerifiedPermit{value: p, hash: Hash(env.Payload), maxSessions: profile.MaxSessionsPerTenant}
	if err = verified.CheckTime(now, 0); err != nil {
		return verified, err
	}
	return verified, nil
}
func (v *VerifiedPermit) CheckTime(now time.Time, remaining time.Duration) error {
	if v == nil {
		return ErrPermit
	}
	n := now.Unix()
	if n < v.value.IssuedAt || !now.Add(remaining).Before(time.Unix(v.value.ExpiresAt, 0)) {
		return ErrExpired
	}
	return nil
}
func (v *VerifiedPermit) Matches(service, subject, tenant, capability, owner, operation, key string) bool {
	if v == nil {
		return false
	}
	p := v.value
	return p.ServicePrincipalID == service && p.HumanSubject == subject && p.TenantID == tenant && p.CapabilityID == capability && p.Owner == owner && p.OperationClass == operation && p.CommandKey == key
}

// ReadDeploymentFile has no symlink/ancestry fallback. Both files are public
// metadata in read-only mounts; no private signing key or bearer is opened.
func ReadDeploymentFile(name string) ([]byte, error) {
	if !filepath.IsAbs(name) || filepath.Clean(name) != name {
		return nil, ErrPermit
	}
	for p := filepath.Dir(name); ; p = filepath.Dir(p) {
		s, e := os.Lstat(p)
		if e != nil || !s.IsDir() || s.Mode().Perm()&0022 != 0 {
			return nil, ErrPermit
		}
		u, ok := s.Sys().(*syscall.Stat_t)
		if !ok || u.Uid != 0 {
			return nil, ErrPermit
		}
		if p == "/" {
			break
		}
	}
	fd, e := syscall.Open(name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return nil, ErrPermit
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	before, e := f.Stat()
	if e != nil || !before.Mode().IsRegular() || before.Mode().Perm()&0022 != 0 || before.Size() < 1 || before.Size() > 65536 {
		return nil, ErrPermit
	}
	u, ok := before.Sys().(*syscall.Stat_t)
	if !ok || u.Uid != 0 || u.Nlink != 1 {
		return nil, ErrPermit
	}
	raw, e := io.ReadAll(io.LimitReader(f, 65537))
	after, e2 := f.Stat()
	path, e3 := os.Lstat(name)
	same := func(a, b os.FileInfo) bool {
		if a == nil || b == nil {
			return false
		}
		x, ok := a.Sys().(*syscall.Stat_t)
		y, ok2 := b.Sys().(*syscall.Stat_t)
		return ok && ok2 && os.SameFile(a, b) && x.Uid == y.Uid && x.Gid == y.Gid && x.Nlink == y.Nlink && x.Mode == y.Mode && x.Size == y.Size && x.Mtim == y.Mtim && x.Ctim == y.Ctim
	}
	if e != nil || e2 != nil || e3 != nil || !same(before, after) || !same(before, path) || before.Size() != int64(len(raw)) {
		return nil, ErrPermit
	}
	return raw, nil
}

// VerifyDeployment preserves a cryptographically valid expired deployment
// session across a manifest upgrade. It is only a bootstrap compatibility
// check: an old manifest never receives new dispatch authority. Verify remains
// strict and must still guard every matching operation.
func VerifyDeployment(profileRaw, policyRaw, envelopeRaw []byte, expectedProfileHash, manifest string, now time.Time) (*VerifiedPermit, error) {
	verified, err := Verify(profileRaw, policyRaw, envelopeRaw, expectedProfileHash, manifest, now)
	if !errors.Is(err, ErrPermit) {
		return verified, err
	}
	var envelope Envelope
	var permit Permit
	if !hash(manifest) || decode(envelopeRaw, &envelope) != nil || decode(envelope.Payload, &permit) != nil ||
		permit.ManifestChecksum == manifest || now.Unix() < permit.ExpiresAt {
		return nil, ErrPermit
	}
	// Treat the payload as untrusted until both existing role signatures and
	// every original scope/limit/trust binding pass under its signed manifest.
	historical, historicalErr := Verify(profileRaw, policyRaw, envelopeRaw, expectedProfileHash, permit.ManifestChecksum, now)
	if historical == nil || !errors.Is(historicalErr, ErrExpired) {
		return nil, ErrPermit
	}
	return historical, ErrExpired
}
