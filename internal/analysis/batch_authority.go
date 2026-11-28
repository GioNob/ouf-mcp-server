package analysis

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

// BatchAuthorities keeps the proof supplied by the trusted Gateway in memory
// for its signed lifetime. A fresh authorized plan/read can supply proof on any
// replica. A digest of the identity context is stored on the analysis instead.
type BatchAuthorities struct {
	mu     sync.Mutex
	proofs map[string]batchAuthority
}

type batchAuthority struct {
	identity orchestration.Identity
	expires  time.Time
	lastUsed time.Time
}

func BatchAuthorityBinding(i orchestration.Identity) string {
	return visibilityDigest(i, orchestration.AuthorizationDecision{})
}

func (a *BatchAuthorities) Observe(id string, i orchestration.Identity) {
	payload, mac, ok := strings.Cut(i.Delegation, ".")
	if !ok || mac == "" || i.ActorType != "HUMAN" || !IdentityScope(i).Valid() || ValidateID(id) != nil {
		return
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	var claims struct {
		Version int    `json:"v"`
		Expiry  int64  `json:"exp"`
		Purpose string `json:"purpose"`
	}
	if err != nil || json.Unmarshal(raw, &claims) != nil || claims.Version != 1 || claims.Purpose != "mcp-execute" {
		return
	}
	// The Gateway verifies the MAC on every owner invocation. This expiry
	// only limits local retention and does not confer dispatch authority.
	now, expires := time.Now(), time.Unix(claims.Expiry, 0)
	if !expires.After(now) || expires.After(now.Add(time.Minute)) {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.proofs == nil {
		a.proofs = make(map[string]batchAuthority)
	}
	a.prune(now)
	prior, present := a.proofs[id]
	if (!present && len(a.proofs) >= 4096) || !expires.After(prior.expires) {
		return
	}
	a.proofs[id] = batchAuthority{identity: i, expires: expires, lastUsed: prior.lastUsed}
}

func (a *BatchAuthorities) prune(now time.Time) {
	for id, proof := range a.proofs {
		if !proof.expires.After(now) {
			delete(a.proofs, id)
		}
	}
}

func (a *BatchAuthorities) next(now time.Time) (string, orchestration.Identity, time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.prune(now)
	var selected string
	var first batchAuthority
	for id, proof := range a.proofs {
		if selected == "" || proof.lastUsed.Before(first.lastUsed) || (proof.lastUsed.Equal(first.lastUsed) && id < selected) {
			selected, first = id, proof
		}
	}
	if selected != "" {
		first.lastUsed = now
		a.proofs[selected] = first
	}
	return selected, first.identity, first.expires
}
