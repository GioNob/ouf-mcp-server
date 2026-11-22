package nextstep

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
)

type WaitRef struct {
	Read     string `json:"r"`
	Args     Object `json:"a"`
	Version  string `json:"v"`
	Resource string `json:"i,omitempty"`
	Deadline string `json:"d"`
	URL      string `json:"u"`
	Decision string `json:"k"`
	Reason   string `json:"c"`
}

type Signer struct{ cipher cipher.AEAD }

func NewSigner(key []byte) (*Signer, error) {
	if len(key) != 32 {
		return nil, errors.New("wait signing key must contain 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Signer{aead}, nil
}

func Version(obs Object) string {
	// Both owner validators cover the complete authorized decision projection.
	if v, ok := get(obs, nil, "observationVersion|version").(string); ok && v != "" {
		return v
	}
	version := get(obs, nil, "stateVersion|lockVersion|rowVersion|expectedRowVersion|expectedVersion|control_version|revision")
	if version == nil {
		return ""
	}
	// Activation phases and human eligibility can change at the same lock version.
	fields := []any{version}
	for _, field := range []string{"state", "status", "challengeId", "challengeStatus", "lifecycleStatus", "run_state", "recoveryAction", "error_code", "allowedActions", "activationProgress.readiness_phase", "activationProgress.readiness_generation", "activationProgress.humanRecheckAllowed"} {
		fields = append(fields, lookup(obs, field))
	}
	raw, _ := json.Marshal(fields)
	sum := sha256.Sum256(raw)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func Resource(obs Object) string {
	s, _ := get(obs, nil, "runId|job_ref").(string)
	return s
}

// Exact selectors remain in the signed read arguments. A conditional receipt
// may omit them; an owner returning a conflicting selector cannot advance it.
func (r WaitRef) MatchesResource(obs Object) bool {
	if obs == nil {
		return false
	}
	for _, field := range []string{"sourceId", "onboardingVersionId", "commandKey", "proposalId"} {
		if expected, bound := r.Args[field]; bound {
			if current, present := obs[field]; present {
				s, ok := current.(string)
				if !ok || expected != s {
					return false
				}
			}
		}
	}
	return r.Resource == "" || Resource(obs) == r.Resource
}

func (r WaitRef) Block(reason, message string) Object {
	s := Block(reason, r.Read, message)
	s["alternatives"].([]Object)[0]["arguments"] = r.Args
	return s
}

func (s *Signer) Issue(binding []byte, r manifest.Rule, args, obs Object, now time.Time) Object {
	link, _ := get(obs, nil, "currentChallenge.reviewUrl|reviewUrl|approvalPath").(string)
	u, err := url.Parse(link)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
		return Block("OWNER_REVIEW_LINK_REQUIRED", r.Tool, "Recupera il collegamento di revisione dalla capability di lettura.")
	}
	manual := func(code, message string) Object {
		blocked := Block(code, r.Tool, message)
		blocked["actor"] = "HUMAN_THS"
		alts := blocked["alternatives"].([]Object)
		blocked["alternatives"] = append([]Object{{
			"action": "WAIT_FOR_HUMAN", "reasonCode": r.Reason, "userMessage": "Apri la pagina indicata per esaminare la decisione.",
			"human": Object{"reviewUrl": link, "decisionKind": r.Decision},
		}}, alts...)
		return blocked
	}
	exp := get(obs, nil, "currentChallenge.expiresAt|expiresAt|expires_at|deadline")
	if r.Decision == "RECHECK" && lookup(obs, "activationProgress.budgetExpiresAt") != nil {
		exp = lookup(obs, "activationProgress.budgetExpiresAt")
	}
	deadline, err := time.Parse(time.RFC3339Nano, fmt.Sprint(exp))
	version := Version(obs)
	if err == nil && !now.Before(deadline) {
		return Block("WAIT_DEADLINE_EXPIRED", r.Tool, "La finestra è scaduta. Verifica lo stato corrente.")
	}
	if err != nil || version == "" || (r.Tool == "ouf.ingestion.status" && Resource(obs) == "") {
		return manual("WAIT_OBSERVATION_REQUIRED", "Verifica versione, risorsa e scadenza della decisione.")
	}
	if s == nil || len(binding) == 0 {
		return manual("WAIT_CONFIGURATION_REQUIRED", "Verifica la configurazione dell’attesa e l’identità del chiamante.")
	}
	ref := WaitRef{r.Tool, args, version, Resource(obs), deadline.UTC().Format(time.RFC3339Nano), link, r.Decision, r.Reason}
	body, err := json.Marshal(ref)
	if err != nil {
		return Block("WAIT_REFERENCE_INVALID", r.Tool, "Recupera una nuova osservazione.")
	}
	nonce := make([]byte, s.cipher.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return Block("WAIT_UNAVAILABLE", r.Tool, "Consulta lo stato della decisione.")
	}
	token := base64.RawURLEncoding.EncodeToString(s.cipher.Seal(nonce, nonce, body, binding))
	if len(token) > 1024 {
		return manual("WAIT_REFERENCE_LIMIT", "Consulta lo stato della decisione.")
	}
	return ref.Next(token)
}

// Only current owner-issued validators have these shapes. Legacy observations
// use our base64 digest and therefore take the unconditional read path.
func (r WaitRef) ReadArgs() Object {
	args := maps.Clone(r.Args)
	switch r.Read {
	case "source.onboarding.read":
		if len(r.Version) == 71 && strings.HasPrefix(r.Version, "sha256:") {
			args["ifObservationVersion"] = r.Version
		}
	case "semantic.proposal.read":
		if len(r.Version) == 64 {
			args["expectedVersion"] = r.Version
		}
	}
	return args
}

func (r WaitRef) Next(token string) Object {
	s := call("WAIT_FOR_HUMAN", r.Reason, "ouf.wait", Object{"waitRef": token})
	s["deadline"] = r.Deadline
	s["human"] = Object{"reviewUrl": r.URL, "decisionKind": r.Decision, "expiresAt": r.Deadline}
	s["userMessage"] = "Apri il collegamento di revisione. La chat attenderà la decisione."
	return s
}

func (s *Signer) Open(token string, binding []byte) (WaitRef, error) {
	var ref WaitRef
	if s == nil || len(binding) == 0 || len(token) < 32 || len(token) > 1024 {
		return ref, errors.New("invalid wait reference")
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(b) <= s.cipher.NonceSize() {
		return ref, errors.New("invalid wait reference")
	}
	n := s.cipher.NonceSize()
	plain, err := s.cipher.Open(nil, b[:n], b[n:], binding)
	if err != nil || json.Unmarshal(plain, &ref) != nil {
		return WaitRef{}, errors.New("invalid wait reference")
	}
	return ref, nil
}
