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
	Read      string `json:"r"`
	Args      Object `json:"a"`
	State     string `json:"s,omitempty"`
	Version   string `json:"v"`
	Resource  string `json:"i,omitempty"`
	Deadline  string `json:"d"`
	URL       string `json:"u"`
	Decision  string `json:"k"`
	Reason    string `json:"c"`
	LinkEnd   string `json:"e,omitempty"`
	LinkOwner string `json:"o,omitempty"`
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
	obs = recoveryObservation(obs)
	version := get(obs, nil, "stateVersion|lockVersion|rowVersion|expectedRowVersion|expectedVersion|control_version|revision")
	if version == nil {
		return ""
	}
	// Activation phases and human eligibility can change at the same lock version.
	fields := []any{version}
	for _, field := range []string{"state", "status", "challengeId", "challengeStatus", "lifecycleStatus", "runState", "run_state", "decisionDeadline", "recoveryAction", "error_code", "allowedActions", "activationProgress.readiness_phase", "activationProgress.readiness_generation", "activationProgress.humanRecheckAllowed"} {
		fields = append(fields, lookup(obs, field))
	}
	raw, _ := json.Marshal(fields)
	sum := sha256.Sum256(raw)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func ObservedState(obs Object) string {
	state, _ := get(obs, nil, "state|workflowState|runState|run_state|status").(string)
	return state
}

func Resource(obs Object) string {
	s, _ := get(obs, nil, "runId|job_ref").(string)
	return s
}

// Exact selectors remain in the signed read arguments. A conditional receipt
// may omit them; an owner returning a conflicting selector cannot advance it.
func (r WaitRef) MatchesResource(obs Object) bool {
	if r.Read == "source.analysis.read" && (obs == nil || obs["analysisId"] != r.Args["analysisId"]) {
		return false
	}
	if obs == nil {
		return false
	}
	for _, field := range []string{"handoffId", "sourceId", "onboardingVersionId", "commandKey", "proposalId"} {
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
	if reason == "WAIT_DEADLINE_EXPIRED" && r.Read == "source.file.upload.status" {
		s["userMessage"] = "L’attesa è scaduta. Rileggi l’esito registrato oppure, su richiesta dell’utente, apri un nuovo selettore."
		s["alternatives"] = append(s["alternatives"].([]Object), Object{"action": "CALL_TOOL", "tool": "source.file.upload", "arguments": Object{}, "reasonCode": reason, "userMessage": "Apri un nuovo selettore su richiesta dell’utente."})
	}
	return s
}

type LinkStatus uint8

const (
	LinkPermanent LinkStatus = iota // Missing configuration or invalid owner response.
	LinkAvailable
	LinkDenied
	LinkTransient
)

type LinkResult struct {
	Status     LinkStatus
	Capability string
	Reason     string
	RetryAfter string
	URL        string
	ExpiresAt  string
}

func validReviewURL(link string) bool {
	u, err := url.Parse(link)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil
}

func OwnerLink(link, end string, now time.Time) LinkResult {
	exp, err := time.Parse(time.RFC3339Nano, end)
	if !validReviewURL(link) || err != nil || !now.Before(exp) {
		return LinkResult{}
	}
	return LinkResult{Status: LinkAvailable, URL: link, ExpiresAt: end}
}

func (s *Signer) Issue(binding []byte, r manifest.Rule, args, obs Object, onboardingLink LinkResult, now time.Time) Object {
	run := r.Tool == "ouf.ingestion.status"
	semantic := r.Tool == "semantic.proposal.read"
	if (run || semantic) && onboardingLink.Status == LinkDenied {
		return PermissionDenied(onboardingLink.Capability, onboardingLink.Reason)
	}
	exp := get(obs, nil, "currentChallenge.expiresAt|expiresAt|expires_at|deadline")
	if r.Decision == "CHOOSE_FILE" {
		exp = obs["waitUntil"]
	}
	if semantic && exp == nil && onboardingLink.ExpiresAt != "" {
		exp = onboardingLink.ExpiresAt
	}
	if run {
		exp = obs["decisionDeadline"]
	}
	if r.Decision == "RECHECK" && lookup(obs, "activationProgress.budgetExpiresAt") != nil {
		exp = lookup(obs, "activationProgress.budgetExpiresAt")
	}
	deadline, err := time.Parse(time.RFC3339Nano, fmt.Sprint(exp))
	// Owners can publish a monitoring cutoff earlier than the decision itself.
	if cutoff, ok := obs["waitUntil"].(string); ok && r.Decision != "CHOOSE_FILE" {
		end, parseErr := time.Parse(time.RFC3339Nano, cutoff)
		if parseErr != nil {
			return ManualBlock("WAIT_OBSERVATION_REQUIRED", "Verifica la scadenza comunicata dall’owner.", "OPERATOR")
		}
		if err != nil || end.Before(deadline) {
			deadline, err = end, nil
		}
	}
	if err == nil && !now.Before(deadline) {
		closed := (WaitRef{Read: r.Tool, Args: args}).Block("WAIT_DEADLINE_EXPIRED", "La finestra è scaduta. Verifica lo stato corrente.")
		if r.Actor != "" {
			closed["actor"] = r.Actor
		}
		return closed
	}
	var link, linkEnd string
	if run || semantic {
		switch onboardingLink.Status {
		case LinkTransient:
			delay, ok := retryDelay(Object{"RetryAfter": onboardingLink.RetryAfter}, now)
			if !ok {
				return ManualBlock("RETRY_WINDOW_UNAVAILABLE", "Chiedi all’operatore di verificare la finestra di ripresa.", "OPERATOR")
			}
			next := call("POLL", "OWNER_REVIEW_LINK_UNAVAILABLE", r.Tool, args)
			next["userMessage"] = "Il collegamento di revisione è temporaneamente indisponibile. Attendi prima di rileggere lo stato."
			next["pollAfterSeconds"], next["terminalStates"] = delay, []string{"WAIT_FOR_HUMAN", "BLOCKED", "DONE", "CALL_TOOL"}
			if exp != nil {
				if err != nil || !now.Add(time.Duration(delay)*time.Second).Before(deadline) {
					return ManualBlock("RETRY_WINDOW_UNAVAILABLE", "Verifica la decisione nella THS. La finestra disponibile non consente un’altra lettura.", "HUMAN_THS")
				}
				next["deadline"] = deadline.UTC().Format(time.RFC3339Nano)
			}
			return next
		case LinkAvailable:
			link, linkEnd = onboardingLink.URL, onboardingLink.ExpiresAt
		}
	} else {
		link, _ = get(obs, nil, "currentChallenge.reviewUrl|reviewUrl|approvalPath").(string)
		if r.Decision == "CHOOSE_FILE" {
			link, _ = obs["pickerUrl"].(string)
		}
	}
	linkExpiry, parseErr := time.Parse(time.RFC3339Nano, linkEnd)
	if !validReviewURL(link) || ((run || semantic) && (parseErr != nil || !now.Before(linkExpiry))) {
		return ManualBlock("OWNER_REVIEW_LINK_REQUIRED", "Apri la THS e verifica la decisione con il responsabile della revisione.", "HUMAN_THS")
	}
	manual := func(code, message string) Object {
		blocked := Block(code, r.Tool, message)
		blocked["actor"] = "HUMAN_THS"
		alts := blocked["alternatives"].([]Object)
		alts[0]["arguments"] = args
		human := Object{"reviewUrl": link, "decisionKind": r.Decision}
		if linkEnd != "" {
			human["expiresAt"] = linkEnd
		}
		instruction := "Apri la pagina indicata per esaminare la decisione."
		if r.Action == "BLOCKED" {
			instruction = r.Message
		}
		blocked["alternatives"] = append([]Object{{
			"action": "WAIT_FOR_HUMAN", "reasonCode": r.Reason, "userMessage": instruction,
			"human": human,
		}}, alts...)
		return blocked
	}
	version := Version(obs)
	if r.Action == "BLOCKED" {
		return manual(r.Reason, r.Message)
	}
	if err != nil || version == "" || (r.Tool == "ouf.ingestion.status" && Resource(obs) == "") {
		return manual("WAIT_OBSERVATION_REQUIRED", "Verifica versione, risorsa e scadenza della decisione.")
	}
	if s == nil || len(binding) == 0 {
		return manual("WAIT_CONFIGURATION_REQUIRED", "Verifica la configurazione dell’attesa e l’identità del chiamante.")
	}
	ref := WaitRef{
		Read: r.Tool, Args: args, Version: version, Resource: Resource(obs),
		Deadline: deadline.UTC().Format(time.RFC3339Nano), URL: link,
		Decision: r.Decision, Reason: r.Reason, LinkEnd: linkEnd,
	}
	ref.State = ObservedState(obs)
	if semantic {
		ref.LinkOwner = "onboarding"
	}
	if original, ok := exp.(string); ok {
		if parsed, err := time.Parse(time.RFC3339Nano, original); err == nil && deadline.Equal(parsed) {
			ref.Deadline = original
		}
	}
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
	if r.LinkEnd != "" {
		s["human"].(Object)["expiresAt"] = r.LinkEnd
	}
	s["userMessage"] = "Apri il collegamento di revisione. La chat attenderà la decisione."
	if r.Decision == "CHOOSE_FILE" {
		s["userMessage"] = "Apri il selettore OUF e scegli il file. La chat attende l’esito."
	}
	if r.Reason == "HUMAN_ACTIVATION_REQUIRED" {
		s["userMessage"] = "Apri la revisione della fonte nella THS e conferma l’attivazione. La chat attenderà il nuovo stato."
	}
	return s
}

func (r WaitRef) Refresh() Object {
	s := call("CALL_TOOL", "REFRESH_REVIEW_OBSERVATION", r.Read, r.Args)
	s["userMessage"] = "Rileggi la decisione per aggiornare il collegamento di revisione."
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
