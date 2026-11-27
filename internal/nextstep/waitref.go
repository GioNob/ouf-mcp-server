package nextstep

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type WaitRef struct {
	NotBefore string   `json:"n,omitempty"`
	Terminal  []string `json:"t,omitempty"`
	ArgsHash  string   `json:"h,omitempty"`
	Read      string   `json:"r"`
	Args      Object   `json:"a"`
	State     string   `json:"s,omitempty"`
	Version   string   `json:"v"`
	Resource  string   `json:"i,omitempty"`
	Deadline  string   `json:"d"`
	URL       string   `json:"u"`
	Decision  string   `json:"k"`
	Reason    string   `json:"c"`
	LinkEnd   string   `json:"e,omitempty"`
	LinkOwner string   `json:"o,omitempty"`
}

type Signer struct {
	cipher            cipher.AEAD
	batchPollNonceKey []byte
}

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
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("ouf.next-step.v1/batch-poll/nonce"))
	return &Signer{cipher: aead, batchPollNonceKey: mac.Sum(nil)}, nil
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
	if obs == nil {
		return false
	}
	for _, field := range []string{"requestId", "jobId", "analysisId", "handoffId", "sourceId", "onboardingVersionId", "commandKey", "proposalId"} {
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
			due, ok := orchestration.OwnerDue(obs, now, now.Add(time.Duration(orchestration.PollIntervalSeconds)*time.Second), onboardingLink.RetryAfter)
			if !ok {
				return ManualBlock("RETRY_WINDOW_UNAVAILABLE", "Chiedi all’operatore di verificare la finestra di ripresa.", "OPERATOR")
			}
			next := call("POLL", "OWNER_REVIEW_LINK_UNAVAILABLE", r.Tool, args)
			next["terminalStates"] = []string{"WAIT_FOR_HUMAN", "BLOCKED", "DONE", "CALL_TOOL"}
			if exp != nil {
				if err != nil || !due.Before(deadline) {
					return ManualBlock("RETRY_WINDOW_UNAVAILABLE", "Verifica la decisione nella THS. La finestra disponibile non consente un’altra lettura.", "HUMAN_THS")
				}
				next["deadline"] = deadline.UTC().Format(time.RFC3339Nano)
			}
			return s.issuePoll(binding, next, obs, now, due)
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
	if at, valid := orchestration.OwnerDue(obs, now, time.Time{}, ""); !valid {
		return manual("RETRY_WINDOW_UNAVAILABLE", "Verifica la finestra di ripresa.")
	} else if at.After(now) {
		ref.NotBefore = at.UTC().Format(time.RFC3339Nano)
	}
	token, err := s.seal(ref, binding)
	if err != nil {
		return manual("WAIT_REFERENCE_LIMIT", "Consulta lo stato della decisione.")
	}
	return ref.Next(token)
}

func (s *Signer) seal(ref WaitRef, binding []byte) (string, error) {
	return s.sealWait(ref, binding, false)
}

func (s *Signer) sealWait(ref WaitRef, binding []byte, stable bool) (string, error) {
	if s == nil || len(binding) == 0 {
		return "", errors.New("missing wait configuration")
	}
	body, err := json.Marshal(ref)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, s.cipher.NonceSize())
	if stable {
		// Stable batch polls repeat the ciphertext only for identical data and
		// identity binding. Derive the nonce using a separate key.
		mac := hmac.New(sha256.New, s.batchPollNonceKey)
		mac.Write(binary.BigEndian.AppendUint64(nil, uint64(len(binding))))
		mac.Write(binding)
		mac.Write(body)
		copy(nonce, mac.Sum(nil))
	} else if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(s.cipher.Seal(nonce, nonce, body, binding))
	if len(token) > 1024 {
		return "", errors.New("wait reference exceeds limit")
	}
	return token, nil
}

func argumentDigest(args Object) string {
	raw, _ := json.Marshal(args)
	sum := sha256.Sum256(raw)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// Machine observations may have no owner-supplied version. A digest of
// the bounded projection still identifies a changed result in that case.
func PollVersion(obs Object) string {
	if version := Version(obs); version != "" {
		return version
	}
	return argumentDigest(obs)
}

// Large inputs travel alongside the reference. Only the signed digest can
// restore them, including the original command key and exact JSON numbers.
func (r *WaitRef) RestoreArgs(args Object) bool {
	if r.ArgsHash == "" {
		return args == nil
	}
	if args == nil || argumentDigest(args) != r.ArgsHash {
		return false
	}
	r.Args = args
	return true
}

func (s *Signer) issuePoll(binding []byte, next, obs Object, now, due time.Time) Object {
	read := next["tool"].(string)
	if read == "ouf.wait" {
		return ManualBlock("WAIT_OBSERVATION_REQUIRED", "Recupera il riferimento all’osservazione originale.", "OPERATOR")
	}
	args := next["arguments"].(Object)
	reason := next["reasonCode"].(string)
	stable := reason == AnalysisRunning && read == "source.analysis.read" && args["view"] == "STATUS"
	deadline, _ := next["deadline"].(string)
	ref := WaitRef{Read: read, Args: args, Reason: reason, Terminal: next["terminalStates"].([]string),
		NotBefore: due.UTC().Format(time.RFC3339Nano), Deadline: deadline, State: ObservedState(obs), Version: PollVersion(obs)}
	if read == "ouf.ingestion.status" {
		ref.Resource = Resource(obs)
	}
	token, err := s.sealWait(ref, binding, stable)
	if err != nil && s != nil && len(binding) != 0 {
		ref.ArgsHash, ref.Args = argumentDigest(args), nil
		token, err = s.sealWait(ref, binding, stable)
		ref.Args = args
	}
	if err != nil {
		return ManualBlock("WAIT_CONFIGURATION_REQUIRED", "Verifica la configurazione dell’attesa e l’identità del chiamante.", "OPERATOR")
	}
	return ref.pollNext(token, now)
}

// Postpone the same observation after a governed admission refusal. Preserve
// both the original lower bound and the original expiration.
func (s *Signer) Postpone(binding []byte, ref WaitRef, due, expiry time.Time) Object {
	if previous, err := time.Parse(time.RFC3339Nano, ref.NotBefore); err == nil && previous.After(due) {
		due = previous
	}
	if ref.Reason == AnalysisRunning && !due.Before(expiry) {
		return ref.Block("WAIT_DEADLINE_EXPIRED", "Rileggi lo stato dell’analisi alla scadenza della finestra.")
	}
	ref.NotBefore = due.UTC().Format(time.RFC3339Nano)
	ref.Deadline = expiry.UTC().Format(time.RFC3339Nano)
	args := ref.Args
	if ref.ArgsHash != "" {
		ref.Args = nil
	}
	token, err := s.seal(ref, binding)
	if err != nil {
		return ManualBlock("WAIT_REFERENCE_LIMIT", "Verifica la finestra di ammissione.", "OPERATOR")
	}
	ref.Args = args
	return ref.Next(token)
}

func (r WaitRef) pollNext(token string, now time.Time) Object {
	args := Object{"waitRef": token}
	if r.ArgsHash != "" {
		args["resumeArguments"] = r.Args
	}
	next := call("POLL", r.Reason, "ouf.wait", args)
	at, _ := time.Parse(time.RFC3339Nano, r.NotBefore)
	remaining := min(max(at.Sub(now), 0), time.Hour)
	next["pollAfterSeconds"] = max(1, int((remaining+time.Second-1)/time.Second))
	next["terminalStates"] = r.Terminal
	next["userMessage"] = "Chiama subito ouf.wait. Il server attende il momento indicato per proseguire."
	if r.Deadline != "" {
		next["deadline"] = r.Deadline
	}
	return next
}

// Only current owner-issued validators have these shapes. Legacy observations
// use our base64 digest and therefore take the unconditional read path.
func (r WaitRef) ReadArgs() Object {
	args := maps.Clone(r.Args)
	if len(r.Terminal) != 0 {
		return args
	}
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
	if len(r.Terminal) != 0 {
		return r.pollNext(token, time.Now())
	}
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

func (r WaitRef) Refresh(e Engine, obs Object) Object {
	s := e.ownerCall("REFRESH_REVIEW_OBSERVATION", r.Read, r.Args, obs)
	if s["action"] == "CALL_TOOL" {
		s["userMessage"] = "Rileggi la decisione per aggiornare il collegamento di revisione."
	}
	return s
}

func (r WaitRef) ResumeCommand(e Engine) Object {
	next := e.ownerCall(r.Reason, r.Read, r.Args, Object{"notBefore": r.NotBefore})
	if next["action"] == "CALL_TOOL" {
		next["userMessage"] = "La finestra è aperta. Riprendi il passo indicato."
	}
	return next
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
	decoder := json.NewDecoder(bytes.NewReader(plain))
	decoder.UseNumber()
	if err != nil || decoder.Decode(&ref) != nil {
		return WaitRef{}, errors.New("invalid wait reference")
	}
	return ref, nil
}
