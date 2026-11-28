package kernel

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/sync/singleflight"
)

type internalWaitReadKey struct{}
type machineWaitReadKey struct{}

type waitAdmission interface {
	ChargeWait(context.Context, orchestration.Identity, string, int) error
	ClaimWaitRead(context.Context, orchestration.Identity, string, time.Duration, time.Time) (bool, error)
}

type waitService struct {
	maxSeconds, callsPerMinute int
	charge                     func(context.Context, orchestration.Identity) error
	claimRead                  func(context.Context, orchestration.Identity, string, time.Duration, time.Time) (bool, error)
	signer                     *nextstep.Signer
	interval, duration         time.Duration
	principalLimit             int
	totalLimit                 int
	reads                      readGate
	group                      singleflight.Group
	mu                         sync.Mutex
	active                     map[string]int
	total                      int
}

// Consume a slot at dispatch time. A delayed process cannot spend a backlog
// of reservations in one burst when its goroutines wake up together.
type readGate struct {
	mu     sync.Mutex
	next   time.Time
	gap    time.Duration
	recent map[string]time.Time
}

func (g *readGate) Wait(ctx context.Context) error {
	return g.waitFor(ctx, "", 0)
}

func (g *readGate) waitFor(ctx context.Context, scope string, interval time.Duration) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		g.mu.Lock()
		now := time.Now()
		delay := max(g.next.Sub(now), g.recent[scope].Sub(now))
		if delay <= 0 {
			g.next = now.Add(g.gap)
			if scope != "" {
				if g.recent == nil {
					g.recent = make(map[string]time.Time)
				}
				for key, end := range g.recent {
					if !now.Before(end) {
						delete(g.recent, key)
					}
				}
				g.recent[scope] = now.Add(interval)
			}
			g.mu.Unlock()
			return nil
		}
		g.mu.Unlock()
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func setting(name string, def, low, high int) (int, error) {
	v, set := os.LookupEnv(name)
	if !set {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < low || n > high {
		return 0, fmt.Errorf("%s must be in [%d,%d]", name, low, high)
	}
	return n, nil
}

func configuredWait(installationKey []byte) (*waitService, error) {
	interval, e1 := setting("OUF_WAIT_INTERVAL_SECONDS", orchestration.PollIntervalSeconds, orchestration.PollIntervalSeconds, 45)
	duration, e2 := setting("OUF_WAIT_DURATION_SECONDS", 45, 5, 45)
	principal, e3 := setting("OUF_WAIT_PER_PRINCIPAL", 2, 1, 100)
	total, e4 := setting("OUF_WAIT_MAX_ACTIVE", 100, 1, 100)
	rps, e5 := setting("OUF_WAIT_OWNER_READS_PER_SECOND", 20, 1, 20)
	maximum, e6 := setting("OUF_WAIT_MAX_SECONDS", nextstep.DefaultWaitMaxSeconds, 1, nextstep.DefaultWaitMaxSeconds)
	calls, e7 := setting("OUF_ADMISSION_CALLS_PER_MINUTE", orchestration.DefaultAdmissionCallsPerMinute, 1, 2147483647)
	if err := errors.Join(e1, e2, e3, e4, e5, e6, e7); err != nil {
		return nil, err
	}
	if duration <= interval {
		return nil, errors.New("OUF wait duration must exceed the read interval")
	}
	w := &waitService{maxSeconds: maximum, callsPerMinute: calls, interval: time.Duration(interval) * time.Second, duration: time.Duration(duration) * time.Second,
		principalLimit: principal, totalLimit: total, active: map[string]int{},
		reads: readGate{gap: (time.Second + time.Duration(rps) - 1) / time.Duration(rps)}}
	if key, set := os.LookupEnv("OUF_WAIT_KEY_BASE64"); set {
		decoded, err := base64.StdEncoding.Strict().DecodeString(key)
		if err != nil {
			return nil, errors.New("invalid OUF_WAIT_KEY_BASE64")
		}
		w.signer, err = nextstep.NewSigner(decoded)
		if err != nil {
			return nil, err
		}
	} else if len(installationKey) >= 32 {
		// Domain separation keeps wait tokens independent of request fingerprints.
		mac := hmac.New(sha256.New, installationKey)
		mac.Write([]byte("ouf.next-step.v1/waitref/aes-256-gcm"))
		var err error
		w.signer, err = nextstep.NewSigner(mac.Sum(nil))
		if err != nil {
			return nil, err
		}
	}
	return w, nil
}

func (w *waitService) admit(scope string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.total >= w.totalLimit || w.active[scope] >= w.principalLimit {
		return false
	}
	w.active[scope]++
	w.total++
	return true
}

func (w *waitService) release(scope string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.total--
	w.active[scope]--
	if w.active[scope] == 0 {
		delete(w.active, scope)
	}
}

func waitResult(changed bool, observation, next nextstep.Object) *mcp.CallToolResult {
	out := nextstep.Object{"changed": changed, "observation": observation, "next": next}
	// The structured result is authoritative. Text-only clients can also
	// consume the same envelope, subject to the serialized result budget.
	b, _ := json.Marshal(out)
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(b)}}, StructuredContent: out}
}

func waitError(result *mcp.CallToolResult, next nextstep.Object) *mcp.CallToolResult {
	body := ownerObject(result)
	if body == nil {
		body = nextstep.Object{}
	}
	body["changed"], body["observation"], body["next"] = false, nil, next
	wire, _ := json.Marshal(body)
	return &mcp.CallToolResult{IsError: true, StructuredContent: body, Content: []mcp.Content{&mcp.TextContent{Text: string(wire)}}}
}

func (s *workflowServer) registerWait(c manifest.Capability) {
	var schema jsonschema.Schema
	if err := json.Unmarshal(c.InputSchema, &schema); err != nil {
		panic(err)
	}
	s.register(&mcp.Tool{Name: c.ToolName, Description: c.Description(), InputSchema: &schema, Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true}},
		func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			var input struct{ WaitRef string }
			if json.Unmarshal(req.Params.Arguments, &input) != nil {
				return errorResult("INVALID_TOOL_ARGUMENTS", false), nil
			}
			return s.wait(ctx, req, input.WaitRef), nil
		})
}

func (s *workflowServer) wait(ctx context.Context, req *mcp.CallToolRequest, token string) (response *mcp.CallToolResult) {
	w := s.waits
	binding := waitBinding(ctx)
	ref, err := w.signer.Open(token, binding)
	var input struct{ ResumeArguments nextstep.Object }
	decoder := json.NewDecoder(bytes.NewReader(req.Params.Arguments))
	decoder.UseNumber()
	if err != nil || decoder.Decode(&input) != nil || !ref.RestoreArgs(input.ResumeArguments) {
		return errorResult("WAIT_REFERENCE_INVALID", false)
	}
	t, ok := s.tools[ref.Read]
	machine := len(ref.Terminal) != 0
	if !ok || (!machine && t.capability.OperationClass != "READ") || ref.Read == "ouf.wait" || !s.valid(ref.Read, ref.Args) {
		return errorResult("WAIT_REFERENCE_INVALID", false)
	}
	batchStatus := machine && ref.Reason == nextstep.AnalysisRunning && ref.Read == "source.analysis.read" && ref.Args["view"] == "STATUS"
	batch := batchStatus || s.batchReadSelector(ctx, ref.Read, ref.Args)
	defer func() {
		if batch {
			markBatchResult(response)
			id, _ := ref.Args["analysisId"].(string)
			if body, ok := response.StructuredContent.(nextstep.Object); ok && analysis.ValidateID(id) == nil {
				body["analysisId"] = id
			}
		}
	}()
	stop := func(code string) *mcp.CallToolResult {
		next := ref.Block(code, "Consulta lo stato aggiornato della decisione.")
		if machine {
			next = nextstep.ManualBlock(code, "Verifica la disponibilità dell’attesa prima di riprendere il lavoro.", "OPERATOR")
		}
		result := waitResult(false, nil, next)
		result.IsError = true
		return result
	}
	due, _ := time.Parse(time.RFC3339Nano, ref.NotBefore)
	exp, err := time.Parse(time.RFC3339Nano, ref.Deadline)
	if machine && ref.Deadline == "" && !due.IsZero() {
		// A reference without an owner cutoff is usable for one finite window after it is due.
		exp, err = due.Add(time.Duration(w.maxSeconds)*time.Second), nil
	}
	if err != nil || !time.Now().Before(exp) {
		return stop("WAIT_DEADLINE_EXPIRED")
	}
	linkEnd, _ := time.Parse(time.RFC3339Nano, ref.LinkEnd)
	// Refresh from a complete authorized observation before exposing a new
	// link. Earlier semantic references could carry a URL supplied by Semantic.
	refreshLink := func() bool {
		return !machine && ref.Read == "semantic.proposal.read" && ref.LinkOwner != "onboarding" ||
			!linkEnd.IsZero() && !time.Now().Before(linkEnd)
	}
	scope := string(binding)
	identity := ctx.Value(identityKey{}).(requestIdentity)
	if !w.admit(scope) {
		return stop("WAIT_CAPACITY_REACHED")
	}
	defer w.release(scope)
	duration := w.duration
	if extend, ok := ctx.Value(responseDeadlineKey{}).(func(time.Time) error); ok && duration > 10*time.Second {
		// The production HTTP server has a 15s write deadline. Some metrics
		// writers do not expose ResponseController; use shorter wait slices.
		if err := extend(time.Now().Add(duration + 2*time.Second)); err != nil {
			duration = 10 * time.Second
		}
	}
	if !machine && w.interval >= duration {
		return stop("WAIT_TRANSPORT_WINDOW_TOO_SHORT")
	}
	until := time.Now().Add(duration)
	if exp.Before(until) {
		until = exp
	}
	ctx, cancel := context.WithDeadline(ctx, until)
	defer cancel()

	windowRefusal := func(result *mcp.CallToolResult) *mcp.CallToolResult {
		body := ownerObject(result)
		end, err := time.Parse(time.RFC3339Nano, fmt.Sprint(body["windowEndsAt"]))
		if err != nil || body["retryable"] != true {
			return stop("WAIT_ADMISSION_UNAVAILABLE")
		}
		end, valid := orchestration.OwnerDue(body, time.Now(), end, "")
		if !valid {
			return stop("RETRY_WINDOW_UNAVAILABLE")
		}
		// A refused wait cannot use another owner's quota. Pace even this response,
		// under the same local concurrency and transport limits as an admitted wait.
		timer := time.NewTimer(time.Until(end))
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			if ctx.Err() == context.Canceled {
				return stop("WAIT_OBSERVATION_UNAVAILABLE")
			}
		}
		if !time.Now().Before(exp) {
			return stop("WAIT_DEADLINE_EXPIRED")
		}
		next := w.signer.Postpone(binding, ref, end, exp)
		return waitError(result, next)
	}
	if w.charge != nil {
		if err := w.charge(ctx, identity.Identity); err != nil {
			var refusal *orchestration.WindowBudgetExhaustedError
			if !errors.As(err, &refusal) {
				return stop("WAIT_ADMISSION_UNAVAILABLE")
			}
			return windowRefusal(governedCallErrorResult(err))
		}
	}
	args := ref.ReadArgs()
	if refreshLink() {
		args = ref.Args
	}
	if !s.valid(ref.Read, args) {
		return stop("WAIT_READ_SCHEMA_CHANGED")
	}
	raw, _ := json.Marshal(args)
	read := *req
	params := *req.Params
	params.Name, params.Arguments = ref.Read, raw
	read.Params = &params

	i := identity
	// A new authorized observation needs a fresh read attempt, including
	// when the outer request has its own idempotency key.
	i.IdempotencyKey = ""
	ctx = context.WithValue(ctx, identityKey{}, i)
	// Machine observations traverse the normal target admission and owner budgets.
	ctx = context.WithValue(ctx, internalWaitReadKey{}, !machine)
	ctx = context.WithValue(ctx, machineWaitReadKey{}, machine)
	authority, _ := json.Marshal([]any{scope, i.Delegation, i.AuthenticationContextRef, i.Claims, i.Scopes, token, ref.Read, args})
	key := fmt.Sprintf("%x", sha256.Sum256(authority))

	// Bind the cadence to the principal and read selector, including overlapping tranches.
	selector, _ := json.Marshal([]any{scope, ref.Read, ref.Args})
	readScope := fmt.Sprintf("%x", sha256.Sum256(selector))
	tokenHash := fmt.Sprintf("%x", sha256.Sum256([]byte(token)))
	var last nextstep.Object
	finish := func() *mcp.CallToolResult {
		if !time.Now().Before(exp) {
			return stop("WAIT_DEADLINE_EXPIRED")
		}
		if ctx.Err() == context.Canceled || time.Now().Before(until) {
			return stop("WAIT_OBSERVATION_UNAVAILABLE")
		}
		if refreshLink() && !time.Now().Before(due) {
			return waitResult(false, last, ref.Refresh(s.engine(ctx), nextstep.Object{"notBefore": due.UTC().Format(time.RFC3339Nano)}))
		}
		if batchStatus {
			return waitResult(false, last, w.signer.Postpone(binding, ref, time.Now().Add(w.interval), exp))
		}
		return waitResult(false, last, ref.Next(token))
	}
	delay := w.interval
	if machine {
		delay = 0
	}
	timer := time.NewTimer(max(delay, time.Until(due)))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		timer.Reset(w.interval)
		if ctx.Err() != nil || !time.Now().Before(until) {
			return finish()
		}
		if s.analysisService != nil && (t.capability.OperationClass == "READ" || t.capability.OperationClass == "SEARCH") {
			// Another observation can extend the clock after this token was signed.
			saved, err := s.analysisService.SavedObservation(ctx, identity.Identity, t.capability.CapabilityID, t.capability.OperationClass, raw)
			if err != nil {
				return stop("OWNER_CLOCK_UNAVAILABLE")
			}
			if saved.Due.After(time.Now()) {
				return waitResult(false, last, w.signer.Postpone(binding, ref, saved.Due, exp))
			}
		}
		if machine && t.capability.OperationClass == "COMMAND" {
			// Resume through the public entry point with the original arguments and admission gates.
			return waitResult(false, nil, ref.ResumeCommand(s.engine(ctx)))
		}
		ch := w.group.DoChan(key, func() (any, error) {
			if err := w.reads.waitFor(ctx, readScope, w.interval); err != nil {
				return nil, err
			}
			if w.claimRead != nil {
				claimed, err := w.claimRead(ctx, identity.Identity, tokenHash, w.interval, exp)
				if err != nil || !claimed {
					return nil, err
				}
				// Start the next interval after the DB claim. Network jitter must not
				// make the next tick arrive just early and skip a whole interval.
				w.reads.mu.Lock()
				w.reads.recent[readScope] = time.Now().Add(w.interval)
				w.reads.mu.Unlock()
			}
			return t.run(ctx, &read)
		})
		select {
		case <-ctx.Done():
			return finish()
		case response := <-ch:
			if !time.Now().Before(exp) {
				return stop("WAIT_DEADLINE_EXPIRED")
			}
			if response.Val == nil && response.Err == nil {
				// Another replica already owns this tick of the signed wait.
				continue
			}
			result, ok := response.Val.(*mcp.CallToolResult)
			if response.Err != nil || !ok || result == nil {
				return finish()
			}
			batch = batch || result.Meta[batchResultMeta] == true
			body := ownerObject(result)
			obs := nextstep.Observation(ref.Read, ref.Args, body)
			if batchStatus {
				projected, _ := result.Meta[batchNextMeta].(nextstep.Object)
				if result.IsError {
					return waitError(result, s.engine(ctx).BatchNext(ref.Read, args, body, true))
				}
				if body["mode"] != analysis.BatchMode || body["analysisId"] != ref.Args["analysisId"] || projected == nil {
					return stop("WAIT_RESOURCE_CHANGED")
				}
				last = nextstep.Object{"analysisId": body["analysisId"], "phase": body["phase"], "revision": body["revision"], "counts": body["counts"]}
				if projected["action"] == "POLL" && projected["reasonCode"] == nextstep.AnalysisRunning {
					continue
				}
				return waitResult(true, last, projected)
			}
			if result.IsError {
				if code, ok := body["code"].(string); ok && nextstep.AdmissionWindowCode(code) {
					return windowRefusal(result)
				}
				if machine && summaryInput(ref.Read, ref.Args) && body["code"] == analysis.ErrExpired.Error() {
					// The typed continuation retains the owner-clock check for validation.
					structured, _ := result.StructuredContent.(nextstep.Object)
					if projected, ok := structured["next"].(nextstep.Object); ok {
						return waitError(result, projected)
					}
				}
				next := s.evaluateNext(s.observedEngine(ctx, result), t.capability, args, body, true, batch)
				return waitError(result, next)
			}
			if machine && body["next"] != nil && (summaryInput(ref.Read, ref.Args) || batch) {
				structured, _ := result.StructuredContent.(nextstep.Object)
				if projected, ok := structured["next"].(nextstep.Object); ok {
					return waitResult(true, obs, projected)
				}
			}
			if !ref.MatchesResource(obs) {
				return stop("WAIT_RESOURCE_CHANGED")
			}
			if ref.Decision == "CHOOSE_FILE" && obs["legacy"] != true && obs["waitUntil"] != nil {
				ownerEnd, err := time.Parse(time.RFC3339Nano, fmt.Sprint(obs["waitUntil"]))
				if err != nil || !ownerEnd.Equal(exp) {
					return stop("WAIT_DEADLINE_CHANGED")
				}
			}
			last = obs
			state := nextstep.ObservedState(obs)
			stateChanged := obs["unchanged"] != true && obs["changed"] != false && ref.State != "" && state != "" && ref.State != state
			invalidHandoff := ref.Decision == "CHOOSE_FILE" && (obs["legacy"] == true || obs["waitUntil"] == nil)
			version := nextstep.Version(obs)
			if machine && version == "" {
				version = nextstep.PollVersion(obs)
			}
			changed := version != ref.Version || stateChanged || invalidHandoff
			if !machine && !changed {
				at, valid := orchestration.OwnerDue(obs, time.Now(), time.Time{}, "")
				if !valid {
					return stop("RETRY_WINDOW_UNAVAILABLE")
				}
				if at.After(time.Now()) {
					return waitResult(false, obs, w.signer.Postpone(binding, ref, at, exp))
				}
			}
			if refreshLink() && (obs["unchanged"] == true || obs["changed"] == false) && len(args) != len(ref.Args) {
				return waitResult(changed, obs, ref.Refresh(s.engine(ctx), obs))
			}
			if machine || changed || refreshLink() {
				if ref.Decision == "CHOOSE_FILE" && state == "PENDING" && obs["legacy"] != true {
					// The read contract omits pickerUrl. Reuse only the URL authenticated for this handoff.
					body = maps.Clone(body)
					body["pickerUrl"] = ref.URL
				}
				next := s.evaluateNext(s.observedEngine(ctx, result), t.capability, args, body, false, batch)
				if !batch {
					next = s.publishedNext(ctx, ref.Read, args, body, next)
				}
				return waitResult(changed, obs, next)
			}
		}
	}
}
