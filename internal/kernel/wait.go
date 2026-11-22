package kernel

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/sync/singleflight"
)

type waitService struct {
	signer             *nextstep.Signer
	interval, duration time.Duration
	principalLimit     int
	totalLimit         int
	reads              readGate
	group              singleflight.Group
	mu                 sync.Mutex
	active             map[string]int
	total              int
}

// Consume a slot at dispatch time. A delayed process cannot spend a backlog
// of reservations in one burst when its goroutines wake up together.
type readGate struct {
	mu   sync.Mutex
	next time.Time
	gap  time.Duration
}

func (g *readGate) Wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		g.mu.Lock()
		now := time.Now()
		delay := g.next.Sub(now)
		if delay <= 0 {
			g.next = now.Add(g.gap)
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
	interval, e1 := setting("OUF_WAIT_INTERVAL_SECONDS", 5, 5, 45)
	duration, e2 := setting("OUF_WAIT_DURATION_SECONDS", 45, 5, 45)
	principal, e3 := setting("OUF_WAIT_PER_PRINCIPAL", 2, 1, 100)
	total, e4 := setting("OUF_WAIT_MAX_ACTIVE", 100, 1, 100)
	rps, e5 := setting("OUF_WAIT_OWNER_READS_PER_SECOND", 20, 1, 20)
	if err := errors.Join(e1, e2, e3, e4, e5); err != nil {
		return nil, err
	}
	if duration <= interval {
		return nil, errors.New("OUF wait duration must exceed the read interval")
	}
	w := &waitService{interval: time.Duration(interval) * time.Second, duration: time.Duration(duration) * time.Second,
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

func (s *workflowServer) wait(ctx context.Context, req *mcp.CallToolRequest, token string) *mcp.CallToolResult {
	w := s.waits
	binding := waitBinding(ctx)
	ref, err := w.signer.Open(token, binding)
	if err != nil {
		return errorResult("WAIT_REFERENCE_INVALID", false)
	}
	t, ok := s.tools[ref.Read]
	if !ok || t.capability.OperationClass != "READ" || ref.Read == "ouf.wait" || !s.valid(ref.Read, ref.Args) {
		return errorResult("WAIT_REFERENCE_INVALID", false)
	}
	stop := func(code string) *mcp.CallToolResult {
		result := waitResult(false, nil, ref.Block(code, "Consulta lo stato aggiornato della decisione."))
		result.IsError = true
		return result
	}
	exp, err := time.Parse(time.RFC3339Nano, ref.Deadline)
	if err != nil || !time.Now().Before(exp) {
		return stop("WAIT_DEADLINE_EXPIRED")
	}
	linkEnd, _ := time.Parse(time.RFC3339Nano, ref.LinkEnd)
	// Refresh from a complete authorized observation before exposing a new
	// link. Earlier semantic references could carry a URL supplied by Semantic.
	refreshLink := func() bool {
		return ref.Read == "semantic.proposal.read" && ref.LinkOwner != "onboarding" ||
			!linkEnd.IsZero() && !time.Now().Before(linkEnd)
	}
	scope := string(binding)
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
	if w.interval >= duration {
		return stop("WAIT_TRANSPORT_WINDOW_TOO_SHORT")
	}
	until := time.Now().Add(duration)
	if exp.Before(until) {
		until = exp
	}
	ctx, cancel := context.WithDeadline(ctx, until)
	defer cancel()

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

	i := ctx.Value(identityKey{}).(requestIdentity)
	// A new authorized observation needs a fresh read attempt, including
	// when the outer request has its own idempotency key.
	i.IdempotencyKey = ""
	ctx = context.WithValue(ctx, identityKey{}, i)
	authority, _ := json.Marshal([]any{scope, i.Delegation, i.AuthenticationContextRef, i.Claims, i.Scopes, ref.Read, args})
	key := fmt.Sprintf("%x", sha256.Sum256(authority))

	var last nextstep.Object
	for {
		timer := time.NewTimer(w.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
		if ctx.Err() != nil {
			if !time.Now().Before(exp) {
				return stop("WAIT_DEADLINE_EXPIRED")
			}
			if last == nil || ctx.Err() == context.Canceled {
				return stop("WAIT_OBSERVATION_UNAVAILABLE")
			}
			if refreshLink() {
				return waitResult(false, last, ref.Refresh())
			}
			return waitResult(false, last, ref.Poll(token))
		}
		ch := w.group.DoChan(key, func() (any, error) {
			if err := w.reads.Wait(ctx); err != nil {
				return nil, err
			}
			return t.run(ctx, &read)
		})
		select {
		case <-ctx.Done():
			if !time.Now().Before(exp) {
				return stop("WAIT_DEADLINE_EXPIRED")
			}
			return stop("WAIT_OBSERVATION_UNAVAILABLE")
		case response := <-ch:
			if !time.Now().Before(exp) {
				return stop("WAIT_DEADLINE_EXPIRED")
			}
			result, ok := response.Val.(*mcp.CallToolResult)
			if response.Err != nil || !ok || result == nil {
				return stop("WAIT_OBSERVATION_UNAVAILABLE")
			}
			body := ownerObject(result)
			obs := nextstep.Observation(ref.Read, ref.Args, body)
			if result.IsError {
				next := s.engine(ctx).Evaluate(t.capability, s.manifest.NextPolicies[t.capability.NextPolicy], ref.Args, body, true)
				denial := waitResult(false, nil, next)
				denial.IsError = true
				return denial
			}
			if !ref.MatchesResource(obs) {
				return stop("WAIT_RESOURCE_CHANGED")
			}
			last = obs
			changed := nextstep.Version(obs) != ref.Version
			if refreshLink() && (obs["unchanged"] == true || obs["changed"] == false) && len(args) != len(ref.Args) {
				return waitResult(changed, obs, ref.Refresh())
			}
			if changed || refreshLink() {
				next := s.engine(ctx).Evaluate(t.capability, s.manifest.NextPolicies[t.capability.NextPolicy], args, body, false)
				return waitResult(changed, obs, next)
			}
		}
	}
}
