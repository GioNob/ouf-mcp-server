package kernel

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type mandateGatewayFixture struct {
	mu             sync.Mutex
	minted         analysis.BatchMandateView
	binding        analysis.BatchStartBinding
	issue          analysis.BatchMandateIssue
	stateURL       string
	peer           *http.Client
	owner          *batchWorkerOwner
	ingress        map[string]int
	admitted       map[string]int
	outage         bool
	denyNext       string
	loseIssue      bool
	loseReplay     bool
	retryWaitUntil time.Time
}

type contractEvidence struct {
	analysis.BatchEvidence
	RequestHash       string `json:"requestHash"`
	ClockSynchronized bool   `json:"clockSynchronized"`
	Observed          int64  `json:"observedAtMs"`
	ValidUntil        int64  `json:"validUntilMs"`
}

func (g *mandateGatewayFixture) evidence(ctx context.Context, q analysis.BatchEvidenceRequest) (contractEvidence, error) {
	b := g.binding
	q.Contract, q.Installation, q.Issuer, q.Tenant, q.Subject, q.Client, q.Workload =
		analysis.BatchMandateContract, b.Installation, b.Issuer, b.Tenant, b.Subject, b.Client, b.Workload
	q.StartRef, q.Mode, q.AssetID, q.ProfileID, q.SchemaHash, q.CommandKey, q.Scope =
		b.StartRef, b.Mode, b.AssetID, b.ProfileID, b.SchemaHash, b.CommandKey, b.Scope
	q.Nonce, q.Generation = uuid.NewString(), 1
	raw, _ := json.Marshal(q)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.stateURL, bytes.NewReader(raw))
	if err != nil {
		return contractEvidence{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.peer.Do(req)
	if err != nil {
		return contractEvidence{}, err
	}
	defer resp.Body.Close()
	var result contractEvidence
	if resp.StatusCode != 200 || json.NewDecoder(resp.Body).Decode(&result) != nil ||
		result.RequestHash != analysis.BatchEvidenceHash(raw) || !result.ClockSynchronized ||
		result.ValidUntil <= result.Observed || result.ValidUntil-result.Observed > 30000 {
		return contractEvidence{}, errors.New("invalid MCP work evidence")
	}
	return result, nil
}

func (g *mandateGatewayFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	defer g.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	reply := func(value any) { _ = json.NewEncoder(w).Encode(value) }
	problem := func(status int, code string) {
		w.WriteHeader(status)
		reply(map[string]string{"code": code})
	}
	if r.TLS == nil || len(r.TLS.VerifiedChains) != 1 || r.Method != http.MethodPost ||
		r.Header.Get("Authorization") != "" || r.Header.Get("X-OUF-Delegation") != "" {
		problem(403, "GATEWAY_WORKLOAD_DENIED")
		return
	}
	decode := func(target any) bool {
		d := json.NewDecoder(r.Body)
		d.DisallowUnknownFields()
		return d.Decode(target) == nil
	}
	match := func(key analysis.BatchMandateKey) bool {
		return key.Ref != "" && key.Ref == g.minted.Ref && key.Generation == g.minted.Generation && key.Binding == g.minted.MandateBinding
	}
	const route = "/internal/analysis-batch/v1/"
	switch r.URL.Path {
	case route + "prepare":
		var in struct{ StartProof string }
		if !decode(&in) {
			problem(400, "ANALYSIS_MANDATE_INVALID")
			return
		}
		out, err := (&mandateTestIssuer{}).Prepare(r.Context(), in.StartProof)
		if err != nil {
			problem(403, "ANALYSIS_START_PROOF_DENIED")
			return
		}
		g.binding, _ = analysis.DecodeBatchStartBinding(out.StartBinding)
		reply(out)
	case route + "issue":
		var in analysis.BatchMandateIssue
		if !decode(&in) {
			problem(400, "ANALYSIS_MANDATE_INVALID")
			return
		}
		if g.loseReplay && in.StartProof == "" {
			g.loseReplay = false
			problem(503, "ANALYSIS_AUTHORIZATION_UNAVAILABLE")
			return
		}
		if in.StartProof != "" {
			proof, err := g.evidence(r.Context(), analysis.BatchEvidenceRequest{Action: "issue", AnalysisID: in.AnalysisID})
			if err != nil || !proof.Bound || proof.PolicyEpoch != in.PolicyEpoch || proof.Phase != "FIRST_ROUND" {
				problem(403, "ANALYSIS_WORK_NOT_BOUND")
				return
			}
			view, err := (&mandateTestIssuer{}).Issue(r.Context(), in)
			if err != nil {
				problem(403, "HUMAN_PROOF_INVALID")
				return
			}
			in.StartProof = ""
			if g.minted.Ref == "" {
				g.minted, g.issue = view, in
			}
			if g.loseIssue {
				g.loseIssue = false
				dropMandateResponse(w)
				return
			}
		}
		if g.minted.Ref == "" || in != g.issue {
			problem(403, "HUMAN_PROOF_REQUIRED")
			return
		}
		reply(g.minted)
	case route + "read":
		g.ingress["read"]++
		var key analysis.BatchMandateKey
		if !decode(&key) || !match(key) {
			problem(403, "MANDATE_BINDING_MISMATCH")
			return
		}
		if g.outage {
			problem(503, "ANALYSIS_AUTHORIZATION_UNAVAILABLE")
			return
		}
		reply(g.minted)
	case route + "expire":
		g.ingress["expire"]++
		var in struct {
			analysis.BatchMandateKey
			Version int `json:"expectedVersion"`
		}
		if !decode(&in) || !match(in.BatchMandateKey) || in.Version != g.minted.Version {
			problem(409, "MANDATE_VERSION_CHANGED")
			return
		}
		proof, err := g.evidence(r.Context(), analysis.BatchEvidenceRequest{Action: "expire", AnalysisID: g.minted.AnalysisID, Version: in.Version})
		if err != nil || !proof.Bound || proof.Phase == "FIRST_ROUND" {
			problem(403, "ANALYSIS_WORK_NOT_BOUND")
			return
		}
		g.minted.State, g.minted.Version = "EXPIRED", g.minted.Version+1
		reply(g.minted)
	case route + "dispatch":
		var in analysis.BatchMandateDispatch
		if !decode(&in) || !match(in.BatchMandateKey) || in.AnalysisID != g.minted.AnalysisID ||
			!analysis.ValidBatchAttempt(in.AttemptID) || !analysis.ValidBatchAttempt(in.CorrelationID) {
			problem(403, "ANALYSIS_MANDATE_BINDING_MISMATCH")
			return
		}
		if analysis.BatchStageForOperation(in.Operation) == "" {
			problem(403, "ANALYSIS_OPERATION_NOT_ALLOWED")
			return
		}
		g.ingress[in.Operation]++
		requestHash, err := analysis.BatchArgumentsHash(in.Arguments)
		var args struct{ RequestID string }
		if err != nil || json.Unmarshal(in.Arguments, &args) != nil {
			problem(400, "ANALYSIS_ARGUMENTS_INVALID")
			return
		}
		proof, err := g.evidence(r.Context(), analysis.BatchEvidenceRequest{Action: "dispatch", AnalysisID: in.AnalysisID, Version: 1,
			Operation: in.Operation, ArgumentsHash: requestHash, AttemptID: in.AttemptID, RequestID: args.RequestID})
		if err != nil || !proof.Bound || proof.PolicyEpoch != g.issue.PolicyEpoch {
			problem(403, "ANALYSIS_WORK_NOT_BOUND")
			return
		}
		if proof.Phase != "FIRST_ROUND" {
			if g.minted.State == "ACTIVE" {
				g.minted.State, g.minted.Version = "EXPIRED", g.minted.Version+1
			}
			problem(403, "ANALYSIS_MANDATE_EXPIRED")
			return
		}
		if g.minted.State != "ACTIVE" {
			problem(403, "MANDATE_"+g.minted.State)
			return
		}
		if g.outage {
			w.Header().Set("Retry-After", "90")
			problem(503, "ANALYSIS_AUTHORIZATION_UNAVAILABLE")
			return
		}
		if g.denyNext != "" {
			if g.denyNext == "MANDATE_REVOKED" {
				g.minted.State, g.minted.Version = "REVOKED", g.minted.Version+1
			}
			problem(403, g.denyNext)
			g.denyNext = ""
			return
		}
		g.admitted[in.Operation]++
		g.dispatch(w, r, in, proof, requestHash, args.RequestID, problem)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (g *mandateGatewayFixture) dispatch(w http.ResponseWriter, r *http.Request, in analysis.BatchMandateDispatch, proof contractEvidence, hash, request string, problem func(int, string)) {
	op := analysis.BatchStageForOperation(in.Operation)
	capability, idem := "ouf."+in.Operation, in.AttemptID
	makeDiscoveryBinding := func(requestID string) string {
		return testMandateBinding("discovery", []string{g.binding.Installation, in.AnalysisID, in.Ref, proof.WorkRef, proof.DiscoveryArgumentsHash, requestID})
	}
	if op == analysis.BatchRequest {
		if proof.WorkRef == "" || proof.DiscoveryArgumentsHash != hash {
			problem(403, "ANALYSIS_WORK_NOT_BOUND")
			return
		}
		var args map[string]any
		_ = json.Unmarshal(in.Arguments, &args)
		capability, idem = "ouf.semantic.discovery", "analysis-batch-v1:"+analysis.Digest([]string{in.Ref, proof.WorkRef})
		args["idempotencyKey"] = idem
		in.Arguments, _ = json.Marshal(args)
	} else if op != analysis.BatchPublished && proof.DiscoveryBinding != makeDiscoveryBinding(request) {
		problem(403, "ANALYSIS_DISCOVERY_REQUEST_NOT_BOUND")
		return
	}
	response, err := g.owner.Execute(r.Context(), orchestration.GatewayRequest{
		CapabilityID: capability, Arguments: in.Arguments, IdempotencyKey: idem,
	}, 0)
	if err != nil {
		dropMandateResponse(w)
		return
	}
	if response.Problem != nil {
		w.Header().Set("Retry-After", response.Problem.RetryAfter)
		problem(response.Problem.Status, response.Problem.Code)
		return
	}
	if op == analysis.BatchRequest {
		var created struct{ RequestID string }
		_ = json.Unmarshal(response.Body, &created)
		w.Header().Set(analysis.BatchDiscoveryBindingHeader, makeDiscoveryBinding(created.RequestID))
	}
	if op == analysis.BatchStatus && !g.retryWaitUntil.IsZero() {
		response.Body, _ = json.Marshal(map[string]any{"state": "RUNNING", "recoveryState": "RETRY_WAIT",
			"requestId": request, "nextAttemptAt": g.retryWaitUntil})
	}
	_, _ = w.Write(response.Body)
}

func dropMandateResponse(w http.ResponseWriter) {
	if h, ok := w.(http.Hijacker); ok {
		if conn, _, err := h.Hijack(); err == nil {
			_ = conn.Close()
		}
	}
}

type mandateTLSFiles struct {
	ca, cert, key string
	clientTLS     *tls.Config
	serverTLS     *tls.Config
}

func testMandateTLS(t *testing.T, serial int64, ca *x509.Certificate, caKey *ecdsa.PrivateKey) mandateTLSFiles {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	cert := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	rawKey, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	write := func(name, kind string, raw []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: raw}), 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	f := mandateTLSFiles{ca: write("ca.pem", "CERTIFICATE", ca.Raw),
		cert: write("leaf.pem", "CERTIFICATE", der), key: write("leaf.key", "PRIVATE KEY", rawKey)}
	f.clientTLS, err = httpclient.BatchMutualTLS(f.ca, f.cert, f.key)
	if err != nil {
		t.Fatal(err)
	}
	f.serverTLS = f.clientTLS.Clone()
	f.serverTLS.ClientAuth, f.serverTLS.ClientCAs = tls.RequireAndVerifyClientCert, f.clientTLS.RootCAs
	return f
}

type mandatePostgresFixture struct {
	store                  *postgres.Store
	snapshot               *manifest.Snapshot
	gateway                *mandateGatewayFixture
	client                 *httpclient.BatchMandateClient
	dsn, id                string
	scope                  analysis.Scope
	stateCert, gatewayCert mandateTLSFiles
	stateServer            *httptest.Server
	now                    time.Time
	evidenceTime           atomic.Int64
}

func newMandatePostgresFixture(t *testing.T, width int, loseIssue bool) *mandatePostgresFixture {
	t.Helper()
	dsn := isolatedSummaryTestDSN(t)
	store, snapshot := summaryTestStore(t, dsn)
	at := time.Now().UTC()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true,
		NotBefore: at.Add(-time.Hour), NotAfter: at.Add(time.Hour), KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	mcpTLS, gatewayTLS := testMandateTLS(t, 2, ca, key), testMandateTLS(t, 3, ca, key)
	owner := &batchWorkerOwner{residuals: 2, requests: map[string]string{}, intents: map[string]string{}}
	f := &mandatePostgresFixture{store: store, snapshot: snapshot, dsn: dsn, now: at, stateCert: mcpTLS, gatewayCert: gatewayTLS,
		scope: analysis.Scope{Service: "ouf-chatgpt", Principal: "mandate-fixture", Tenant: "tenant"}}
	f.evidenceTime.Store(at.UnixNano())
	f.gateway = &mandateGatewayFixture{owner: owner, ingress: map[string]int{}, admitted: map[string]int{}, loseIssue: loseIssue, loseReplay: loseIssue,
		peer: &http.Client{Transport: &http.Transport{TLSClientConfig: gatewayTLS.clientTLS}, Timeout: 4 * time.Second}}
	f.startState(t)
	server := httptest.NewUnstartedServer(f.gateway)
	server.TLS = gatewayTLS.serverTLS
	server.StartTLS()
	t.Cleanup(server.Close)
	f.client, err = httpclient.NewBatchMandateClient(server.URL, mcpTLS.ca, mcpTLS.cert, mcpTLS.key)
	if err != nil {
		t.Fatal(err)
	}
	pin := analysis.Profile{AssetID: uuid.NewString(), ProfileID: uuid.NewString(), SchemaHash: "sha256:" + strings.Repeat("a", 64), Version: 1, ColumnCount: width}
	profile := &mandateProfileFixture{pin}
	service := &orchestration.Service{RequireDelegation: true, Auth: mandateTestAuth{&batchProfileAuth{}},
		Admission: store, Gateway: profile, FingerprintKey: make([]byte, 32)}
	session := revisionSessionForScope(t, service, f.scope, batchWorkerScopes, mandateTestIngress(t, f.client, f.scope, batchWorkerScopes))
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "source.analysis.plan", Arguments: map[string]string{
		"mode": analysis.BatchMode, "assetId": pin.AssetID, "profileId": pin.ProfileID, "schemaHash": strings.TrimPrefix(pin.SchemaHash, "sha256:"), "commandKey": uuid.NewString(),
	}})
	if err != nil || result == nil || result.IsError != loseIssue {
		t.Fatalf("plan: %+v %v", result, err)
	}
	_ = session.Close()
	f.id = analysis.BatchAnalysisID(f.scope, pin, mandateTestEpoch)
	f.now = time.Now().UTC()
	f.evidenceTime.Store(f.now.UnixNano())
	return f
}

type mandateProfileFixture struct{ pin analysis.Profile }

func (f *mandateProfileFixture) Execute(_ context.Context, in orchestration.GatewayRequest, _ time.Duration) (orchestration.GatewayResponse, error) {
	if in.CapabilityID != "ouf.managed-source.preview" {
		return orchestration.GatewayResponse{}, fmt.Errorf("unexpected interactive operation %s", in.CapabilityID)
	}
	columns := make([]map[string]any, f.pin.ColumnCount)
	for ordinal := range columns {
		prefix := "published"
		if ordinal < 2 {
			prefix = "residual"
		}
		columns[ordinal] = map[string]any{"ordinal": ordinal, "name": fmt.Sprintf("%s_%d", prefix, ordinal), "inferredType": "string"}
	}
	raw, _ := json.Marshal(struct {
		analysis.Profile
		View       string           `json:"view"`
		Offset     int              `json:"offset"`
		NextOffset int              `json:"nextOffset"`
		HasMore    bool             `json:"hasMore"`
		Columns    []map[string]any `json:"columns"`
	}{f.pin, "COLUMNS", 0, f.pin.ColumnCount, false, columns})
	return orchestration.GatewayResponse{Status: 200, Body: raw}, nil
}

func (f *mandatePostgresFixture) startState(t *testing.T) {
	t.Helper()
	sum, _ := f.snapshot.Checksum()
	h := httpclient.BatchEvidenceHandler{Store: f.store, ManifestChecksum: sum,
		Clock: func() (time.Time, bool) { return time.Unix(0, f.evidenceTime.Load()).UTC(), true }}
	leaf := f.gatewayCert.clientTLS.Certificates[0].Certificate[0]
	h.GatewayLeafSHA256 = sha256.Sum256(leaf)
	f.stateServer = httptest.NewUnstartedServer(h)
	f.stateServer.TLS = f.stateCert.serverTLS
	f.stateServer.StartTLS()
	t.Cleanup(f.stateServer.Close)
	f.gateway.mu.Lock()
	f.gateway.stateURL = f.stateServer.URL + "/internal/analysis-batch/v1/state"
	f.gateway.mu.Unlock()
}

func (f *mandatePostgresFixture) restart(t *testing.T) {
	t.Helper()
	f.stateServer.Close()
	f.store.Close()
	store, err := postgres.Open(t.Context(), f.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	f.store = store
	f.startState(t)
}

func (f *mandatePostgresFixture) worker() analysis.BatchWorker {
	return analysis.BatchWorker{Store: f.store, Manifest: f.snapshot, Mandates: f.client, Now: func() time.Time { return f.now }}
}

func (f *mandatePostgresFixture) step(t *testing.T) bool {
	t.Helper()
	worked, err := f.worker().RunDue(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return worked
}

func (f *mandatePostgresFixture) dbAdvance(_ *testing.T, delta time.Duration) {
	f.now = f.now.Add(delta)
	f.evidenceTime.Store(f.now.UnixNano())
}

func (f *mandatePostgresFixture) drain(t *testing.T) {
	t.Helper()
	for range 100 {
		if !f.step(t) {
			return
		}
	}
	t.Fatal("mandated queue did not drain")
}

func (f *mandatePostgresFixture) column(t *testing.T, ordinal int) (string, string, int, *time.Time, *time.Time) {
	t.Helper()
	var stage, code string
	var attempts int
	var started, due *time.Time
	err := f.store.Pool().QueryRow(t.Context(), `select b.work_stage,coalesce(c.outcome->>'reasonCode',''),b.retry_attempts,b.retry_started_at,b.due_at
		from ouf_mcp.analysis_batch_column b join ouf_mcp.analysis_column c using(analysis_id,ordinal)
		where b.analysis_id=$1 and b.ordinal=$2`, f.id, ordinal).Scan(&stage, &code, &attempts, &started, &due)
	if err != nil {
		t.Fatal(err)
	}
	return stage, code, attempts, started, due
}

func (f *mandatePostgresFixture) calls(operation string) (int, int, int) {
	f.gateway.mu.Lock()
	defer f.gateway.mu.Unlock()
	return f.gateway.ingress[operation], f.gateway.admitted[operation], len(f.gateway.owner.requests)
}

func TestBatchMandatePostgresClosedChatRestartAndLostResponse(t *testing.T) {
	f := newMandatePostgresFixture(t, 32, true)
	f.gateway.mu.Lock()
	f.gateway.owner.loseResponse = true
	f.gateway.owner.statusNotBefore = f.now.Add(3 * time.Minute)
	f.gateway.mu.Unlock()
	f.restart(t)
	f.dbAdvance(t, 2*time.Minute)
	f.drain(t) // Reconciles the committed issue and loses a committed creation response.
	_, _, attempts, started, due := f.column(t, 0)
	if attempts != 1 || started == nil || due == nil || !due.After(f.now) {
		t.Fatalf("lost response checkpoint attempts=%d started=%v due=%v", attempts, started, due)
	}
	f.restart(t)
	f.dbAdvance(t, 2*time.Minute)
	f.drain(t)
	f.dbAdvance(t, time.Minute)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.worker().Run(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	defer func() {
		cancel()
		<-done
	}()
	var matched, ready, empty, pending int
	var mandateState string
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		err := f.store.Pool().QueryRow(ctx, `select h.matched,h.ready,h.zero_count,h.pending,m.state
			from ouf_mcp.analysis_counts h join ouf_mcp.analysis_batch_mandate m using(analysis_id)
			where h.analysis_id=$1 order by h.seq desc limit 1`, f.id).Scan(&matched, &ready, &empty, &pending, &mandateState)
		if err != nil {
			t.Fatal(err)
		}
		if pending == 0 && mandateState == "EXPIRED" {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("queue did not complete and expire its mandate with the chat closed: pending=%d mandate=%s", pending, mandateState)
		case <-tick.C:
		}
	}
	if matched != 30 || ready != 1 || empty != 1 {
		t.Fatalf("outcomes %d %d %d %d", matched, ready, empty, pending)
	}
	if sent, admitted, effects := f.calls("semantic.discovery.request"); sent != 3 || admitted != 3 || effects != 2 {
		t.Fatalf("discovery calls=%d admissions=%d effects=%d", sent, admitted, effects)
	}
	for _, op := range []string{"semantic.discovery.status", "semantic.discovery.candidates"} {
		if _, admitted, _ := f.calls(op); admitted != 2 {
			t.Fatalf("%s admissions=%d", op, admitted)
		}
	}
}

func TestBatchMandatePostgresRevokedDuringBackoff(t *testing.T) {
	f := newMandatePostgresFixture(t, 1, false)
	f.dbAdvance(t, time.Second)
	if !f.step(t) {
		t.Fatal("missing published lookup")
	}
	f.gateway.mu.Lock()
	f.gateway.owner.throttle = true
	f.gateway.mu.Unlock()
	f.drain(t)
	_, _, attempts, started, due := f.column(t, 0)
	if attempts != 1 || started == nil || due == nil || due.Before(f.now.Add(90*time.Second)) {
		t.Fatal("Retry-After and first attempt were not checkpointed")
	}
	f.gateway.mu.Lock()
	f.gateway.minted.State, f.gateway.minted.Version = "REVOKED", 2
	f.gateway.mu.Unlock()
	f.restart(t)
	f.dbAdvance(t, 2*time.Minute)
	f.drain(t)
	_, code, after, original, due := f.column(t, 0)
	sent, _, effects := f.calls("semantic.discovery.request")
	if code != "MANDATE_REVOKED" || after != attempts || original == nil || !original.Equal(*started) || due != nil || sent != 1 || effects != 0 {
		t.Fatalf("revocation code=%s attempts=%d started=%v due=%v sent=%d effects=%d", code, after, original, due, sent, effects)
	}
}

func TestBatchMandatePostgresRevokedDuringSemanticRetryWait(t *testing.T) {
	f := newMandatePostgresFixture(t, 1, false)
	f.dbAdvance(t, time.Second)
	f.gateway.mu.Lock()
	f.gateway.owner.statusNotBefore = f.now.Add(time.Minute)
	f.gateway.retryWaitUntil = f.now.Add(3 * time.Minute)
	f.gateway.mu.Unlock()
	f.drain(t)
	f.dbAdvance(t, 2*time.Minute)
	f.drain(t)
	stage, code, attempts, started, due := f.column(t, 0)
	sent, admitted, _ := f.calls("semantic.discovery.status")
	if stage != analysis.BatchStatus || code != "" || attempts != 0 || started != nil ||
		due == nil || due.Before(f.now.Add(time.Minute)) || sent != 1 || admitted != 1 {
		t.Fatalf("Semantic RETRY_WAIT checkpoint: %s %s attempts=%d started=%v due=%v sent=%d admitted=%d",
			stage, code, attempts, started, due, sent, admitted)
	}
	reads, _, _ := f.calls("read")
	f.gateway.mu.Lock()
	f.gateway.minted.State, f.gateway.minted.Version = "REVOKED", 2
	f.gateway.mu.Unlock()
	f.restart(t)
	f.dbAdvance(t, 2*time.Minute)
	f.drain(t)
	_, code, attempts, _, due = f.column(t, 0)
	afterReads, _, _ := f.calls("read")
	afterSent, afterAdmitted, _ := f.calls("semantic.discovery.status")
	if code != "MANDATE_REVOKED" || attempts != 0 || due != nil || afterReads != reads+1 ||
		afterSent != sent || afterAdmitted != admitted {
		t.Fatalf("resumed revoked wait: %s attempts=%d due=%v reads=%d sent=%d admitted=%d",
			code, attempts, due, afterReads-reads, afterSent-sent, afterAdmitted-admitted)
	}
}

func TestBatchMandatePostgresSignedExpiryDuringGatewayOutage(t *testing.T) {
	f := newMandatePostgresFixture(t, 1, false)
	f.dbAdvance(t, time.Second)
	if !f.step(t) {
		t.Fatal("missing published lookup")
	}
	f.gateway.mu.Lock()
	f.gateway.outage = true
	expires := time.UnixMilli(f.gateway.minted.ExpiresAtMs)
	f.gateway.mu.Unlock()
	f.drain(t)
	_, _, attempts, started, due := f.column(t, 0)
	if attempts != 1 || started == nil || due == nil || !due.Before(expires) {
		t.Fatalf("missing pre-expiry retry: attempts=%d started=%v due=%v", attempts, started, due)
	}
	f.restart(t)
	f.dbAdvance(t, expires.Sub(f.now))
	f.drain(t)
	_, code, after, original, due := f.column(t, 0)
	reads, _, _ := f.calls("read")
	expirations, _, _ := f.calls("expire")
	sent, admitted, effects := f.calls("semantic.discovery.request")
	if code != "MANDATE_EXPIRED" || after != attempts || original == nil || !original.Equal(*started) ||
		due != nil || reads != 0 || expirations != 0 || sent != 1 || admitted != 0 || effects != 0 {
		t.Fatalf("signed expiry: %s attempts=%d started=%v due=%v reads=%d expirations=%d sent=%d admitted=%d effects=%d",
			code, after, original, due, reads, expirations, sent, admitted, effects)
	}
}

func TestBatchMandatePostgresAttestedDenialDoesNotSpendAttempt(t *testing.T) {
	f := newMandatePostgresFixture(t, 1, false)
	f.dbAdvance(t, time.Second)
	f.gateway.mu.Lock()
	f.gateway.denyNext = "CURRENT_RIGHTS_DENIED"
	f.gateway.mu.Unlock()
	f.drain(t)
	_, code, attempts, started, due := f.column(t, 0)
	if code != "CURRENT_RIGHTS_DENIED" || attempts != 0 || started != nil || due != nil {
		t.Fatalf("denied reservation checkpoint code=%s attempts=%d started=%v due=%v", code, attempts, started, due)
	}
}

func TestBatchMandatePostgresDenialBeforeInterruptedCheckpoint(t *testing.T) {
	f := newMandatePostgresFixture(t, 1, false)
	f.dbAdvance(t, time.Second)
	if !f.step(t) {
		t.Fatal("missing published lookup")
	}
	started := f.now.Truncate(time.Microsecond)
	if _, err := f.store.Pool().Exec(t.Context(), `update ouf_mcp.analysis_batch_column
		set retry_attempts=127,retry_started_at=$2 where analysis_id=$1 and ordinal=0`, f.id, started); err != nil {
		t.Fatal(err)
	}
	f.gateway.mu.Lock()
	f.gateway.denyNext = "MANDATE_REVOKED"
	f.gateway.mu.Unlock()
	worker := f.worker()
	crashed := &failBatchCheckpoint{Store: f.store, stage: analysis.BatchRequest}
	worker.Store = crashed
	if worked, err := worker.RunDue(t.Context()); !worked || err == nil || crashed.claimed.Stage != analysis.BatchRequest {
		t.Fatalf("expected checkpoint interruption: worked=%t err=%v", worked, err)
	}
	_, code, attempts, original, due := f.column(t, 0)
	sent, admitted, effects := f.calls("semantic.discovery.request")
	if code != "" || attempts != 127 || original == nil || !original.Equal(started) ||
		due == nil || sent != 1 || admitted != 0 || effects != 0 {
		t.Fatalf("interrupted denial: %s attempts=%d started=%v due=%v sent=%d admitted=%d effects=%d",
			code, attempts, original, due, sent, admitted, effects)
	}
	reads, _, _ := f.calls("read")
	f.restart(t)
	f.dbAdvance(t, analysis.BatchLease+time.Second)
	f.drain(t)
	_, code, attempts, original, due = f.column(t, 0)
	afterReads, _, _ := f.calls("read")
	sent, admitted, effects = f.calls("semantic.discovery.request")
	if code != "MANDATE_REVOKED" || attempts != 127 || original == nil || !original.Equal(started) ||
		due != nil || afterReads != reads+1 || sent != 1 || admitted != 0 || effects != 0 {
		t.Fatalf("restarted denial: %s attempts=%d started=%v due=%v reads=%d sent=%d admitted=%d effects=%d",
			code, attempts, original, due, afterReads-reads, sent, admitted, effects)
	}
}

func TestBatchMandatePostgresOutageAndCumulativeBudget(t *testing.T) {
	t.Run("recovers after hours", func(t *testing.T) {
		f := newMandatePostgresFixture(t, 1, false)
		f.dbAdvance(t, time.Second)
		f.step(t)
		f.gateway.mu.Lock()
		f.gateway.outage = true
		f.gateway.mu.Unlock()
		f.drain(t)
		_, _, attempts, started, _ := f.column(t, 0)
		f.restart(t)
		f.dbAdvance(t, 2*time.Hour)
		f.drain(t)
		_, _, after, original, _ := f.column(t, 0)
		if attempts != 1 || after != 2 || started == nil || original == nil || !original.Equal(*started) {
			t.Fatalf("budget did not accumulate across restart: %d %d %v %v", attempts, after, started, original)
		}
		f.gateway.mu.Lock()
		f.gateway.outage = false
		f.gateway.mu.Unlock()
		f.dbAdvance(t, 2*time.Hour)
		f.drain(t)
		if sent, admitted, effects := f.calls("semantic.discovery.request"); sent != 2 || admitted != 1 || effects != 1 {
			t.Fatalf("recovery sent=%d admitted=%d effects=%d", sent, admitted, effects)
		}
	})
	t.Run("cannot restart elapsed budget", func(t *testing.T) {
		f := newMandatePostgresFixture(t, 1, false)
		f.dbAdvance(t, time.Second)
		f.step(t)
		f.gateway.mu.Lock()
		f.gateway.outage = true
		f.gateway.mu.Unlock()
		f.drain(t)
		// Exercise the budget deadline while the signed binding is still valid.
		if _, err := f.store.Pool().Exec(t.Context(), `update ouf_mcp.analysis_batch_column
			set retry_started_at=$2 where analysis_id=$1 and ordinal=0`, f.id, f.now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
		f.restart(t)
		f.dbAdvance(t, 23*time.Hour)
		f.drain(t)
		_, code, attempts, _, due := f.column(t, 0)
		if code != "RETRY_BUDGET_EXHAUSTED" || attempts != 1 || due != nil {
			t.Fatalf("budget restarted: %s %d %v", code, attempts, due)
		}
		if sent, _, effects := f.calls("semantic.discovery.request"); sent != 1 || effects != 0 {
			t.Fatalf("sent after exhaustion: sent=%d effects=%d", sent, effects)
		}
	})
	t.Run("attempt cap survives restart", func(t *testing.T) {
		f := newMandatePostgresFixture(t, 1, false)
		f.dbAdvance(t, time.Second)
		f.step(t)
		if _, err := f.store.Pool().Exec(t.Context(), `update ouf_mcp.analysis_batch_column
			set retry_attempts=127,retry_started_at=$2 where analysis_id=$1 and ordinal=0`, f.id, f.now); err != nil {
			t.Fatal(err)
		}
		f.gateway.mu.Lock()
		f.gateway.outage = true
		f.gateway.mu.Unlock()
		f.restart(t)
		f.drain(t)
		_, code, attempts, _, due := f.column(t, 0)
		if code != "RETRY_BUDGET_EXHAUSTED" || attempts != 128 || due != nil {
			t.Fatalf("attempt cap restarted: %s %d %v", code, attempts, due)
		}
	})
}

func TestBatchMandatePostgresLateWorkerCannotProveWork(t *testing.T) {
	f := newMandatePostgresFixture(t, 1, false)
	f.dbAdvance(t, time.Second)
	f.step(t)
	sum, _ := f.snapshot.Checksum()
	old, err := f.store.ClaimMandatedBatch(t.Context(), f.scope, f.id, sum, f.now, 8)
	if err != nil || old.Stage != analysis.BatchRequest || len(old.Columns) != 1 {
		t.Fatalf("old lease: %+v %v", old, err)
	}
	cap, _ := analysis.Find(f.snapshot, analysis.BatchTool)
	limits, err := analysis.PublishedLimits(cap)
	if err != nil {
		t.Fatal(err)
	}
	key := "batch:" + analysis.Digest([]any{analysis.BatchMode, f.scope, old.Profile.AssetID, old.Profile.ProfileID, 0, 1})
	args, _ := json.Marshal(map[string]any{"idempotencyKey": key, "intent": "residual 0",
		"preferredLanguages": limits.Languages, "requestedArtifactType": "PROPERTY"})
	old.DispatchHash, _ = analysis.BatchArgumentsHash(args)
	if err := f.store.BeginBatchDispatch(t.Context(), old, []int{0}, f.now); err != nil {
		t.Fatal(err)
	}
	f.restart(t)
	f.dbAdvance(t, analysis.BatchLease+time.Second)
	f.step(t)
	late, _, err := f.client.Dispatch(t.Context(), analysis.BatchMandateDispatch{BatchMandateKey: old.Mandate.Key(),
		AnalysisID: f.id, Operation: "semantic.discovery.request", Arguments: args, AttemptID: old.Fence, CorrelationID: uuid.NewString()}, 8192)
	if err != nil || late.Problem == nil || late.Problem.Code != "ANALYSIS_WORK_NOT_BOUND" {
		t.Fatalf("late work gained admission: %+v %v", late, err)
	}
	if _, admitted, effects := f.calls("semantic.discovery.request"); admitted != 1 || effects != 1 {
		t.Fatalf("late worker admitted=%d effects=%d", admitted, effects)
	}
}
