package evidence

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
)

type pairwiseEvidenceStore struct {
	mu    sync.Mutex
	byKey map[string]Verified
}

func (s *pairwiseEvidenceStore) InsertVerifiedEvidence(_ context.Context, in Verified) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := in.BackendOwner + "\x00" + in.BackendRequestID
	if old, ok := s.byKey[key]; ok {
		if old.PayloadHash != in.PayloadHash {
			return Result{}, ErrIngressConflict
		}
		return Result{EvidenceRef: old.EvidenceRef, Replay: true}, nil
	}
	s.byKey[key] = in
	return Result{EvidenceRef: in.EvidenceRef}, nil
}

func gatewayNormalize(t *testing.T, gatewayRoot string, payload map[string]any, service string) map[string]any {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	script := `import json,sys
from tools.mcp_dispatch import BackendResponse,TrustedIdentity
from tools.mcp_evidence_ingress import MCPEvidenceIngressMediator
class I:
 def ingest(self,service,path,body,headers,timeout):
  print(body.decode()); return BackendResponse(201,b'{}',{})
p=json.loads(sys.stdin.read()); MCPEvidenceIngressMediator(I()).ingest(json.dumps(p).encode(),{'X-Correlation-ID':'pairwise'},TrustedIdentity(sys.argv[1],'owner-worker','tenant','SERVICE','authn','authz',frozenset({'mcp.evidence.submit'})))`
	cmd := exec.Command("python3", "-c", script, service)
	cmd.Dir = gatewayRoot
	cmd.Stdin = bytes.NewReader(raw)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("gateway ingress rejected payload: %v: %s", err, out)
	}
	var got map[string]any
	if err = json.Unmarshal(out, &got); err != nil {
		t.Fatalf("invalid gateway canonical output: %s: %v", out, err)
	}
	return got
}

func TestGatewayEvidenceIngressPairwise(t *testing.T) {
	root := os.Getenv("GATEWAY_PAIRWISE_ROOT")
	if root == "" {
		t.Skip("GATEWAY_PAIRWISE_ROOT is not set")
	}
	root, _ = filepath.Abs(root)
	store := &pairwiseEvidenceStore{byKey: map[string]Verified{}}
	svc := Service{Store: store}
	base := map[string]any{"BackendRequestID": "udp-pairwise-1", "Kind": "OWNER_RESULT", "TerminalState": "SUCCEEDED", "OutcomeCode": "SUCCEEDED", "ResultRef": "result://udp/pairwise", "ActualDistinctObjects": 1, "ObjectHashes": []string{"v1:hmac-sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}}
	canonical := gatewayNormalize(t, root, base, "ouf-udp-object-resolution")
	input := Input{ClaimedOwner: canonical["ClaimedOwner"].(string), BackendRequestID: canonical["BackendRequestID"].(string), Kind: canonical["Kind"].(string), TerminalState: canonical["TerminalState"].(string), OutcomeCode: canonical["OutcomeCode"].(string), ResultRef: canonical["ResultRef"].(string), ActualDistinctObjects: int64(canonical["ActualDistinctObjects"].(float64))}
	for _, h := range canonical["ObjectHashes"].([]any) {
		input.ObjectHashes = append(input.ObjectHashes, h.(string))
	}
	identity := TrustedIdentity{Authenticated: true, ServicePrincipalID: "ouf-api-gateway", BackendOwner: "udp-object-resolution"}
	first, err := svc.Ingest(context.Background(), identity, input)
	if err != nil || first.Replay {
		t.Fatalf("first ingress=%+v err=%v", first, err)
	}
	second, err := svc.Ingest(context.Background(), identity, input)
	if err != nil || !second.Replay || second.EvidenceRef != first.EvidenceRef {
		t.Fatalf("replay=%+v err=%v", second, err)
	}
	input.ResultRef = "result://udp/different"
	if _, err = svc.Ingest(context.Background(), identity, input); err != ErrIngressConflict {
		t.Fatalf("conflict err=%v", err)
	}
	if _, err = svc.Ingest(context.Background(), TrustedIdentity{Authenticated: true, ServicePrincipalID: "ouf-api-gateway", BackendOwner: "attacker"}, input); err != ErrOwnerMismatch {
		t.Fatalf("forged owner err=%v", err)
	}

	noDispatch := map[string]any{"BackendRequestID": "udp-pairwise-no-dispatch", "Kind": "OWNER_PROVES_NO_DISPATCH", "TerminalState": "FAILED", "OutcomeCode": "DISPATCH_NOT_STARTED", "ResultRef": "", "ActualDistinctObjects": 0, "ObjectHashes": []string{}}
	canonical = gatewayNormalize(t, root, noDispatch, "ouf-udp-object-resolution")
	if canonical["Kind"] != "OWNER_PROVES_NO_DISPATCH" || canonical["ActualDistinctObjects"].(float64) != 0 {
		t.Fatalf("no-dispatch not preserved: %+v", canonical)
	}

	bad := map[string]any{"BackendRequestID": "udp-pairwise-bad", "Kind": "OWNER_RESULT", "TerminalState": "SUCCEEDED", "OutcomeCode": "SUCCEEDED", "ResultRef": "result://udp/bad", "ActualDistinctObjects": 1, "ObjectHashes": []string{"v2:hmac-sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}}
	raw, _ := json.Marshal(bad)
	script := `import sys
from tools.mcp_dispatch import TrustedIdentity
from tools.mcp_evidence_ingress import MCPEvidenceIngressMediator
class I:
 def ingest(self,*a): raise Exception('network must not be reached')
MCPEvidenceIngressMediator(I()).ingest(sys.stdin.buffer.read(),{'X-Correlation-ID':'pairwise'},TrustedIdentity('ouf-udp-object-resolution','w','t','SERVICE','a','z',frozenset({'mcp.evidence.submit'})))`
	cmd := exec.Command("python3", "-c", script)
	cmd.Dir = root
	cmd.Stdin = bytes.NewReader(raw)
	if err = cmd.Run(); err == nil {
		t.Fatal("unsupported hash version was accepted")
	}

	concurrent := Input{ClaimedOwner: "udp-object-resolution", BackendRequestID: "udp-concurrent", Kind: "OWNER_RESULT", TerminalState: "SUCCEEDED", OutcomeCode: "SUCCEEDED", ResultRef: "result://udp/concurrent", ActualDistinctObjects: 0, ObjectHashes: []string{}}
	var wg sync.WaitGroup
	refs := make(chan uuid.UUID, 8)
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := svc.Ingest(context.Background(), identity, concurrent)
			if e != nil {
				errs <- e
				return
			}
			refs <- r.EvidenceRef
		}()
	}
	wg.Wait()
	close(refs)
	close(errs)
	for e := range errs {
		t.Fatalf("concurrent ingress err=%v", e)
	}
	var expected uuid.UUID
	for ref := range refs {
		if expected == uuid.Nil {
			expected = ref
		}
		if ref != expected {
			t.Fatalf("concurrent refs differ %s %s", expected, ref)
		}
	}
}
