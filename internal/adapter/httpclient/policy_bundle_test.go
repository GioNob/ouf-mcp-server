package httpclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/authorization"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

func TestPolicyBundleTransportIntegrity(t *testing.T) {
	bundle := `{"bundleId":"transport","version":1,"publishedAt":"2026-09-17T00:00:00Z","capabilities":[],"grants":[]}`
	sum := sha256.Sum256([]byte(bundle))
	valid := `{"bundleId":"transport","bundleVersion":1,"activatedAt":"2026-09-17T00:00:01Z","bundle":` + bundle + `,"contentHash":"` + hex.EncodeToString(sum[:]) + `"}`
	cases := map[string]string{
		"valid":    valid,
		"tampered": strings.Replace(valid, `"capabilities":[]`, `"capabilities": [ ]`, 1),
		"changed":  strings.Replace(valid, `"grants":[]`, `"grants":null`, 1),
		"unknown":  strings.Replace(valid, `"grants":[]`, `"grants":[],"unknownMandatory":true`, 1),
		"trailing": valid + `{}`,
		"oversize": strings.Repeat(" ", 5*1024*1024+65537),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer fixture" {
					t.Error("workload identity missing")
				}
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			endpoint, _ := url.Parse(server.URL)
			client := &PolicyBundleClient{Endpoint: endpoint, Client: server.Client(), TokenSource: StaticTokenSource("fixture")}
			_, err := client.FetchActive(context.Background())
			// Whitespace is removed by the documented compact-JSON transport hash.
			wantValid := name == "valid" || name == "tampered"
			if (err == nil) != wantValid {
				t.Fatalf("unexpected integrity result: %v", err)
			}
		})
	}
}

// Exercise the real wire decoder and cache copy together: decoding alone did
// not detect omitted constraints being re-encoded as invalid empty strings.
func TestConstrainedPolicyBundleTransportToCache(t *testing.T) {
	bundle := `{"bundleId":"transport","version":1,"publishedAt":"2026-09-17T00:00:00Z","capabilities":[{"capabilityId":"ouf.system.status","operation":"READ","requiredScope":"operations.status.read","allowedActors":["HUMAN"]}],"grants":[{"grantId":"status","capabilityId":"ouf.system.status","tenantId":"tenant-a","subjectId":"user-a","validFrom":"2020-01-01T00:00:00Z","validUntil":"2100-01-01T00:00:00Z","constraints":{"effect":"ALLOW","resourceType":"capability","allowedDetailLevels":["PUBLIC_OPERATIONAL"]}}]}`
	sum := sha256.Sum256([]byte(bundle))
	body := `{"bundleId":"transport","bundleVersion":1,"activatedAt":"2026-09-17T00:00:01Z","bundle":` + bundle + `,"contentHash":"` + hex.EncodeToString(sum[:]) + `"}`
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fixture" {
			t.Error("workload identity missing")
		}
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL)
	cache := authorization.NewCache(&PolicyBundleClient{Endpoint: endpoint, Client: server.Client(), TokenSource: StaticTokenSource("fixture")})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatalf("valid constrained wire bundle rejected: %v", err)
	}
	request := orchestration.AuthorizationRequest{
		Identity:     orchestration.Identity{PrincipalID: "user-a", TenantID: "tenant-a", ActorType: "HUMAN", AuthenticationContextRef: "1", Issuer: "issuer", Audience: "ouf", Scopes: []string{"operations.status.read"}},
		Resource:     orchestration.ResourceContext{Attributes: map[string]string{"detailLevel": "PUBLIC_OPERATIONAL"}},
		CapabilityID: "ouf.system.status", OperationClass: "READ",
	}
	if decision, err := cache.Authorize(context.Background(), request); err != nil || !decision.Allowed || decision.DecisionRef != "transport:1:ouf.system.status" {
		t.Fatalf("unexpected authorization: %+v %v", decision, err)
	}
	request.Identity.PrincipalID = "other-user"
	if decision, err := cache.Authorize(context.Background(), request); err != nil || decision.Allowed {
		t.Fatalf("subject restriction lost: %+v %v", decision, err)
	}
}

func TestPolicyBundleSlowBodyRespectsContext(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL)
	client := &PolicyBundleClient{Endpoint: endpoint, Client: server.Client(), TokenSource: StaticTokenSource("fixture")}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := client.FetchActive(ctx); err == nil {
		t.Fatal("slow response escaped deadline")
	}
}
