package kernel

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/authorization"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const profileAsset = "00000000-0000-4000-8000-000000000001"
const profileJob = "00000000-0000-4000-8000-000000000002"
const otherProfileJob = "00000000-0000-4000-8000-000000000003"

func TestManagedProfileJobContextIsClosedAndDoesNotAssignCSVLabels(t *testing.T) {
	c := manifest.Capability{CapabilityID: "ouf.managed-source.preview", Owner: "onboarding", OperationClass: "READ", RequiredAuthorization: "ouf.managed-source.preview"}
	args, _ := json.Marshal(map[string]string{"assetId": profileAsset, "jobId": profileJob})
	cap, resource, err := managedProfileJobAuthorization(c, args, "tenant-a")
	if err != nil || cap != c.CapabilityID || resource.ResourceType != "managed-profile-job" || resource.ResourceID != profileJob || resource.Attributes["assetRef"] != profileAsset || resource.Attributes["jobRef"] != profileJob || resource.Attributes["detailLevel"] != "TENANT_OPERATIONAL" {
		t.Fatalf("wrong context: %+v %v", resource, err)
	}
	if _, exists := resource.Attributes["dataAccessLabel"]; exists {
		t.Fatal("CSV data label invented")
	}
	for _, invalid := range []string{
		`{"assetId":"bad","jobId":"bad"}`,
		`{"assetId":"` + profileAsset + `","jobId":"` + profileJob + `","resourceId":"forged"}`,
		`{"assetId":"` + profileAsset + `","jobId":"` + profileJob + `","profileId":"` + profileAsset + `"}`,
	} {
		if _, _, err := managedProfileJobAuthorization(c, []byte(invalid), "tenant-a"); err == nil {
			t.Fatal("invalid selection accepted")
		}
	}
	c.Owner = "other"
	if _, _, err := managedProfileJobAuthorization(c, args, "tenant-a"); err == nil {
		t.Fatal("manifest owner drift accepted")
	}
}

func TestManagedProfileJobSDKUsesRealPolicyBeforeDispatch(t *testing.T) {
	const capID = "ouf.managed-source.preview"
	now := time.Now().UTC()
	cache := authorization.NewCache(&ingestionBundleSource{authorization.ActivePolicyBundle{
		BundleID: "profile-job", BundleVersion: 1, ActivatedAt: now,
		Bundle: authorization.PolicyBundle{BundleID: "profile-job", Version: 1, PublishedAt: now,
			Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: capID, Operation: "READ", RequiredScope: capID, AllowedActors: []string{"HUMAN"}}},
			Grants: []authorization.Grant{{
				GrantID: "one-job", CapabilityID: capID, TenantID: "tenant-a", SubjectID: "user-a",
				ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour),
				Constraints: &authorization.GrantConstraints{Effect: "ALLOW", ResourceType: "managed-profile-job", ResourceID: profileJob,
					ResourceAttributes: map[string]string{"module": "ONBOARDING", "assetRef": profileAsset, "jobRef": profileJob},
					AllowedDetailLevels: []string{"TENANT_OPERATIONAL"}},
			}},
		},
	}})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	var rejectOwner atomic.Bool
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var envelope orchestration.GatewayRequest
		if json.NewDecoder(r.Body).Decode(&envelope) != nil || envelope.CapabilityID != capID || envelope.Owner != "onboarding" || r.Header.Get("X-OUF-Delegation") != "proof" || r.Header.Get("Authorization") != "Bearer workload" {
			t.Error("governed owner transport changed")
		}
		if rejectOwner.Load() {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":"OWNER_DENIED"}`))
			return
		}
		_, _ = w.Write([]byte(`{"jobId":"` + profileJob + `","status":"FAILED","attempts":5}`))
	}))
	defer gateway.Close()
	endpoint, _ := url.Parse(gateway.URL)
	admission := &statusAdmission{}
	service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: admission,
		Gateway: &httpclient.GatewayClient{Endpoint: endpoint, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")},
		FingerprintKey: make([]byte, 32)}
	handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, subject, tenant, scope string
		args                        map[string]any
		allowed                     bool
	}{
		{"exact-job", "user-a", "tenant-a", capID, map[string]any{"assetId": profileAsset, "jobId": profileJob}, true},
		{"other-job", "user-a", "tenant-a", capID, map[string]any{"assetId": profileAsset, "jobId": otherProfileJob}, false},
		{"other-asset", "user-a", "tenant-a", capID, map[string]any{"assetId": otherProfileJob, "jobId": profileJob}, false},
		{"other-subject", "user-b", "tenant-a", capID, map[string]any{"assetId": profileAsset, "jobId": profileJob}, false},
		{"other-tenant", "user-a", "tenant-b", capID, map[string]any{"assetId": profileAsset, "jobId": profileJob}, false},
		{"missing-scope", "user-a", "tenant-a", "mcp.connect", map[string]any{"assetId": profileAsset, "jobId": profileJob}, false},
		{"profile-content", "user-a", "tenant-a", capID, map[string]any{"assetId": profileAsset, "profileId": profileJob}, false},
		{"collection", "user-a", "tenant-a", capID, map[string]any{"view": "ASSETS"}, false},
		{"forged-context", "user-a", "tenant-a", capID, map[string]any{"assetId": profileAsset, "jobId": profileJob, "resourceType": "capability"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := calls.Load()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for k, v := range map[string]string{
					"X-OUF-Gateway-Verified": "true", "X-OUF-Delegation": "proof", "X-OUF-Service-Principal": "ouf-chatgpt",
					"X-OUF-Principal-ID": tc.subject, "X-OUF-Tenant-ID": tc.tenant, "X-OUF-Actor-Type": "HUMAN",
					"X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "gateway", "X-OUF-Granted-Scopes": tc.scope,
				} {
					r.Header.Set(k, v)
				}
				handler.ServeHTTP(w, r)
			}))
			defer server.Close()
			session, err := mcp.NewClient(&mcp.Implementation{Name: "profile-job", Version: "1"}, nil).Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "source.file.preview", Arguments: tc.args})
			if err != nil || result == nil || result.IsError == tc.allowed {
				t.Fatalf("unexpected result: %+v %v", result, err)
			}
			want := before
			if tc.allowed {
				want++
			}
			if calls.Load() != want {
				t.Fatal("denied input reached Gateway")
			}
			if tc.allowed {
				rejectOwner.Store(true)
				denied, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "source.file.preview", Arguments: tc.args})
				if err != nil || !denied.IsError || calls.Load() != want+1 {
					t.Fatal("owner denial was hidden or retried")
				}
				rejectOwner.Store(false)
			}
		})
	}
}
