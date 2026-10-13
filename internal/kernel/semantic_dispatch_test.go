package kernel

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/GioNob/ouf-mcp-server/internal/adapter/httpclient"
	"github.com/GioNob/ouf-mcp-server/internal/authorization"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func assertSemanticGatewaySchema(t *testing.T, name string, in orchestration.GatewayRequest) {
	t.Helper()
	if in.MaxResultBytes != 262144 {
		t.Errorf("semantic dispatch byte limit %d exceeds Gateway contract", in.MaxResultBytes)
	}
	root := os.Getenv("GATEWAY_SEMANTIC_PAIRWISE_ROOT")
	if root == "" {
		return
	}
	body, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("python3", "-B", "-c", "import json,sys,jsonschema; jsonschema.validate(json.load(sys.stdin),json.load(open(sys.argv[1])))", filepath.Join(root, "schemas", "mcp-gateway-semantic-"+name+"-dispatch-v1.json"))
	command.Stdin = bytes.NewReader(body)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("actual Gateway semantic %s schema rejected MCP envelope: %s: %v", name, output, err)
	}
}

func TestSemanticSearchUsesPublishedReadDescriptorAndPreservesArguments(t *testing.T) {
	const capID = "ouf.semantic.search"
	now := time.Now().UTC()
	cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{BundleID: "permissions", BundleVersion: 1, ActivatedAt: now, Bundle: authorization.PolicyBundle{BundleID: "permissions", Version: 1, PublishedAt: now, Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: capID, Operation: "READ", RequiredScope: capID, AllowedActors: []string{"HUMAN"}}}, Grants: []authorization.Grant{{GrantID: "proposer", CapabilityID: capID, TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}}}}})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/internal/capabilities/v1/execute/semantic/search" || r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "opaque-proof" {
			t.Error("incorrect governed transport")
		}
		var in orchestration.GatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatal(err)
		}
		var args map[string]any
		assertSemanticGatewaySchema(t, "search", in)
		json.Unmarshal(in.Arguments, &args)
		if args["q"] != "Cinema" || args["limit"] != float64(1) || args["type"] != "CLASS" || args["namespace"] != "urn:test:" || args["domain"] != "urn:test:domain" || args["range"] != "urn:test:range" || len(args) != 6 || in.OperationClass != "READ" || in.Owner != "semantic" || in.Identity.Delegation != "" {
			t.Error("semantic search arguments or owner changed")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `[]`)
	}))
	defer gateway.Close()
	u, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
	service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: &statusAdmission{}, Gateway: &httpclient.GatewayClient{Endpoint: u, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")}, FingerprintKey: make([]byte, 32)}
	handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []string{capID, "mcp.connect"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for k, v := range map[string]string{"X-OUF-Delegation": "opaque-proof", "X-OUF-Gateway-Verified": "true", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": "user-a", "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "gateway", "X-OUF-Granted-Scopes": scope} {
				r.Header.Set(k, v)
			}
			handler.ServeHTTP(w, r)
		}))
		client := mcp.NewClient(&mcp.Implementation{Name: "semantic-test", Version: "1"}, nil)
		session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
		if err != nil {
			t.Fatal(err)
		}
		tools, err := session.ListTools(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, tool := range tools.Tools {
			if tool.Name == "authorization.policy.admin" || tool.Name == "authorization.proposal.confirm" {
				t.Fatal("human admin exposed as tool")
			}
			if tool.Name == "semantic.search" && (tool.Annotations == nil || !tool.Annotations.ReadOnlyHint) {
				t.Fatal("semantic search mislabelled read-only")
			}
		}
		before := calls.Load()
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "semantic.search", Arguments: map[string]any{"q": "Cinema", "limit": 1, "type": "CLASS", "namespace": "urn:test:", "domain": "urn:test:domain", "range": "urn:test:range"}})
		if err != nil {
			t.Fatal(err)
		}
		if scope == capID {
			if result.IsError || calls.Load() != before+1 {
				t.Fatalf("semantic search failed: %+v", result)
			}
		} else if !result.IsError || calls.Load() != before {
			t.Fatal("unauthorized semantic search reached Gateway")
		}
		session.Close()
		server.Close()
	}
}

func TestSemanticGetPreservesExactPublishedReference(t *testing.T) {
	const capID = "ouf.semantic.consultation.read"
	const scopeID = "ouf.semantic.read"
	now := time.Now().UTC()
	cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{BundleID: "permissions", BundleVersion: 1, ActivatedAt: now, Bundle: authorization.PolicyBundle{BundleID: "permissions", Version: 1, PublishedAt: now, Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: capID, Operation: "READ", RequiredScope: scopeID, AllowedActors: []string{"HUMAN"}}}, Grants: []authorization.Grant{{GrantID: "proposer", CapabilityID: capID, TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}}}}})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/internal/capabilities/v1/execute/semantic/get" || r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "opaque-proof" {
			t.Error("incorrect governed transport")
		}
		var in orchestration.GatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatal(err)
		}
		var args map[string]any
		assertSemanticGatewaySchema(t, "get", in)
		json.Unmarshal(in.Arguments, &args)
		if args["semanticId"] != "urn:test:Cinema" || args["revisionId"] != "11111111-1111-4111-8111-111111111111" || args["publicationSetId"] != "22222222-2222-4222-8222-222222222222" || len(args) != 3 || in.OperationClass != "READ" || in.Owner != "semantic" || in.Identity.Delegation != "" {
			t.Error("semantic get arguments or owner changed")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `[]`)
	}))
	defer gateway.Close()
	u, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
	service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: &statusAdmission{}, Gateway: &httpclient.GatewayClient{Endpoint: u, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")}, FingerprintKey: make([]byte, 32)}
	handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []string{scopeID, "mcp.connect"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for k, v := range map[string]string{"X-OUF-Delegation": "opaque-proof", "X-OUF-Gateway-Verified": "true", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": "user-a", "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "gateway", "X-OUF-Granted-Scopes": scope} {
				r.Header.Set(k, v)
			}
			handler.ServeHTTP(w, r)
		}))
		client := mcp.NewClient(&mcp.Implementation{Name: "semantic-test", Version: "1"}, nil)
		session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
		if err != nil {
			t.Fatal(err)
		}
		tools, err := session.ListTools(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, tool := range tools.Tools {
			if tool.Name == "authorization.policy.admin" || tool.Name == "authorization.proposal.confirm" {
				t.Fatal("human admin exposed as tool")
			}
			if tool.Name == "semantic.get" && (tool.Annotations == nil || !tool.Annotations.ReadOnlyHint) {
				t.Fatal("semantic get mislabelled read-only")
			}
		}
		before := calls.Load()
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "semantic.get", Arguments: map[string]any{"semanticId": "urn:test:Cinema", "revisionId": "11111111-1111-4111-8111-111111111111", "publicationSetId": "22222222-2222-4222-8222-222222222222"}})
		if err != nil {
			t.Fatal(err)
		}
		if scope == scopeID {
			if result.IsError || calls.Load() != before+1 {
				t.Fatalf("semantic get failed: %+v", result)
			}
		} else if !result.IsError || calls.Load() != before {
			t.Fatal("unauthorized semantic get reached Gateway")
		}
		session.Close()
		server.Close()
	}
}

func TestSemanticProposalCreateUsesGovernedClosedBinding(t *testing.T) {
	const capID = "ouf.semantic.propose"
	now := time.Now().UTC()
	cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{BundleID: "permissions", BundleVersion: 1, ActivatedAt: now, Bundle: authorization.PolicyBundle{BundleID: "permissions", Version: 1, PublishedAt: now, Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: capID, Operation: "COMMAND", RequiredScope: "ouf.semantic.propose", AllowedActors: []string{"HUMAN"}}}, Grants: []authorization.Grant{{GrantID: "proposer", CapabilityID: capID, TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}}}}})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/internal/capabilities/v1/execute/semantic/proposals/create" || r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "opaque-proof" {
			t.Error("incorrect governed transport")
		}
		var in orchestration.GatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatal(err)
		}
		var args map[string]any
		assertSemanticGatewaySchema(t, "proposal-create", in)
		json.Unmarshal(in.Arguments, &args)
		if args["proposalKey"] != "teatri-test" || in.OperationClass != "COMMAND" || in.Owner != "semantic" || in.Identity.Delegation != "" {
			t.Error("proposal intent or owner changed")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `[]`)
	}))
	defer gateway.Close()
	u, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
	service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: &statusAdmission{}, Gateway: &httpclient.GatewayClient{Endpoint: u, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")}, FingerprintKey: make([]byte, 32)}
	handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []string{"ouf.semantic.propose", "mcp.connect"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for k, v := range map[string]string{"X-OUF-Delegation": "opaque-proof", "X-OUF-Gateway-Verified": "true", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": "user-a", "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "gateway", "X-OUF-Granted-Scopes": scope} {
				r.Header.Set(k, v)
			}
			handler.ServeHTTP(w, r)
		}))
		client := mcp.NewClient(&mcp.Implementation{Name: "semantic-test", Version: "1"}, nil)
		session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
		if err != nil {
			t.Fatal(err)
		}
		tools, err := session.ListTools(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, tool := range tools.Tools {
			if tool.Name == "authorization.policy.admin" || tool.Name == "authorization.proposal.confirm" {
				t.Fatal("human admin exposed as tool")
			}
			if tool.Name == "semantic.propose" && (tool.Annotations == nil || tool.Annotations.ReadOnlyHint != false) {
				t.Fatal("semantic search mislabelled read-only")
			}
		}
		before := calls.Load()
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "semantic.propose", Arguments: map[string]any{"proposalKey": "teatri-test", "commands": []any{map[string]any{"semanticId": "urn:test:Teatro", "namespace": "urn:test:", "localName": "Teatro", "artifactType": "CLASS", "ownerRef": "owner", "authorityRef": "authority", "semanticVersion": "1.0.0", "labels": map[string]any{"it": "Teatro"}, "definition": map[string]any{}}}}})
		if err != nil {
			t.Fatal(err)
		}
		if scope == "ouf.semantic.propose" {
			if result.IsError || calls.Load() != before+1 {
				t.Fatalf("semantic search failed: %+v", result)
			}
		} else if !result.IsError || calls.Load() != before {
			t.Fatal("unauthorized semantic search reached Gateway")
		}
		session.Close()
		server.Close()
	}
}

func TestSemanticProposalValidateUsesGovernedClosedBinding(t *testing.T) {
	const capID = "ouf.semantic.review.prepare"
	now := time.Now().UTC()
	cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{BundleID: "permissions", BundleVersion: 1, ActivatedAt: now, Bundle: authorization.PolicyBundle{BundleID: "permissions", Version: 1, PublishedAt: now, Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: capID, Operation: "COMMAND", RequiredScope: "ouf.semantic.review.prepare", AllowedActors: []string{"HUMAN"}}}, Grants: []authorization.Grant{{GrantID: "proposer", CapabilityID: capID, TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}}}}})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/internal/capabilities/v1/execute/semantic/proposals/validate" || r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "opaque-proof" {
			t.Error("incorrect governed transport")
		}
		var in orchestration.GatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatal(err)
		}
		var args map[string]any
		assertSemanticGatewaySchema(t, "proposal-validate", in)
		json.Unmarshal(in.Arguments, &args)
		if args["proposalKey"] != "teatri-test" || in.OperationClass != "COMMAND" || in.Owner != "semantic" || in.Identity.Delegation != "" {
			t.Error("proposal intent or owner changed")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `[]`)
	}))
	defer gateway.Close()
	u, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
	service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: &statusAdmission{}, Gateway: &httpclient.GatewayClient{Endpoint: u, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")}, FingerprintKey: make([]byte, 32)}
	handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []string{"ouf.semantic.review.prepare", "mcp.connect"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for k, v := range map[string]string{"X-OUF-Delegation": "opaque-proof", "X-OUF-Gateway-Verified": "true", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": "user-a", "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "gateway", "X-OUF-Granted-Scopes": scope} {
				r.Header.Set(k, v)
			}
			handler.ServeHTTP(w, r)
		}))
		client := mcp.NewClient(&mcp.Implementation{Name: "semantic-test", Version: "1"}, nil)
		session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
		if err != nil {
			t.Fatal(err)
		}
		tools, err := session.ListTools(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, tool := range tools.Tools {
			if tool.Name == "authorization.policy.admin" || tool.Name == "authorization.proposal.confirm" {
				t.Fatal("human admin exposed as tool")
			}
			if tool.Name == "semantic.review.prepare" && (tool.Annotations == nil || tool.Annotations.ReadOnlyHint != false) {
				t.Fatal("semantic search mislabelled read-only")
			}
		}
		before := calls.Load()
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "semantic.review.prepare", Arguments: map[string]any{"proposalKey": "teatri-test", "expectedVersion": 0}})
		if err != nil {
			t.Fatal(err)
		}
		if scope == "ouf.semantic.review.prepare" {
			if result.IsError || calls.Load() != before+1 {
				t.Fatalf("semantic search failed: %+v", result)
			}
		} else if !result.IsError || calls.Load() != before {
			t.Fatal("unauthorized semantic search reached Gateway")
		}
		session.Close()
		server.Close()
	}
}

func TestSemanticProposalRequestUsesGovernedClosedBinding(t *testing.T) {
	const capID = "ouf.semantic.approval.request"
	now := time.Now().UTC()
	cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{BundleID: "permissions", BundleVersion: 1, ActivatedAt: now, Bundle: authorization.PolicyBundle{BundleID: "permissions", Version: 1, PublishedAt: now, Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: capID, Operation: "COMMAND", RequiredScope: "ouf.semantic.approval.request", AllowedActors: []string{"HUMAN"}}}, Grants: []authorization.Grant{{GrantID: "proposer", CapabilityID: capID, TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}}}}})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/internal/capabilities/v1/execute/semantic/proposals/request" || r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "opaque-proof" {
			t.Error("incorrect governed transport")
		}
		var in orchestration.GatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatal(err)
		}
		var args map[string]any
		assertSemanticGatewaySchema(t, "proposal-request", in)
		json.Unmarshal(in.Arguments, &args)
		if args["proposalKey"] != "teatri-test" || in.OperationClass != "COMMAND" || in.Owner != "semantic" || in.Identity.Delegation != "" {
			t.Error("proposal intent or owner changed")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `[]`)
	}))
	defer gateway.Close()
	u, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
	service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: &statusAdmission{}, Gateway: &httpclient.GatewayClient{Endpoint: u, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")}, FingerprintKey: make([]byte, 32)}
	handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []string{"ouf.semantic.approval.request", "mcp.connect"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for k, v := range map[string]string{"X-OUF-Delegation": "opaque-proof", "X-OUF-Gateway-Verified": "true", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": "user-a", "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "gateway", "X-OUF-Granted-Scopes": scope} {
				r.Header.Set(k, v)
			}
			handler.ServeHTTP(w, r)
		}))
		client := mcp.NewClient(&mcp.Implementation{Name: "semantic-test", Version: "1"}, nil)
		session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
		if err != nil {
			t.Fatal(err)
		}
		tools, err := session.ListTools(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, tool := range tools.Tools {
			if tool.Name == "authorization.policy.admin" || tool.Name == "authorization.proposal.confirm" {
				t.Fatal("human admin exposed as tool")
			}
			if tool.Name == "semantic.approval.request" && (tool.Annotations == nil || tool.Annotations.ReadOnlyHint != false) {
				t.Fatal("semantic search mislabelled read-only")
			}
		}
		before := calls.Load()
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "semantic.approval.request", Arguments: map[string]any{"proposalKey": "teatri-test", "expectedVersion": 1}})
		if err != nil {
			t.Fatal(err)
		}
		if scope == "ouf.semantic.approval.request" {
			if result.IsError || calls.Load() != before+1 {
				t.Fatalf("semantic search failed: %+v", result)
			}
		} else if !result.IsError || calls.Load() != before {
			t.Fatal("unauthorized semantic search reached Gateway")
		}
		session.Close()
		server.Close()
	}
}

func TestSemanticProposalReadUsesGovernedClosedBinding(t *testing.T) {
	const capID = "ouf.semantic.proposal.read"
	now := time.Now().UTC()
	cache := authorization.NewCache(statusBundle{authorization.ActivePolicyBundle{BundleID: "permissions", BundleVersion: 1, ActivatedAt: now, Bundle: authorization.PolicyBundle{BundleID: "permissions", Version: 1, PublishedAt: now, Capabilities: []authorization.CapabilityDescriptor{{CapabilityID: capID, Operation: "READ", RequiredScope: "ouf.semantic.propose", AllowedActors: []string{"HUMAN"}}}, Grants: []authorization.Grant{{GrantID: "proposer", CapabilityID: capID, TenantID: "tenant-a", SubjectID: "user-a", ValidFrom: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour)}}}}})
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	gateway := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/internal/capabilities/v1/execute/semantic/proposals/read" || r.Header.Get("Authorization") != "Bearer workload" || r.Header.Get("X-OUF-Delegation") != "opaque-proof" {
			t.Error("incorrect governed transport")
		}
		var in orchestration.GatewayRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatal(err)
		}
		var args map[string]any
		assertSemanticGatewaySchema(t, "proposal-read", in)
		json.Unmarshal(in.Arguments, &args)
		if args["proposalKey"] != "teatri-test" || in.OperationClass != "READ" || in.Owner != "semantic" || in.Identity.Delegation != "" {
			t.Error("proposal intent or owner changed")
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `[]`)
	}))
	defer gateway.Close()
	u, _ := url.Parse(gateway.URL + "/internal/capabilities/v1/execute")
	service := &orchestration.Service{RequireDelegation: true, Auth: cache, Admission: &statusAdmission{}, Gateway: &httpclient.GatewayClient{Endpoint: u, Client: gateway.Client(), TokenSource: httpclient.StaticTokenSource("workload")}, FingerprintKey: make([]byte, 32)}
	handler, err := NewGovernedHTTPHandler(slog.New(slog.NewTextHandler(io.Discard, nil)), service)
	if err != nil {
		t.Fatal(err)
	}
	for _, scope := range []string{"ouf.semantic.propose", "mcp.connect"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for k, v := range map[string]string{"X-OUF-Delegation": "opaque-proof", "X-OUF-Gateway-Verified": "true", "X-OUF-Service-Principal": "ouf-chatgpt", "X-OUF-Principal-ID": "user-a", "X-OUF-Tenant-ID": "tenant-a", "X-OUF-Actor-Type": "HUMAN", "X-OUF-Authentication-Context-Ref": "1", "X-OUF-Token-Issuer": "issuer", "X-OUF-Token-Audience": "gateway", "X-OUF-Granted-Scopes": scope} {
				r.Header.Set(k, v)
			}
			handler.ServeHTTP(w, r)
		}))
		client := mcp.NewClient(&mcp.Implementation{Name: "semantic-test", Version: "1"}, nil)
		session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: server.URL}, nil)
		if err != nil {
			t.Fatal(err)
		}
		tools, err := session.ListTools(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, tool := range tools.Tools {
			if tool.Name == "authorization.policy.admin" || tool.Name == "authorization.proposal.confirm" {
				t.Fatal("human admin exposed as tool")
			}
			if tool.Name == "semantic.proposal.read" && (tool.Annotations == nil || tool.Annotations.ReadOnlyHint != true) {
				t.Fatal("semantic search mislabelled read-only")
			}
		}
		before := calls.Load()
		result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: "semantic.proposal.read", Arguments: map[string]any{"proposalKey": "teatri-test"}})
		if err != nil {
			t.Fatal(err)
		}
		if scope == "ouf.semantic.propose" {
			if result.IsError || calls.Load() != before+1 {
				t.Fatalf("semantic search failed: %+v", result)
			}
		} else if !result.IsError || calls.Load() != before {
			t.Fatal("unauthorized semantic search reached Gateway")
		}
		session.Close()
		server.Close()
	}
}
