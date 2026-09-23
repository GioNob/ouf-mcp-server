package operational

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/GioNob/ouf-mcp-server/internal/statusview"
	"github.com/GioNob/ouf-mcp-server/internal/trustedclaims"
)

type SelfStatusProvider interface {
	SystemStatus(context.Context, orchestration.Identity) ([]byte, error)
}

type ownerAPI struct {
	auth       orchestration.AuthorizationPort
	self       SelfStatusProvider
	aggregator *Aggregator
}

func NewOwnerAPI(self SelfStatusProvider, aggregator ...*Aggregator) *ownerAPI {
	api := &ownerAPI{self: self}
	if len(aggregator) > 0 {
		api.aggregator = aggregator[0]
	}
	return api
}

// WithAuthorization installs the owner-local policy evaluator. Status fails closed without it.
func (h *ownerAPI) WithAuthorization(auth orchestration.AuthorizationPort) *ownerAPI {
	h.auth = auth
	return h
}

func (h *ownerAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	if r.Header.Get("X-OUF-Gateway-Verified") != "true" {
		http.Error(w, "trusted Gateway context required", http.StatusUnauthorized)
		return
	}
	tenantID := strings.TrimSpace(r.Header.Get("X-OUF-Tenant-ID"))
	if tenantID == "" {
		http.Error(w, "tenant context required", http.StatusBadRequest)
		return
	}
	if h.self == nil {
		http.Error(w, "owner status provider unavailable", http.StatusServiceUnavailable)
		return
	}
	identity := orchestration.Identity{
		Delegation:               r.Header.Get("X-OUF-Delegation"),
		Issuer:                   r.Header.Get("X-OUF-Token-Issuer"),
		Audience:                 r.Header.Get("X-OUF-Token-Audience"),
		Scopes:                   strings.Fields(r.Header.Get("X-OUF-Granted-Scopes")),
		ServicePrincipalID:       r.Header.Get("X-OUF-Service-Principal"),
		PrincipalID:              r.Header.Get("X-OUF-Principal-ID"),
		TenantID:                 tenantID,
		ActorType:                r.Header.Get("X-OUF-Actor-Type"),
		AuthenticationContextRef: r.Header.Get("X-OUF-Authentication-Context-Ref"),
	}

	var body []byte
	var err error
	identity.Claims, err = trustedclaims.Read(r)
	if err != nil {
		http.Error(w, "invalid trusted claims", http.StatusBadRequest)
		return
	}
	switch r.URL.Path {
	case "/api/internal/v1/mcp/operations/status":
		if strings.TrimSpace(r.Header.Get("X-OUF-Principal-ID")) == "" || strings.TrimSpace(r.Header.Get("X-OUF-Authorization-Decision-Ref")) == "" {
			http.Error(w, "NOT_AUTHORIZED", http.StatusForbidden)
			return
		}
		if h.auth == nil {
			http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
			return
		}
		decision, authErr := h.auth.Authorize(r.Context(), orchestration.AuthorizationRequest{Identity: identity, CapabilityID: statusview.Capability, Owner: "mcp", OperationClass: "READ", Resource: orchestration.ResourceContext{ResourceType: "capability", TenantID: identity.TenantID, Attributes: map[string]string{"detailLevel": statusview.Public}}})
		if authErr != nil {
			http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
			return
		}
		if !decision.Allowed || decision.DecisionRef != r.Header.Get("X-OUF-Authorization-Decision-Ref") || decision.PermittedDetailLevel != statusview.Public || decision.ResourceScope["tenantId"] != identity.TenantID || decision.ResourceScope["resourceType"] != "capability" {
			http.Error(w, "NOT_AUTHORIZED", http.StatusForbidden)
			return
		}
		body, err = h.self.SystemStatus(r.Context(), identity)
		if err == nil {
			body, err = statusview.Project(body)
		}
	case "/api/internal/v1/mcp/operations/summary", "/api/internal/v1/mcp/operations/incidents":
		capability := "ouf.operations.summary"
		if strings.HasSuffix(r.URL.Path, "/incidents") {
			capability = "ouf.operations.incidents"
		}
		if identity.PrincipalID == "" || identity.Delegation == "" || r.Header.Get("X-OUF-Authorization-Decision-Ref") == "" {
			http.Error(w, "NOT_AUTHORIZED", http.StatusForbidden)
			return
		}
		if h.auth == nil {
			http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
			return
		}
		decision, authErr := h.auth.Authorize(r.Context(), orchestration.AuthorizationRequest{Identity: identity, CapabilityID: capability, Owner: "mcp", OperationClass: "READ", Resource: orchestration.ResourceContext{ResourceType: "capability", TenantID: identity.TenantID, Attributes: map[string]string{"detailLevel": "TENANT_OPERATIONAL"}}})
		if authErr != nil {
			http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
			return
		}
		if !decision.Allowed || decision.DecisionRef != r.Header.Get("X-OUF-Authorization-Decision-Ref") || decision.PermittedDetailLevel != "TENANT_OPERATIONAL" || decision.ResourceScope["tenantId"] != identity.TenantID || decision.ResourceScope["resourceType"] != "capability" {
			http.Error(w, "NOT_AUTHORIZED", http.StatusForbidden)
			return
		}

		if h.aggregator == nil {
			http.Error(w, "operational aggregator unavailable", http.StatusServiceUnavailable)
			return
		}
		raw, readErr := io.ReadAll(io.LimitReader(r.Body, (64<<10)+1))
		if readErr != nil || len(raw) > 64<<10 {
			http.Error(w, "invalid operational request", http.StatusBadRequest)
			return
		}
		if len(raw) == 0 {
			raw = []byte(`{}`)
		}
		var query incidentQuery
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&query) != nil || decoder.Decode(new(any)) != io.EOF || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			http.Error(w, "invalid operational request", http.StatusBadRequest)
			return
		}
		if query.State != nil && (capability != "ouf.operations.incidents" || (*query.State != "OPEN" && *query.State != "RECOVERING" && *query.State != "RESOLVED")) {
			http.Error(w, "invalid incident state", http.StatusBadRequest)
			return
		}
		limit := 50
		if query.Limit != nil {
			limit = *query.Limit
		}
		if limit < 1 || limit > 100 || (query.SourceID != nil && (len(*query.SourceID) < 1 || len(*query.SourceID) > 200)) {
			http.Error(w, "invalid operational request", http.StatusBadRequest)
			return
		}
		if capability == "ouf.operations.incidents" {
			if _, queryErr := prepareIncidentQuery(&query, identity); queryErr != nil {
				http.Error(w, "invalid incident query", http.StatusBadRequest)
				return
			}
		} else {
			if query.Until != nil || query.Cursor != nil || query.JobID != nil || query.Severity != nil {
				http.Error(w, "invalid summary query", http.StatusBadRequest)
				return
			}
			now := time.Now().UTC()
			if query.Since == nil {
				since := now.Add(-24 * time.Hour)
				query.Since = &since
			}
			if query.Since.After(now) || query.Since.Before(now.Add(-30*24*time.Hour)) {
				http.Error(w, "invalid operational window", http.StatusBadRequest)
				return
			}
		}
		query.Limit = &limit
		raw, err = json.Marshal(query)
		if err != nil {
			http.Error(w, "invalid operational request", http.StatusBadRequest)
			return
		}
		in := aggregateRequest{
			Identity: identity, Arguments: raw, AttemptID: r.Header.Get("X-Tool-Attempt-ID"),
			CorrelationID: r.Header.Get("X-Correlation-ID"), RequestedLimit: limit,
		}
		if r.URL.Path == "/api/internal/v1/mcp/operations/summary" {
			body, err = h.aggregator.Summary(r.Context(), in)
		} else {
			body, err = h.aggregator.Incidents(r.Context(), in)
		}
	default:
		http.NotFound(w, r)
		return
	}
	if errors.Is(err, orchestration.ErrUnauthorized) {
		http.Error(w, "NOT_AUTHORIZED", http.StatusForbidden)
		return
	}
	if err != nil {
		http.Error(w, "operational state unavailable", http.StatusServiceUnavailable)
		return
	}
	if !json.Valid(body) {
		http.Error(w, "invalid owner response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
