package operational

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/GioNob/ouf-mcp-server/internal/statusview"
	"github.com/GioNob/ouf-mcp-server/internal/trustedclaims"
)

type SelfStatusProvider interface {
	SystemStatus(context.Context, orchestration.Identity) ([]byte, error)
}

type ownerAPI struct {
	delegationWorkload string
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

// WithAuthorization installs the owner-local policy evaluator. All reads fail closed without it.
func (h *ownerAPI) WithAuthorization(auth orchestration.AuthorizationPort) *ownerAPI {
	h.auth = auth
	return h
}

// WithOperationalDelegation binds the only workload allowed to consume the
// Gateway-verified aggregate context. Empty configuration remains fail-closed.
func (h *ownerAPI) WithOperationalDelegation(workload string) *ownerAPI {
    h.delegationWorkload = strings.TrimSpace(workload)
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
		if !h.authorize(w, r, identity, statusview.Capability, statusview.Public) {
			return
		}
		body, err = h.self.SystemStatus(r.Context(), identity)
		if err == nil {
			body, err = statusview.Project(body)
		}
	case "/api/internal/v1/mcp/operations/summary", "/api/internal/v1/mcp/operations/incidents":
		if h.aggregator == nil {
			http.Error(w, "operational aggregator unavailable", http.StatusServiceUnavailable)
			return
		}
		capability := "ouf.operations.incidents"
		if r.URL.Path == "/api/internal/v1/mcp/operations/summary" {
			capability = "ouf.operations.summary"
		}
		if !h.authorize(w, r, identity, capability, "TENANT_OPERATIONAL") {
			return
		}
		if !h.aggregateDelegation(w, r, &identity, capability) {
            return
        }
        raw, readErr := io.ReadAll(io.LimitReader(r.Body, (64<<10)+1))
		if readErr != nil {
			http.Error(w, "invalid operational request", http.StatusBadRequest)
			return
		}
		if len(raw) > 64<<10 {
			http.Error(w, "operational request too large", http.StatusRequestEntityTooLarge)
			return
		}
		if len(raw) == 0 {
			raw = []byte(`{}`)
		}
		limit, valid := operationalQuery(raw, capability == "ouf.operations.incidents")
		if !valid {
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
	if err != nil {
		http.Error(w, "operational state unavailable", http.StatusServiceUnavailable)
		return
	}
	if len(body) > 512<<10 {
		http.Error(w, "operational result budget exceeded", http.StatusServiceUnavailable)
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

func (h *ownerAPI) authorize(w http.ResponseWriter, r *http.Request, identity orchestration.Identity, capability, detail string) bool {
	if strings.TrimSpace(identity.PrincipalID) == "" || strings.TrimSpace(r.Header.Get("X-OUF-Authorization-Decision-Ref")) == "" {
		http.Error(w, "NOT_AUTHORIZED", http.StatusForbidden)
		return false
	}
	if h.auth == nil {
		http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
		return false
	}
	decision, err := h.auth.Authorize(r.Context(), orchestration.AuthorizationRequest{
		Identity: identity, CapabilityID: capability, Owner: "mcp", OperationClass: "READ",
		Resource: orchestration.ResourceContext{ResourceType: "capability", TenantID: identity.TenantID, Attributes: map[string]string{"detailLevel": detail}},
	})
	if err != nil {
		http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
		return false
	}
	if !decision.Allowed || decision.DecisionRef != r.Header.Get("X-OUF-Authorization-Decision-Ref") || decision.PermittedDetailLevel != detail || decision.ResourceScope["tenantId"] != identity.TenantID || decision.ResourceScope["resourceType"] != "capability" {
		http.Error(w, "NOT_AUTHORIZED", http.StatusForbidden)
		return false
	}
	return true
}

// Match the published typed query, including channel-neutral callers. Reject
// duplicate keys so different producer parsers cannot interpret a filter differently.
func operationalQuery(raw []byte, incidents bool) (int, bool) {
	if !utf8.Valid(raw) {
		return 0, false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return 0, false
	}
	seen := make(map[string]bool)
	limit := 50
	for decoder.More() {
		token, err = decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return 0, false
		}
		seen[key] = true
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return 0, false
		}
		switch key {
		case "limit":
			var requested *int
			if json.Unmarshal(value, &requested) != nil || requested == nil || *requested < 1 || *requested > 100 {
				return 0, false
			}
			limit = *requested
		case "sourceId", "state", "since":
			var text string
			if json.Unmarshal(value, &text) != nil || text == "" {
				return 0, false
			}
			switch key {
			case "sourceId":
				if utf8.RuneCountInString(text) > 200 {
					return 0, false
				}
			case "state":
				if !incidents || (text != "OPEN" && text != "RECOVERING" && text != "RESOLVED") {
					return 0, false
				}
			case "since":
				if _, err := time.Parse(time.RFC3339Nano, text); err != nil {
					return 0, false
				}
			}
		default:
			return 0, false
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return 0, false
	}
	if _, err = decoder.Token(); err != io.EOF {
		return 0, false
	}
	return limit, true
}

// This is verified Gateway context, not a signature verifier or a grant.
// Owner NetworkPolicy and the Gateway marker remain prerequisites; every
// subsequent governed Gateway hop verifies the original proof and current IAM.
func (h *ownerAPI) aggregateDelegation(w http.ResponseWriter, r *http.Request, identity *orchestration.Identity, capability string) bool {
    proof := r.Header.Get("X-OUF-Delegation")
    marker := r.Header.Get("X-OUF-Delegation-Verified")
    if proof == "" && marker == "" {
        // Channel-neutral legacy projections remain explicitly partial when
        // their governed caller requires delegation and has none.
        return true
    }
    if h.delegationWorkload == "" || marker != "mcp-operational-aggregate-v1" ||
        identity.ActorType != "HUMAN" || identity.ServicePrincipalID != h.delegationWorkload ||
        r.Header.Get("X-OUF-Capability-ID") != capability || !boundedDelegationProof(proof) {
        http.Error(w, "invalid aggregate delegated context", http.StatusForbidden)
        return false
    }
    identity.Delegation = proof
    return true
}

func boundedDelegationProof(proof string) bool {
    if len(proof) > 16384 { return false }
    parts := strings.Split(proof, ".")
    if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) != 43 { return false }
    for _, part := range parts {
        for _, char := range part {
            if !(char >= 'a' && char <= 'z') && !(char >= 'A' && char <= 'Z') &&
                !(char >= '0' && char <= '9') && char != '_' && char != '-' { return false }
        }
    }
    return true
}
