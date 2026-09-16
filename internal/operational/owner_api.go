package operational

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type SelfStatusProvider interface {
	SystemStatus(context.Context, orchestration.Identity) ([]byte, error)
}

type ownerAPI struct {
	self       SelfStatusProvider
	aggregator *Aggregator
}

func NewOwnerAPI(self SelfStatusProvider, aggregator ...*Aggregator) http.Handler {
	api := &ownerAPI{self: self}
	if len(aggregator) > 0 {
		api.aggregator = aggregator[0]
	}
	return api
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
		ServicePrincipalID:       r.Header.Get("X-OUF-Service-Principal"),
		PrincipalID:              r.Header.Get("X-OUF-Principal-ID"),
		TenantID:                 tenantID,
		ActorType:                r.Header.Get("X-OUF-Actor-Type"),
		AuthenticationContextRef: r.Header.Get("X-OUF-Authentication-Context-Ref"),
	}

	var body []byte
	var err error
	switch r.URL.Path {
	case "/api/internal/v1/mcp/operations/status":
		body, err = h.self.SystemStatus(r.Context(), identity)
	case "/api/internal/v1/mcp/operations/summary", "/api/internal/v1/mcp/operations/incidents":
		if h.aggregator == nil {
			http.Error(w, "operational aggregator unavailable", http.StatusServiceUnavailable)
			return
		}
		raw, readErr := io.ReadAll(io.LimitReader(r.Body, 64<<10))
		if readErr != nil {
			http.Error(w, "invalid operational request", http.StatusBadRequest)
			return
		}
		if len(raw) == 0 {
			raw = []byte(`{}`)
		}
		var query struct {
			Limit int `json:"limit"`
		}
		if json.Unmarshal(raw, &query) != nil {
			http.Error(w, "invalid operational request", http.StatusBadRequest)
			return
		}
		in := aggregateRequest{
			Identity: identity, Arguments: raw, AttemptID: r.Header.Get("X-Tool-Attempt-ID"),
			CorrelationID: r.Header.Get("X-Correlation-ID"), RequestedLimit: query.Limit,
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
	if !json.Valid(body) {
		http.Error(w, "invalid owner response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
