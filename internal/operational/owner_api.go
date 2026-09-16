package operational

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

type SelfStatusProvider interface {
	SystemStatus(context.Context, orchestration.Identity) ([]byte, error)
}

type ownerAPI struct{ self SelfStatusProvider }

func NewOwnerAPI(self SelfStatusProvider) http.Handler {
	return &ownerAPI{self: self}
}

func (h *ownerAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.Method != http.MethodPost || r.URL.Path != "/api/internal/v1/mcp/operations/status" {
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
	body, err := h.self.SystemStatus(r.Context(), identity)
	if err != nil {
		http.Error(w, "system status unavailable", http.StatusServiceUnavailable)
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
