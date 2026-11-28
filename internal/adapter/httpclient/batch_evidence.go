package httpclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
)

type BatchEvidenceStore interface {
	BatchMandateEvidence(context.Context, analysis.BatchEvidenceRequest, string, time.Time) (analysis.BatchEvidence, error)
}

type BatchEvidenceHandler struct {
	Store             BatchEvidenceStore
	ManifestChecksum  string
	GatewayLeafSHA256 [sha256.Size]byte
	Clock             func() (time.Time, bool)
}

func (h BatchEvidenceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	peer := sha256.Sum256(r.TLS.PeerCertificates[0].Raw)
	if subtle.ConstantTimeCompare(peer[:], h.GatewayLeafSHA256[:]) != 1 {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 65536))
	var input analysis.BatchEvidenceRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err != nil || !json.Valid(body) || decoder.Decode(&input) != nil || !validEvidenceRequest(input) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	now, synced := h.Clock()
	if !synced {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	proof, err := h.Store.BatchMandateEvidence(r.Context(), input, h.ManifestChecksum, now)
	observed, synced := h.Clock()
	validUntil := observed.Add(30 * time.Second)
	if !proof.LeaseUntil.IsZero() && proof.LeaseUntil.Before(validUntil) {
		validUntil = proof.LeaseUntil
	}
	if err != nil || !synced || observed.Before(now) || validUntil.UnixMilli() <= observed.UnixMilli() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	hash := sha256.Sum256(body)
	_ = json.NewEncoder(w).Encode(struct {
		analysis.BatchEvidence
		RequestHash       string `json:"requestHash"`
		ClockSynchronized bool   `json:"clockSynchronized"`
		ObservedAt        int64  `json:"observedAtMs"`
		ValidUntil        int64  `json:"validUntilMs"`
	}{proof, hex.EncodeToString(hash[:]), true, observed.UnixMilli(), validUntil.UnixMilli()})
}

func validEvidenceRequest(in analysis.BatchEvidenceRequest) bool {
	if in.Contract != analysis.BatchMandateContract || in.Generation != 1 || !analysis.ValidBatchAttempt(in.Nonce) ||
		!analysis.ValidBatchHash(in.AnalysisID) || !analysis.ValidBatchHash(in.SchemaHash) ||
		!analysis.ValidBatchAttempt(in.StartRef) || !analysis.ValidBatchAttempt(in.CommandKey) {
		return false
	}
	switch in.Action {
	case "dispatch":
		stage := analysis.BatchStageForOperation(in.Operation)
		read := stage == analysis.BatchStatus || stage == analysis.BatchCandidates
		return in.Version > 0 && stage != "" && analysis.ValidBatchHash(in.ArgumentsHash) &&
			analysis.ValidBatchAttempt(in.AttemptID) &&
			((read && analysis.CanonicalUUID(in.RequestID)) || (!read && in.RequestID == ""))
	case "issue", "expire":
		return (in.Action == "issue" && in.Version == 0 || in.Action == "expire" && in.Version > 0) &&
			in.Operation == "" && in.ArgumentsHash == "" && in.AttemptID == "" && in.RequestID == ""
	default:
		return false
	}
}
