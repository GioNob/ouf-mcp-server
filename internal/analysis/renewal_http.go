package analysis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

const renewalRoutes = "/internal/analysis/v1/renewals/"

type RenewalChannel struct {
	BodyBytes                                                        int64
	ProposalSeconds                                                  int
	ReviewBase, CheckEndpoint, Issuer, Audience, Workload, THSClient string
}

func RenewalConfiguration() (RenewalChannel, error) {
	c := RenewalChannel{ReviewBase: os.Getenv("OUF_ANALYSIS_RENEWAL_REVIEW_BASE"), CheckEndpoint: os.Getenv("OUF_ANALYSIS_RENEWAL_CHECK_ENDPOINT"),
		Issuer: os.Getenv("OUF_ANALYSIS_RENEWAL_ISSUER"), Audience: os.Getenv("OUF_ANALYSIS_RENEWAL_AUDIENCE"),
		Workload: os.Getenv("OUF_ANALYSIS_WORKLOAD"), THSClient: os.Getenv("OUF_ANALYSIS_RENEWAL_THS_CLIENT")}
	if raw, set := os.LookupEnv("OUF_ANALYSIS_RENEWAL_PROPOSAL_SECONDS"); set {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxRenewalSeconds {
			return c, ErrInvalid
		}
		c.ProposalSeconds = n
	}
	if raw, set := os.LookupEnv("OUF_ANALYSIS_RENEWAL_BODY_BYTES"); set {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 1 || n > RenewalBodyMax {
			return c, ErrInvalid
		}
		c.BodyBytes = n
	}
	for _, setting := range [][2]string{{c.ReviewBase, "/trusted-human/analysis-renewal/"}, {c.CheckEndpoint, RenewalCheckPath}} {
		endpoint, path := setting[0], setting[1]
		if endpoint == "" {
			continue
		}
		u, err := url.Parse(endpoint)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(endpoint, "#") || u.EscapedPath() != path {
			return c, ErrInvalid
		}
	}
	for _, pin := range []string{c.Issuer, c.Audience, c.THSClient, c.Workload} {
		if pin != strings.TrimSpace(pin) {
			return c, ErrInvalid
		}
	}
	if c.THSClient != "" && c.THSClient == c.Workload {
		return c, ErrInvalid
	}
	return c, nil
}

func (c RenewalChannel) Configured() bool {
	return c.ReviewBase != "" && c.CheckEndpoint != "" && c.Issuer != "" && c.Audience != "" && c.Workload != "" && c.THSClient != ""
}

// The signing key stays at the Gateway. Only the pinned, verified TLS
// client may attest a forwarded context. The Gateway checks the MAC again
// when the opaque decide proof is returned on renewal-check.
func (c RenewalChannel) identity(r *http.Request, gateway, action string, raw []byte, now time.Time) (orchestration.Identity, time.Time, error) {
	var i orchestration.Identity
	deny := orchestration.ErrUnauthorized
	var end time.Time
	if !gatewayPeer(r, gateway) || len(r.Header.Values("X-OUF-Delegation")) != 1 {
		return i, end, deny
	}
	proof := r.Header.Get("X-OUF-Delegation")
	encoded, mac, ok := strings.Cut(proof, ".")
	if !ok || len(proof) > 16384 || len(mac) != 43 {
		return i, end, deny
	}
	encoding := base64.RawURLEncoding.Strict()
	if _, err := encoding.DecodeString(mac); err != nil {
		return i, end, deny
	}
	payload, err := encoding.DecodeString(encoded)
	if err != nil || !ValidEvidenceJSON(payload) {
		return i, end, deny
	}
	if _, ok = closedFields(payload, "v", "purpose", "aud", "iss", "workload", "principal", "tenant", "actor", "client", "acr", "scope", "iat", "exp", "requestHash"); !ok {
		return i, end, deny
	}
	var p struct {
		V, Iat, Exp                                                                            int64
		Purpose, Aud, Iss, Workload, Principal, Tenant, Actor, Client, Acr, Scope, RequestHash string
	}
	if json.Unmarshal(payload, &p) != nil || p.V != 1 || p.Purpose != "analysis-renewal-"+action || p.Aud != c.Audience || p.Iss != c.Issuer ||
		p.Workload != c.Workload || p.Actor != "HUMAN" || p.Client != c.THSClient || p.Principal == "" || p.Tenant == "" || p.Acr == "" ||
		p.Scope != "ouf.analysis.renewal."+action || p.Iat > now.Unix() || p.Iat < now.Unix()-60 || p.Exp <= now.Unix() || p.Exp > p.Iat+60 ||
		utf8.RuneCountInString(p.Principal) > 4096 || utf8.RuneCountInString(p.Acr) > 4096 || p.RequestHash != BytesDigest(raw) {
		return i, end, deny
	}
	return orchestration.Identity{Issuer: p.Iss, Audience: p.Aud, PrincipalID: p.Principal, TenantID: p.Tenant, ServicePrincipalID: p.Workload, ActorType: p.Actor,
		AuthenticationContextRef: p.Acr, Delegation: proof}, time.Unix(p.Exp, 0), nil
}

type RenewalBackoff struct {
	Cause      error
	RetryAfter string
	NotBefore  time.Time
}

func (e *RenewalBackoff) Error() string { return e.Cause.Error() }
func (e *RenewalBackoff) Unwrap() error { return e.Cause }

func renewalProblem(w http.ResponseWriter, err error) {
	status, code := 503, ErrIssueUnavailable.Error()
	switch {
	case errors.Is(err, ErrInvalid):
		status, code = 400, ErrInvalid.Error()
	case errors.Is(err, orchestration.ErrUnauthorized):
		status, code = 403, "ANALYSIS_AUTHORITY_DENIED"
	case errors.Is(err, ErrScope):
		status, code = 404, "ANALYSIS_MANDATE_UNKNOWN"
	case errors.Is(err, ErrCommandConflict), errors.Is(err, ErrRenewalUsed), errors.Is(err, ErrFenced):
		status, code = 409, SafeError(err)
	case errors.Is(err, ErrRunExhausted):
		status, code = 409, ErrFenced.Error()
	case errors.Is(err, ErrMandateExpired):
		status, code = 410, ErrMandateExpired.Error()
	case errors.Is(err, ErrRunPressure):
		status, code = 429, ErrRunPressure.Error()
	}
	var retry *RenewalBackoff
	body := map[string]string{"code": code}
	if (status == 429 || status == 503) && errors.As(err, &retry) && retry.RetryAfter != "" {
		w.Header().Set("Retry-After", retry.RetryAfter)
		if !retry.NotBefore.IsZero() {
			body["notBefore"] = retry.NotBefore.UTC().Format(time.RFC3339Nano)
		}
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func RenewalHandler(store RenewalStore, authority RunRenewalAuthority, c RenewalChannel, gateway string, clock func() time.Time) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		action := strings.TrimPrefix(r.URL.Path, renewalRoutes)
		if action != "read" && action != "decide" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			w.WriteHeader(405)
			return
		}
		if !c.Configured() || authority == nil || store == nil {
			renewalProblem(w, ErrIssueUnavailable)
			return
		}
		limit := c.BodyBytes
		if limit == 0 {
			limit = RenewalBodyMax
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
		if err != nil {
			renewalProblem(w, ErrInvalid)
			return
		}
		now := clock()
		i, end, err := c.identity(r, gateway, action, raw, now)
		if err != nil {
			renewalProblem(w, err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), end.Sub(now))
		defer cancel()
		var out any
		if action == "read" {
			var b struct{ ReviewRef string }
			if _, ok := closedFields(raw, "reviewRef"); !ok || !ValidEvidenceJSON(raw) || json.Unmarshal(raw, &b) != nil || !reviewRefPattern.MatchString(b.ReviewRef) {
				renewalProblem(w, ErrInvalid)
				return
			}
			out, err = store.ReadRenewal(ctx, i, b.ReviewRef)
		} else {
			var command RenewalCommand
			command, err = ParseRenewalCommand(raw)
			if err == nil {
				out, err = store.DecideRenewal(ctx, i, command, authority)
			}
		}
		if err != nil {
			renewalProblem(w, err)
			return
		}
		if !clock().Before(end) {
			renewalProblem(w, orchestration.ErrUnauthorized)
			return
		}
		result, err := json.Marshal(out)
		if err != nil || int64(len(result)) > limit {
			renewalProblem(w, ErrIssueUnavailable)
			return
		}
		_, _ = w.Write(result)
	})
}
