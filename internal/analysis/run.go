package analysis

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const RunContract = "ouf.analysis.run.v2"
const MandateContract = "ouf.analysis.mandate.v1"
const RunBudgetProfile = "analysis-run-v2"

var ErrCommandConflict = errors.New("ANALYSIS_COMMAND_CONFLICT")
var ErrRunDrift = errors.New("ANALYSIS_SCOPE_DRIFT")
var ErrRunPressure = errors.New("ANALYSIS_BACKOFF")
var ErrRunExhausted = errors.New("ANALYSIS_BUDGET_EXHAUSTED")
var ErrMandateExpired = errors.New("ANALYSIS_MANDATE_EXPIRED")

// Positive integer ceilings. A reservation charges its full maximum until
// an authenticated, fenced receipt replaces it with actual consumption.
type RunCost struct {
	Calls, Attempts, Objects, Bytes, Millis int64
}

func (c RunCost) Fits(limit RunCost) bool {
	return c.Calls >= 0 && c.Calls <= limit.Calls && c.Attempts >= 0 && c.Attempts <= limit.Attempts &&
		c.Objects >= 0 && c.Objects <= limit.Objects && c.Bytes >= 0 && c.Bytes <= limit.Bytes &&
		c.Millis >= 0 && c.Millis <= limit.Millis
}

func (c RunCost) Add(d RunCost) RunCost {
	return RunCost{c.Calls + d.Calls, c.Attempts + d.Attempts, c.Objects + d.Objects, c.Bytes + d.Bytes, c.Millis + d.Millis}
}

type RunLimits struct {
	OwnerDedupQualified                                                                     bool
	Maximum                                                                                 RunCost
	Shared                                                                                  RunSharedLimits
	ProofSeconds, WaitSeconds, LifetimeSeconds, ActivePlans, ProviderSlots, PreviewPageSize int

	RenewalPercent int `json:",omitempty"`
	RenewalSeconds int `json:",omitempty"`
}

type RunSharedLimits struct {
	TenantBytes, SourceBytes                                            int64
	TenantQueue, SourceQueue, TenantSlots, SourceSlots, DependencySlots int
	TenantPerMinute, SourcePerMinute, DependencyPerMinute               int
}

func (l RunSharedLimits) Valid() bool {
	if l.SourceBytes < 1 || l.TenantBytes < l.SourceBytes || l.TenantBytes > 1<<44 {
		return false
	}
	for _, n := range []int{l.TenantQueue, l.SourceQueue, l.TenantSlots, l.SourceSlots, l.DependencySlots, l.TenantPerMinute, l.SourcePerMinute, l.DependencyPerMinute} {
		if n < 1 || n > 100000 {
			return false
		}
	}
	return true
}

func (l RunLimits) Valid() bool {
	return l.ProofSeconds >= 1 && l.ProofSeconds <= 30 && l.Shared.Valid() && l.PreviewPageSize >= 1 && l.PreviewPageSize <= 128 && (RunCost{1, 1, 1, 1, 1}).Fits(l.Maximum) && l.Maximum.Fits(RunCost{1000000, 1000000, 100000000, 1 << 40, 86400000}) &&
		l.WaitSeconds >= 1 && l.WaitSeconds <= 3600 && l.LifetimeSeconds >= 1 && l.LifetimeSeconds <= 86400 &&
		l.RenewalPercent >= 0 && l.RenewalPercent <= maxRenewalPercent && l.RenewalSeconds >= 0 && l.RenewalSeconds <= maxRenewalSeconds &&
		l.ActivePlans >= 1 && l.ActivePlans <= 128 && l.ProviderSlots >= 1 && l.ProviderSlots <= 128
}

type RunSpec struct {
	ProposalSeconds                                int    `json:",omitempty"`
	SourceFile                                     string `json:",omitempty"`
	Onboarding                                     bool
	Scope                                          Scope
	Issuer, Workload, CommandKey, Manifest, Policy string
	Profile                                        Profile
	Limits                                         RunLimits
}

func (s RunSpec) ID() string { return AnalysisID(s.Scope, s.Profile) }

func (s RunSpec) Valid() bool {
	key, err := uuid.Parse(s.CommandKey)
	return s.ProposalSeconds >= 0 && s.ProposalSeconds <= maxRenewalSeconds && err == nil && key.String() == s.CommandKey && key.Version() == 4 && key.Variant() == uuid.RFC4122 &&
		s.Scope.Valid() && s.Issuer != "" && s.Workload != "" && s.Policy != "" && s.Limits.Valid() && s.Profile.Valid() &&
		CanonicalUUID(s.Profile.AssetID) && CanonicalUUID(s.Profile.ProfileID) && strings.ToLower(s.Profile.SchemaHash) == s.Profile.SchemaHash &&
		ValidateID(s.Manifest) == nil && strings.ToLower(s.Manifest) == s.Manifest
}

type Mandate struct {
	ID         string    `json:"mandateId"`
	Generation string    `json:"generation"`
	Decision   string    `json:"decisionRef"`
	Issued     time.Time `json:"issuedAt"`
	Expires    time.Time `json:"expiresAt"`
	Operations []string  `json:"allowedOperations"`
}

func AllowedOperation(op string) bool {
	switch op {
	case "source.file.preview", "ouf.semantic.search.batch", "semantic.discovery.request", "semantic.discovery.status", "semantic.discovery.candidates":
		return true
	}
	return false
}

func DecodeMandate(raw []byte) (Mandate, error) {
	var m Mandate
	if !ValidEvidenceJSON(raw) {
		return m, ErrInvalid
	}
	_, ok := closedFields(raw, "mandateId", "generation", "decisionRef", "issuedAt", "expiresAt", "allowedOperations")
	if !ok || json.Unmarshal(raw, &m) != nil || !CanonicalUUID(m.ID) || len(m.Decision) == 0 || len([]rune(m.Decision)) > 256 ||
		m.Issued.IsZero() || !m.Expires.After(m.Issued) || !slices.Contains(m.Operations, "source.file.preview") {
		return Mandate{}, ErrInvalid
	}
	gen, err := strconv.ParseInt(m.Generation, 10, 64)
	if err != nil || gen < 0 || strconv.FormatInt(gen, 10) != m.Generation {
		return Mandate{}, ErrInvalid
	}
	for i, op := range m.Operations {
		if !AllowedOperation(op) || slices.Contains(m.Operations[:i], op) {
			return Mandate{}, ErrInvalid
		}
	}
	return m, nil
}

func MandateStatus(m Mandate, raw []byte) (state, decision string) {
	var status struct {
		MandateID, Generation, State, DecisionRef string
		ExpiresAt                                 time.Time
	}
	_, ok := closedFields(raw, "mandateId", "generation", "state", "expiresAt", "decisionRef")
	if !ok || !ValidEvidenceJSON(raw) || json.Unmarshal(raw, &status) != nil ||
		status.MandateID != m.ID || status.Generation != m.Generation || status.DecisionRef == "" ||
		len(status.DecisionRef) > 256 || status.ExpiresAt.IsZero() || (status.State == "ACTIVE" && !status.ExpiresAt.Equal(m.Expires)) {
		return "DRIFT", ""
	}
	switch status.State {
	case "ACTIVE", "REVOKED", "EXPIRED":
		return status.State, status.DecisionRef
	default:
		return "DRIFT", ""
	}
}

type RunRecord struct {
	RunSpec
	Mandate                                 Mandate
	Started, Deadline, BudgetEnd, NotBefore time.Time
	State, Reason, MandateState             string
	Seq                                     int64
	Fence                                   int64 `json:"-"`
}

// Gateway hashes sorted ASCII object keys, UTF-8 strings and decimal
// integers. UseNumber preserves integers beyond the binary64 exact range.
func MandateHash(raw []byte) (string, error) {
	wire, err := mandateWire(raw)
	if err != nil {
		return "", err
	}
	return BytesDigest([]byte(wire)), nil
}

func mandateWire(raw []byte) (string, error) {
	if !ValidEvidenceJSON(raw) {
		return "", ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var value map[string]any
	if d.Decode(&value) != nil || value == nil {
		return "", ErrInvalid
	}
	var b strings.Builder
	if err := writeMandateValue(&b, value); err != nil {
		return "", err
	}
	return b.String(), nil
}

func BytesDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func writeMandateValue(b *strings.Builder, value any) error {
	switch v := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			for _, c := range k {
				if c > 127 {
					return ErrInvalid
				}
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(jcsString(k))
			b.WriteByte(':')
			if err := writeMandateValue(b, v[k]); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	case []any:
		b.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := writeMandateValue(b, item); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case string:
		b.WriteString(jcsString(v))
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return ErrInvalid
		}
		b.WriteString(strconv.FormatInt(n, 10))
	case bool:
		b.WriteString(strconv.FormatBool(v))
	default:
		return ErrInvalid
	}
	return nil
}
