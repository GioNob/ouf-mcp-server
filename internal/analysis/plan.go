// Package analysis coordinates finite, profile-bound semantic evidence work.
// It grants no semantic adoption, publication or source-mapping authority.
package analysis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/google/uuid"
)

const ProfileVersion = "ouf.profile-analysis.v1"

// Capacity bounds, not performance measurements or quotas granted per column.
const MaxTasks = 16384
const MaxPlanBatch = 128
const MaxResultBytes = 262144
const MaxRetainedBytes = 64 << 20
const MaxRetryAttempts = 128

var ErrInvalid = errors.New("ANALYSIS_INPUT_INVALID")
var ErrScope = errors.New("ANALYSIS_NOT_FOUND_OR_SCOPE_CHANGED")
var ErrConflict = errors.New("ANALYSIS_REVISION_CONFLICT")
var ErrCapacity = errors.New("ANALYSIS_CAPACITY_HELD")
var ErrUnavailable = errors.New("ANALYSIS_CAPABILITY_INACTIVE")
var ErrFenced = errors.New("ANALYSIS_STALE_EXECUTION")

type Scope struct{ Service, Principal, Tenant string }

func IdentityScope(i orchestration.Identity) Scope {
	return Scope{i.ServicePrincipalID, i.PrincipalID, i.TenantID}
}
func (s Scope) Valid() bool { return s.Service != "" && s.Principal != "" && s.Tenant != "" }

type Profile struct {
	AssetID     string `json:"assetId"`
	ProfileID   string `json:"profileId"`
	Version     int    `json:"profileVersion"`
	SchemaHash  string `json:"schemaHash"`
	ColumnCount int    `json:"columnCount"`
}

func (p Profile) Valid() bool {
	b, e := hex.DecodeString(strings.TrimPrefix(p.SchemaHash, "sha256:"))
	return uuid.Validate(p.AssetID) == nil && uuid.Validate(p.ProfileID) == nil && p.Version > 0 && strings.HasPrefix(p.SchemaHash, "sha256:") && e == nil && len(b) == 32 && p.ColumnCount >= 1 && p.ColumnCount <= 1024
}

type Work struct {
	Tool      string          `json:"tool"`
	Arguments json.RawMessage `json:"arguments"`
	Columns   []int           `json:"columns"`
	// Only status observations have a sequence. Each is useful planned work,
	// never an automatic retry or a way to reset the underlying retry guard.
	Observation int `json:"observation,omitempty"`
}
type Task struct {
	ID string `json:"taskId"`
	Work
	State          string          `json:"state"`
	Code           string          `json:"code,omitempty"`
	Attempts       int             `json:"attempts"`
	Calls          int64           `json:"calls"`
	ResultBytes    int64           `json:"resultBytes"`
	CallTimeMS     int64           `json:"callTimeMs"`
	Waits          int64           `json:"windowWaits"`
	MaxBytes       int64           `json:"maxBytes"`
	MaxTimeMS      int64           `json:"maxTimeMs"`
	MaxObjects     int64           `json:"maxObjects"`
	Result         json.RawMessage `json:"result,omitempty"`
	AttemptID      string          `json:"attemptId,omitempty"`
	NotBefore      time.Time       `json:"notBefore,omitempty"`
	FirstAttemptAt time.Time       `json:"firstAttemptAt,omitempty"`
	CompletedAt    time.Time       `json:"completedAt,omitempty"`
	LeaseUntil     time.Time       `json:"leaseUntil,omitempty"`
	Fence          string          `json:"-"`
}
type Budget struct {
	PlannedCalls         int64 `json:"plannedCalls"`
	MaxUsefulCallTimeMS  int64 `json:"maxUsefulCallTimeMs"`
	MaxUsefulResultBytes int64 `json:"maxUsefulResultBytes"`
	Calls                int64 `json:"observedCalls"`
	CallTimeMS           int64 `json:"observedCallTimeMs"`
	ResultBytes          int64 `json:"observedResultBytes"`
	Completed            int   `json:"completedTasks"`
	Waiting              int   `json:"waitingTasks"`
	Held                 int   `json:"heldTasks"`
}
type View struct {
	ID       string  `json:"analysisId"`
	Revision int64   `json:"revision"`
	Profile  Profile `json:"profile"`
	Budget   Budget  `json:"budget"`
	Tasks    []Task  `json:"tasks"`
	Offset   int     `json:"offset"`
	HasMore  bool    `json:"hasMore"`
	// Per-column evidence collection, never a semantic correctness decision.
	CoveredColumns  int `json:"columnsWithPlannedWork"`
	EvidenceColumns int `json:"columnsWithCompletedPlannedWork"`
}
type Plan struct {
	ID               string
	Scope            Scope
	Profile          Profile
	Manifest         string
	ExpectedRevision int64
	Tasks            []Task
}
type Finish struct {
	State, Code, AttemptID string
	Body                   json.RawMessage
	Calls, ElapsedMS       int64
	NotBefore              time.Time
	WindowWait             bool
}
type Store interface {
	PlanAnalysis(context.Context, Plan) (View, error)
	ReadAnalysis(context.Context, Scope, string, string, int, int) (View, error)
	ReadAnalysisTask(context.Context, Scope, string, string, string) (Task, error)
	ClaimAnalysisTask(context.Context, Scope, string, string, string) (Task, error)
	FinishAnalysisTask(context.Context, Scope, string, string, string, Finish) (Task, error)
}

func Digest(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func AnalysisID(s Scope, p Profile) string {
	// Manifest/conversation/correlation never create a fresh logical analysis.
	return Digest([]any{ProfileVersion, s, p.AssetID, p.ProfileID})
}
func allowed(tool string) bool {
	switch tool {
	case "semantic.search", "semantic.get", "semantic.discovery.request", "semantic.discovery.status", "semantic.discovery.candidates":
		return true
	}
	return false
}
func Find(snapshot *manifest.Snapshot, tool string) (manifest.Capability, bool) {
	for _, c := range snapshot.Capabilities {
		if c.ToolName == tool {
			return c, true
		}
	}
	return manifest.Capability{}, false
}
func Build(scope Scope, profile Profile, snapshot *manifest.Snapshot, expected int64, work []Work) (Plan, error) {
	if !scope.Valid() || !profile.Valid() || expected < 0 || len(work) < 1 || len(work) > MaxPlanBatch {
		return Plan{}, ErrInvalid
	}
	checksum, err := snapshot.Checksum()
	if err != nil {
		return Plan{}, err
	}
	p := Plan{ID: AnalysisID(scope, profile), Scope: scope, Profile: profile, Manifest: checksum, ExpectedRevision: expected}
	unique := map[string]int{}
	for _, w := range work {
		c, ok := Find(snapshot, w.Tool)
		if !ok || !allowed(w.Tool) || !c.ToolEligible || c.MCPClass != "MCP_TOOL" {
			return Plan{}, ErrInvalid
		}
		if len(w.Arguments) > 8192 || len(w.Columns) < 1 || len(w.Columns) > 1024 {
			return Plan{}, ErrInvalid
		}
		if w.Tool == "semantic.discovery.status" {
			if w.Observation < 1 || w.Observation > 128 {
				return Plan{}, ErrInvalid
			}
		} else if w.Observation != 0 {
			return Plan{}, ErrInvalid
		}
		var value map[string]any
		if json.Unmarshal(w.Arguments, &value) != nil || value == nil {
			return Plan{}, ErrInvalid
		}
		// An owner idempotency key is generated from the stable analysis/task intent.
		// It cannot be supplied by the analyst or regenerated by a new conversation.
		if w.Tool == "semantic.discovery.request" {
			if _, exists := value["idempotencyKey"]; exists {
				return Plan{}, ErrInvalid
			}
			value["idempotencyKey"] = "analysis:" + Digest([]any{p.ID, w.Tool, value})
		}
		var schema jsonschema.Schema
		if json.Unmarshal(c.InputSchema, &schema) != nil {
			return Plan{}, ErrInvalid
		}
		resolved, e := schema.Resolve(nil)
		if e != nil || resolved.Validate(value) != nil {
			return Plan{}, ErrInvalid
		}
		w.Arguments, _ = json.Marshal(value)
		w.Columns = append([]int(nil), w.Columns...)
		sort.Ints(w.Columns)
		columns := w.Columns[:0]
		for _, n := range w.Columns {
			if n < 0 || n >= profile.ColumnCount {
				return Plan{}, ErrInvalid
			}
			if len(columns) == 0 || columns[len(columns)-1] != n {
				columns = append(columns, n)
			}
		}
		w.Columns = columns
		var cap struct {
			Calls   int64 `json:"hardMaxToolCalls"`
			Bytes   int64 `json:"hardMaxResultBytes"`
			MS      int64 `json:"hardMaxElapsedMs"`
			Objects int64 `json:"hardMaxDistinctObjects"`
		}
		if json.Unmarshal(c.BaseCostEnvelope, &cap) != nil || cap.Calls != 1 || cap.Bytes < 1 || cap.Bytes > MaxResultBytes || cap.MS < 1 || cap.MS > 10000 || cap.Objects < 0 || cap.Objects > 100 {
			return Plan{}, ErrInvalid
		}
		id := Digest([]any{w.Tool, json.RawMessage(w.Arguments), w.Observation})
		if n, found := unique[id]; found {
			p.Tasks[n].Columns = Union(p.Tasks[n].Columns, w.Columns)
			continue
		}
		unique[id] = len(p.Tasks)
		p.Tasks = append(p.Tasks, Task{ID: id, Work: w, State: "READY", MaxBytes: cap.Bytes, MaxTimeMS: cap.MS, MaxObjects: cap.Objects})
	}
	return p, nil
}
func Union(a, b []int) []int {
	seen := map[int]bool{}
	for _, v := range a {
		seen[v] = true
	}
	for _, v := range b {
		seen[v] = true
	}
	out := make([]int, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}
func Calculate(tasks []Task) (Budget, int, int) {
	var b Budget
	columns := map[int]bool{}
	for _, t := range tasks {
		b.PlannedCalls++
		b.MaxUsefulCallTimeMS += t.MaxTimeMS
		b.MaxUsefulResultBytes += t.MaxBytes
		b.Calls += t.Calls
		b.CallTimeMS += t.CallTimeMS
		b.ResultBytes += t.ResultBytes
		switch t.State {
		case "SUCCEEDED":
			b.Completed++
		case "WAITING":
			b.Waiting++
		case "HELD":
			b.Held++
		}
		for _, n := range t.Columns {
			done, seen := columns[n]
			columns[n] = t.State == "SUCCEEDED" && (!seen || done)
		}
	}
	complete := 0
	for _, v := range columns {
		if v {
			complete++
		}
	}
	return b, len(columns), complete
}
func ValidateID(id string) error {
	b, e := hex.DecodeString(id)
	if e != nil || len(b) != 32 {
		return ErrInvalid
	}
	return nil
}
func SafeError(err error) string {
	for _, e := range []error{ErrInvalid, ErrScope, ErrConflict, ErrCapacity, ErrUnavailable, ErrFenced, orchestration.ErrUnauthorized} {
		if errors.Is(err, e) {
			return e.Error()
		}
	}
	return "ANALYSIS_OPERATION_HELD"
}
func (p Profile) String() string {
	return fmt.Sprintf("%s:%s:%d:%s:%d", p.AssetID, p.ProfileID, p.Version, p.SchemaHash, p.ColumnCount)
}
