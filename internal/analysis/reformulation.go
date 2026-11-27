package analysis

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

// The request is a compare-and-swap token for this column's current discovery.
type Reformulation struct {
	RequestID string `json:"requestId"`
	Column    int    `json:"column"`
	Intent    string `json:"intent"`
}

type DecisionKeyStore interface {
	DecisionKeys(context.Context, Scope, string, string, string) (string, string, error)
}

type ReformulationStore interface {
	ReformulationColumn(context.Context, Scope, string, string, string, int) (int, bool, error)
	Reformulate(context.Context, Plan, Reformulation, int, func(Task) (Task, error)) (View, error)
}

func (c Coordinator) Reformulate(ctx context.Context, i orchestration.Identity, source, key string, r Reformulation, limit int) (View, error) {
	ctx = c.withOwnerObservationKey(ctx)
	k, err := uuid.Parse(key)
	if ValidateID(source) != nil || err != nil || k.Version() != 4 || k.String() != key || !CanonicalUUID(r.RequestID) ||
		r.Column < 0 || r.Column >= 1024 || !UsableColumnName(r.Intent) || limit < 0 || limit > 16 {
		return View{}, ErrInvalid
	}
	checksum, _, err := c.SummaryAccess(ctx, i)
	if err != nil {
		return View{}, err
	}
	scope := IdentityScope(i)
	old, err := c.Store.ReadAnalysis(ctx, scope, source, checksum, 0, 1)
	if err != nil {
		return View{}, err
	}
	if err = c.EvidenceAccess(ctx, i, Summary{Epoch: old.Epoch, Visibility: old.Visibility, Hint: Hint{Tool: "semantic.discovery.candidates"}}); err != nil {
		return View{}, err
	}
	store, ok := c.Store.(ReformulationStore)
	if !ok {
		return View{}, ErrUnavailable
	}
	p := Plan{ID: source, Scope: scope, Profile: old.Profile, Epoch: old.Epoch, Manifest: checksum,
		Seed: old.Seed, Visibility: old.Visibility, CommandKey: key, CommandDigest: Digest([]any{"reformulate", source, r})}
	// Pre-epoch plans may have an inferred authorization epoch but retain a
	// legacy analysis ID. Check that epoch above and preserve the original ID.
	if source == AnalysisID(scope, old.Profile) {
		p.Epoch = ""
	}
	return store.Reformulate(ctx, p, r, limit, func(previous Task) (Task, error) {
		return reformulatedTask(p, c.Manifest, previous, r)
	})
}

func reformulatedTask(p Plan, m *manifest.Snapshot, previous Task, r Reformulation) (Task, error) {
	var args map[string]any
	if previous.Tool != "semantic.discovery.request" || previous.State != "SUCCEEDED" || json.Unmarshal(previous.Arguments, &args) != nil || args == nil {
		return Task{}, ErrReplanState
	}
	old, _ := args["intent"].(string)
	if strings.EqualFold(strings.TrimSpace(old), strings.TrimSpace(r.Intent)) {
		return Task{}, ErrInvalid
	}
	delete(args, "idempotencyKey")
	args["intent"] = r.Intent
	raw, _ := json.Marshal(args)
	work := Work{Tool: previous.Tool, Columns: []int{r.Column}, Arguments: raw}
	// Separate owner keys even when two columns choose identical wording.
	built, err := buildWork(Plan{ID: Digest([]any{p.ID, r.RequestID, r.Column}), Profile: p.Profile}, m, []Work{work})
	if err != nil {
		return Task{}, err
	}
	task := built.Tasks[0]
	var binding struct{ IdempotencyKey string }
	if err := json.Unmarshal(task.Arguments, &binding); err != nil {
		return Task{}, err
	}
	task.OwnerKey = binding.IdempotencyKey
	task.Reformulated = true
	return task, nil
}
