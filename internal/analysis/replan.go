package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"slices"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

// ReplanStore locks the source and records the command outcome in the same
// transaction as the new plan. Only task inputs are passed to the builder.
type ReplanCommand struct {
	Scope                          Scope
	Manifest, Source, Key, Epoch   string
	Visibility, PreviousVisibility string
}

type ReplanStore interface {
	ReplanAnalysis(context.Context, ReplanCommand, func(*Plan, []Task) error) (View, error)
}

func (c Coordinator) Replan(ctx context.Context, i orchestration.Identity, source, key string) (View, error) {
	ctx = c.withOwnerObservationKey(ctx)
	k, err := uuid.Parse(key)
	if ValidateID(source) != nil || err != nil || k.Version() != 4 || k.String() != key {
		return View{}, ErrInvalid
	}
	scope := IdentityScope(i)
	epoch, err := c.Epoch(ctx, i)
	if err != nil {
		return View{}, err
	}
	checksum, err := c.Manifest.Checksum()
	if err != nil {
		return View{}, err
	}
	old, err := c.Store.ReadAnalysis(ctx, scope, source, checksum, 0, 1)
	if err != nil && !errors.Is(err, ErrScope) {
		return View{}, err
	}
	command := ReplanCommand{Scope: scope, Manifest: checksum, Source: source, Key: key, Epoch: epoch}
	if old.Visibility != "" {
		batch, ok := Find(c.Manifest, BatchTool)
		if !ok || batch.PublicationState != "ACTIVE" {
			return View{}, ErrUnavailable
		}
		d, err := c.decision(ctx, i, batch)
		if err != nil {
			return View{}, err
		}
		if epoch != decisionEpoch(d) {
			return View{}, ErrEpochChanged
		}
		command.Visibility = visibilityDigest(i, d)
		if d.BundleVersion > 1 {
			previous := d
			previous.BundleVersion--
			command.PreviousVisibility = visibilityDigest(i, previous)
		}
	}
	store, ok := c.Store.(ReplanStore)
	if !ok {
		return View{}, ErrUnavailable
	}
	return store.ReplanAnalysis(ctx, command, func(p *Plan, tasks []Task) error {
		return cloneInputs(p, tasks, c.Manifest, source)
	})
}

func cloneInputs(p *Plan, inputs []Task, m *manifest.Snapshot, source string) error {
	if !p.Profile.Valid() || len(inputs) > MaxTasks || len(p.Headers) != p.Profile.ColumnCount {
		return ErrCapacity
	}
	// Residual discovery keys include the parent intent (including context).
	// Recover that association from the batch inputs, including split batches.
	residuals := map[string]Task{}
	legacyResiduals := map[string]bool{}
	split := map[string]bool{}
	for _, t := range inputs {
		if t.Tool != BatchTool {
			continue
		}
		var b Batch
		if json.Unmarshal(t.Arguments, &b) != nil {
			return ErrInvalid
		}
		for _, child := range append(SplitBatch(t, false), SplitBatch(t, true)...) {
			split[child.ID] = true
		}
		for _, i := range b.Intents {
			for _, fallback := range []bool{false, true} {
				old := t.ResidualTasks(source, i, fallback)
				fresh := t.ResidualTasks(p.ID, i, fallback)
				for n, child := range old {
					residuals[child.ID] = fresh[n]
				}
			}
			for _, n := range t.Groups[i.ID] {
				if description := t.DiscoveryText[n]; description != "" {
					legacy := i
					legacy.Text = description
					legacy.ID = Digest([]string{i.ID, description})
					legacyResiduals[ResidualTask(source, legacy, []int{n}, MaxResultBytes, false).ID] = true
					legacyResiduals[ResidualTask(source, legacy, []int{n}, MaxResultBytes, true).ID] = true
				}
			}
		}
	}
	for _, t := range inputs {
		// A column reformulation was bound to a completed request in the old epoch.
		// Re-evaluate the original input before making any new decision.
		if t.Reformulated {
			continue
		}
		if len(t.Columns) == 0 || len(t.Columns) > p.Profile.ColumnCount {
			return ErrInvalid
		}
		for _, n := range t.Columns {
			if n < 0 || n >= p.Profile.ColumnCount {
				return ErrInvalid
			}
		}
		if child, ok := residuals[t.ID]; ok {
			child.AwaitParent = true
			p.Tasks = append(p.Tasks, child)
			continue
		}
		// The fresh batch can regenerate these children using the header.
		// Carrying their old descriptive arguments forward would duplicate work.
		if legacyResiduals[t.ID] {
			continue
		}
		if t.Tool == BatchTool {
			cap, ok := Find(m, BatchTool)
			if !ok || cap.PublicationState != "ACTIVE" {
				return ErrUnavailable
			}
			l, err := PublishedLimits(cap)
			var b Batch
			if err != nil || json.Unmarshal(t.Arguments, &b) != nil || b.Contract != BatchContract || b.Limit < 1 || b.Limit > l.Hits || len(b.Intents) < 1 || len(b.Intents) > l.MaxIntents {
				return ErrInvalid
			}
			fresh := batchTask(b.Intents, b.Limit, t.Groups, t.DiscoveryText, l.ResponseBytes)
			if len(fresh.Arguments) > l.RequestBytes || fresh.ID != t.ID || !slices.Equal(fresh.Columns, t.Columns) {
				return ErrInvalid
			}
			fresh.AwaitParent = split[t.ID]
			p.Tasks = append(p.Tasks, fresh)
			continue
		}
		if p.Visibility == "" && t.OwnerIntent != "" {
			var intent Intent
			if ValidateID(t.OwnerIntent) != nil || json.Unmarshal(t.Arguments, &intent) != nil {
				return ErrInvalid
			}
			intent.ID = t.OwnerIntent
			if ResidualTask(source, intent, t.Columns, MaxResultBytes, true).ID != t.ID {
				return ErrInvalid
			}
			fresh := ResidualTask(p.ID, intent, t.Columns, MaxResultBytes, true)
			fresh.Compatibility = "PUBLISHED_BATCH_UNAVAILABLE"
			p.Tasks = append(p.Tasks, fresh)
			continue
		}
		w := t.Work
		compact, err := json.Marshal(w.Arguments)
		if err != nil {
			return ErrInvalid
		}
		w.Arguments = compact
		if w.Tool == "semantic.discovery.request" {
			var args map[string]json.RawMessage
			if json.Unmarshal(w.Arguments, &args) != nil || args == nil {
				return ErrInvalid
			}
			delete(args, "idempotencyKey")
			w.Arguments, _ = json.Marshal(args)
		}
		// Build validates the public schema, ordinal, observation and byte
		// bounds and generates the owner command key from the new plan ID.
		built, err := buildWork(Plan{ID: p.ID, Profile: p.Profile, Epoch: p.Epoch}, m, []Work{w})
		if err != nil {
			return err
		}
		p.Tasks = append(p.Tasks, built.Tasks[0])
	}
	return nil
}
