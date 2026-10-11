package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/operational"
	"github.com/google/uuid"
)

func TestPublicationCaptureImmutableLineageAcrossCursorAppend(t *testing.T) {
	store, binding, frame := cursorFixture(t)
	ctx := context.Background()
	frame.Filters = json.RawMessage(`{"sourceId":"source-a"}`)
	frame.PublicationCoverage = true
	base := operational.PublicationVector{Profile: operational.PublicationVectorProfile, Filters: frame.Filters}
	for _, name := range []string{"GATEWAY", "INGESTION"} {
		base.Owners = append(base.Owners, operational.PublicationPosition{Name: name, Bookmark: uuid.NewString(), SnapshotRef: uuid.NewString(), ItemsDigest: hash64(name)})
	}
	parent := uuid.New()
	frame.PublicationParent = &parent
	frame.PublicationBase = &base
	for i := range frame.Owners {
		for _, old := range base.Owners {
			if old.Name == frame.Owners[i].Name {
				frame.Owners[i].PublicationParent = old.Bookmark
				frame.Owners[i].PublicationBookmark = uuid.NewString()
			}
		}
	}
	node, err := store.CreateIncidentCursor(ctx, binding, frame)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*operational.CursorFrame){
		func(f *operational.CursorFrame) { f.PublicationCoverage = false },
		func(f *operational.CursorFrame) { other := uuid.New(); f.PublicationParent = &other },
		func(f *operational.CursorFrame) { f.PublicationBase.Owners[0].SnapshotRef = uuid.NewString() },
		func(f *operational.CursorFrame) { f.Owners[0].PublicationBookmark = uuid.NewString() },
	} {
		next := nextCursorFrame(frame)
		copyBase := base
		copyBase.Owners = append([]operational.PublicationPosition{}, base.Owners...)
		next.PublicationBase = &copyBase
		mutate(&next)
		if _, err = store.CommitIncidentCursorPage(ctx, node.Token, binding, hash64("page"), &next); !errors.Is(err, operational.ErrCursorConflict) {
			t.Fatal("captured publication scope changed", err)
		}
	}
	valid := nextCursorFrame(frame)
	if _, err = store.CommitIncidentCursorPage(ctx, node.Token, binding, hash64("page"), &valid); err != nil {
		t.Fatal("valid publication append rejected", err)
	}
}
