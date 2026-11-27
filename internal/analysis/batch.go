package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

const BatchMode = "BATCH_V1"
const BatchMaxResultBytes = 8192

type BatchStart struct {
	ID       string
	Profile  Profile
	Epoch    string
	Revision int64
}

type BatchStore interface {
	ReadBatchStart(context.Context, Scope, string, string) (BatchStart, error)
	LookupBatchStartCommand(context.Context, Scope, string, string, string) (BatchStart, bool, error)
	SaveBatchStart(context.Context, Scope, string, string, string, BatchStart) (BatchStart, error)
}

func BatchAnalysisID(scope Scope, profile Profile, epoch string) string {
	return Digest([]any{ProfileVersion, BatchMode, scope, profile.AssetID, profile.ProfileID, epoch})
}

// BatchPreviewProfile pins the first owner page. The batch worker will verify
// the remaining ordinal coverage before using any header for discovery.
func BatchPreviewProfile(body []byte, asset, profile string, limit int) (Profile, error) {
	pin, _, err := previewPage(body, asset, profile, Profile{}, 0, limit)
	return pin, err
}

func (c Coordinator) StartBatch(ctx context.Context, i orchestration.Identity, asset, profile, hash, key string) (BatchStart, error) {
	store, ok := c.Store.(BatchStore)
	if !ok {
		return BatchStart{}, ErrUnavailable
	}
	command, err := uuid.Parse(key)
	if !CanonicalUUID(asset) || !CanonicalUUID(profile) || err != nil || command.String() != key || command.Version() != 4 ||
		command.Variant() != uuid.RFC4122 || hash != strings.ToLower(hash) {
		return BatchStart{}, ErrInvalid
	}
	epoch, err := c.Epoch(ctx, i)
	if err != nil {
		return BatchStart{}, err
	}
	checksum, err := c.Manifest.Checksum()
	if err != nil {
		return BatchStart{}, err
	}
	scope := IdentityScope(i)
	intent := Digest([]string{BatchMode, asset, profile, hash})
	if start, found, err := store.LookupBatchStartCommand(ctx, scope, checksum, key, intent); found || err != nil {
		if err == nil && start.Epoch != epoch {
			err = ErrEpochChanged
		}
		return start, err
	}
	id := BatchAnalysisID(scope, Profile{AssetID: asset, ProfileID: profile}, epoch)
	start, err := store.ReadBatchStart(ctx, scope, id, checksum)
	if errors.Is(err, ErrScope) {
		preview, _ := Find(c.Manifest, "source.file.preview")
		const pageSize = 2
		args, _ := json.Marshal(map[string]any{"assetId": asset, "profileId": profile, "view": "COLUMNS", "offset": 0, "limit": pageSize})
		r, err := c.Service.Call(ctx, orchestration.Invocation{Identity: i, CapabilityID: preview.CapabilityID, Owner: preview.Owner, OperationClass: "READ", GatewayBindingRef: preview.GatewayBindingRef, ManifestChecksum: checksum, Arguments: args, IdempotencyKey: uuid.NewString(), CorrelationID: uuid.NewString(), Window: orchestration.AdmissionWindow, Timeout: 10 * time.Second, RetryThreshold: orchestration.AdmissionRetryThreshold, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 1, ResultBytes: 1 << 20}})
		if err != nil {
			return BatchStart{}, err
		}
		if r.Problem != nil {
			switch r.Problem.Status {
			case http.StatusForbidden:
				return BatchStart{}, &OwnerDenial{ReasonCode: r.Problem.Code, CapabilityID: preview.CapabilityID, PolicyVersion: r.Problem.PolicyVersion}
			case http.StatusTooManyRequests, http.StatusServiceUnavailable:
				return BatchStart{}, ErrUnavailable
			}
		}
		if r.Replay || r.Problem != nil {
			return BatchStart{}, ErrProfile
		}
		pin, err := BatchPreviewProfile(r.Body, asset, profile, pageSize)
		if err != nil {
			return BatchStart{}, err
		}
		start = BatchStart{ID: id, Profile: pin, Epoch: epoch}
	} else if err != nil {
		return BatchStart{}, err
	}
	if start.Profile.SchemaHash != hash {
		return BatchStart{}, ErrProfile
	}
	saved, err := store.SaveBatchStart(ctx, scope, checksum, key, intent, start)
	if err == nil && saved.Epoch != epoch {
		err = ErrEpochChanged
	}
	return saved, err
}
