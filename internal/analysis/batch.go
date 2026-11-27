package analysis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
)

const BatchMode = "BATCH_V1"
const BatchMaxResultBytes = 8192
const BatchMaxItems = 8

type BatchStart struct {
	ID          string
	Profile     Profile
	Epoch       string
	Revision    int64
	Initialized bool
}

type BatchPreviewColumn struct {
	Name string `json:"-"`
	BatchColumnMetadata
	Intent string `json:"intent"`
}

type BatchColumnMetadata struct {
	DataType string            `json:"dataType"`
	Examples []json.RawMessage `json:"examples"`
}

type BatchStore interface {
	ReadBatchStart(context.Context, Scope, string, string) (BatchStart, error)
	LookupBatchStartCommand(context.Context, Scope, string, string, string) (BatchStart, bool, error)
	SaveBatchStart(context.Context, Scope, string, string, string, BatchStart) (BatchStart, error)
	BatchPreviewProgress(context.Context, Scope, string, string, string) (Profile, int, error)
	SaveBatchPreviewPage(context.Context, Scope, string, string, string, Profile, int, []BatchPreviewColumn) error
}

func BatchAnalysisID(scope Scope, profile Profile, epoch string) string {
	return Digest([]any{ProfileVersion, BatchMode, scope, profile.AssetID, profile.ProfileID, epoch})
}

// BatchPreviewProfile pins the first owner page for the public preview entry.
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
		if err != nil || start.Initialized {
			return start, err
		}
	}
	id := BatchAnalysisID(scope, Profile{AssetID: asset, ProfileID: profile}, epoch)
	start, err := store.ReadBatchStart(ctx, scope, id, checksum)
	if err != nil && !errors.Is(err, ErrScope) {
		return BatchStart{}, err
	}
	if !start.Initialized {
		pin, err := c.prepareBatchProfile(ctx, i, store, checksum, id, epoch, asset, profile, hash, start.Profile)
		if err != nil {
			return BatchStart{}, err
		}
		start = BatchStart{ID: id, Profile: pin, Epoch: epoch}
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

func (c Coordinator) prepareBatchProfile(ctx context.Context, i orchestration.Identity, store BatchStore, checksum, id, epoch, asset, profile, hash string, retained Profile) (Profile, error) {
	pageSize := c.PreviewPageSize
	if pageSize == 0 {
		limits, err := SummaryLimits(c.Manifest)
		if err != nil {
			return Profile{}, err
		}
		pageSize = limits.PreviewPageSize
	}
	if pageSize < 1 || pageSize > 128 {
		return Profile{}, ErrInvalid
	}
	if retained.Valid() && retained.SchemaHash != hash {
		return Profile{}, ErrProfile
	}
	scope := IdentityScope(i)
	pin, offset, err := store.BatchPreviewProgress(ctx, scope, id, checksum, epoch)
	if err != nil {
		return Profile{}, err
	}
	for offset == 0 || offset < pin.ColumnCount {
		body, err := c.readPreviewPage(ctx, i, checksum, asset, profile, offset, pageSize)
		if err != nil {
			return Profile{}, err
		}
		page, cols, err := verifiedPreviewPage(body, asset, profile, pin, offset, pageSize)
		if err != nil || page.SchemaHash != hash || (retained.Valid() && page != retained) {
			return Profile{}, ErrProfile
		}
		pageObject := previewObject(body)
		columns := make([]BatchPreviewColumn, len(cols))
		for n, col := range cols {
			metadata := previewObject(col.Raw)
			dataType := "unknown"
			for _, key := range []string{"dataType", "inferredType", "inferredDataType", "type"} {
				if value, ok := metadata[key].(string); ok && value != "" && utf8.RuneCountInString(value) <= 80 && !strings.ContainsRune(value, 0) {
					dataType = value
					break
				}
			}
			columns[n] = BatchPreviewColumn{Name: col.Name, Intent: ColumnIntent(col.Name), BatchColumnMetadata: BatchColumnMetadata{
				DataType: dataType, Examples: previewExamples(pageObject, metadata, 2),
			}}
		}
		if err = store.SaveBatchPreviewPage(ctx, scope, id, checksum, epoch, page, offset, columns); err != nil {
			return Profile{}, err
		}
		pin, offset = page, offset+len(columns)
	}
	if pin.SchemaHash != hash || (retained.Valid() && pin != retained) {
		return Profile{}, ErrProfile
	}
	current, err := c.Epoch(ctx, i)
	if err == nil && current != epoch {
		err = ErrEpochChanged
	}
	return pin, err
}

func (c Coordinator) readPreviewPage(ctx context.Context, i orchestration.Identity, checksum, asset, profile string, offset, limit int) ([]byte, error) {
	preview, _ := Find(c.Manifest, "source.file.preview")
	args, _ := json.Marshal(map[string]any{"assetId": asset, "profileId": profile, "view": "COLUMNS", "offset": offset, "limit": limit})
	r, err := c.Service.Call(ctx, orchestration.Invocation{Identity: i, CapabilityID: preview.CapabilityID, Owner: preview.Owner, OperationClass: "READ", GatewayBindingRef: preview.GatewayBindingRef, ManifestChecksum: checksum, Arguments: args, IdempotencyKey: uuid.NewString(), CorrelationID: uuid.NewString(), Window: orchestration.AdmissionWindow, Timeout: 10 * time.Second, RetryThreshold: orchestration.AdmissionRetryThreshold, Maximum: orchestration.Cost{ToolCalls: 1, DistinctObjects: 1, ResultBytes: 1 << 20}})
	if err != nil {
		return nil, err
	}
	if r.Problem != nil {
		switch r.Problem.Status {
		case http.StatusForbidden:
			return nil, &OwnerDenial{ReasonCode: r.Problem.Code, CapabilityID: preview.CapabilityID, PolicyVersion: r.Problem.PolicyVersion}
		case http.StatusTooManyRequests, http.StatusServiceUnavailable:
			return nil, ErrUnavailable
		}
	}
	if r.Replay || r.Problem != nil {
		return nil, ErrProfile
	}
	return r.Body, nil
}

func previewObject(raw []byte) map[string]any {
	var out map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	_ = decoder.Decode(&out)
	return out
}

func PreviewAllowsExamples(metadata map[string]any) bool {
	for _, key := range []string{"redacted", "valuesRedacted", "distinctValuesRedacted"} {
		if flag, present := metadata[key]; present && flag != false {
			return false
		}
	}
	for _, key := range []string{"dataAccessLabel", "accessLabel"} {
		if label, present := metadata[key]; present && label != "OPEN" && label != "PUBLIC" {
			return false
		}
	}
	return true
}

// Retain only scalars from the owner's filtered distinct list.
func previewExamples(page, column map[string]any, limit int) []json.RawMessage {
	examples := make([]json.RawMessage, 0, limit)
	if !PreviewAllowsExamples(page) || !PreviewAllowsExamples(column) {
		return examples
	}
	values, _ := column["distinctValues"].([]any)
	for _, value := range values {
		switch v := value.(type) {
		case string:
			marker := strings.ToUpper(strings.Trim(v, " []<>*"))
			if marker == "" || marker == "REDACTED" || marker == "MASKED" || utf8.RuneCountInString(v) > 256 || strings.ContainsRune(v, 0) {
				continue
			}
		case bool, json.Number, float64, int:
		default:
			continue
		}
		raw, err := json.Marshal(value)
		if err == nil && !slices.ContainsFunc(examples, func(other json.RawMessage) bool { return bytes.Equal(raw, other) }) {
			examples = append(examples, raw)
			if len(examples) == limit {
				break
			}
		}
	}
	return examples
}
