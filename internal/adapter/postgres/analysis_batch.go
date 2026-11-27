package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"slices"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/jackc/pgx/v5"
)

type batchProfile struct {
	analysis.Profile
	Mode  string `json:"mode"`
	Epoch string `json:"policyEpoch,omitempty"`
}

func readBatchStart(ctx context.Context, q planReceiptReader, scope analysis.Scope, id, checksum string) (analysis.BatchStart, error) {
	var raw []byte
	var stored string
	var revision int64
	err := q.QueryRow(ctx, `select profile,manifest_checksum,revision from ouf_mcp.profile_analysis
		where analysis_id=$1 and service_principal_id=$2 and principal_id=$3 and tenant_id=$4 and profile->>'mode'=$5`,
		id, scope.Service, scope.Principal, scope.Tenant, analysis.BatchMode).Scan(&raw, &stored, &revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return analysis.BatchStart{}, analysis.ErrScope
	}
	if err != nil {
		return analysis.BatchStart{}, err
	}
	var pin batchProfile
	if !slices.Contains(manifest.AnalysisManifests(checksum), stored) || json.Unmarshal(raw, &pin) != nil ||
		!pin.Profile.Valid() || id != analysis.BatchAnalysisID(scope, pin.Profile, pin.Epoch) {
		return analysis.BatchStart{}, analysis.ErrProfile
	}
	return analysis.BatchStart{ID: id, Profile: pin.Profile, Epoch: pin.Epoch, Revision: revision}, nil
}

func (s *Store) ReadBatchStart(ctx context.Context, scope analysis.Scope, id, checksum string) (analysis.BatchStart, error) {
	return readBatchStart(ctx, s.pool, scope, id, checksum)
}

func (s *Store) LookupBatchStartCommand(ctx context.Context, scope analysis.Scope, checksum, key, intent string) (analysis.BatchStart, bool, error) {
	target, found, err := planReceipt(ctx, s.pool, scope, key, intent)
	if !found || err != nil {
		return analysis.BatchStart{}, found, err
	}
	start, err := s.ReadBatchStart(ctx, scope, target, checksum)
	return start, true, err
}

func (s *Store) SaveBatchStart(ctx context.Context, scope analysis.Scope, checksum, key, intent string, start analysis.BatchStart) (analysis.BatchStart, error) {
	if !scope.Valid() || !start.Profile.Valid() || start.ID != analysis.BatchAnalysisID(scope, start.Profile, start.Epoch) {
		return analysis.BatchStart{}, analysis.ErrInvalid
	}
	asView := func(v analysis.BatchStart, err error) (analysis.View, error) {
		return analysis.View{ID: v.ID, Profile: v.Profile, Revision: v.Revision, Epoch: v.Epoch}, err
	}
	v, err := s.planCommandWithReplay(ctx, scope, checksum, key, intent, func(tx pgx.Tx) (analysis.View, error) {
		raw, err := json.Marshal(batchProfile{Profile: start.Profile, Mode: analysis.BatchMode, Epoch: start.Epoch})
		if err != nil {
			return analysis.View{}, err
		}
		// Use the candidate checksum so the predecessor cannot open a batch as legacy work.
		_, err = tx.Exec(ctx, `insert into ouf_mcp.profile_analysis
			(analysis_id,service_principal_id,principal_id,tenant_id,profile,manifest_checksum)
			values($1,$2,$3,$4,$5,$6) on conflict do nothing`,
			start.ID, scope.Service, scope.Principal, scope.Tenant, raw, checksum)
		if err != nil {
			return analysis.View{}, err
		}
		retained, err := readBatchStart(ctx, tx, scope, start.ID, checksum)
		if err == nil && retained.Profile != start.Profile {
			err = analysis.ErrProfile
		}
		return asView(retained, err)
	}, func(target string) (analysis.View, error) {
		return asView(s.ReadBatchStart(ctx, scope, target, checksum))
	})
	return analysis.BatchStart{ID: v.ID, Profile: v.Profile, Epoch: v.Epoch, Revision: v.Revision}, err
}
