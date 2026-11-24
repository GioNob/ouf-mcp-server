package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"slices"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/jackc/pgx/v5"
)

func (s *Store) PreviewProgress(ctx context.Context, scope analysis.Scope, id, manifest string) (analysis.Profile, []string, error) {
	var profile analysis.Profile
	var raw []byte
	var headers []string
	err := s.pool.QueryRow(ctx, `select profile,headers from ouf_mcp.analysis_preview_progress
		where analysis_id=$1 and service_principal_id=$2 and principal_id=$3 and tenant_id=$4 and manifest_checksum=$5`,
		id, scope.Service, scope.Principal, scope.Tenant, manifest).Scan(&raw, &headers)
	if errors.Is(err, pgx.ErrNoRows) {
		return profile, nil, nil
	}
	if err == nil && (json.Unmarshal(raw, &profile) != nil || !profile.Valid() || analysis.AnalysisID(scope, profile) != id || len(headers) > profile.ColumnCount) {
		err = analysis.ErrProfile
	}
	return profile, headers, err
}

func (s *Store) SavePreviewPage(ctx context.Context, scope analysis.Scope, id, manifest string, profile analysis.Profile, offset int, page []string) error {
	if !scope.Valid() || !profile.Valid() || analysis.AnalysisID(scope, profile) != id ||
		offset < 0 || len(page) == 0 || len(page) > 128 || offset+len(page) > profile.ColumnCount {
		return analysis.ErrProfile
	}
	pin, err := json.Marshal(profile)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `insert into ouf_mcp.analysis_preview_progress as p
		(analysis_id,service_principal_id,principal_id,tenant_id,manifest_checksum,profile,headers)
		select $1,$2,$3,$4,$5,$6,$7 where $8::int=0
		on conflict do nothing`, id, scope.Service, scope.Principal, scope.Tenant, manifest, pin, page, offset)
	if err != nil || tag.RowsAffected() == 1 {
		return err
	}
	tag, err = s.pool.Exec(ctx, `update ouf_mcp.analysis_preview_progress set headers=headers || $7::text[]
		where analysis_id=$1 and service_principal_id=$2 and principal_id=$3 and tenant_id=$4 and manifest_checksum=$5
		  and profile=$6 and cardinality(headers)=$8`, id, scope.Service, scope.Principal, scope.Tenant, manifest, pin, page, offset)
	if err != nil || tag.RowsAffected() == 1 {
		return err
	}
	// Another planner can commit this same verified prefix first.
	p, headers, err := s.PreviewProgress(ctx, scope, id, manifest)
	if err != nil {
		return err
	}
	if p != profile || len(headers) < offset+len(page) || !slices.Equal(headers[offset:offset+len(page)], page) {
		return analysis.ErrProfile
	}
	return nil
}
