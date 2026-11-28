package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/jackc/pgx/v5"
)

type batchProfile struct {
	analysis.Profile
	Mode      string `json:"mode"`
	Epoch     string `json:"policyEpoch,omitempty"`
	Authority string `json:"authorityBinding,omitempty"`
}

func readBatchStart(ctx context.Context, q planReceiptReader, scope analysis.Scope, id, checksum string) (analysis.BatchStart, error) {
	var raw []byte
	var stored string
	var revision int64
	var createdAt time.Time
	var initialized bool
	var m analysis.BatchMandate
	err := q.QueryRow(ctx, `select p.profile,p.manifest_checksum,p.revision,p.created_at,
		exists(select from ouf_mcp.analysis_counts where analysis_id=$1),
		coalesce(m.start_ref::text,''),coalesce(m.start_binding,''),coalesce(m.mandate_ref::text,''),
		coalesce(m.mandate_binding,''),coalesce(m.command_key::text,''),coalesce(m.generation,0),
		coalesce(m.state,''),coalesce(m.reason_code,'')
		from ouf_mcp.profile_analysis p left join ouf_mcp.analysis_batch_mandate m using(analysis_id)
		where p.analysis_id=$1 and p.service_principal_id=$2 and p.principal_id=$3 and p.tenant_id=$4 and p.profile->>'mode'=$5`,
		id, scope.Service, scope.Principal, scope.Tenant, analysis.BatchMode).Scan(&raw, &stored, &revision, &createdAt, &initialized,
		&m.StartRef, &m.StartBinding, &m.Ref, &m.Binding, &m.CommandKey, &m.Generation, &m.State, &m.Reason)
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
	if m.State == "ACTIVE" {
		view := m.BoundView()
		if view.AnalysisID != id || view.ExpiresAtMs <= 0 {
			return analysis.BatchStart{}, analysis.ErrProfile
		}
		m.Version = view.Version
	}
	return analysis.BatchStart{ID: id, Profile: pin.Profile, CreatedAt: createdAt, Epoch: pin.Epoch, Authority: pin.Authority, Revision: revision, Initialized: initialized, Mandate: m}, nil
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
	return s.saveBatchStart(ctx, scope, checksum, key, intent, start, analysis.BatchMandate{})
}

func (s *Store) SaveBatchMandateStart(ctx context.Context, scope analysis.Scope, checksum, key, intent string, start analysis.BatchStart, mandate analysis.BatchMandate) (analysis.BatchStart, error) {
	if mandate.CommandKey != key || !analysis.ValidBatchAttempt(mandate.StartRef) || !analysis.ValidBatchHash(start.Epoch) ||
		mandate.Generation != 1 || mandate.StartBinding == "" {
		return analysis.BatchStart{}, analysis.ErrInvalid
	}
	return s.saveBatchStart(ctx, scope, checksum, key, intent, start, mandate)
}

func (s *Store) saveBatchStart(ctx context.Context, scope analysis.Scope, checksum, key, intent string, start analysis.BatchStart, mandate analysis.BatchMandate) (analysis.BatchStart, error) {
	if !scope.Valid() || !start.Profile.Valid() || start.ID != analysis.BatchAnalysisID(scope, start.Profile, start.Epoch) {
		return analysis.BatchStart{}, analysis.ErrInvalid
	}
	seedBatch := func(tx pgx.Tx) (analysis.View, error) {
		raw, err := json.Marshal(batchProfile{Profile: start.Profile, Mode: analysis.BatchMode, Epoch: start.Epoch, Authority: start.Authority})
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
		if err = lockBatchAnalysis(ctx, tx, scope, start.ID); err != nil {
			return analysis.View{}, err
		}
		retained, err := readBatchStart(ctx, tx, scope, start.ID, checksum)
		if err != nil {
			return analysis.View{}, err
		}
		if retained.Profile != start.Profile {
			return analysis.View{}, analysis.ErrProfile
		}
		if retained.Authority != "" && retained.Authority != start.Authority {
			return analysis.View{}, analysis.ErrScope
		}
		if !retained.Initialized {
			if err = seedBatchColumns(ctx, tx, scope, checksum, start); err != nil {
				return analysis.View{}, err
			}
		}
		if mandate.StartRef != "" {
			if retained.Mandate.StartRef != "" {
				if retained.Mandate.StartRef != mandate.StartRef || retained.Mandate.CommandKey != key ||
					retained.Mandate.Generation != mandate.Generation || retained.Mandate.StartBinding != mandate.StartBinding {
					return analysis.View{}, analysis.ErrConflict
				}
			} else {
				tag, err := tx.Exec(ctx, `insert into ouf_mcp.analysis_batch_mandate
					(analysis_id,start_ref,generation,command_key,start_binding,control_due)
					select $1,$2,$3,$4,$5,clock_timestamp()+interval '65 seconds'
					where not exists(select from ouf_mcp.analysis_batch_column where analysis_id=$1
						and (work_stage<>'PUBLISHED' or retry_attempts<>0 or fence is not null))`,
					start.ID, mandate.StartRef, mandate.Generation, key, mandate.StartBinding)
				if err != nil {
					return analysis.View{}, err
				}
				if tag.RowsAffected() != 1 {
					return analysis.View{}, analysis.ErrConflict
				}
			}
		}
		return analysis.View{ID: retained.ID, Profile: retained.Profile, Revision: retained.Revision, Epoch: retained.Epoch}, nil
	}
	v, err := s.planCommandWithReplay(ctx, scope, checksum, key, intent, seedBatch, func(target string) (analysis.View, error) {
		if target != start.ID {
			return analysis.View{}, analysis.ErrProfile
		}
		// A 66a receipt may refer to a profile pin without any columns yet.
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return analysis.View{}, err
		}
		defer tx.Rollback(ctx)
		view, err := seedBatch(tx)
		if err == nil {
			err = tx.Commit(ctx)
		}
		return view, err
	})
	if err != nil {
		return analysis.BatchStart{}, err
	}
	return s.ReadBatchStart(ctx, scope, v.ID, checksum)
}

func lockBatchAnalysis(ctx context.Context, tx pgx.Tx, scope analysis.Scope, id string) error {
	var found bool
	err := tx.QueryRow(ctx, `select true from ouf_mcp.profile_analysis
		where analysis_id=$1 and service_principal_id=$2 and principal_id=$3 and tenant_id=$4 and profile->>'mode'=$5
		for no key update`, id, scope.Service, scope.Principal, scope.Tenant, analysis.BatchMode).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return analysis.ErrScope
	}
	return err
}

func seedBatchColumns(ctx context.Context, tx pgx.Tx, scope analysis.Scope, checksum string, start analysis.BatchStart) error {
	pin, err := json.Marshal(start.Profile)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `with verified as materialized (
		select (h.n-1)::int as ordinal,h.name,p.batch_columns->((h.n-1)::int) as col
		from ouf_mcp.analysis_preview_progress p cross join lateral unnest(p.headers) with ordinality as h(name,n)
		where p.analysis_id=$1 and p.service_principal_id=$2 and p.principal_id=$3 and p.tenant_id=$4
		and p.manifest_checksum=$5 and p.profile=$6 and cardinality(p.headers)=$7 and p.batch_columns is not null
	), initial as (
		insert into ouf_mcp.analysis_column(analysis_id,ordinal,header,next_tool,outcome)
		select $1,ordinal,name,'',case when col->>'intent'='' then
			'{"state":"BLOCKED","reasonCode":"ANALYSIS_COLUMN_INVALID"}'::jsonb else '{"state":"PENDING"}'::jsonb end
		from verified returning analysis_id,ordinal,outcome
	), versions as (
		insert into ouf_mcp.analysis_column_version select analysis_id,ordinal,1,outcome from initial
	), states as (
		insert into ouf_mcp.analysis_batch_column(analysis_id,ordinal,state,due_at,intent,metadata)
		select i.analysis_id,i.ordinal,case when v.col->>'intent'='' then 'CLOSED' else 'PENDING' end,
			case when v.col->>'intent'<>'' then transaction_timestamp() end,v.col->>'intent',v.col-'intent'
		from initial i join verified v using(ordinal) returning state
	)
	insert into ouf_mcp.analysis_counts(analysis_id,seq,total,blocked,pending,onboarding)
	select $1,1,count(*),count(*) filter(where state='CLOSED'),count(*) filter(where state='PENDING'),true
	from states having count(*)=$7`, start.ID, scope.Service, scope.Principal, scope.Tenant, checksum, pin, start.Profile.ColumnCount)
	if err == nil && tag.RowsAffected() != 1 {
		err = analysis.ErrProfile
	}
	return err
}

func (s *Store) BatchPreviewProgress(ctx context.Context, scope analysis.Scope, id, checksum, epoch string) (analysis.Profile, int, error) {
	var pin analysis.Profile
	var raw []byte
	var offset int
	err := s.pool.QueryRow(ctx, `select profile,cardinality(headers) from ouf_mcp.analysis_preview_progress
		where analysis_id=$1 and service_principal_id=$2 and principal_id=$3 and tenant_id=$4
		and manifest_checksum=$5 and batch_columns is not null`, id, scope.Service, scope.Principal, scope.Tenant, checksum).Scan(&raw, &offset)
	if errors.Is(err, pgx.ErrNoRows) {
		return pin, 0, nil
	}
	if err == nil && (json.Unmarshal(raw, &pin) != nil || !pin.Valid() || id != analysis.BatchAnalysisID(scope, pin, epoch) || offset > pin.ColumnCount) {
		err = analysis.ErrProfile
	}
	return pin, offset, err
}

func (s *Store) SaveBatchPreviewPage(ctx context.Context, scope analysis.Scope, id, checksum, epoch string, pin analysis.Profile, offset int, columns []analysis.BatchPreviewColumn) error {
	if !scope.Valid() || !pin.Valid() || id != analysis.BatchAnalysisID(scope, pin, epoch) ||
		offset < 0 || len(columns) == 0 || len(columns) > 128 || offset+len(columns) > pin.ColumnCount {
		return analysis.ErrProfile
	}
	names := make([]string, len(columns))
	for n, col := range columns {
		names[n] = col.Name
	}
	profile, err := json.Marshal(pin)
	if err != nil {
		return err
	}
	data, err := json.Marshal(columns)
	if err != nil {
		return err
	}
	if offset == 0 {
		tag, err := s.pool.Exec(ctx, `insert into ouf_mcp.analysis_preview_progress
			(analysis_id,service_principal_id,principal_id,tenant_id,manifest_checksum,profile,headers,batch_columns)
			values($1,$2,$3,$4,$5,$6,$7,$8) on conflict do nothing`, id, scope.Service, scope.Principal, scope.Tenant, checksum, profile, names, data)
		if err != nil || tag.RowsAffected() == 1 {
			return err
		}
	}
	tag, err := s.pool.Exec(ctx, `update ouf_mcp.analysis_preview_progress set headers=headers || $7::text[],batch_columns=batch_columns || $8::jsonb
		where analysis_id=$1 and service_principal_id=$2 and principal_id=$3 and tenant_id=$4 and manifest_checksum=$5
		and profile=$6 and batch_columns is not null and cardinality(headers)=$9`, id, scope.Service, scope.Principal, scope.Tenant, checksum, profile, names, data, offset)
	if err != nil || tag.RowsAffected() == 1 {
		return err
	}
	var samePage bool
	err = s.pool.QueryRow(ctx, `select exists(select from ouf_mcp.analysis_preview_progress p
		where analysis_id=$1 and service_principal_id=$2 and principal_id=$3 and tenant_id=$4 and manifest_checksum=$5
		and profile=$6 and headers[$9+1:$9+cardinality($7::text[])]=$7
		and (select jsonb_agg(value order by n) from jsonb_array_elements(p.batch_columns) with ordinality as b(value,n)
		     where n>$9 and n<=$9+cardinality($7::text[]))=$8)`, id, scope.Service, scope.Principal, scope.Tenant, checksum, profile, names, data, offset).Scan(&samePage)
	if err == nil && !samePage {
		err = analysis.ErrProfile
	}
	return err
}
