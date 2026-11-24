package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/jackc/pgx/v5"
)

func (s *Store) RunSummary(ctx context.Context, scope analysis.Scope, issuer, id string, pos analysis.Position, page bool) (analysis.Summary, error) {
	var out analysis.Summary
	r, err := s.ReadRun(ctx, scope, issuer, id)
	if err != nil {
		return out, err
	}
	if r.Mandate.ID == "" {
		return out, analysis.ErrUnavailable
	}
	var counts []byte
	err = s.pool.QueryRow(ctx, `select seq,counts,state,reason from ouf_mcp.analysis_run_snapshot
		where analysis_id=$1 and seq<=$2 order by seq desc limit 1`, id, min(pos.Seq, r.Seq)).Scan(&r.Seq, &counts, &r.State, &r.Reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, analysis.ErrScope
	}
	if err != nil {
		return out, err
	}
	if json.Unmarshal(counts, &out.Counts) != nil || (pos.Last >= 0 && pos.Seq != r.Seq) {
		return out, analysis.ErrRunDrift
	}
	out.Run = &r
	out.Onboarding = r.Onboarding
	out.Visibility = r.Policy
	out.Seq = r.Seq
	out.Stopped = r.State == "BLOCKED"
	out.Hint = analysis.Hint{AssetID: r.Profile.AssetID, ProfileID: r.Profile.ProfileID, Reason: r.Reason}
	if !page {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `select coalesce(v.outcome,'{"state":"PENDING"}'::jsonb) ||
		jsonb_build_object('ordinal',n,'changedSeq',coalesce(v.seq,0)::text)
		from generate_series($2::int,$3::int) n left join lateral
		(select outcome,seq from ouf_mcp.analysis_run_outcome where analysis_id=$1 and ordinal=n and seq<=$4 order by seq desc limit 1) v on true
		order by n`, id, pos.Last+1, min(pos.Last+pos.Limit, out.Counts.Total-1), out.Seq)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw json.RawMessage
		if err = rows.Scan(&raw); err != nil {
			return out, err
		}
		out.Items = append(out.Items, raw)
	}
	return out, rows.Err()
}

func (s *Store) AuditRunSummary(ctx context.Context, r analysis.RunRecord, decision string, first, last int) error {
	_, err := s.pool.Exec(ctx, `insert into ouf_mcp.analysis_run_read(analysis_id,seq,decision_ref,first_ordinal,last_ordinal)
		values($1,$2,$3,$4,$5)`, r.ID(), r.Seq, decision, first, last)
	return err
}

// Claim a machine observation across replicas. A state change is always
// observable. Repeated reads of the same live version share one cadence.
func (s *Store) ClaimRunPoll(ctx context.Context, r analysis.RunRecord, interval time.Duration) (time.Time, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return time.Time{}, false, err
	}
	defer tx.Rollback(ctx)
	var next time.Time
	var allowed bool
	err = tx.QueryRow(ctx, `select coalesce(poll_after,started),poll_seq is null or poll_after<=clock_timestamp()
		or state<>'RUNNING' or deadline<=clock_timestamp()
		from ouf_mcp.analysis_run where analysis_id=$1 for update`, r.ID()).Scan(&next, &allowed)
	if err != nil {
		return next, false, err
	}
	if allowed {
		err = tx.QueryRow(ctx, `update ouf_mcp.analysis_run set poll_seq=$2,poll_after=least(deadline,clock_timestamp()+$3::bigint*interval '1 microsecond')
			where analysis_id=$1 returning poll_after`, r.ID(), r.Seq, interval.Microseconds()).Scan(&next)
		if err != nil {
			return next, false, err
		}
	}
	return next, allowed, tx.Commit(ctx)
}
