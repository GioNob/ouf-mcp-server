package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/jackc/pgx/v5"
)

func (s *Store) DueRun(ctx context.Context, workload string) (analysis.RunRecord, error) {
	return scanRun(s.pool.QueryRow(ctx, "select "+runColumns+` from ouf_mcp.analysis_run r
		where state='RUNNING' and spec->>'Workload'=$1 and least(not_before,budget_end,(mandate->>'expiresAt')::timestamptz)<=clock_timestamp() and not exists
		(select 1 from ouf_mcp.analysis_run_attempt a where a.analysis_id=r.analysis_id and
		 ((a.finished_at is null and a.lease_end>clock_timestamp()) or a.proof_until>clock_timestamp()))
		order by not_before,analysis_id limit 1`, workload))
}

func (s *Store) NextRunWork(ctx context.Context, r analysis.RunRecord, worker string) (analysis.RunWork, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `select checkpoint->'Queue'->0 from ouf_mcp.analysis_run
		where analysis_id=$1 and seq=$2 and state='RUNNING'`, r.ID(), r.Seq).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return analysis.RunWork{}, analysis.ErrFenced
	}
	var item analysis.RunItem
	if err != nil {
		return analysis.RunWork{}, err
	}
	if json.Unmarshal(raw, &item) != nil || item.Intent == "" {
		return analysis.RunWork{}, analysis.ErrRunDrift
	}
	return item.Work(worker), nil
}

type RunReceipt struct {
	Decision, Correlation, Code string
	Body                        []byte
	Elapsed                     time.Duration
	RetryAfter                  time.Time
}

// Apply the authenticated receipt, owner request binding, checkpoint, column
// versions, counters and actual spend in one transaction. Failure rolls back
// every publication, retaining the prior maximum reservation for recovery.
func (s *Store) FinishRunWork(ctx context.Context, c analysis.ReservationCheck, worker string, m *manifest.Snapshot, receipt RunReceipt) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	r, err := scanRun(tx.QueryRow(ctx, "select "+runColumns+" from ouf_mcp.analysis_run where analysis_id=$1 for update", c.AnalysisID))
	if err != nil {
		return err
	}
	var raw []byte
	var maxCost analysis.RunCost
	var current, finished bool
	var priorReceipt string
	receiptHash := analysis.Digest([]string{analysis.BytesDigest(receipt.Body), receipt.Code, receipt.Decision, receipt.Correlation})
	var end time.Time
	err = tx.QueryRow(ctx, `select r.checkpoint,a.fence=r.fence and a.fence::text=$4 and a.generation=$5
		and a.operation=$6 and a.arguments_hash=$7 and a.mandate_id=$8 and a.intent_key=$9,
		a.finished_at is not null,coalesce(a.receipt_hash,''),a.lease_end,a.calls,a.attempts,a.objects,a.bytes,a.millis
		from ouf_mcp.analysis_run_attempt a join ouf_mcp.analysis_run r using(analysis_id)
		where a.analysis_id=$1 and a.attempt_id=$2 and a.worker=$3`, c.AnalysisID, c.AttemptID, worker, c.Fence, c.Generation, c.Operation, c.ArgumentsHash, c.MandateID, c.IntentKey).
		Scan(&raw, &current, &finished, &priorReceipt, &end, &maxCost.Calls, &maxCost.Attempts, &maxCost.Objects, &maxCost.Bytes, &maxCost.Millis)
	if errors.Is(err, pgx.ErrNoRows) {
		return analysis.ErrFenced
	}
	if err != nil {
		return err
	}
	if finished && priorReceipt == receiptHash {
		return nil
	}
	if finished || !current || r.State != "RUNNING" {
		return analysis.ErrFenced
	}
	now, err := s.runClock(ctx, tx)
	if err != nil {
		return err
	}
	if !now.Before(end) || !now.Before(r.Mandate.Expires) || !now.Before(r.BudgetEnd) {
		return analysis.ErrFenced
	}
	var cp analysis.RunCheckpoint
	if json.Unmarshal(raw, &cp) != nil || len(cp.Queue) == 0 {
		return analysis.ErrRunDrift
	}
	item := cp.Queue[0]
	work := item.Work(worker)
	hash, err := analysis.MandateHash(work.Arguments)
	if err != nil || work.Intent != c.IntentKey || hash != c.ArgumentsHash || work.Operation != c.Operation {
		return analysis.ErrRunDrift
	}
	before := append([]analysis.Outcome(nil), cp.Outcomes...)
	state, reason := "RUNNING", "ANALYSIS_RUNNING"
	next := now
	var request string
	if receipt.Code == "" {
		if receipt.Decision == "" || len(receipt.Decision) > 256 || receipt.Correlation == "" || len(receipt.Correlation) > 256 {
			return analysis.ErrFenced
		}
		request, err = cp.Advance(r, m, receipt.Body, now)
		if err != nil {
			return err
		}
		if len(cp.Queue) == 0 {
			state = cp.Counts().State()
			reason = ""
		} else if cp.Queue[0].Task.NotBefore.After(now) {
			next = cp.Queue[0].Task.NotBefore
			if receipt.RetryAfter.After(next) {
				next = receipt.RetryAfter
				cp.Queue[0].Task.NotBefore = next
			}
		}
	} else {
		switch receipt.Code {
		case "SEM_BATCH_REQUEST_TOO_LARGE", "SEM_BATCH_RESULT_TOO_LARGE":
			if item.Task.Tool != analysis.BatchTool {
				return analysis.ErrInvalid
			}
			children := analysis.SplitBatch(item.Task, receipt.Code == "SEM_BATCH_RESULT_TOO_LARGE")
			cp.Queue = cp.Queue[1:]
			if len(children) == 0 {
				for _, n := range item.Task.Columns {
					cp.Outcomes[n] = analysis.Outcome{State: "BLOCKED", Reason: receipt.Code}
				}
			} else {
				for _, task := range children {
					key, e := analysis.MandateHash(task.Arguments)
					if e != nil {
						return e
					}
					cp.Queue = append(cp.Queue, analysis.RunItem{Task: task, Intent: key})
				}
			}
			if len(cp.Queue) == 0 {
				state = cp.Counts().State()
				reason = receipt.Code
			}
		case "ANALYSIS_BACKOFF":
			next = receipt.RetryAfter
			if !next.After(now) {
				return analysis.ErrInvalid
			}
			reason = receipt.Code
		case "ANALYSIS_CANCELLED", "ANALYSIS_MANDATE_REVOKED", "ANALYSIS_MANDATE_EXPIRED", "ANALYSIS_OWNER_FAILURE", "ANALYSIS_SCOPE_DRIFT":
			state, reason = "BLOCKED", receipt.Code
		default:
			return analysis.ErrInvalid
		}
	}
	if request != "" {
		_, err = tx.Exec(ctx, "insert into ouf_mcp.analysis_run_request(analysis_id,request_id,intent_key) values($1,$2,$3)", r.ID(), request, c.IntentKey)
		if err != nil {
			return err
		}
	}
	if c.RequestID != "" && receipt.Code == "" {
		o := cp.Outcomes[item.Task.Columns[0]]
		if o.State == "CANDIDATES_READY" || o.State == "NO_CANDIDATES" || o.Reason == "DISCOVERY_FAILED" || o.Reason == "DISCOVERY_CANCELLED" {
			_, err = tx.Exec(ctx, "update ouf_mcp.analysis_run_request set finished_at=$4 where analysis_id=$1 and request_id=$2 and intent_key=$3", r.ID(), c.RequestID, c.IntentKey, now)
			if err != nil {
				return err
			}
		}
	}
	// Charge the operation's distinct-object ceiling unless the response
	// provides a verified count. Transport errors never refund a reservation.
	actual := maxCost
	actual.Bytes = int64(len(receipt.Body))
	actual.Millis = (receipt.Elapsed.Nanoseconds() + int64(time.Millisecond) - 1) / int64(time.Millisecond)
	if receipt.Code == "" {
		switch work.Operation {
		case "source.file.preview", "semantic.discovery.request", "semantic.discovery.status":
			actual.Objects = 1
		case "semantic.discovery.candidates":
			if o := cp.Outcomes[item.Task.Columns[0]]; o.Count != nil {
				actual.Objects = int64(*o.Count)
			}
		}
	}
	if work.Operation == "ouf.semantic.search.batch" && receipt.Code == "" {
		rows, _ := analysis.AcceptedBatch(item.Task, receipt.Body)
		distinct := map[string]bool{}
		for _, row := range rows {
			for _, hit := range row.Hits {
				distinct[hit.RevisionID] = true
			}
		}
		actual.Objects = int64(len(distinct))
	}
	if !actual.Fits(maxCost) {
		return analysis.ErrInvalid
	}
	account, _ := json.Marshal(actual)
	_, err = tx.Exec(ctx, `update ouf_mcp.analysis_run_attempt set actual=$2,finished_at=$3,decision_ref=$4,correlation_ref=$5,outcome_code=$6,receipt_hash=$7
		where attempt_id=$1`, c.AttemptID, account, now, receipt.Decision, receipt.Correlation, reason, receiptHash)
	if err != nil {
		return err
	}
	for n, o := range cp.Outcomes {
		if o == before[n] {
			continue
		}
		raw, err := json.Marshal(o)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "insert into ouf_mcp.analysis_run_outcome values($1,$2,$3,$4)", r.ID(), n, r.Seq+1, raw)
		if err != nil {
			return err
		}
	}
	checkpoint, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	counts, _ := json.Marshal(cp.Counts())
	_, err = tx.Exec(ctx, `update ouf_mcp.analysis_run set checkpoint=$2,counts=$3,state=$4,reason=$5,not_before=$6,seq=seq+1,
		mandate_state=case $5 when 'ANALYSIS_MANDATE_EXPIRED' then 'EXPIRED' when 'ANALYSIS_MANDATE_REVOKED' then 'REVOKED' else mandate_state end
		where analysis_id=$1`, r.ID(), checkpoint, counts, state, reason, next)
	if err != nil {
		return err
	}
	if state == "COMPLETE" || state == "PARTIAL" {
		if _, err = tx.Exec(ctx, "update ouf_mcp.analysis_run_incident set state='RESOLVED' where analysis_id=$1 and state='RECOVERING'", r.ID()); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) RunAttemptCount(ctx context.Context, c analysis.ReservationCheck) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, "select count(*) from ouf_mcp.analysis_run_attempt where analysis_id=$1 and intent_key=$2 and operation=$3", c.AnalysisID, c.IntentKey, c.Operation).Scan(&n)
	return n, err
}

func (s *Store) DelayRun(ctx context.Context, r analysis.RunRecord) error {
	_, err := s.pool.Exec(ctx, `update ouf_mcp.analysis_run set not_before=greatest(not_before,clock_timestamp()+interval '5 seconds'),
		reason='ANALYSIS_BACKOFF',seq=seq+1 where analysis_id=$1 and seq=$2 and state='RUNNING'`, r.ID(), r.Seq)
	return err
}

func (s *Store) HoldRunAttempt(ctx context.Context, c analysis.ReservationCheck, worker, reason string) error {
	switch reason {
	case "ANALYSIS_SCOPE_DRIFT", "ANALYSIS_EFFECT_UNCERTAIN", "ANALYSIS_MANDATE_REVOKED", "ANALYSIS_OWNER_FAILURE":
	default:
		return analysis.ErrInvalid
	}
	tag, err := s.pool.Exec(ctx, `update ouf_mcp.analysis_run r set state='BLOCKED',reason=$4,seq=seq+1,mandate_state=case when $4='ANALYSIS_MANDATE_REVOKED' then 'REVOKED' else mandate_state end from ouf_mcp.analysis_run_attempt a
		where a.attempt_id=$1 and a.worker=$2 and a.analysis_id=$3 and a.analysis_id=r.analysis_id and r.state='RUNNING'
		and a.fence=r.fence and a.finished_at is null and a.lease_end>clock_timestamp()`, c.AttemptID, worker, c.AnalysisID, reason)
	if err == nil && tag.RowsAffected() != 1 {
		return analysis.ErrFenced
	}
	return err
}

func (s *Store) ReconcileRunAttempt(ctx context.Context, c analysis.ReservationCheck, worker string) error {
	tag, err := s.pool.Exec(ctx, `update ouf_mcp.analysis_run r set not_before=a.lease_end,reason='ANALYSIS_RECONCILING',seq=seq+1
		from ouf_mcp.analysis_run_attempt a where a.attempt_id=$1 and a.analysis_id=$2 and a.worker=$3 and a.analysis_id=r.analysis_id
		and a.fence=r.fence and r.state='RUNNING' and a.finished_at is null`, c.AttemptID, c.AnalysisID, worker)
	if err == nil && tag.RowsAffected() != 1 {
		return analysis.ErrFenced
	}
	return err
}

func (s *Store) RecordCrossing(ctx context.Context, c analysis.ReservationCheck, worker, decision, correlation string, body []byte) error {
	if decision == "" || len(decision) > 256 || correlation == "" || len(correlation) > 256 {
		return s.HoldRunAttempt(ctx, c, worker, "ANALYSIS_MANDATE_REVOKED")
	}
	var observed struct{ RequestID string }
	if analysis.ValidEvidenceJSON(body) {
		_ = json.Unmarshal(body, &observed)
	}
	if !analysis.CanonicalUUID(observed.RequestID) {
		observed.RequestID = ""
	}
	// Append only diagnostic evidence to the originating attempt, even
	// when cancellation won. A late reply never changes the checkpoint.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var fence int64
	if err = tx.QueryRow(ctx, "select fence from ouf_mcp.analysis_run where analysis_id=$1 for update", c.AnalysisID).Scan(&fence); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `update ouf_mcp.analysis_run_attempt set crossing_decision=$4,crossing_correlation=$5,
		crossing_hash=$6,crossing_request=nullif($7,'')::uuid where attempt_id=$1 and analysis_id=$2 and worker=$3
		and (crossing_hash is null or crossing_hash=$6)`, c.AttemptID, c.AnalysisID, worker, decision, correlation, analysis.BytesDigest(body), observed.RequestID)
	if err == nil && tag.RowsAffected() != 1 {
		return analysis.ErrFenced
	}
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `update ouf_mcp.analysis_run r set state='BLOCKED',reason='ANALYSIS_MANDATE_REVOKED',mandate_state='REVOKED',seq=seq+1
		from ouf_mcp.analysis_run_attempt a where r.analysis_id=$1 and r.state='RUNNING' and a.attempt_id=$2 and a.analysis_id=r.analysis_id and a.fence=$3`,
		c.AnalysisID, c.AttemptID, fence)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
