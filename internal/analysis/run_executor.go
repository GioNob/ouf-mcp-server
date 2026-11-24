package analysis

import (
	"encoding/json"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/manifest"
)

// A checkpoint contains only verified column metadata and owner work. The
// caller commits it together with accounting under the reservation fence.
type RunCheckpoint struct {
	Headers  []string
	Queue    []RunItem
	Outcomes []Outcome
}

type RunItem struct {
	Task       Task
	Intent     string
	Completion *int
}

func previewItem(p Profile, offset, pageSize int) RunItem {
	count := min(pageSize, p.ColumnCount-offset)
	args, _ := json.Marshal(map[string]any{"assetId": p.AssetID, "profileId": p.ProfileID, "view": "COLUMNS", "offset": offset, "limit": count})
	cols := make([]int, count)
	for i := range cols {
		cols[i] = offset + i
	}
	key, _ := MandateHash(args)
	return RunItem{Intent: key, Task: Task{ID: key, Work: Work{Tool: "source.file.preview", Arguments: args, Columns: cols}, MaxObjects: 1, MaxBytes: 1 << 20, MaxTimeMS: 10000}}
}

func NewRunCheckpoint(p Profile, pageSize int) (RunCheckpoint, error) {
	if !p.Valid() || pageSize < 1 || pageSize > 128 {
		return RunCheckpoint{}, ErrInvalid
	}
	c := RunCheckpoint{Queue: []RunItem{previewItem(p, 0, pageSize)}, Outcomes: make([]Outcome, p.ColumnCount)}
	for i := range c.Outcomes {
		c.Outcomes[i].State = "PENDING"
	}
	return c, nil
}

func (item RunItem) Work(worker string) RunWork {
	t := item.Task
	op := t.Tool
	if op == BatchTool {
		op = "ouf.semantic.search.batch"
	}
	return RunWork{Worker: worker, Operation: op, Intent: item.Intent, Arguments: t.Arguments, Ordinals: t.Columns,
		Maximum: RunCost{Calls: 1, Attempts: 1, Objects: t.MaxObjects, Bytes: t.MaxBytes, Millis: t.MaxTimeMS}}
}

func (c RunCheckpoint) Counts() Counts {
	out := Counts{Total: len(c.Outcomes)}
	for _, o := range c.Outcomes {
		switch o.State {
		case "PENDING":
			out.Pending++
		case "MATCHED_PUBLISHED":
			out.Matched++
		case "CANDIDATES_READY":
			out.Ready++
		case "NO_CANDIDATES":
			out.Zero++
		case "BLOCKED":
			out.Blocked++
		}
	}
	return out
}

// Advance a private copy loaded for one claim. A failed validation or
// stale commit discards the copy. Only the next admitted call has effects.
func (c *RunCheckpoint) Advance(r RunRecord, m *manifest.Snapshot, body []byte, now time.Time) (string, error) {
	if len(c.Queue) == 0 || len(c.Outcomes) != r.Profile.ColumnCount {
		return "", ErrRunDrift
	}
	item := c.Queue[0]
	t := item.Task
	if len(body) > int(t.MaxBytes) || !ValidEvidenceJSON(body) {
		return "", ErrInvalid
	}
	c.Queue = c.Queue[1:]
	set := func(cols []int, o Outcome) {
		for _, n := range cols {
			c.Outcomes[n] = o
		}
	}
	switch t.Tool {
	case "source.file.preview":
		var page struct{ Offset, Limit int }
		if json.Unmarshal(t.Arguments, &page) != nil || page.Offset != len(c.Headers) {
			return "", ErrRunDrift
		}
		p, names, err := previewPage(body, r.Profile.AssetID, r.Profile.ProfileID, r.Profile, page.Offset, page.Limit)
		if err != nil || p != r.Profile {
			return "", ErrRunDrift
		}
		c.Headers = append(c.Headers, names...)
		if len(c.Headers) < p.ColumnCount {
			c.Queue = append(c.Queue, previewItem(p, len(c.Headers), page.Limit))
			break
		}
		cap, ok := Find(m, BatchTool)
		if !ok || cap.PublicationState != "ACTIVE" {
			return "", ErrUnavailable
		}
		plan, err := BuildPublished(r.Scope, p, m, c.Headers, nil, r.Policy, 0)
		if err != nil {
			return "", err
		}
		for i, name := range plan.Headers {
			if name == "" {
				c.Outcomes[i] = Outcome{State: "BLOCKED", Reason: "ANALYSIS_COLUMN_INVALID"}
			}
		}
		for _, task := range plan.Tasks {
			if task.State == "HELD" {
				set(task.Columns, Outcome{State: "BLOCKED", Reason: task.Code})
			} else {
				key, err := MandateHash(task.Arguments)
				if err != nil {
					return "", err
				}
				c.Queue = append(c.Queue, RunItem{Intent: key, Task: task})
			}
		}
	case BatchTool:
		rows, ok := AcceptedBatch(t, body)
		var b Batch
		if !ok || json.Unmarshal(t.Arguments, &b) != nil {
			return "", ErrInvalid
		}
		for i, row := range rows {
			cols := t.Groups[row.ID]
			if row.State == "MATCH" {
				set(cols, Outcome{State: "MATCHED_PUBLISHED", Match: &row.Hits[0]})
			} else {
				child := ResidualTask(r.ID(), b.Intents[i], cols, MaxResultBytes, false)
				if child.State == "HELD" {
					set(cols, Outcome{State: "BLOCKED", Reason: child.Code})
				} else {
					c.Queue = append(c.Queue, RunItem{Intent: row.ID, Task: child})
				}
			}
		}
	case "semantic.discovery.request":
		var receipt struct{ RequestID string }
		if json.Unmarshal(body, &receipt) != nil || !CanonicalUUID(receipt.RequestID) {
			return "", ErrInvalid
		}
		set(t.Columns, Outcome{State: "PENDING", RequestID: receipt.RequestID})
		c.enqueueObservation(item, "semantic.discovery.status", receipt.RequestID, nil)
		return receipt.RequestID, nil
	case "semantic.discovery.status", "semantic.discovery.candidates":
		var args struct{ RequestID string }
		if json.Unmarshal(t.Arguments, &args) != nil || !CanonicalUUID(args.RequestID) {
			return "", ErrInvalid
		}
		out, next := DiscoveryEvidenceWithCompletion(t.Tool, args.RequestID, body, item.Completion, now)
		set(t.Columns, out)
		if next != "" {
			c.enqueueObservation(item, next, args.RequestID, CompletedCount(args.RequestID, body))
			if next == "semantic.discovery.status" {
				c.Queue[0].Task.NotBefore = now.Add(5 * time.Second)
			}
		}
	default:
		return "", ErrInvalid
	}

	return "", nil
}

func (c *RunCheckpoint) enqueueObservation(parent RunItem, tool, request string, completion *int) {
	args := map[string]any{"requestId": request}
	if tool == "semantic.discovery.candidates" {
		args["includeState"] = true
	}
	raw, _ := json.Marshal(args)
	t := Task{Work: Work{Tool: tool, Columns: parent.Task.Columns, Arguments: raw}, MaxObjects: 100, MaxBytes: MaxResultBytes, MaxTimeMS: 3000}
	// Observe the accepted owner job before opening the next one.
	c.Queue = append([]RunItem{{Intent: parent.Intent, Task: t, Completion: completion}}, c.Queue...)
}
