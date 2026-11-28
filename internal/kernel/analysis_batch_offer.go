package kernel

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func localBatchResult(body, next nextstep.Object) *mcp.CallToolResult {
	return markBatchResult(&mcp.CallToolResult{StructuredContent: body, Meta: mcp.Meta{batchNextMeta: next}})
}

func batchReadFailure(id string, err error) *mcp.CallToolResult {
	result := analysisReadError(err)
	if analysis.ValidateID(id) == nil {
		body := ownerObject(result)
		body["analysisId"] = id
		result.StructuredContent = body
	}
	return markBatchResult(result)
}

func (s *workflowServer) readAnalysisBatch(ctx context.Context, c analysis.Coordinator, identity orchestration.Identity, id, view string) *mcp.CallToolResult {
	store, ok := c.Store.(analysis.BatchOfferStore)
	if !ok {
		return batchUnavailable(id)
	}
	checksum, err := c.Manifest.Checksum()
	if err != nil {
		return batchReadFailure(id, err)
	}
	scope := analysis.IdentityScope(identity)
	engine := s.engine(ctx)
	expires := time.Now().Add(time.Duration(s.summaryLimits.SummaryCursorSeconds) * time.Second).Unix()
	var pack func(analysis.BatchOffer) ([]analysis.BatchOfferItem, error)
	if view == "BATCH" {
		pack = func(offer analysis.BatchOffer) ([]analysis.BatchOfferItem, error) {
			return s.packBatchOffer(engine, scope, offer, expires)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	read, err := store.ReadBatch(ctx, scope, id, checksum, pack)
	if err != nil {
		return batchReadFailure(id, err)
	}
	if read.BatchStart.Initialized && c.Service.ObserveBatchAuthority != nil {
		c.Service.ObserveBatchAuthority(id, identity)
	}
	var next nextstep.Object
	if read.Offer.ID != "" {
		if err = checkBatchEvidence(ctx, c, identity, read.Epoch, read.Offer.Items); err != nil {
			return batchReadFailure(id, err)
		}
		body, offerNext := s.batchOfferBody(engine, scope, read.Offer, expires)
		if view == "BATCH" {
			return localBatchResult(body, offerNext)
		}
		next = offerNext
	} else {
		next = engine.BatchStatusNext(id, read.Reason, read.CreatedAt)
	}
	body := nextstep.Object{"analysisId": id, "mode": analysis.BatchMode, "revision": read.Revision, "phase": read.Phase, "counts": read.Counts}
	return localBatchResult(body, next)
}

func checkBatchEvidence(ctx context.Context, c analysis.Coordinator, identity orchestration.Identity, epoch string, items []analysis.BatchOfferItem) error {
	evidence := analysis.Summary{Epoch: epoch}
	for _, item := range items {
		evidence.Hint.RequestID = item.Item.RequestID
		if item.Item.CandidateCount > 0 {
			evidence.Counts.Ready++
		}
	}
	return c.EvidenceAccess(ctx, identity, evidence)
}

func (s *workflowServer) batchOfferBody(engine nextstep.Engine, scope analysis.Scope, offer analysis.BatchOffer, expires int64) (nextstep.Object, nextstep.Object) {
	items := make([]analysis.BatchItem, len(offer.Items))
	ordinal, cursor := 0, ""
	for n, evidence := range offer.Items {
		item := evidence.Item
		pos, more := batchColumnStart(item)
		if more {
			item.CandidateCursor = s.batchColumnToken(scope, offer, evidence, pos, expires)
			if cursor == "" {
				ordinal, cursor = item.Ordinal, item.CandidateCursor
			}
		}
		items[n] = item
	}
	body := nextstep.Object{"mode": analysis.BatchMode, "view": "BATCH", "analysisId": offer.AnalysisID,
		"batchId": offer.ID, "expectedRevision": offer.Revision, "items": items}
	return body, engine.BatchDecisionNext(offer.AnalysisID, offer.ID, offer.Revision, ordinal, cursor)
}

func batchFits(body, next nextstep.Object) bool {
	body["next"] = next
	wire, err := json.Marshal(&mcp.CallToolResult{StructuredContent: body, Content: []mcp.Content{&mcp.TextContent{Text: batchResultText}}})
	delete(body, "next")
	return err == nil && len(wire) <= analysis.BatchMaxResultBytes-summaryEnvelopeReserve
}

func (s *workflowServer) packBatchOffer(engine nextstep.Engine, scope analysis.Scope, full analysis.BatchOffer, expires int64) ([]analysis.BatchOfferItem, error) {
	offered := full
	offered.Items = make([]analysis.BatchOfferItem, 0, len(full.Items))
	fits := func() bool {
		body, next := s.batchOfferBody(engine, scope, offered, expires)
		return batchFits(body, next)
	}
	for _, evidence := range full.Items {
		item := &evidence.Item
		item.TruncatedFields = nil
		if len(item.Examples) != 0 {
			item.Examples = []json.RawMessage{}
			item.TruncatedFields = append(item.TruncatedFields, "examples")
		}
		item.Name, item.TruncatedFields = batchAbbreviate(item.Name, "name", item.TruncatedFields)
		item.Intent, item.TruncatedFields = batchAbbreviate(item.Intent, "intent", item.TruncatedFields)
		if len(item.Candidates) > 0 {
			item.Candidates = []analysis.BatchCandidate{batchShortCandidate(item.Candidates[0])}
		}
		offered.Items = append(offered.Items, evidence)
		if !fits() {
			offered.Items = offered.Items[:len(offered.Items)-1]
			break
		}
	}
	if len(offered.Items) == 0 {
		return nil, analysis.ErrProfile
	}
	// First reserve the required projection for each selected column. Add
	// optional data in ordinal order using the decorated MCP wire size.
	for n := range offered.Items {
		item, source := &offered.Items[n].Item, full.Items[n].Item
		for _, field := range []struct {
			name string
			dst  *string
			text string
		}{{"name", &item.Name, source.Name}, {"intent", &item.Intent, source.Intent}} {
			if *field.dst != field.text {
				short, fields := *field.dst, item.TruncatedFields
				*field.dst, item.TruncatedFields = field.text, withoutBatchField(fields, field.name)
				if !fits() {
					*field.dst, item.TruncatedFields = short, fields
				}
			}
		}
		if len(source.Candidates) > 0 && len(item.Candidates[0].TruncatedFields) > 0 {
			short := item.Candidates[0]
			item.Candidates[0] = source.Candidates[0]
			if !fits() {
				item.Candidates[0] = short
			}
		}
		for index, example := range source.Examples {
			fields := item.TruncatedFields
			item.Examples = append(item.Examples, example)
			if index+1 == len(source.Examples) {
				item.TruncatedFields = withoutBatchField(fields, "examples")
			}
			if !fits() {
				item.Examples = item.Examples[:index]
				item.TruncatedFields = fields
				break
			}
		}
		for _, candidate := range source.Candidates[len(item.Candidates):] {
			item.Candidates = append(item.Candidates, candidate)
			if !fits() {
				item.Candidates = item.Candidates[:len(item.Candidates)-1]
				break
			}
		}
	}
	return offered.Items, nil
}

func withoutBatchField(fields []string, field string) []string {
	out := slices.Clone(fields)
	return slices.DeleteFunc(out, func(value string) bool { return value == field })
}

func batchShortCandidate(c analysis.BatchCandidate) analysis.BatchCandidate {
	c.TruncatedFields = slices.Clone(c.TruncatedFields)
	c.Label, c.TruncatedFields = batchAbbreviate(c.Label, "label", c.TruncatedFields)
	c.Term, c.TruncatedFields = batchAbbreviate(c.Term, "term", c.TruncatedFields)
	return c
}

func batchAbbreviate(text, field string, fields []string) (string, []string) {
	const maxBytes = 96
	wire, _ := json.Marshal(text)
	if len(wire) <= maxBytes {
		return text, fields
	}
	short := batchTextPrefix(text, maxBytes)
	if !slices.Contains(fields, field) {
		fields = append(fields, field)
	}
	return short, fields
}

func batchTextPrefix(text string, limit int) string {
	runes := []rune(text)
	// Find the largest rune prefix that fits after JSON escaping.
	low, high := 0, len(runes)
	for low < high {
		mid := low + (high-low+1)/2
		wire, _ := json.Marshal(string(runes[:mid]))
		if len(wire) <= limit {
			low = mid
		} else {
			high = mid - 1
		}
	}
	return string(runes[:low])
}

const (
	batchCandidateList = iota
	batchNameDetail
	batchIntentDetail
	batchExampleDetail
	batchLabelDetail
	batchTermDetail
	batchPageLabelDetail
	batchPageTermDetail
)

type batchColumnPosition struct {
	kind, index, offset int
}

func batchColumnStart(item analysis.BatchItem) (batchColumnPosition, bool) {
	if len(item.Candidates) < item.CandidateCount {
		return batchColumnPosition{kind: batchCandidateList, index: len(item.Candidates)}, true
	}
	return nextBatchDetail(item, batchColumnPosition{}, nil)
}

func batchColumnBinding(id, batchID string, ordinal int) string {
	return analysis.Digest([]any{"ouf.analysis.batch.candidates.v1", id, batchID, ordinal})
}

func batchColumnSnapshot(offer analysis.BatchOffer, item analysis.BatchOfferItem) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(analysis.Digest([]any{offer.ID, offer.Revision,
		item.Item.Ordinal, item.Item.Attempt, item.Item.RequestID, item.EvidenceVersion})))
}

func (s *workflowServer) batchColumnToken(scope analysis.Scope, offer analysis.BatchOffer, item analysis.BatchOfferItem, pos batchColumnPosition, expires int64) string {
	return (analysis.Position{Seq: int64(pos.index), Limit: pos.kind, Last: pos.offset, Expires: expires, Snapshot: batchColumnSnapshot(offer, item)}).
		Token(s.key, scope, batchColumnBinding(offer.AnalysisID, offer.ID, item.Item.Ordinal))
}

func nextBatchDetail(item analysis.BatchItem, after batchColumnPosition, examples []json.RawMessage) (batchColumnPosition, bool) {
	if after.kind < batchNameDetail && slices.Contains(item.TruncatedFields, "name") {
		return batchColumnPosition{kind: batchNameDetail}, true
	}
	if after.kind < batchIntentDetail && slices.Contains(item.TruncatedFields, "intent") {
		return batchColumnPosition{kind: batchIntentDetail}, true
	}
	if slices.Contains(item.TruncatedFields, "examples") {
		if after.kind < batchExampleDetail {
			return batchColumnPosition{kind: batchExampleDetail, index: len(item.Examples)}, true
		}
		if after.kind == batchExampleDetail && after.index+1 < len(examples) {
			return batchColumnPosition{kind: batchExampleDetail, index: after.index + 1}, true
		}
	}
	for n, candidate := range item.Candidates {
		if after.kind >= batchLabelDetail && n < after.index {
			continue
		}
		if (after.kind < batchLabelDetail || n > after.index) && slices.Contains(candidate.TruncatedFields, "label") {
			return batchColumnPosition{kind: batchLabelDetail, index: n}, true
		}
		if (after.kind < batchTermDetail || n > after.index) && slices.Contains(candidate.TruncatedFields, "term") {
			return batchColumnPosition{kind: batchTermDetail, index: n}, true
		}
	}
	return batchColumnPosition{}, false
}

func (s *workflowServer) readBatchCandidates(ctx context.Context, c analysis.Coordinator, identity orchestration.Identity, start analysis.BatchStart, input nextstep.Object) *mcp.CallToolResult {
	store, ok := c.Store.(analysis.BatchOfferStore)
	if !ok {
		return batchUnavailable(start.ID)
	}
	batchID, _ := input["batchId"].(string)
	ordinal, _ := input["ordinal"].(float64)
	token, _ := input["cursor"].(string)
	scope := analysis.IdentityScope(identity)
	engine := s.engine(ctx)
	position, err := analysis.OpenPosition(token, s.key, scope, batchColumnBinding(start.ID, batchID, int(ordinal)), time.Now())
	if err != nil {
		result := batchReadFailure(start.ID, err)
		if errors.Is(err, analysis.ErrExpired) {
			result.Meta[batchNextMeta] = engine.BatchPageNext(nextstep.Object{"analysisId": start.ID, "view": "BATCH"}, true)
		}
		return result
	}
	if position.Seq < 0 || position.Seq >= 100 || position.Limit > batchPageTermDetail {
		return batchReadFailure(start.ID, analysis.ErrCursor)
	}
	pos := batchColumnPosition{kind: position.Limit, index: int(position.Seq), offset: position.Last}
	offset := 0
	if pos.kind == batchCandidateList || pos.kind >= batchLabelDetail {
		offset = pos.index
	}
	checksum, err := c.Manifest.Checksum()
	if err != nil {
		return batchReadFailure(start.ID, err)
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	offer, item, err := store.ReadBatchColumn(ctx, scope, start.ID, checksum, batchID, int(ordinal), offset)
	if err != nil {
		return batchReadFailure(start.ID, err)
	}
	if position.Snapshot != batchColumnSnapshot(offer, item) {
		return batchReadFailure(start.ID, analysis.ErrScope)
	}
	if err = checkBatchEvidence(ctx, c, identity, start.Epoch, []analysis.BatchOfferItem{item}); err != nil {
		return batchReadFailure(start.ID, err)
	}
	body := nextstep.Object{"mode": analysis.BatchMode, "view": "CANDIDATES", "analysisId": start.ID,
		"batchId": batchID, "ordinal": int(ordinal), "attempt": item.Item.Attempt,
		"requestId": item.Item.RequestID, "candidateCount": item.Item.CandidateCount, "candidates": []analysis.BatchCandidate{}}
	nextFor := func(p batchColumnPosition, more bool) nextstep.Object {
		if !more {
			delete(body, "nextCursor")
			return engine.BatchPageNext(nextstep.Object{"analysisId": start.ID, "view": "BATCH"}, false)
		}
		cursor := s.batchColumnToken(scope, offer, item, p, position.Expires)
		body["nextCursor"] = cursor
		return engine.BatchPageNext(nextstep.Object{"analysisId": start.ID, "view": "CANDIDATES", "batchId": batchID,
			"ordinal": int(ordinal), "cursor": cursor}, false)
	}
	for _, frozen := range offer.Items {
		if frozen.Item.Ordinal != int(ordinal) {
			continue
		}
		var next nextstep.Object
		if pos.kind == batchCandidateList {
			next, err = batchCandidatePage(body, item.Item, frozen.Item, pos, nextFor)
		} else {
			next, err = batchDetailPage(body, item.Item, frozen.Item, pos, nextFor)
		}
		if err != nil {
			return batchReadFailure(start.ID, err)
		}
		return localBatchResult(body, next)
	}
	return batchReadFailure(start.ID, analysis.ErrScope)
}

func batchCandidatePage(body nextstep.Object, item, frozen analysis.BatchItem, pos batchColumnPosition, nextFor func(batchColumnPosition, bool) nextstep.Object) (nextstep.Object, error) {
	if pos.index >= item.CandidateCount || len(item.Candidates) == 0 {
		return nil, analysis.ErrProfile
	}
	items := make([]analysis.BatchCandidate, 0, analysis.BatchMaxItems)
	var next nextstep.Object
	for n, candidate := range item.Candidates[:min(len(item.Candidates), analysis.BatchMaxItems)] {
		index := pos.index + n
		after, more := batchColumnPosition{kind: batchCandidateList, index: index + 1}, index+1 < item.CandidateCount
		if !more {
			after, more = nextBatchDetail(frozen, batchColumnPosition{}, nil)
		}
		items = append(items, candidate)
		body["candidates"] = items
		candidateNext := nextFor(after, more)
		if batchFits(body, candidateNext) {
			next = candidateNext
			continue
		}
		if n > 0 {
			body["candidates"] = items[:n]
			return nextFor(batchColumnPosition{kind: batchCandidateList, index: index}, true), nil
		}
		items[0] = batchShortCandidate(candidate)
		if slices.Contains(items[0].TruncatedFields, "label") {
			after, more = batchColumnPosition{kind: batchPageLabelDetail, index: index}, true
		} else if slices.Contains(items[0].TruncatedFields, "term") {
			after, more = batchColumnPosition{kind: batchPageTermDetail, index: index}, true
		}
		return nextFor(after, more), nil
	}
	return next, nil
}

func batchDetailPage(body nextstep.Object, item, frozen analysis.BatchItem, pos batchColumnPosition, nextFor func(batchColumnPosition, bool) nextstep.Object) (nextstep.Object, error) {
	detail := nextstep.Object{"offset": pos.offset}
	var text string
	switch pos.kind {
	case batchNameDetail:
		detail["field"], text = "name", item.Name
	case batchIntentDetail:
		detail["field"], text = "intent", item.Intent
	case batchExampleDetail:
		if pos.index >= len(item.Examples) {
			return nil, analysis.ErrCursor
		}
		detail["field"], detail["exampleIndex"] = "examples", pos.index
		raw := item.Examples[pos.index]
		if json.Unmarshal(raw, &text) != nil {
			text = string(raw)
		}
	case batchLabelDetail, batchTermDetail, batchPageLabelDetail, batchPageTermDetail:
		if len(item.Candidates) == 0 {
			return nil, analysis.ErrCursor
		}
		candidate := item.Candidates[0]
		detail["candidateId"] = candidate.ID
		detail["field"], text = "label", candidate.Label
		if pos.kind == batchTermDetail || pos.kind == batchPageTermDetail {
			detail["field"], text = "term", candidate.Term
		}
	}
	runes := []rune(text)
	if pos.offset >= len(runes) {
		return nil, analysis.ErrCursor
	}
	part := batchTextPrefix(string(runes[pos.offset:]), 4096)
	detail["text"] = part
	body["detail"] = detail
	if end := pos.offset + len([]rune(part)); end < len(runes) {
		pos.offset = end
		return nextFor(pos, true), nil
	}
	if pos.kind == batchPageLabelDetail || pos.kind == batchPageTermDetail {
		if pos.kind == batchPageLabelDetail {
			short := batchShortCandidate(item.Candidates[0])
			if slices.Contains(short.TruncatedFields, "term") {
				return nextFor(batchColumnPosition{kind: batchPageTermDetail, index: pos.index}, true), nil
			}
		}
		if pos.index+1 < item.CandidateCount {
			return nextFor(batchColumnPosition{kind: batchCandidateList, index: pos.index + 1}, true), nil
		}
		pos = batchColumnPosition{}
	}
	p, more := nextBatchDetail(frozen, pos, item.Examples)
	return nextFor(p, more), nil
}
