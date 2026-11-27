package analysis

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/GioNob/ouf-mcp-server/internal/analysis/unicode151"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
)

const BatchTool = "semantic.search.batch"
const BatchContract = "ouf.semantic.published-batch.v1"
const InvalidOwner = "ANALYSIS_OWNER_RESPONSE_INVALID"

type BatchLimits struct {
	MaxIntents    int      `json:"batchMaxIntents"`
	RequestBytes  int      `json:"batchMaxRequestBytes"`
	ResponseBytes int64    `json:"batchMaxResponseBytes"`
	Hits          int      `json:"limitPerIntent"`
	Languages     []string `json:"preferredLanguages"`
}

func PublishedLimits(cap manifest.Capability) (BatchLimits, error) {
	var l BatchLimits
	if cap.CapabilityID != "ouf.semantic.search.batch" || cap.Owner != "semantic" || cap.OperationClass != "READ" ||
		cap.GatewayBindingRef != "capability://ouf.semantic.search.batch" || cap.RequiredAuthorization != "ouf.semantic.search" ||
		cap.ToolEligible || cap.MCPClass != "MCP_INTERNAL" || !slices.Equal(cap.AllowedActors, []string{"HUMAN"}) {
		return l, ErrInvalid
	}
	var cost struct {
		HardMaxResultBytes     int64
		HardMaxDistinctObjects int
	}
	if json.Unmarshal(cap.OperationalLimits, &l) != nil || json.Unmarshal(cap.BaseCostEnvelope, &cost) != nil ||
		l.MaxIntents < 1 || l.MaxIntents > 64 || l.RequestBytes < 1 || l.RequestBytes > 65536 ||
		l.ResponseBytes < 1 || l.ResponseBytes > 262144 || cost.HardMaxResultBytes < 1 || l.Hits < 1 || l.Hits > 8 {
		return l, ErrInvalid
	}
	if cost.HardMaxDistinctObjects < l.Hits || cost.HardMaxDistinctObjects > 100 {
		return l, ErrInvalid
	}
	l.MaxIntents = min(l.MaxIntents, cost.HardMaxDistinctObjects/l.Hits)
	l.ResponseBytes = min(l.ResponseBytes, cost.HardMaxResultBytes)
	_, err := languages(l.Languages)
	return l, err
}

type Intent struct {
	ID        string            `json:"intentId"`
	Text      string            `json:"intent"`
	Languages []string          `json:"preferredLanguages"`
	Type      string            `json:"requestedArtifactType"`
	Context   map[string]string `json:"context"`
}
type Batch struct {
	Contract string   `json:"contract"`
	Limit    int      `json:"limitPerIntent"`
	Intents  []Intent `json:"intents"`
}

var languageTag = regexp.MustCompile(`^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8}){0,3}$`)

func languages(in []string) ([]string, error) {
	if len(in) < 1 || len(in) > 5 {
		return nil, ErrInvalid
	}
	out := make([]string, 0, len(in))
	for _, tag := range in {
		if !languageTag.MatchString(tag) {
			return nil, ErrInvalid
		}
		tag = strings.ToLower(tag)
		if slices.Contains(out, tag) {
			return nil, ErrInvalid
		}
		out = append(out, tag)
	}
	return out, nil
}

// RFC 8785 string serialization. The only object keys in this tuple are
// the three ASCII context keys, for which UTF-16 order equals byte order.
func jcsString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, c := range s {
		switch c {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteRune(c)
		case '\b':
			b.WriteString(`\b`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\f':
			b.WriteString(`\f`)
		case '\r':
			b.WriteString(`\r`)
		default:
			if c < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, c)
			} else {
				b.WriteRune(c)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

func (i *Intent) identify() error {
	size := utf8.RuneCountInString(i.Text)
	if !utf8.ValidString(i.Text) || size < 1 || size > 256 {
		return ErrInvalid
	}
	text := unicode151.Normalize(i.Text)
	if text == "" || utf8.RuneCountInString(text) > 256 {
		return ErrInvalid
	}
	langs, err := languages(i.Languages)
	if err != nil {
		return err
	}
	i.Languages = langs
	if i.Type != "PROPERTY" && i.Type != "CLASS" && i.Type != "DATATYPE" {
		return ErrInvalid
	}
	if i.Context == nil {
		i.Context = map[string]string{}
	}
	fields := make([]string, 0, len(i.Context))
	for k, v := range i.Context {
		maximum := 2048
		switch k {
		case "namespace":
			maximum = 512
		case "domain", "range":
		default:
			return ErrInvalid
		}
		if v == "" || !utf8.ValidString(v) || utf8.RuneCountInString(v) > maximum {
			return ErrInvalid
		}
		fields = append(fields, jcsString(k)+":"+jcsString(v))
	}
	sort.Strings(fields)
	quoted := make([]string, len(langs))
	for n, v := range langs {
		quoted[n] = jcsString(v)
	}
	canonical := `["norm-v1",` + jcsString(text) + `,[` + strings.Join(quoted, ",") + `],` + jcsString(i.Type) + `,{` + strings.Join(fields, ",") + `}]`
	i.ID = fmt.Sprintf("%x", sha256.Sum256([]byte(canonical)))
	return nil
}

func batchTask(list []Intent, limit int, columns map[string][]int, discoveryText map[int]string, responseBytes int64) Task {
	args, _ := json.Marshal(Batch{BatchContract, limit, list})
	groups := make(map[string][]int, len(list))
	var ordinals []int
	for _, v := range list {
		groups[v.ID] = columns[v.ID]
		ordinals = append(ordinals, columns[v.ID]...)
	}
	sort.Ints(ordinals)
	texts := map[int]string{}
	for _, n := range ordinals {
		if text, ok := discoveryText[n]; ok {
			texts[n] = text
		}
	}
	return Task{DiscoveryText: texts, ID: Digest([]any{BatchTool, args}), Work: Work{Tool: BatchTool, Arguments: args, Columns: ordinals}, Groups: groups,
		State: "READY", MaxBytes: responseBytes, MaxTimeMS: 3000, MaxObjects: int64(len(list) * limit)}
}

// The caller supplies a verified, contiguous profile. Work overrides are
// scoped to named ordinals. Unmentioned columns use the plan's defaults.
func BuildPublished(scope Scope, profile Profile, snapshot *manifest.Snapshot, headers []string, work []Work, visibility string, envelopeBytes int, epoch ...string) (Plan, error) {
	if len(headers) != profile.ColumnCount || !profile.Valid() || !scope.Valid() || visibility == "" {
		return Plan{}, ErrInvalid
	}
	cap, ok := Find(snapshot, BatchTool)
	if !ok {
		return Plan{}, ErrUnavailable
	}
	l, err := PublishedLimits(cap)
	if err != nil {
		return Plan{}, err
	}
	checksum, err := snapshot.Checksum()
	if err != nil {
		return Plan{}, err
	}
	p := Plan{ID: AnalysisID(scope, profile, epoch...), Scope: scope, Profile: profile, Manifest: checksum, Headers: slices.Clone(headers), Visibility: visibility, Seed: Digest(work)}
	if len(epoch) != 0 {
		p.Epoch = epoch[0]
	}
	var ordered []Intent
	columns := map[string][]int{}
	discoveryText := map[int]string{}
	previewNames := map[int]string{}
	for _, w := range work {
		if w.PreviewName != "" {
			for _, n := range w.Columns {
				previewNames[n] = w.PreviewName
			}
		}
	}
	overrides, err := intentOverrides(work, len(headers))
	if err != nil {
		return Plan{}, err
	}
	for n, name := range headers {
		if name == "" {
			continue
		}
		i, ok := overrides[n]
		if !ok {
			// Without a client-supplied intent, look up the original label.
			// Word splitting here changes exact matches and the norm-v1 group.
			i = Intent{Text: name, Languages: l.Languages, Type: "PROPERTY"}
		}
		if err := i.identify(); err != nil {
			if ok && i.Text != name && i.Text != ColumnIntent(name) {
				return Plan{}, err
			}
			// Only an unusable profile term is isolated. Invalid options are input errors.
			test := i
			test.Text = "x"
			if test.identify() != nil {
				return Plan{}, err
			}
			p.Headers[n] = ""
			continue
		}
		// Keep preview wording for review. The verified header drives both
		// the published lookup and the initial residual discovery.
		if ok && usesVerifiedHeader(i.Text, previewNames[n], name) {
			description := i.Text
			i.Text = name
			if i.identify() != nil {
				p.Headers[n] = ""
				continue
			}
			discoveryText[n] = description
		}
		if _, seen := columns[i.ID]; !seen {
			ordered = append(ordered, i)
		}
		columns[i.ID] = append(columns[i.ID], n)
	}
	var chunk []Intent
	flush := func() {
		if len(chunk) > 0 {
			p.Tasks = append(p.Tasks, batchTask(chunk, l.Hits, columns, discoveryText, l.ResponseBytes))
			chunk = nil
		}
	}
	for _, i := range ordered {
		list := append(chunk, i)
		body, _ := json.Marshal(Batch{BatchContract, l.Hits, list})
		if len(list) > l.MaxIntents || len(body)+envelopeBytes > l.RequestBytes {
			flush()
			body, _ = json.Marshal(Batch{BatchContract, l.Hits, []Intent{i}})
			if len(body)+envelopeBytes > l.RequestBytes {
				t := batchTask([]Intent{i}, l.Hits, columns, discoveryText, l.ResponseBytes)
				t.State, t.Code = "HELD", "SEM_BATCH_REQUEST_TOO_LARGE"
				p.Tasks = append(p.Tasks, t)
				continue
			}
		}
		chunk = append(chunk, i)
	}
	flush()
	return p, nil
}

// ResidualTasks uses the published intent for the initial discovery.
func (t Task) ResidualTasks(planID string, i Intent, fallback bool) []Task {
	columns := t.Groups[i.ID]
	if len(columns) == 0 {
		return nil
	}
	if !fallback {
		return []Task{ResidualTask(planID, i, slices.Clone(columns), MaxResultBytes, false)}
	}
	tasks := make([]Task, 0, len(columns))
	for _, n := range columns {
		tasks = append(tasks, ResidualTask(planID, i, []int{n}, MaxResultBytes, true))
	}
	return tasks
}

func ResidualTask(planID string, i Intent, columns []int, maxBytes int64, fallback bool) Task {
	key := Digest([]string{planID, i.ID})
	code := "PUBLISHED_RESIDUAL"
	if fallback {
		key = Digest([]any{planID, i.ID, columns})
		code = "PUBLISHED_BATCH_UNAVAILABLE"
	}
	args, _ := json.Marshal(map[string]any{"intent": i.Text, "preferredLanguages": i.Languages, "requestedArtifactType": i.Type, "idempotencyKey": "analysis:" + key})
	t := Task{ID: Digest([]any{"semantic.discovery.request", key}), Work: Work{Tool: "semantic.discovery.request", Arguments: args, Columns: columns}, State: "READY", Code: code, MaxBytes: maxBytes, MaxTimeMS: 3000, MaxObjects: 100, OwnerIntent: i.ID, OwnerKey: "analysis:" + key}
	if i.Type == "DATATYPE" {
		t.State, t.Code = "HELD", "DISCOVERY_TYPE_UNSUPPORTED"
	}
	return t
}

func intentOverrides(work []Work, width int) (map[int]Intent, error) {
	overrides := map[int]Intent{}
	for _, w := range work {
		if w.Tool != "semantic.discovery.request" || w.Observation != 0 || len(w.Columns) == 0 || !ValidEvidenceJSON(w.Arguments) {
			return nil, ErrInvalid
		}
		var args map[string]json.RawMessage
		var i Intent
		if json.Unmarshal(w.Arguments, &args) != nil || json.Unmarshal(w.Arguments, &i) != nil {
			return nil, ErrInvalid
		}
		for k, v := range args {
			if string(v) == "null" || (k != "intent" && k != "preferredLanguages" && k != "requestedArtifactType" && k != "context") {
				return nil, ErrInvalid
			}
		}
		for _, n := range w.Columns {
			if n < 0 || n >= width {
				return nil, ErrInvalid
			}
			if _, exists := overrides[n]; exists {
				return nil, ErrInvalid
			}
			overrides[n] = i
		}
	}
	return overrides, nil
}

// Preserve v1 discovery inputs (including their wider intent and type
// ranges). Contextual T1 inputs still need distinct persisted owner keys.
func buildWithoutBatch(scope Scope, profile Profile, m *manifest.Snapshot, headers []string, expected int64, work []Work, epoch ...string) (Plan, error) {
	if !scope.Valid() || !profile.Valid() || expected < 0 || len(work) < 1 || len(work) > MaxPlanBatch ||
		(len(headers) != 0 && len(headers) != profile.ColumnCount) {
		return Plan{}, ErrInvalid
	}
	checksum, err := m.Checksum()
	if err != nil {
		return Plan{}, err
	}
	p := Plan{ID: AnalysisID(scope, profile, epoch...), Scope: scope, Profile: profile, Manifest: checksum, ExpectedRevision: expected, Headers: headers}
	if len(epoch) != 0 {
		p.Epoch = epoch[0]
	}
	var legacy, contextual []Work
	previewNames := make(map[int]string)
	for _, w := range work {
		var i Intent
		if err := json.Unmarshal(w.Arguments, &i); err != nil {
			return Plan{}, ErrInvalid
		}
		if i.Context == nil && i.Type != "DATATYPE" {
			if len(w.Arguments) > 8192 || len(w.Columns) < 1 || len(w.Columns) > 1024 {
				return Plan{}, ErrInvalid
			}
			legacy = append(legacy, initialLegacyDiscovery(p.Headers, w, i.Text)...)
		} else {
			contextual = append(contextual, w)
			if w.PreviewName != "" {
				for _, n := range w.Columns {
					previewNames[n] = w.PreviewName
				}
			}
		}
	}
	overrides, err := intentOverrides(contextual, profile.ColumnCount)
	if err != nil {
		return Plan{}, err
	}
	for n := 0; n < profile.ColumnCount; n++ {
		i, ok := overrides[n]
		if !ok {
			continue
		}
		var description string
		if n < len(p.Headers) && usesVerifiedHeader(i.Text, previewNames[n], p.Headers[n]) {
			description, i.Text = i.Text, p.Headers[n]
		}
		if err = i.identify(); err != nil {
			return Plan{}, err
		}
		task := ResidualTask(p.ID, i, []int{n}, MaxResultBytes, true)
		if description != "" {
			task.DiscoveryText = map[int]string{n: description}
		}
		p.Tasks = append(p.Tasks, task)
	}
	if len(legacy) > 0 {
		p, err = buildWork(p, m, legacy)
		if err != nil {
			return Plan{}, err
		}
	}
	for n := range p.Tasks {
		p.Tasks[n].Compatibility = "PUBLISHED_BATCH_UNAVAILABLE"
		if p.Tasks[n].State == "READY" {
			p.Tasks[n].Code = p.Tasks[n].Compatibility
		}
	}
	return p, nil
}

func usesVerifiedHeader(intent, previewName, header string) bool {
	return header != "" && intent != "" && intent != header && (intent == ColumnIntent(header) || previewName == header)
}

func initialLegacyDiscovery(headers []string, w Work, intent string) []Work {
	if len(headers) == 0 {
		return []Work{w}
	}
	var rewritten []Work
	var unchanged []int
	var args map[string]json.RawMessage
	byHeader := make(map[string]int)
	for _, n := range w.Columns {
		if n < 0 || n >= len(headers) || !usesVerifiedHeader(intent, w.PreviewName, headers[n]) {
			unchanged = append(unchanged, n)
			continue
		}
		header := headers[n]
		if index, ok := byHeader[header]; ok {
			rewritten[index].Columns = append(rewritten[index].Columns, n)
			continue
		}
		if args == nil {
			_ = json.Unmarshal(w.Arguments, &args)
		}
		args["intent"], _ = json.Marshal(header)
		task := w
		task.Arguments, _ = json.Marshal(args)
		task.Columns, task.discoveryText = []int{n}, intent
		byHeader[header] = len(rewritten)
		rewritten = append(rewritten, task)
	}
	if len(rewritten) == 0 {
		return []Work{w}
	}
	if len(unchanged) > 0 {
		w.Columns = unchanged
		rewritten = append(rewritten, w)
	}
	return rewritten
}
