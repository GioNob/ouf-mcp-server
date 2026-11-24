package analysis

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/GioNob/ouf-mcp-server/internal/orchestration"
)

const RenewalContract = "ouf.analysis.renewal.v1"
const RenewalTool = "source.analysis.renewal.propose"
const RenewalCheckPath = "/internal/analysis/v1/mandates/renewal-check"
const RenewalBodyMax = 131072
const RenewalVersionMax = 1<<53 - 1

var reviewRefPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{22,128}$`)
var commandKeyPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

type RenewalItem struct {
	Field   string `json:"field"`
	Comment string `json:"comment"`
}

type ProposalVersion struct {
	Version   int64     `json:"proposalVersion"`
	Reason    string    `json:"reason"`
	Remaining []int     `json:"remainingOrdinals"`
	Initial   RunCost   `json:"initialMaximum"`
	Consumed  RunCost   `json:"consumed"`
	Increment RunCost   `json:"increment"`
	Total     RunCost   `json:"totalMaximum"`
	Expires   time.Time `json:"expiresAt"`
}

type RenewalProposal struct {
	ProposalVersion
	Contract         string           `json:"contract"`
	ReviewRef        string           `json:"reviewRef"`
	AnalysisID       string           `json:"analysisId"`
	IncidentID       string           `json:"incidentId"`
	ExpectedSeq      int64            `json:"expectedSeq,string"`
	MandateID        string           `json:"mandateId"`
	Generation       string           `json:"generation"`
	Hash             string           `json:"proposalHash"`
	Previous         *ProposalVersion `json:"previous"`
	RequestedChanges []RenewalItem    `json:"requestedChanges"`
	State            string           `json:"proposalState"`
	PlanLabel        string           `json:"planLabel"`
}

func PlanLabel(file string, columns int) string {
	suffix := fmt.Sprintf("%d colonne", columns)
	name := sourceBasename(file)
	if name == "" {
		return suffix
	}
	suffix = " · " + suffix
	chars := []rune(name)
	return string(chars[:min(len(chars), 200-utf8.RuneCountInString(suffix))]) + suffix
}

func sourceBasename(file string) string {
	name := strings.TrimSpace(path.Base(strings.ReplaceAll(file, "\\", "/")))
	if name == "." || name == ".." || name == "/" || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return ""
	}
	chars := []rune(name)
	return string(chars[:min(len(chars), 200)])
}

type RenewalCommand struct {
	ReviewRef   string        `json:"reviewRef"`
	Version     int64         `json:"proposalVersion"`
	Hash        string        `json:"proposalHash"`
	ExpectedSeq string        `json:"expectedSeq"`
	Key         string        `json:"commandKey"`
	Decision    string        `json:"decision"`
	Items       []RenewalItem `json:"items"`
}

func ParseRenewalCommand(raw []byte) (RenewalCommand, error) {
	var c RenewalCommand
	fields, ok := closedFields(raw, "reviewRef", "proposalVersion", "proposalHash", "expectedSeq", "commandKey", "decision", "items")
	if !ok || !ValidEvidenceJSON(raw) || json.Unmarshal(raw, &c) != nil || !reviewRefPattern.MatchString(c.ReviewRef) ||
		c.Version < 1 || c.Version > RenewalVersionMax || ValidateID(c.Hash) != nil || strings.ToLower(c.Hash) != c.Hash ||
		!canonicalSequence(c.ExpectedSeq) || !commandKeyPattern.MatchString(c.Key) || c.Items == nil || len(c.Items) > 16 {
		return c, ErrInvalid
	}
	switch c.Decision {
	case "APPROVE", "CHANGES_REQUESTED", "CLOSE_PARTIAL":
	default:
		return c, ErrInvalid
	}
	var items []json.RawMessage
	if json.Unmarshal(fields["items"], &items) != nil {
		return c, ErrInvalid
	}
	for i, item := range c.Items {
		if _, ok := closedFields(items[i], "field", "comment"); !ok || strings.TrimSpace(item.Comment) == "" || utf8.RuneCountInString(item.Comment) > 500 {
			return c, ErrInvalid
		}
		switch item.Field {
		case "Calls", "Attempts", "Objects", "Bytes", "Millis", "general":
		default:
			return c, ErrInvalid
		}
	}
	return c, nil
}

type RenewalRevision struct {
	AnalysisID       string        `json:"analysisId"`
	IncidentID       string        `json:"incidentId"`
	Version          int64         `json:"proposalVersion"`
	Key              string        `json:"commandKey"`
	RequestedChanges []RenewalItem `json:"requestedChanges"`
	Increment        *RunCost      `json:"increment,omitempty"`
}

type RenewalStore interface {
	ReadRenewal(context.Context, orchestration.Identity, string) (RenewalProposal, error)
	DecideRenewal(context.Context, orchestration.Identity, RenewalCommand, RunRenewalAuthority) (json.RawMessage, error)
}

type RenewalPlanner interface {
	CurrentRenewal(context.Context, RunRecord) (RenewalProposal, error)
	ReviseRenewal(context.Context, orchestration.Identity, RenewalRevision) (RenewalProposal, error)
}

func (c RunCost) Dimensions() map[string]int64 {
	return map[string]int64{"Calls": c.Calls, "Attempts": c.Attempts, "Objects": c.Objects, "Bytes": c.Bytes, "Millis": c.Millis}
}
