package kernel

import (
	"errors"
	"strings"
	"testing"

	"github.com/GioNob/ouf-mcp-server/internal/analysis"
	"github.com/GioNob/ouf-mcp-server/internal/manifest"
	"github.com/GioNob/ouf-mcp-server/internal/nextstep"
)

func TestSummaryLimitsAndErrors(t *testing.T) {
	m, err := manifest.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ key, value string }{
		{"OUF_ANALYSIS_PREVIEW_PAGE_SIZE", "0"}, {"OUF_ANALYSIS_PREVIEW_PAGE_SIZE", "129"},
		{"OUF_ANALYSIS_SUMMARY_PAGE_BYTES", "4095"}, {"OUF_ANALYSIS_SUMMARY_PAGE_BYTES", "1048577"},
		{"OUF_ANALYSIS_CURSOR_SECONDS", "0"}, {"OUF_ANALYSIS_CURSOR_SECONDS", "86401"},
	} {
		t.Run(test.key+test.value, func(t *testing.T) {
			t.Setenv(test.key, test.value)
			if _, err := configuredSummary(m); err == nil {
				t.Fatal("invalid installation limit accepted")
			}
		})
	}
	for _, test := range []struct {
		err    error
		code   string
		status int
	}{
		{analysis.ErrInvalid, "ANALYSIS_INPUT_INVALID", 400},
		{analysis.ErrCursor, "ANALYSIS_CURSOR_INVALID", 400},
		{analysis.ErrExpired, "ANALYSIS_CURSOR_EXPIRED", 410},
		{analysis.ErrScope, "ANALYSIS_NOT_FOUND_OR_SCOPE_CHANGED", 404},
		{analysis.ErrProfile, "ANALYSIS_PROFILE_CHANGED", 409},
		{analysis.ErrConflict, "ANALYSIS_REVISION_CONFLICT", 409},
		{errors.New("private database detail"), "ANALYSIS_STORE_UNAVAILABLE", 503},
	} {
		id := strings.Repeat("b", 64)
		r := summaryFailure(test.err, id)
		body := ownerObject(r)
		if !r.IsError || len(body) != 2 || body["code"] != test.code || r.Meta["ouf/httpStatus"] != test.status {
			t.Fatal(body, r.Meta)
		}
		n := body["next"].(nextstep.Object)
		if test.err == analysis.ErrExpired {
			args := n["arguments"].(nextstep.Object)
			if n["action"] != "CALL_TOOL" || len(args) != 2 || args["view"] != "SUMMARY" || args["analysisId"] != id {
				t.Fatal(n)
			}
		}
	}
}
