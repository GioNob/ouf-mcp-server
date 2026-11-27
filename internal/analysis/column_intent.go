package analysis

import (
	"strings"
	"unicode"
)

// ColumnIntent supplies initial wording. Authored and retained intents keep
// their original text and identity.
func ColumnIntent(name string) string {
	runes := []rune(name)
	var b strings.Builder
	for i, r := range runes {
		if r == '_' {
			b.WriteByte(' ')
			continue
		}
		if i > 0 && unicode.IsUpper(r) {
			prev := runes[i-1]
			if unicode.IsLower(prev) || unicode.IsDigit(prev) ||
				(unicode.IsUpper(prev) && i+1 < len(runes) && unicode.IsLower(runes[i+1])) {
				b.WriteByte(' ')
				if i+1 < len(runes) && unicode.IsLower(runes[i+1]) {
					r = unicode.ToLower(r)
				}
			}
		}
		b.WriteRune(r)
	}
	text := strings.Join(strings.Fields(b.String()), " ")
	// Inserted spaces must not push an accepted header past the discovery limit.
	limit := 2000
	if len(runes) <= 256 {
		// A header that fit the published batch also fits after word splitting.
		limit = 256
	}
	if expanded := []rune(text); len(expanded) > limit {
		return strings.TrimSpace(string(expanded[:limit]))
	}
	return text
}
