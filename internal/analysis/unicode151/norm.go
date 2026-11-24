// Package unicode151 implements norm-v1 with the owner's ICU4J 75.1 tables.
package unicode151

import (
	"bufio"
	"bytes"
	"compress/gzip"
	_ "embed"
	"strconv"
	"strings"
)

//go:embed tables.gz
var tables []byte

var decomposed = map[rune][]rune{}
var folded = map[rune][]rune{}
var combining = map[rune]int{}
var composed = map[[2]rune]rune{}
var whitespace = map[rune]bool{}

func init() {
	reader, err := gzip.NewReader(bytes.NewReader(tables))
	if err != nil {
		panic(err)
	}
	defer reader.Close()
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		values := make([]rune, len(fields)-1)
		for i, field := range fields[1:] {
			n, err := strconv.Atoi(field)
			if err != nil {
				panic(err)
			}
			values[i] = rune(n)
		}
		c := values[0]
		switch fields[0] {
		case "D":
			decomposed[c] = values[1:]
		case "F":
			folded[c] = values[1:]
		case "C":
			combining[c] = int(values[1])
		case "P":
			composed[[2]rune{values[1], values[2]}] = c
		case "W":
			whitespace[c] = true
		default:
			panic("unknown Unicode table")
		}
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
}

// Canonical decomposition, stable ordering by combining class, then
// canonical composition (including Hangul pairs and composition exclusions).
func nfc(input []rune) []rune {
	var ordered []rune
	for _, c := range input {
		parts := decomposed[c]
		if parts == nil {
			parts = []rune{c}
		}
		for _, part := range parts {
			i := len(ordered)
			ordered = append(ordered, part)
			class := combining[part]
			for class != 0 && i > 0 && combining[ordered[i-1]] > class {
				ordered[i] = ordered[i-1]
				i--
			}
			ordered[i] = part
		}
	}
	out := ordered[:0]
	starter, previous := -1, 0
	for _, c := range ordered {
		class := combining[c]
		if starter >= 0 && (previous == 0 || previous < class) {
			if replacement, ok := composed[[2]rune{out[starter], c}]; ok {
				out[starter] = replacement
				continue
			}
		}
		if class == 0 {
			starter = len(out)
		}
		out = append(out, c)
		previous = class
	}
	return out
}

func Normalize(s string) string {
	var fold []rune
	for _, c := range nfc([]rune(s)) {
		if f := folded[c]; f != nil {
			fold = append(fold, f...)
		} else {
			fold = append(fold, c)
		}
	}
	var result strings.Builder
	space := false
	for _, c := range nfc(fold) {
		if whitespace[c] {
			space = result.Len() > 0
			continue
		}
		if space {
			result.WriteByte(' ')
			space = false
		}
		result.WriteRune(c)
	}
	return result.String()
}
