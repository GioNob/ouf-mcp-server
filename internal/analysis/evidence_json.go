package analysis

import (
	"bytes"
	"encoding/json"
	"strconv"
	"unicode/utf8"
)

// encoding/json accepts duplicate keys and replaces invalid Unicode with U+FFFD.
// Validate the original bytes before interpreting owner evidence or measuring a value map.
func ValidEvidenceJSON(raw []byte) bool {
	if !json.Valid(raw) || !utf8.Valid(raw) {
		return false
	}
	for rest := raw; len(rest) > 0; {
		i := bytes.IndexByte(rest, '\\')
		if i < 0 {
			break
		}
		rest = rest[i+1:]
		if rest[0] != 'u' {
			rest = rest[1:]
			continue
		}
		unit, _ := strconv.ParseUint(string(rest[1:5]), 16, 16)
		rest = rest[5:]
		if unit < 0xd800 || unit > 0xdfff {
			continue
		}
		if unit > 0xdbff || len(rest) < 6 || rest[0] != '\\' || rest[1] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(rest[2:6]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		rest = rest[6:]
	}
	return uniqueJSONMembers(json.NewDecoder(bytes.NewReader(raw)))
}

func uniqueJSONMembers(d *json.Decoder) bool {
	token, err := d.Token()
	if err != nil {
		return false
	}
	delim, compound := token.(json.Delim)
	if !compound {
		return true
	}
	var keys map[string]bool
	if delim == '{' {
		keys = make(map[string]bool)
	}
	for d.More() {
		if keys != nil {
			key, err := d.Token()
			name, ok := key.(string)
			if err != nil || !ok || keys[name] {
				return false
			}
			keys[name] = true
		}
		if !uniqueJSONMembers(d) {
			return false
		}
	}
	_, err = d.Token()
	return err == nil
}
