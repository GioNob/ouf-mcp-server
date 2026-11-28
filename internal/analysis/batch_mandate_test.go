package analysis

import "testing"

func TestBatchArgumentsHashMatchesGatewayASCIIJSON(t *testing.T) {
	input := []byte(`{ "limit":2, "intent":"Caffè < 🚀", "includeState":true }`)
	// SHA-256 of the 67a sorted ASCII representation.
	const expected = "10842f2e277f9aa43ebccf3cdc732f5e6962dd042b903b62fb737f3257c2d057"
	got, err := BatchArgumentsHash(input)
	if err != nil || got != expected {
		t.Fatalf("arguments digest = %s, %v", got, err)
	}
}
