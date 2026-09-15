package httpclient

import "testing"

func TestGovernedEndpointRejectsArbitraryAndRedirectProneURLs(t *testing.T) {
	for _, raw := range []string{"https://gateway.example/internal/execute?target=evil", "http://gateway.example/internal/execute", "https://user:pass@gateway.example/x"} {
		if _, err := governed(raw); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	if _, err := governed("https://gateway.example/internal/capabilities/v1/execute"); err != nil {
		t.Fatal(err)
	}
}
