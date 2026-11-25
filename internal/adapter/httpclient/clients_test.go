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

func TestDenialPolicyVersionAcceptsOnlyNonnegativeIntegers(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want int64
	}{
		{"0", 0}, {"77", 77}, {"9223372036854775807", 1<<63 - 1},
		{"null", -1}, {"-1", -1}, {"77.5", -1}, {"9223372036854775808", -1}, {`"77"`, -1},
	} {
		p := decodeOwnerProblem([]byte(`{"code":"NO_APPLICABLE_GRANT","policyVersion":`+tc.raw+"}"), 403)
		if p.Code != "NO_APPLICABLE_GRANT" || (p.PolicyVersion == nil) != (tc.want < 0) || (p.PolicyVersion != nil && *p.PolicyVersion != tc.want) {
			t.Fatalf("version %s: %+v", tc.raw, p)
		}
	}
}
