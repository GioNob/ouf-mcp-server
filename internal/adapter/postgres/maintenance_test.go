package postgres

import "testing"

func TestDebtCacheDelta(t *testing.T) {
	tests := []struct {
		name                        string
		cached, authoritative, want int64
		wantErr                     bool
	}{
		{"no drift", 4, 4, 0, false}, {"missing debt", 2, 5, 3, false}, {"stale excess cache", 7, 3, -4, false}, {"negative cached", -1, 0, 0, true}, {"negative authoritative", 0, -1, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := debtCacheDelta(tc.cached, tc.authoritative)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("delta=%d want=%d", got, tc.want)
			}
		})
	}
}
