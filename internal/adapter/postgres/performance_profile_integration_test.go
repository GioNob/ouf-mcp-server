package postgres

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestBudgetReserveP95Under25msLANProfileFixture(t *testing.T) {
	s, ctx, checksum := concurrencyFixture(t)
	const n = 80
	durations := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		req := concurrencyRequest(checksum, "tenant-perf-"+uuid.NewString(), "v1:hmac-sha256:"+fmt.Sprintf("%064x", i+1000))
		req.Maximum.DistinctObjects = 0
		start := time.Now()
		if _, err := s.Reserve(ctx, req); err != nil {
			t.Fatal(err)
		}
		durations = append(durations, time.Since(start))
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	p95 := durations[(n*95)/100-1]
	if p95 >= 25*time.Millisecond {
		t.Fatalf("budget Reserve p95=%s target<25ms in local PostgreSQL LAN-profile fixture", p95)
	}
}
