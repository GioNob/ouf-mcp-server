// Command retention runs one bounded PostgreSQL maintenance batch for T2.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if !clean(ctx) {
		fmt.Fprintln(os.Stderr, "ANALYSIS_RETENTION_FAILED")
		os.Exit(1)
	}
}

func clean(ctx context.Context) bool {
	workload := os.Getenv("OUF_ANALYSIS_WORKLOAD")
	dsn := os.Getenv("MCP_DATABASE_URL")
	_, retention, err := analysis.RunMaintenanceConfiguration()
	if err != nil || workload == "" || dsn == "" {
		return false
	}
	store, err := postgres.Open(ctx, dsn)
	if err != nil {
		return false
	}
	defer store.Close()
	return store.CleanRunHistory(ctx, workload, retention) == nil
}
