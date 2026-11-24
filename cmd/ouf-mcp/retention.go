package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"time"

	pg "github.com/GioNob/ouf-mcp-server/internal/adapter/postgres"
	"github.com/GioNob/ouf-mcp-server/internal/adapter/postgres/maintenance"
	"github.com/GioNob/ouf-mcp-server/internal/analysis"
)

type retentionResult struct {
	Status         string              `json:"status"`
	SQLSHA256      string              `json:"sqlSha256"`
	OwnerACLSHA256 *string             `json:"ownerAclSha256"`
	Counts         *maintenance.Counts `json:"counts"`
}

func runRetentionCommand(ctx context.Context, out io.Writer, args []string, dsn string) int {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	result := retentionResult{Status: "UNVERIFIED", SQLSHA256: maintenance.SQLDigest()}
	exit := executeRetention(ctx, args, dsn, &result)
	if err := json.NewEncoder(out).Encode(result); err != nil {
		return 1
	}
	return exit
}

func executeRetention(ctx context.Context, args []string, dsn string, result *retentionResult) int {
	if len(args) != 1 || dsn == "" {
		return 2
	}
	var workload, appRole string
	var databaseOID uint64
	var ttl analysis.RunRetention
	limit := maintenance.MaxBatch
	var err error
	switch args[0] {
	case "migrate-analysis-retention-019":
		appRole = os.Getenv("MCP_MIGRATION_APPLICATION_ROLE")
		databaseOID, err = strconv.ParseUint(os.Getenv("MCP_MIGRATION_DATABASE_OID"), 10, 32)
		if err != nil || databaseOID == 0 || appRole == "" {
			return 2
		}
	case "analysis-retention-once":
		workload = os.Getenv("OUF_ANALYSIS_WORKLOAD")
		_, ttl, err = analysis.RunMaintenanceConfiguration()
		if err != nil || workload == "" {
			return 2
		}
		if raw, ok := os.LookupEnv("OUF_ANALYSIS_RETENTION_BATCH"); ok {
			limit, err = strconv.Atoi(raw)
			if err != nil || limit < 1 || limit > maintenance.MaxBatch {
				return 2
			}
		}
	default:
		return 2
	}
	store, err := pg.Open(ctx, dsn)
	if err != nil {
		return 1
	}
	defer store.Close()
	var acl string
	var counts maintenance.Counts
	if args[0] == "migrate-analysis-retention-019" {
		if store.Ready(ctx) != nil {
			return 1
		}
		acl, err = maintenance.Install(ctx, store.Pool(), uint32(databaseOID), appRole)
	} else {
		acl, counts, err = maintenance.Once(ctx, store.Pool(), workload, ttl, limit)
	}
	if err != nil {
		return 1
	}
	result.Status = "VERIFIED"
	result.OwnerACLSHA256 = &acl
	result.Counts = &counts
	return 0
}
