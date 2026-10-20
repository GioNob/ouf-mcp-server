#!/usr/bin/env bash
set -euo pipefail

test "$(go env GOVERSION)" = "go1.26.9" || { echo "Go 1.26.9 is required" >&2; exit 1; }
test -z "$(gofmt -l cmd internal)" || { echo "gofmt changes required" >&2; gofmt -d cmd internal; exit 1; }
python3 -m unittest scripts.test_r4a_managed_upload_mode scripts.test_r4a_attachment_rollout
go vet ./...
# The PET LAN latency profile must not include race-detector/other package overhead.
# This mandatory uncached gate retains the original 25 ms limit and 80 samples.
test -n "${MCP_TEST_DATABASE_URL:-}" || { echo "MCP_TEST_DATABASE_URL required for mandatory LAN DB gate" >&2; exit 1; }
go test -count=1 -tags=performance -p=1 ./internal/adapter/postgres -run '^TestBudgetReserveP95Under25msLANProfileFixture$' -v
go test ./...
go test -race ./...
mkdir -p build
go build -trimpath -o build/ouf-mcp ./cmd/ouf-mcp
./scripts/verify-no-legacy.sh
! grep -REn '\\ir|\\i ' internal/adapter/postgres/migrations
test "$(grep -c 'pg.Migrate(ctx' cmd/ouf-mcp/main.go)" -eq 1
