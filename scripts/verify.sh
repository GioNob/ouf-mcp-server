#!/usr/bin/env bash
set -euo pipefail

test "$(go env GOVERSION)" = "go1.25.0" || { echo "Go 1.25.0 is required" >&2; exit 1; }
test -z "$(gofmt -l cmd internal)" || { echo "gofmt changes required" >&2; gofmt -d cmd internal; exit 1; }
go vet ./...
go test ./...
go test -race ./...
mkdir -p build
go build -trimpath -o build/ouf-mcp ./cmd/ouf-mcp
./scripts/verify-no-legacy.sh
