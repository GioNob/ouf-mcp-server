#!/usr/bin/env bash
set -euo pipefail

if grep -RInE '2025-11-25|initialize' --include='*.go' --include='*.json' cmd internal --exclude='*_test.go'; then
  echo "Legacy MCP lifecycle leaked into product code" >&2
  exit 1
fi

grep -q 'r.Header.Get("Mcp-Session-Id") != ""' internal/kernel/kernel.go
grep -q 'StreamableHTTPOptions{' internal/kernel/kernel.go
grep -q 'Stateless:[[:space:]]*true' internal/kernel/kernel.go
