#!/usr/bin/env bash
set -euo pipefail

sdk_root="${1:?usage: check-modern-sdk-support.sh <java-sdk-source-root>}"
required_version='2026-07-28'

version_hits="$(find "$sdk_root" -path '*/src/main/java/*.java' -type f -print0 \
  | xargs -0 grep -Il -- "$required_version" 2>/dev/null || true)"
discover_hits="$(find "$sdk_root" -path '*/src/main/java/*.java' -type f -print0 \
  | xargs -0 grep -Il -- 'server/discover' 2>/dev/null || true)"

if [[ -z "$version_hits" || -z "$discover_hits" ]]; then
  printf 'NO_GO sdk_missing_modern_profile version=%s server_discover=%s\n' \
    "$([[ -n "$version_hits" ]] && printf present || printf absent)" \
    "$([[ -n "$discover_hits" ]] && printf present || printf absent)"
  exit 1
fi

printf 'CANDIDATE sdk_mentions_modern_profile; official 2026-07-28 conformance is still mandatory\n'
