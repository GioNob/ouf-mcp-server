#!/usr/bin/env bash
set -euo pipefail
evidence_dir="${1:-target/feasibility-evidence}"
mkdir -p "$evidence_dir"
printf 'scope=LEGACY_COMPATIBILITY_PROBE\npet_conformance=NOT_CLAIMED\npet_required_protocol=2026-07-28\nprobe_protocol=2025-11-25\n' >"$evidence_dir/SCOPE.txt"
./mvnw -B -ntp clean package
npm ci
./mvnw -B -ntp exec:java >"$evidence_dir/server.log" 2>&1 & server_pid="$!"
cleanup(){ kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true; }
trap cleanup EXIT
for attempt in $(seq 1 60); do grep -q OUF_MCP_FEASIBILITY_READY "$evidence_dir/server.log" && break; sleep 1; done
grep -q OUF_MCP_FEASIBILITY_READY "$evidence_dir/server.log"
for scenario in server-initialize ping tools-list dns-rebinding-protection; do
  npx --yes @modelcontextprotocol/conformance@0.1.16 server --url http://localhost:18090/mcp --scenario "$scenario" --output-dir "$evidence_dir/conformance-$scenario"
done
npm run test:client | tee "$evidence_dir/official-client.json"
printf 'sdk_java=2.0.1\nclient_typescript=1.30.0\nconformance=0.1.16\ntransport=streamable-http\nlegacy_probe=PASS\nmodern_pet_conformance=NOT_TESTED\ndecision=NO_GO\n' >"$evidence_dir/RESULT.txt"
(cd "$evidence_dir" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum >SHA256SUMS)
