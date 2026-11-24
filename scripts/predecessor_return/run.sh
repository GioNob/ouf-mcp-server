#!/usr/bin/env bash
set -euo pipefail
: "${OLD_IMAGE:?deployed image ID required}" "${NEW_IMAGE:?candidate image ID required}"
dir=$(cd "$(dirname "$0")" && pwd)
scope="d080-${GITHUB_RUN_ID:-local}-$$"
db="$scope-db"
fixture="$scope-fixture"
app="$scope-app"
cleanup() {
  docker rm -fv "$app" "$fixture" "$db" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# One isolated loopback namespace for PostgreSQL, the synthetic owner and MCP.
docker run --pull=never -d --name "$db" --network none \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=mcp_test postgres:17 >/dev/null
shared=(--pull=never --network "container:$db")
sql() { docker exec -i "$db" psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d mcp_test "$@"; }
for ((i=0; i<30; i++)); do
  if docker exec "$db" pg_isready -h 127.0.0.1 -U postgres -d mcp_test >/dev/null; then break; fi
  sleep 1
done
sql <<'SQL'
create role mcp_app login;
alter default privileges for role postgres grant select,insert,update on tables to mcp_app;
alter default privileges for role postgres grant usage on sequences to mcp_app;
SQL
docker run "${shared[@]}" -d --name "$fixture" -v "$dir:/test:ro" \
  python:3.12-alpine python -B /test/owner.py >/dev/null
probe() { docker exec "$fixture" python -B /test/probe.py "$@"; }
probe wait-owner

migrate() {
  docker run "${shared[@]}" --rm \
    -e 'MCP_DATABASE_URL=postgres://postgres@127.0.0.1/mcp_test?sslmode=disable' "$1" migrate
}
schema() { sql -c 'select max(version) from ouf_mcp.schema_migration'; }
start() {
  local image=$1 phase=$2
  docker run "${shared[@]}" -d --name "$app" \
    -e 'MCP_DATABASE_URL=postgres://mcp_app@127.0.0.1/mcp_test?sslmode=disable' \
    -e MCP_AUTHORIZATION_BUNDLE_ENDPOINT=http://127.0.0.1:8081/bundle \
    -e MCP_GATEWAY_ENDPOINT=http://127.0.0.1:8081/execute \
    -e MCP_WORKLOAD_TOKEN=d080-fixture -e MCP_FINGERPRINT_KEY=d080-synthetic-fingerprint-key-for-ci \
    "$image" server >/dev/null
  [[ "$(docker inspect --format '{{.Image}}' "$app")" == "$image" ]]
  if ! probe wait-ready; then
    echo "::error::$phase failed readiness on schema $(schema); stopping before probes"
    if [[ "$phase" == predecessor-return ]]; then
      printf '## Domande aperte\n\nRitorno fermato alla readiness (schema %s → %s). Consultare il log di avvio del predecessore.\n' \
        "$before" "$after" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
    fi
    docker logs "$app"
    exit 1
  fi
}
stop() { docker stop "$app" >/dev/null && docker rm "$app" >/dev/null; }

# Join each phase's actual command to its audit, reconciled budget and claim.
# Keep stable IDs as witnesses across migrations and the final retry.
witness() {
  local row
  row=$(sql -v key="d080-discovery-key-$1" <<'SQL'
select a.attempt_id, c.request_hash, a.replay_result_id, e.audit_event_id, w.budget_window_id
from ouf_mcp.idempotency_claim c
join ouf_mcp.tool_attempt a using(attempt_id)
join ouf_mcp.audit_event e using(attempt_id)
join ouf_mcp.attempt_admission_context x using(attempt_id)
join ouf_mcp.budget_reservation r using(attempt_id)
join ouf_mcp.budget_window w on w.budget_window_id=x.budget_window_id
where c.idempotency_key=:'key' and c.capability_id='ouf.semantic.discovery'
  and a.state='SUCCEEDED' and a.replay_result_id is not null
  and r.budget_window_id=w.budget_window_id and r.state='RECONCILED' and r.actual_tool_calls=1
  and e.event_type='TOOL_ATTEMPT_RECONCILED' and w.consumed_tool_calls>0;
SQL
  )
  if [[ -z "$row" || "$row" == *$'\n'* ]]; then
    echo "Expected one complete database witness for $1" >&2
    return 1
  fi
  echo "$row"
}

migrate "$OLD_IMAGE"
sql -c 'grant usage on schema ouf_mcp to mcp_app'
before=$(schema)
echo "Predecessor schema $before"
if [[ "${PREDECESSOR:-}" == 843d22059e03bcd58569fff9960bb46ae552b2b2 && "$before" != 15 ]]; then
  echo "::error::The deployed 843d2205 artifact must initialize schema 15"
  exit 1
fi
start "$OLD_IMAGE" predecessor
probe seed predecessor
old_witness=$(witness predecessor)
stop

migrate "$NEW_IMAGE"
after=$(schema)
echo "Candidate schema $after"
start "$NEW_IMAGE" candidate
probe read-predecessor
probe seed candidate
new_witness=$(witness candidate)
stop

start "$OLD_IMAGE" predecessor-return
probe return
[[ "$(witness predecessor)" == "$old_witness" ]]
[[ "$(witness candidate)" == "$new_witness" ]]
witness return >/dev/null
echo "PASS predecessor-return (schema $before -> $after): reads, writes and deduplicated retries" \
  | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
