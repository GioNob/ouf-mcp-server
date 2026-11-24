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
  local run_env=()
  if [[ "$phase" == issuing* ]]; then
    run_env=(-v "$dir/../../internal/manifest/capabilities.json:/fixture-manifest.json:ro"
      -e OUF_ANALYSIS_RUN_ENABLED=true -e OUF_ANALYSIS_WORKLOAD=fixture-mcp
      -e 'OUF_ANALYSIS_RUN_LIMITS={"OwnerDedupQualified":true,"ProofSeconds":5,"WaitSeconds":60,"LifetimeSeconds":3600,"ActivePlans":1,"ProviderSlots":1,"PreviewPageSize":1,"Maximum":{"Calls":8,"Attempts":8,"Objects":256,"Bytes":4194304,"Millis":60000},"Shared":{"TenantBytes":16777216,"SourceBytes":8388608,"TenantQueue":2,"SourceQueue":2,"TenantSlots":2,"SourceSlots":1,"DependencySlots":2,"TenantPerMinute":8,"SourcePerMinute":8,"DependencyPerMinute":8}}'
      -e OUF_ANALYSIS_MANIFEST_FILE=/fixture-manifest.json)
  fi
  docker run "${shared[@]}" "${run_env[@]}" -d --name "$app" \
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
if [[ "${PREDECESSOR:-}" == 092bcdc2f5cc7e20a41c6c419e3e5466da822448 ]]; then
  [[ "$before" == 15 && ( "$after" == 16 || "$after" == 17 ) ]]
fi
if [[ "${PREDECESSOR:-}" == ea4033ef04579e5bbd59b897133bffa09bc0a803 ]]; then
  [[ "$before" == 16 && "$after" == 17 ]]
fi
base50t=9be4dd80e1e7c6a0d1b3f86eda8fb02c4909fc91
base50u=fbeb46eeb9e9499a544e34220505b1c2a711581e
if [[ "${PREDECESSOR:-}" == "$base50u" ]]; then
  [[ "$before" == 18 && "$after" == 19 ]]
  # MCP-50V-10: ordinary readiness precedes the separately authorized purge.
  purge='ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz)'
  [[ "$(sql -c "select to_regprocedure('$purge') is null")" == t ]]
  sql < "$dir/../../internal/adapter/postgres/maintenance/019_analysis_run_retention.sql"
  [[ "$(sql -c "select has_function_privilege('ouf_mcp_maintenance_role','$purge','EXECUTE')
    and not has_function_privilege('mcp_app','$purge','EXECUTE')
    and not exists(select 1 from pg_class where oid in ('ouf_mcp.analysis_run_read'::regclass,
      'ouf_mcp.analysis_run_snapshot'::regclass,'ouf_mcp.analysis_run_attempt'::regclass)
      and has_table_privilege('mcp_app',oid,'DELETE'))")" == t ]]
fi
if [[ "${PREDECESSOR:-}" == "$base50t" ]]; then
  [[ "$before" == 17 && "$after" == 18 ]]
fi
start "$NEW_IMAGE" candidate
if [[ "${PREDECESSOR:-}" == ea4033ef04579e5bbd59b897133bffa09bc0a803 || "${PREDECESSOR:-}" == "$base50t" || "${PREDECESSOR:-}" == "$base50u" ]]; then
  # MCP-50T-09: candidate must execute the exact ea4033e plan before return.
  probe execute-predecessor
else
  probe read-predecessor
fi
probe seed candidate
new_witness=$(witness candidate)
stop

# MCP-50U-10. Capture a real candidate checkpoint between admission and issue.
# The synthetic execute origin uses HTTP. The mandate client requires HTTPS,
# so issue fails before transmission and no worker runs in this probe.
run_witness() {
  local selection=""
  if [[ "${PREDECESSOR:-}" == "$base50u" && -n "${returned_writes:-}" ]]; then
    selection="order by r.started,r.analysis_id limit 1"
  fi
  sql -c "select r.analysis_id,r.state,c.command_key,r.seq,r.fence,r.issue_attempts,r.deadline,md5(r.checkpoint::text),
    (select count(*) from ouf_mcp.analysis_run_attempt where analysis_id=r.analysis_id)
    from ouf_mcp.analysis_run r join ouf_mcp.analysis_run_command c using(analysis_id)
    where r.state='BLOCKED' and r.reason='ANALYSIS_AUTHORITY_UNAVAILABLE' and r.mandate is null
    $selection"
}
hardening_witness() {
  sql -c "select r.analysis_id,r.seq,r.fence,r.issue_attempts,c.command_key,
    md5(r.checkpoint::text),s.seq,s.captured_at,s.state,s.reason
    from ouf_mcp.analysis_run r join ouf_mcp.analysis_run_command c using(analysis_id)
    join ouf_mcp.analysis_run_snapshot s using(analysis_id)
    order by r.analysis_id,c.command_key,s.seq"
}
if [[ "${PREDECESSOR:-}" == "$base50t" || "${PREDECESSOR:-}" == "$base50u" ]]; then
  start "$NEW_IMAGE" issuing
  probe issue-checkpoint
  checkpoint=$(run_witness)
  [[ -n "$checkpoint" && "$checkpoint" != *$'\n'* ]]
  stop
fi

start "$OLD_IMAGE" predecessor-return
probe return
[[ "$(witness predecessor)" == "$old_witness" ]]
[[ "$(witness candidate)" == "$new_witness" ]]
witness return >/dev/null
if [[ "${PREDECESSOR:-}" == "$base50t" || "${PREDECESSOR:-}" == "$base50u" ]]; then
  [[ "$(run_witness)" == "$checkpoint" ]]
  stop
  # MCP-50V-08: 50u can replay its T2 issue command against schema 019.
  if [[ "${PREDECESSOR:-}" == "$base50u" ]]; then
    start "$OLD_IMAGE" issuing-return
    probe issue-checkpoint
    returned_writes=1
    probe issue-checkpoint return
    probe issue-checkpoint return
    [[ "$(sql -c "select count(*) from ouf_mcp.analysis_run r join ouf_mcp.analysis_run_snapshot s using(analysis_id,seq) where r.reason='ANALYSIS_AUTHORITY_UNAVAILABLE' and s.captured_at is not null")" == 2 ]]
    [[ "$(run_witness)" == "$checkpoint" ]]
    returned_witness=$(hardening_witness)
    stop
    migrate "$NEW_IMAGE"
  fi
  start "$NEW_IMAGE" issuing-resume
  probe issue-checkpoint
  [[ "$(run_witness)" == "$checkpoint" ]]
  if [[ "${PREDECESSOR:-}" == "$base50u" ]]; then
    probe issue-checkpoint return
    [[ "$(hardening_witness)" == "$returned_witness" ]]
  fi
fi
echo "PASS predecessor-return (schema $before -> $after): reads, writes and deduplicated retries" \
  | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
