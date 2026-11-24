# Sourced by run.sh inside the isolated PostgreSQL/image PET.
retention_command() {
  local user=$1 command=$2
  shift 2
  local dsn="postgres://$user@localhost/mcp_test?sslmode=disable"
  if [[ "$user" == retention_fixture_job ]]; then
    dsn+='&role=ouf_mcp_maintenance_role'
  fi
  docker run "${shared[@]}" --rm \
    -e "MCP_DATABASE_URL=$dsn" \
    -e OUF_ANALYSIS_WORKLOAD=retention-fixture \
    "$@" "$NEW_IMAGE" "$command"
}

retention_install() {
  retention_command postgres migrate-analysis-retention-019 \
    -e MCP_MIGRATION_APPLICATION_ROLE=mcp_app -e "MCP_MIGRATION_DATABASE_OID=$retention_oid"
}

retention_batch() {
  retention_command retention_fixture_job analysis-retention-once "$@"
}

retention_record() {
  local record=$1 count=$2
  [[ -n "$record" && "$record" != *$'\n'* ]]
  jq -e --arg sql "$retention_sql" --arg acl "$retention_acl" --argjson n "$count" '
    keys == ["counts","ownerAclSha256","sqlSha256","status"] and
    .status == "VERIFIED" and .sqlSha256 == $sql and .ownerAclSha256 == $acl and
    (.ownerAclSha256 | test("^[0-9a-f]{64}$")) and
    .counts == {reads:$n,snapshots:$n,attempts:$n}' <<<"$record" >/dev/null
}

retention_remaining() {
  sql <<'SQL'
select r.analysis_id=repeat('b',64),
 (select count(*) from ouf_mcp.analysis_run_read where analysis_id=r.analysis_id),
 (select count(*) from ouf_mcp.analysis_run_snapshot where analysis_id=r.analysis_id),
 (select count(*) from ouf_mcp.analysis_run_attempt where analysis_id=r.analysis_id),
 (select count(*) from ouf_mcp.analysis_run_request where analysis_id=r.analysis_id and finished_at is null)
from ouf_mcp.analysis_run r where r.workload='retention-fixture' order by r.analysis_id;
SQL
}

retention_smoke() {
  # MCP-55I-01: same candidate entry point, real catalog, repeatable admin phase.
  retention_oid=$(sql -c 'select oid from pg_database where datname=current_database()')
  [[ "$OLD_IMAGE" =~ ^sha256:[0-9a-f]{64}$ && "$NEW_IMAGE" =~ ^sha256:[0-9a-f]{64}$ && "$OLD_IMAGE" != "$NEW_IMAGE" ]]
  retention_sql=$(sha256sum "$dir/../../internal/adapter/postgres/maintenance/019_analysis_run_retention.sql" | cut -d' ' -f1)
  local first failed
  first=$(retention_install)
  retention_acl=$(jq -er '.ownerAclSha256' <<<"$first")
  retention_record "$first" 0
  [[ "$(retention_install)" == "$first" ]]
  if failed=$(retention_command mcp_app analysis-retention-once); then return 1; fi
  jq -e '.status=="UNVERIFIED" and .counts==null and .ownerAclSha256==null' <<<"$failed" >/dev/null

  # MCP-55I-02: both terminal runs have old history. Only the second retains
  # an unresolved owner request. The current snapshot is always retained.
  sql <<'SQL'
create role retention_fixture_job login;
grant ouf_mcp_maintenance_role to retention_fixture_job;
insert into ouf_mcp.analysis_run(analysis_id,issuer,workload,tenant,subject,checkpoint,counts,spec,
 started,budget_end,deadline,not_before,state,seq)
 select repeat(k,64),'fixture','retention-fixture','fixture','fixture','{}','{}','{"Workload":"retention-fixture"}',
 now()-interval '101 days',now()-interval '100 days',now()-interval '100 days',
 now()-interval '101 days','COMPLETE',259 from (values ('a'),('b')) as v(k);
insert into ouf_mcp.analysis_run_snapshot(analysis_id,seq,counts,state,reason,captured_at)
 select r.analysis_id,n,'{}','RUNNING','',now()-interval '100 days'
 from ouf_mcp.analysis_run r cross join generate_series(1,258) n where workload='retention-fixture';
update ouf_mcp.analysis_run_snapshot set captured_at=now()-interval '100 days'
 where analysis_id in (repeat('a',64),repeat('b',64)) and seq=259;
insert into ouf_mcp.analysis_run_read(analysis_id,seq,decision_ref,first_ordinal,last_ordinal,served_at)
 select analysis_id,259,'fixture',0,0,now()-interval '100 days'
 from ouf_mcp.analysis_run cross join generate_series(1,258) where workload='retention-fixture';
insert into ouf_mcp.analysis_run_attempt(attempt_id,analysis_id,fence,generation,mandate_id,intent_key,operation,
 arguments_hash,ordinals_ref,ordinals,provider,issuer,tenant,subject,worker,reserved_at,lease_end,
 calls,attempts,objects,bytes,millis,actual,finished_at)
 select gen_random_uuid(),analysis_id,n,'1',gen_random_uuid(),'key','op','hash','ref','{}','fixture',
 issuer,tenant,subject,gen_random_uuid(),now()-interval '101 days',now()-interval '100 days',
 1,1,0,1,1,'{}',now()-interval '100 days' from ouf_mcp.analysis_run cross join generate_series(1,258) n
 where workload='retention-fixture';
insert into ouf_mcp.analysis_run_request(analysis_id,request_id,intent_key) values(repeat('b',64),gen_random_uuid(),'pending');
SQL
  # Live ACL drift blocks the same binary before it touches eligible data.
  local signature='ouf_mcp.purge_analysis_run_history(text,timestamptz,timestamptz,timestamptz,integer)'
  sql -c "grant execute on function $signature to mcp_app"
  if failed=$(retention_batch); then return 1; fi
  jq -e '.status=="UNVERIFIED" and .counts==null' <<<"$failed" >/dev/null
  [[ "$(retention_remaining)" == $'f|258|259|258|0\nt|258|259|258|1' ]]
  sql -c "revoke execute on function $signature from mcp_app"
  retention_record "$(retention_batch)" 256
  [[ "$(retention_remaining)" == $'f|2|3|2|0\nt|258|259|258|1' ]]
}
