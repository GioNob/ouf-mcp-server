#!/usr/bin/env bash
set -euo pipefail

# This script never accepts a database URL or forwards runtime credentials.
# Docker exec reaches only the newly created, network-isolated test container.
root=$(git rev-parse --show-toplevel)
baseline=d0a2675eb306eff5b84126f0a09fa645649cd149
test "$(go env GOVERSION)" = go1.25.13
command -v docker >/dev/null
git -C "$root" cat-file -e "$baseline^{commit}"
test -z "$(git -C "$root" status --porcelain --untracked-files=no)" || {
  echo 'A clean tracked candidate checkout is required' >&2
  exit 1
}
work=$(mktemp -d "${TMPDIR:-/tmp}/ouf-release-acceptance.XXXXXX")
container_id=''
cleanup() {
  if test -n "$container_id"; then
    docker rm -fv "$container_id" >/dev/null
  fi
  rm -rf -- "$work"
}
trap cleanup EXIT
mkdir "$work/old"
git -C "$root" archive "$baseline" | tar -x -C "$work/old"
(
  cd "$work/old"
  GOWORK=off CGO_ENABLED=0 GOOS=linux go test -c -o "$work/old-postgres.test" ./internal/adapter/postgres
)
(
  cd "$root"
  GOWORK=off CGO_ENABLED=0 GOOS=linux go test -c -o "$work/new-postgres.test" ./internal/adapter/postgres
)

container_id=$(docker run -d --network none \
  --label ouf.purpose=isolated-release-acceptance \
  --tmpfs /var/lib/postgresql/data:rw \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=ouf_release_acceptance \
  postgres:17)
ready=false
for attempt in $(seq 1 30); do
  if docker exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d ouf_release_acceptance >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
test "$ready" = true || { echo 'Isolated PostgreSQL did not become ready' >&2; exit 1; }
test "$(docker inspect --format '{{.HostConfig.NetworkMode}}' "$container_id")" = none
test "$(docker inspect --format '{{len .HostConfig.PortBindings}}' "$container_id")" = 0
docker cp "$work/old-postgres.test" "$container_id:/tmp/old-postgres.test"
docker cp "$work/new-postgres.test" "$container_id:/tmp/new-postgres.test"
dsn='postgres://postgres@127.0.0.1:5432/ouf_release_acceptance?sslmode=disable'
run_test() {
  docker exec -e MCP_TEST_DATABASE_URL="$dsn" "$container_id" "$@"
}

echo "BASELINE_COMMIT=$baseline"
echo "CANDIDATE_COMMIT=$(git -C "$root" rev-parse HEAD)"
echo "POSTGRES_IMAGE_ID=$(docker inspect --format '{{.Image}}' "$container_id")"
sha256sum "$work/old-postgres.test" "$work/new-postgres.test"

# Real legacy code: creates schema 001..007 and exercises its lifecycle.
run_test /tmp/old-postgres.test -test.run '^TestDurableLifecycle$' -test.v -test.timeout 90s
# Candidate checks real upgrade before ordinary tests can auto-migrate.
docker exec -e MCP_TEST_DATABASE_URL="$dsn" -e MCP_ISOLATED_UPGRADE=1 "$container_id" \
  /tmp/new-postgres.test -test.run '^TestReleaseUpgradeFromSeven$' -test.v -test.timeout 90s
# Admission, successful/retried calls, debt, recovery and summary reservations.
run_test /tmp/new-postgres.test \
  -test.run '^(TestDurableLifecycle|TestRetryClassification.*|TestWindowBudget.*|TestDebtAdmissionReadsActiveDebt|TestSummaryProducerAdmissionsRetryAbortedTransactions|TestConcurrencyGate.*)$' \
  -test.v -test.timeout 180s
# Rollback compatibility uses the real old adapter on the expanded schema,
# with fresh synthetic scopes. It does not authorize same-window live rollback.
run_test /tmp/old-postgres.test -test.run '^TestDurableLifecycle$' -test.v -test.timeout 90s
echo 'ROLLBACK_SCHEMA_COMPATIBILITY_PASS'
echo 'ISOLATED_RELEASE_ACCEPTANCE_PASS'
