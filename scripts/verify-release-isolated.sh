#!/usr/bin/env bash
set -euo pipefail

# This script never accepts a database URL or forwards runtime credentials.
# Docker exec reaches only the newly created, network-isolated test container.
root=$(git rev-parse --show-toplevel)
baseline=d0a2675eb306eff5b84126f0a09fa645649cd149
command -v docker >/dev/null
docker_cmd=(docker)
case "${OUF_RELEASE_DOCKER_SUDO:-0}" in
  0) ;;
  1) sudo -v; docker_cmd=(sudo docker) ;;
  *) echo 'OUF_RELEASE_DOCKER_SUDO must be 0 or 1' >&2; exit 1 ;;
esac
go_mode=${OUF_RELEASE_GO_MODE:-native}
case "$go_mode" in
  native) test "$(go env GOVERSION)" = go1.25.13 ;;
  docker) test "$("${docker_cmd[@]}" run --rm --network none golang:1.25.13 go env GOVERSION)" = go1.25.13 ;;
  *) echo 'OUF_RELEASE_GO_MODE must be native or docker' >&2; exit 1 ;;
esac
git -C "$root" cat-file -e "$baseline^{commit}"
test -z "$(git -C "$root" status --porcelain --untracked-files=no)" || {
  echo 'A clean tracked candidate checkout is required' >&2
  exit 1
}
work=$(mktemp -d "${TMPDIR:-/tmp}/ouf-release-acceptance.XXXXXX")
container_id=''
cleanup() {
  if test -n "$container_id"; then
    "${docker_cmd[@]}" rm -fv "$container_id" >/dev/null
  fi
  rm -rf -- "$work"
}
trap cleanup EXIT
run_go() {
  if test "$go_mode" = native; then
    GOWORK=off CGO_ENABLED=0 GOOS=linux go "$@"
  else
    # Mount only the clean source and this run's temporary directory. Do not
    # mount the Docker socket, home, live configuration or shared /tmp.
    # Match the invoking user so builds cannot leave root-owned Git objects.
    "${docker_cmd[@]}" run --rm --user "$(id -u):$(id -g)" \
      --mount "type=bind,src=$root,dst=$root,readonly" \
      --mount "type=bind,src=$work,dst=$work" --workdir "$PWD" \
      -e GOWORK=off -e CGO_ENABLED=0 -e GOOS=linux \
      -e "GOCACHE=$work/go-cache" -e "GOPATH=$work/go-path" \
      -e 'GOFLAGS=-modcacherw -buildvcs=false' \
      golang:1.25.13 go "$@"
  fi
}
mkdir "$work/old"
git -C "$root" archive "$baseline" | tar -x -C "$work/old"
(
  cd "$work/old"
  run_go test -c -o "$work/old-postgres.test" ./internal/adapter/postgres
)
(
  cd "$root"
  run_go test -c -o "$work/new-postgres.test" ./internal/adapter/postgres
)

container_id=$("${docker_cmd[@]}" run -d --network none \
  --label ouf.purpose=isolated-release-acceptance \
  --tmpfs /var/lib/postgresql/data:rw \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=ouf_release_acceptance \
  postgres:17)
ready=false
for attempt in $(seq 1 30); do
  if "${docker_cmd[@]}" exec "$container_id" pg_isready -h 127.0.0.1 -U postgres -d ouf_release_acceptance >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
test "$ready" = true || { echo 'Isolated PostgreSQL did not become ready' >&2; exit 1; }
test "$("${docker_cmd[@]}" inspect --format '{{.HostConfig.NetworkMode}}' "$container_id")" = none
test "$("${docker_cmd[@]}" inspect --format '{{len .HostConfig.PortBindings}}' "$container_id")" = 0
"${docker_cmd[@]}" cp "$work/old-postgres.test" "$container_id:/tmp/old-postgres.test"
"${docker_cmd[@]}" cp "$work/new-postgres.test" "$container_id:/tmp/new-postgres.test"
dsn='postgres://postgres@127.0.0.1:5432/ouf_release_acceptance?sslmode=disable'
run_test() {
  "${docker_cmd[@]}" exec -e MCP_TEST_DATABASE_URL="$dsn" "$container_id" "$@"
}

echo "GO_MODE=$go_mode"
echo "BASELINE_COMMIT=$baseline"
echo "CANDIDATE_COMMIT=$(git -C "$root" rev-parse HEAD)"
echo "POSTGRES_IMAGE_ID=$("${docker_cmd[@]}" inspect --format '{{.Image}}' "$container_id")"
sha256sum "$work/old-postgres.test" "$work/new-postgres.test"

# Real legacy code: creates schema 001..007 and exercises its lifecycle.
run_test /tmp/old-postgres.test -test.run '^TestDurableLifecycle$' -test.v -test.timeout 90s
# Candidate checks real upgrade before ordinary tests can auto-migrate.
"${docker_cmd[@]}" exec -e MCP_TEST_DATABASE_URL="$dsn" -e MCP_ISOLATED_UPGRADE=1 "$container_id" \
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
