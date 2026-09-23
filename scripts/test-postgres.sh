#!/usr/bin/env bash
set -euo pipefail

port=${GRIND_TEST_PGPORT:-$((20000 + RANDOM % 20000))}
root="$(mktemp -d "${TMPDIR:-/tmp}/grind-postgres.XXXXXX")"
cluster="$root/data"
started=0
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cleanup() {
  if [[ "$started" == 1 ]]; then
    pg_ctl -D "$cluster" -m immediate stop >/dev/null
  fi
  rm -rf "$root"
}
trap cleanup EXIT

if pg_isready -h 127.0.0.1 -p "$port" >/dev/null 2>&1; then
  echo "port $port is already in use; refusing to use a non-disposable database" >&2
  exit 1
fi

initdb -D "$cluster" --username=grind --auth-local=trust --auth-host=trust >/dev/null
pg_ctl -D "$cluster" -o "-h 127.0.0.1 -p $port" -l "$root/postgres.log" start >/dev/null
started=1
createdb -h 127.0.0.1 -p "$port" -U grind grind_test
createdb -h 127.0.0.1 -p "$port" -U grind grind_queue_test
createdb -h 127.0.0.1 -p "$port" -U grind oban_test
createdb -h 127.0.0.1 -p "$port" -U grind grind_owner_a
createdb -h 127.0.0.1 -p "$port" -U grind grind_owner_b
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_bad
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_v1
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_v2
createdb -h 127.0.0.1 -p "$port" -U grind grind_resolution_route_a
createdb -h 127.0.0.1 -p "$port" -U grind grind_resolution_route_b
createdb -h 127.0.0.1 -p "$port" -U grind grind_consumer_test

printf 'Disposable PostgreSQL %s at 127.0.0.1:%s (Grind and Oban test databases)\n' "$(postgres --version | awk '{print $3}')" "$port"
GRIND_TEST_DATABASE_URL="postgres://grind@127.0.0.1:$port/grind_test?sslmode=disable" \
GRIND_TEST_QUEUE_DATABASE_URL="postgres://grind@127.0.0.1:$port/grind_queue_test?sslmode=disable" \
GRIND_TEST_OWNER_A_URL="postgres://grind@127.0.0.1:$port/grind_owner_a?sslmode=disable" \
GRIND_TEST_OWNER_B_URL="postgres://grind@127.0.0.1:$port/grind_owner_b?sslmode=disable" \
GRIND_TEST_SCHEMA_BAD_URL="postgres://grind@127.0.0.1:$port/grind_schema_bad?sslmode=disable" \
GRIND_TEST_SCHEMA_V1_URL="postgres://grind@127.0.0.1:$port/grind_schema_v1?sslmode=disable" \
GRIND_TEST_SCHEMA_V2_URL="postgres://grind@127.0.0.1:$port/grind_schema_v2?sslmode=disable" \
GRIND_TEST_RESOLUTION_ROUTE_A_URL="postgres://grind@127.0.0.1:$port/grind_resolution_route_a?sslmode=disable" \
GRIND_TEST_RESOLUTION_ROUTE_B_URL="postgres://grind@127.0.0.1:$port/grind_resolution_route_b?sslmode=disable" \
GRIND_TEST_MARKER="$root/database-test-ran" \
  gleam test
for contract in admission-read-passed storage-owner-passed incompatible-schema-rejected v1-migration-preserved-data v2-legacy-resolution-preserved committed-success-passed codec-contract-rejected typed-business-failure-passed automatic-contract-skip-passed queue-batch-policy-passed scheduled-due-time-passed batch-partial-commit-count-passed expired-attempt-takeover-passed expired-attempt-quarantine-passed mixed-consumer-policy-rejected concurrent-policy-start-single-winner audited-uncertain-resolution-passed resolution-payload-bound resolution-rebind-owner-checked; do
  if ! grep -q "$contract" "$root/database-test-ran"; then
    echo "PostgreSQL integration contract did not execute: $contract" >&2
    exit 1
  fi
done

(
  cd "$repo_root/oracle"
  mix deps.get --check-locked
  GRIND_OBAN_TEST_DATABASE_URL="postgres://grind@127.0.0.1:$port/oban_test?sslmode=disable" \
  GRIND_ORACLE_MARKER="$root/oracle-test-ran" \
    mix run run.exs
)
if ! grep -q "oban-oracle-passed" "$root/oracle-test-ran"; then
  echo "pinned Oban oracle harness did not execute" >&2
  exit 1
fi

consumer_bad_url="postgres://grind@127.0.0.1:$port/grind_database_missing?sslmode=disable"
(
  cd "$repo_root/consumer"
  GRIND_CONSUMER_DATABASE_URL="postgres://grind@127.0.0.1:$port/grind_consumer_test?sslmode=disable" \
  GRIND_CONSUMER_STORAGE_FAILURE_URL="$consumer_bad_url" \
  GRIND_CONSUMER_TEST_MARKER="$root/consumer-test-ran" \
    gleam test
)
for contract in two-worker-consumer-passed consumer-storage-failure-passed; do
  if ! grep -q "$contract" "$root/consumer-test-ran"; then
    echo "external-consumer integration contract did not execute: $contract" >&2
    exit 1
  fi
done
