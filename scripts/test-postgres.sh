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
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_fresh
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_markers
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_missing_jobs
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_missing_migrations
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_missing_resolutions
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_missing_ack
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_missing_attempt_sequence
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_atomic
createdb -h 127.0.0.1 -p "$port" -U grind grind_resolution_route_a
createdb -h 127.0.0.1 -p "$port" -U grind grind_resolution_route_b
createdb -h 127.0.0.1 -p "$port" -U grind grind_consumer_test

printf 'Disposable PostgreSQL %s at 127.0.0.1:%s (Grind and Oban test databases)\n' "$(postgres --version | awk '{print $3}')" "$port"
GRIND_TEST_DATABASE_URL="postgres://grind@127.0.0.1:$port/grind_test?sslmode=disable" \
GRIND_TEST_QUEUE_DATABASE_URL="postgres://grind@127.0.0.1:$port/grind_queue_test?sslmode=disable" \
GRIND_TEST_OWNER_A_URL="postgres://grind@127.0.0.1:$port/grind_owner_a?sslmode=disable" \
GRIND_TEST_OWNER_B_URL="postgres://grind@127.0.0.1:$port/grind_owner_b?sslmode=disable" \
GRIND_TEST_SCHEMA_BAD_URL="postgres://grind@127.0.0.1:$port/grind_schema_bad?sslmode=disable" \
GRIND_TEST_SCHEMA_FRESH_URL="postgres://grind@127.0.0.1:$port/grind_schema_fresh?sslmode=disable" \
GRIND_TEST_SCHEMA_MARKERS_URL="postgres://grind@127.0.0.1:$port/grind_schema_markers?sslmode=disable" \
GRIND_TEST_SCHEMA_MISSING_JOBS_URL="postgres://grind@127.0.0.1:$port/grind_schema_missing_jobs?sslmode=disable" \
GRIND_TEST_SCHEMA_MISSING_MIGRATIONS_URL="postgres://grind@127.0.0.1:$port/grind_schema_missing_migrations?sslmode=disable" \
GRIND_TEST_SCHEMA_MISSING_RESOLUTIONS_URL="postgres://grind@127.0.0.1:$port/grind_schema_missing_resolutions?sslmode=disable" \
GRIND_TEST_SCHEMA_MISSING_ACK_URL="postgres://grind@127.0.0.1:$port/grind_schema_missing_ack?sslmode=disable" \
GRIND_TEST_SCHEMA_MISSING_ATTEMPT_SEQUENCE_URL="postgres://grind@127.0.0.1:$port/grind_schema_missing_attempt_sequence?sslmode=disable" \
GRIND_TEST_SCHEMA_ATOMIC_URL="postgres://grind@127.0.0.1:$port/grind_schema_atomic?sslmode=disable" \
GRIND_TEST_RESOLUTION_ROUTE_A_URL="postgres://grind@127.0.0.1:$port/grind_resolution_route_a?sslmode=disable" \
GRIND_TEST_RESOLUTION_ROUTE_B_URL="postgres://grind@127.0.0.1:$port/grind_resolution_route_b?sslmode=disable" \
GRIND_TEST_MARKER="$root/database-test-ran" \
  gleam test
for contract in admission-read-passed storage-owner-passed incompatible-schema-rejected schema-v10-conservative-recovery-installed-and-idempotent legacy-future-schema-markers-rejected missing-schema-artifacts-not-repaired failed-fresh-install-rolled-back committed-success-passed codec-contract-rejected typed-business-failure-passed worker-discard-distinct-outcome-passed worker-cancel-distinct-outcome-passed worker-uncertainty-reconciliable-no-retry cancel-before-run-committed worker-snooze-scheduled-passed worker-snooze-receipt-rollback-passed worker-snooze-delay-receipt-conflict-passed worker-snooze-audited-replay-refunds-current-attempt default-retry-backoff-database-time-passed retry-delay-maximum-postgres-ack-passed worker-retry-first-attempt-scheduled worker-retry-declined-without-error-codec long-handler-wait-passed stale-consumer-handle-rejected supervised-owner-restart-resumed-polling foreign-consumer-stop-owner-preserved consumer-stop-timeout-owner-survived consumer-stop-drained-active-worker consumer-stop-forced-active-work-retained automatic-drain-paused-poll-and-renewed overlapping-claims-skip-locked lease-renewal-loss-fenced-passed renewal-storage-error-retried-passed closed-pool-renewal-recovered-passed unstarted-worker-claim-released-passed temporary-worker-death-quarantined-no-replay dead-idle-worker-claim-released independent-consumers-single-live-claim consumer-capacity-two-enforced automatic-consumer-capacity-two-enforced automatic-contract-skip-passed queue-batch-policy-passed scheduled-due-time-passed batch-partial-commit-count-passed expired-attempt-audited-replay-passed expired-attempt-quarantine-passed quarantine-bounded-passed audited-uncertain-resolution-passed resolution-payload-bound resolution-rebind-owner-checked durable-ack-receipt-passed ack-commit-connection-loss-unknown-passed; do
  if ! grep -q "$contract" "$root/database-test-ran"; then
    echo "PostgreSQL integration contract did not execute: $contract" >&2
    exit 1
  fi
  done
  for contract in cancel-running-ack-wins cancel-after-completion-preserved cancel-running-uncertain-compact-receipt cancel-running-worker-cancel-compact-receipt cancel-pending-expiry-quarantined; do
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
