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

# Guard against a repeat of the stale-pog-fork build-cache incident: Grind
# once depended on a `lostbean/pog` git fork, and after that dependency was
# dropped for vanilla Hex `pog`, a previously cached `build/` directory kept
# compiled BEAM artifacts from the old fork around, so `gleam test` silently
# ran against it instead of the manifest's own resolved (Hex) source —
# `gleam deps download` alone will not fix this, since it trusts
# `build/packages/packages.toml`'s own record of what is already downloaded
# rather than checking the package directories on disk. Refuse to proceed if
# either manifest still resolves `pog` from anything but Hex, and always run
# `gleam clean` first (the officially supported way to discard a project's
# `build/` directory, forcing every dependency to be re-resolved and
# recompiled from the manifest's own sources) so a leftover compiled fork
# can never be silently reused even when the manifest itself is correct.
# This does cost a full rebuild on every gate run; accepted as the simplest
# guard that cannot itself drift from how `gleam` actually tracks its cache.
for manifest in "$repo_root/manifest.toml" "$repo_root/consumer/manifest.toml"; do
  if grep -E '^\s*\{ name = "pog",' "$manifest" | grep -qv 'source = "hex"'; then
    echo "$manifest resolves pog from a non-Hex source; refusing to run the gate against a forked dependency" >&2
    exit 1
  fi
done
(cd "$repo_root" && gleam clean)
(cd "$repo_root/consumer" && gleam clean)

if pg_isready -h 127.0.0.1 -p "$port" >/dev/null 2>&1; then
  echo "port $port is already in use; refusing to use a non-disposable database" >&2
  exit 1
fi

initdb -D "$cluster" --username=grind --auth-local=trust --auth-host=trust >/dev/null
# grind_never_standby never connects, so ordinary commits (synchronous_commit=local)
# stay local and fast; a test that raises its own transaction's synchronous_commit
# back to "on" (see the Increment 2 lost-reply tests) will park in SyncRep until
# something terminates that backend — raising it anywhere else would hang forever.
pg_ctl -D "$cluster" -o "-h 127.0.0.1 -p $port -c synchronous_standby_names=grind_never_standby -c synchronous_commit=local" -l "$root/postgres.log" start >/dev/null
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
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_missing_unique_submissions
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_atomic
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_concurrent
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_partial
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_upgrade
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_upgrade_fresh
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_future_foreign
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_shape
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_mixed_case
psql -h 127.0.0.1 -p "$port" -U grind -d grind_schema_mixed_case -c \
  'CREATE SCHEMA "MixedCase"' >/dev/null
psql -h 127.0.0.1 -p "$port" -U grind -d grind_schema_mixed_case -c \
  'ALTER DATABASE grind_schema_mixed_case SET search_path = "MixedCase"' >/dev/null
createdb -h 127.0.0.1 -p "$port" -U grind grind_resolution_route_a
createdb -h 127.0.0.1 -p "$port" -U grind grind_resolution_route_b
createdb -h 127.0.0.1 -p "$port" -U grind grind_consumer_test
createdb -h 127.0.0.1 -p "$port" -U grind grind_repeatable_read_test
createdb -h 127.0.0.1 -p "$port" -U grind grind_quarantine_test
createdb -h 127.0.0.1 -p "$port" -U grind grind_fault_proxy
createdb -h 127.0.0.1 -p "$port" -U grind grind_migration_deadline
createdb -h 127.0.0.1 -p "$port" -U grind grind_squirrel_check
# Increment R1: dedicated database whose default session isolation is
# `repeatable read`, not this cluster's default `read committed`, so the
# uniqueness admission transaction's dependency on read-committed semantics
# (a plain read after an advisory-lock wait must see what committed while
# waiting) is exercised against a real, differently-configured PostgreSQL
# session rather than assumed from the cluster's own default.
psql -h 127.0.0.1 -p "$port" -U grind -d grind_repeatable_read_test -c \
  "ALTER DATABASE grind_repeatable_read_test SET default_transaction_isolation = 'repeatable read'" >/dev/null

# Grind's schema is now available (applied to a dedicated, otherwise-unused
# database): check that Squirrel's generated src/grind/internal/sql.gleam is
# still up to date with src/**/sql/*.sql before running anything else. This
# is the only place a live database backs `gleam run -m squirrel check`;
# `nix flake check` stays database-free.
# Applies the same cigogne-format `up` sections (priv/migrations/*.sql)
# `src/grind/internal/migrations.gleam` mirrors, exactly as an application
# importing them via cigogne would — see README, "Migrations".
squirrel_check_up_sql="$root/schema-up.sql"
: >"$squirrel_check_up_sql"
for migration_file in "$repo_root"/priv/migrations/*.sql; do
  # `tr -d '\r'` first: a CRLF-saved migration file would otherwise leave a
  # trailing `\r` on the guard lines below, silently matching nothing.
  tr -d '\r' <"$migration_file" |
    sed -n '/^--- migration:up$/,/^--- migration:down$/p' |
    sed '1d;$d' >>"$squirrel_check_up_sql"
done
if [[ ! -s "$squirrel_check_up_sql" ]]; then
  echo "extracted migration up SQL is empty; check priv/migrations/*.sql's own --- migration:up/down guards" >&2
  exit 1
fi
psql -h 127.0.0.1 -p "$port" -U grind -d grind_squirrel_check -v ON_ERROR_STOP=1 \
  -f "$squirrel_check_up_sql" >/dev/null
(
  cd "$repo_root"
  DATABASE_URL="postgres://grind@127.0.0.1:$port/grind_squirrel_check?sslmode=disable" \
    gleam run -m squirrel check
)

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
GRIND_TEST_SCHEMA_MISSING_UNIQUE_SUBMISSIONS_URL="postgres://grind@127.0.0.1:$port/grind_schema_missing_unique_submissions?sslmode=disable" \
GRIND_TEST_SCHEMA_ATOMIC_URL="postgres://grind@127.0.0.1:$port/grind_schema_atomic?sslmode=disable" \
GRIND_TEST_SCHEMA_CONCURRENT_URL="postgres://grind@127.0.0.1:$port/grind_schema_concurrent?sslmode=disable" \
GRIND_TEST_SCHEMA_PARTIAL_URL="postgres://grind@127.0.0.1:$port/grind_schema_partial?sslmode=disable" \
GRIND_TEST_SCHEMA_UPGRADE_URL="postgres://grind@127.0.0.1:$port/grind_schema_upgrade?sslmode=disable" \
GRIND_TEST_SCHEMA_UPGRADE_FRESH_URL="postgres://grind@127.0.0.1:$port/grind_schema_upgrade_fresh?sslmode=disable" \
GRIND_TEST_SCHEMA_FUTURE_FOREIGN_URL="postgres://grind@127.0.0.1:$port/grind_schema_future_foreign?sslmode=disable" \
GRIND_TEST_SCHEMA_SHAPE_URL="postgres://grind@127.0.0.1:$port/grind_schema_shape?sslmode=disable" \
GRIND_TEST_SCHEMA_MIXED_CASE_URL="postgres://grind@127.0.0.1:$port/grind_schema_mixed_case?sslmode=disable" \
GRIND_TEST_RESOLUTION_ROUTE_A_URL="postgres://grind@127.0.0.1:$port/grind_resolution_route_a?sslmode=disable" \
GRIND_TEST_RESOLUTION_ROUTE_B_URL="postgres://grind@127.0.0.1:$port/grind_resolution_route_b?sslmode=disable" \
GRIND_TEST_REPEATABLE_READ_URL="postgres://grind@127.0.0.1:$port/grind_repeatable_read_test?sslmode=disable" \
GRIND_TEST_QUARANTINE_URL="postgres://grind@127.0.0.1:$port/grind_quarantine_test?sslmode=disable" \
GRIND_TEST_FAULT_PROXY_URL="postgres://grind@127.0.0.1:$port/grind_fault_proxy?sslmode=disable" \
GRIND_TEST_MIGRATION_DEADLINE_URL="postgres://grind@127.0.0.1:$port/grind_migration_deadline?sslmode=disable" \
GRIND_TEST_MARKER="$root/database-test-ran" \
  gleam test
for contract in admission-read-passed storage-owner-passed incompatible-schema-rejected schema-v11-fresh-install-idempotent-passed legacy-future-schema-markers-rejected missing-schema-artifacts-not-repaired failed-fresh-install-rolled-back future-version-precedes-shape-check-passed migrate-concurrent-migrators-single-marker migrate-partial-failure-resumes-passed migrate-missing-relation-shape-detected migrate-mixed-case-schema-no-op-passed migrate-upgrade-harness-passed committed-success-passed codec-contract-rejected typed-business-failure-passed worker-discard-distinct-outcome-passed worker-cancel-distinct-outcome-passed worker-uncertainty-reconciliable-no-retry cancel-before-run-committed worker-snooze-scheduled-passed worker-snooze-receipt-rollback-passed worker-snooze-delay-receipt-conflict-passed worker-snooze-audited-replay-refunds-current-attempt default-retry-backoff-database-time-passed retry-delay-maximum-postgres-ack-passed worker-retry-first-attempt-scheduled worker-retry-declined-without-error-codec long-handler-wait-passed stale-consumer-handle-rejected supervised-owner-restart-resumed-polling foreign-consumer-stop-owner-preserved consumer-stop-timeout-owner-survived consumer-stop-drained-active-worker consumer-stop-forced-active-work-retained automatic-drain-paused-poll-and-renewed overlapping-claims-skip-locked lease-renewal-loss-fenced-passed renewal-storage-error-retried-passed closed-pool-renewal-recovered-passed unstarted-worker-claim-released-passed temporary-worker-death-quarantined-no-replay dead-idle-worker-claim-released independent-consumers-single-live-claim consumer-capacity-two-enforced automatic-consumer-capacity-two-enforced automatic-consumer-polls-while-capacity-free-passed automatic-contract-skip-passed queue-batch-policy-passed scheduled-due-time-passed batch-partial-commit-count-passed expired-attempt-audited-replay-passed expired-attempt-quarantine-passed quarantine-bounded-passed audited-uncertain-resolution-passed resolution-payload-bound resolution-rebind-owner-checked durable-ack-receipt-passed ack-commit-connection-loss-unknown-passed automatic-ack-commit-connection-loss-recovers-passed automatic-ack-retry-bounded-eventually-uncertain-passed ack-committed-reply-lost-reconciled-passed ack-committed-reply-lost-store-unavailable-unknown-passed lease-renewal-before-ack-passed exact-expiry-rejected ack-after-database-expiry-stale-no-receipt-passed automatic-wakeup-database-deadline-passed forced-stop-pool-cleanup-recovered coordinator-loss-quarantined-no-replay owner-loss-pool-restart-quarantined-no-replay stop-after-coordinator-gone-without-drain stale-shutdown-grace-timer-scoped-to-incarnation unique-pre-storage-rejections-passed unique-admission-existing-conflict-passed unique-json-equality-cases-passed unique-worker-identity-isolation-passed unique-plain-submit-non-participation-passed unique-queue-scope-passed unique-state-eligibility-matrix-passed unique-state-live-transition-passed unique-period-predicate-exact-instant-passed unique-period-from-insertion-boundary-passed unique-period-from-schedule-past-boundary-passed unique-period-from-schedule-future-extends-window-passed unique-period-while-retained-old-row-passed unique-receipt-idempotent-replay-passed unique-receipt-different-input-conflict-passed reconcile-unique-mismatched-pending-conflict-passed unique-receipt-replay-returns-observed-state-passed unique-receipt-output-codec-change-conflict-passed unique-concurrent-forced-overlap-passed unique-concurrent-mixed-scope-passed unique-receipt-ordering-b-returns-a-decision-passed unique-contended-lock-wait-passed unique-reschedule-row-lock-contention-passed unique-lock-timeout-no-leak-passed unique-admission-safe-under-repeatable-read-passed ack-duplicate-ok-under-pinned-isolation-passed resolution-concurrent-same-outcome-applied-once unique-reschedule-moves-available-at-passed unique-reschedule-non-scheduled-unchanged-passed unique-reschedule-due-time-claimable-passed unique-reschedule-race-incomplete-existing-passed unique-reschedule-race-scheduled-only-inserted-passed unique-closed-before-send-recovers-passed unique-aborted-commit-is-commit-unknown-passed unique-committed-reply-lost-inserted-passed unique-committed-reply-lost-store-unavailable-passed unique-reschedule-reply-lost-rescheduled-passed unique-selected-key-scoping-passed unique-selected-key-equality-not-containment-passed call-safely-wrapper-closed-pool-passed close-stale-handle-preserves-deadline-passed reconcile-acknowledgement-receipt-job-mismatch-passed acknowledged-observation-commit-ordering-passed acknowledged-observation-isolation-passed acknowledged-observation-absent-on-commit-unknown-passed acknowledged-observation-absent-on-stale-ack-passed acknowledged-observation-reconciled-after-lost-reply-passed acknowledged-observation-committed-state-overrides-proposal-passed acknowledged-observation-overflow-reports-dropped-passed acknowledged-observation-raising-handler-outcome-unchanged-passed forwarder-crash-loop-pool-survives-passed acknowledged-observation-available-at-committed-retry-passed acknowledged-observation-available-at-committed-snooze-passed acknowledged-observation-available-at-none-cancel-overrides-retry-passed acknowledged-observation-reconciled-on-sequential-duplicate-passed acknowledged-observation-reconciled-on-concurrent-duplicate-passed admitted-observation-plain-submit-passed admitted-observation-unique-inserted-reconciled-passed admitted-observation-unique-existing-conflict-passed admitted-observation-existing-over-executing-available-at-none-passed admitted-observation-absent-on-submission-conflict-passed admitted-observation-absent-from-reconcile-unique-passed claimed-observation-emission-passed claimed-observation-absent-when-nothing-due-passed quarantined-observation-emission-passed quarantined-observation-absent-passed resolved-observation-replied-reconciled-passed resolved-observation-absent-passed cancellation-observation-emission-passed cancellation-observation-absent-passed released-observation-emission-passed released-observation-absent-passed contract-mismatch-observation-emission-passed contract-mismatch-observation-absent-passed claimed-precedes-acknowledged-ordering-passed admitted-observation-in-call-post-commit-unknown-reconciled-passed resolved-observation-absent-on-commit-unknown-passed cancellation-observation-absent-on-commit-unknown-passed quarantine-covers-unregistered-worker-version-passed quarantine-expired-global-operation-passed submit-with-id-first-submit-inserted-passed submit-with-id-retry-returns-original-passed submit-with-id-different-input-conflict-passed submit-with-id-committed-reply-lost-inserted-passed submit-with-id-concurrent-one-row-passed submit-with-id-concurrent-different-input-conflict-passed admitted-observation-submit-with-id-passed unique-contended-lock-wait-default-settings-passed fault-proxy-pass-through-passed fault-proxy-t1-observed fault-proxy-t2-observed fault-proxy-t4-observed fault-proxy-t5-observed fault-proxy-t3-observed fault-proxy-defect2-observed migration-deadline-long-step-succeeds-passed; do
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
for contract in two-worker-consumer-passed consumer-storage-failure-passed consumer-retry-and-cancellation-passed consumer-uncertainty-audited-recovery-passed consumer-unique-admission-existing-conflict-retry-passed consumer-unique-reschedule-across-queues-passed consumer-observes-acknowledged-passed consumer-observes-claimed-passed consumer-submit-with-id-retry-passed; do
  if ! grep -q "$contract" "$root/consumer-test-ran"; then
    echo "external-consumer integration contract did not execute: $contract" >&2
    exit 1
  fi
done
