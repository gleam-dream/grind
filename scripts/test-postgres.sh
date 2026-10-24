#!/usr/bin/env bash
set -euo pipefail

port=${GRIND_TEST_PGPORT:-$((20000 + RANDOM % 20000))}
root="$(mktemp -d "${TMPDIR:-/tmp}/grind-postgres.XXXXXX")"
cluster="$root/data"
started=0
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
oracle_results=${GRIND_ORACLE_RESULTS_ROOT:-"$repo_root/oracle/results/core-$(date -u +%Y%m%dT%H%M%SZ)-$$"}
log_dir=${GRIND_TEST_LOG_DIR:-"$repo_root/.ci-results/postgres-$(date -u +%Y%m%dT%H%M%SZ)-$$"}
mkdir -p "$log_dir"
cleanup() {
  local status=$?
  if [[ -f "$root/postgres.log" ]]; then
    cp "$root/postgres.log" "$log_dir/postgres.log" || status=1
  fi
  if [[ "$started" == 1 ]]; then
    pg_ctl -D "$cluster" -m immediate stop >/dev/null || status=1
  fi
  if [[ -d "$oracle_results" && -f "$root/postgres.log" ]]; then
    cp "$root/postgres.log" "$oracle_results/postgres.log" || status=1
  fi
  rm -rf "$root"
  return "$status"
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
# `build/` directory, forcing every dependency to be downloaded and
# recompiled from the manifest's own sources) so a leftover compiled fork
# can never be silently reused even when the manifest itself is correct.
# This does cost a full rebuild on every gate run; accepted as the simplest
# guard that cannot itself drift from how `gleam` actually tracks its cache.
for manifest in "$repo_root/manifest.toml" "$repo_root/consumer/manifest.toml" "$repo_root/examples/usage/manifest.toml"; do
  if grep -E '^\s*\{ name = "pog",' "$manifest" | grep -qv 'source = "hex"'; then
    echo "$manifest resolves pog from a non-Hex source; refusing to run the gate against a forked dependency" >&2
    exit 1
  fi
done
# Preserve only configuration fingerprints to avoid redundant Hex resolution.
# Downloaded source, packages.toml and all compiled output are still discarded.
python3 "$repo_root/scripts/prepare-dependencies.py" --clean "$repo_root" "$repo_root/consumer" "$repo_root/examples/usage"

if pg_isready -h 127.0.0.1 -p "$port" >/dev/null 2>&1; then
  echo "port $port is already in use; refusing to use a non-disposable database" >&2
  exit 1
fi

initdb -D "$cluster" --username=grind --auth-local=trust --auth-host=trust >/dev/null
# grind_never_standby never connects, so ordinary commits (synchronous_commit=local)
# stay local and fast; a test that raises its own transaction's synchronous_commit
# back to "on" (see the lost-reply tests) will park in SyncRep until
# something terminates that backend — raising it anywhere else would hang forever.
pg_ctl -D "$cluster" -o "-h 127.0.0.1 -p $port -k $root -c synchronous_standby_names=grind_never_standby -c synchronous_commit=local" -l "$root/postgres.log" start >/dev/null
started=1
createdb -h 127.0.0.1 -p "$port" -U grind grind_test
createdb -h 127.0.0.1 -p "$port" -U grind grind_queue_test
createdb -h 127.0.0.1 -p "$port" -U grind oban_test
createdb -h 127.0.0.1 -p "$port" -U grind grind_oracle_test
createdb -h 127.0.0.1 -p "$port" -U grind oban_paired_test
createdb -h 127.0.0.1 -p "$port" -U grind grind_owner_a
createdb -h 127.0.0.1 -p "$port" -U grind grind_user_schema_fallback
createdb -h 127.0.0.1 -p "$port" -U grind grind_migration_collision_submissions
createdb -h 127.0.0.1 -p "$port" -U grind grind_migration_collision_resolutions
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_bad
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_fresh
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_markers
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_missing_jobs
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_missing_migrations
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_missing_resolutions
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_missing_ack
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_missing_attempt_sequence
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_missing_unique_submissions
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_missing_fk
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_atomic
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_concurrent
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_partial
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_upgrade
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_upgrade_fresh
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_future_foreign
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_shape
createdb -h 127.0.0.1 -p "$port" -U grind grind_schema_mixed_case
createdb -h 127.0.0.1 -p "$port" -U grind grind_consumer_test
createdb -h 127.0.0.1 -p "$port" -U grind grind_usage_test
createdb -h 127.0.0.1 -p "$port" -U grind grind_usage_cigogne
createdb -h 127.0.0.1 -p "$port" -U grind grind_repeatable_read_test
createdb -h 127.0.0.1 -p "$port" -U grind grind_quarantine_test
createdb -h 127.0.0.1 -p "$port" -U grind grind_fault_proxy
createdb -h 127.0.0.1 -p "$port" -U grind grind_migration_deadline
createdb -h 127.0.0.1 -p "$port" -U grind grind_migration_lock
createdb -h 127.0.0.1 -p "$port" -U grind grind_prune_test
createdb -h 127.0.0.1 -p "$port" -U grind grind_prune_owner_b
createdb -h 127.0.0.1 -p "$port" -U grind grind_squirrel_check
# Cigogne end-to-end (a fresh install cigogne itself applies, and its own
# concurrent-with-`migrate` serialization) plus the upgrade-harness genuine
# lost-reply `reconcile_unique` scenario.
createdb -h 127.0.0.1 -p "$port" -U grind grind_cigogne_e2e
createdb -h 127.0.0.1 -p "$port" -U grind grind_cigogne_e2e_fresh
createdb -h 127.0.0.1 -p "$port" -U grind grind_cigogne_concurrent
createdb -h 127.0.0.1 -p "$port" -U grind grind_upgrade_lost_reply
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
# importing them via cigogne would — see `docs/design/design.typ`, "Migration execution".
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
GRIND_TEST_USER_SCHEMA_FALLBACK_URL="postgres://grind@127.0.0.1:$port/grind_user_schema_fallback?sslmode=disable" \
GRIND_TEST_MIGRATION_COLLISION_SUBMISSIONS_URL="postgres://grind@127.0.0.1:$port/grind_migration_collision_submissions?sslmode=disable" \
GRIND_TEST_MIGRATION_COLLISION_RESOLUTIONS_URL="postgres://grind@127.0.0.1:$port/grind_migration_collision_resolutions?sslmode=disable" \
GRIND_TEST_SCHEMA_BAD_URL="postgres://grind@127.0.0.1:$port/grind_schema_bad?sslmode=disable" \
GRIND_TEST_SCHEMA_FRESH_URL="postgres://grind@127.0.0.1:$port/grind_schema_fresh?sslmode=disable" \
GRIND_TEST_SCHEMA_MARKERS_URL="postgres://grind@127.0.0.1:$port/grind_schema_markers?sslmode=disable" \
GRIND_TEST_SCHEMA_MISSING_JOBS_URL="postgres://grind@127.0.0.1:$port/grind_schema_missing_jobs?sslmode=disable" \
GRIND_TEST_SCHEMA_MISSING_MIGRATIONS_URL="postgres://grind@127.0.0.1:$port/grind_schema_missing_migrations?sslmode=disable" \
GRIND_TEST_SCHEMA_MISSING_RESOLUTIONS_URL="postgres://grind@127.0.0.1:$port/grind_schema_missing_resolutions?sslmode=disable" \
GRIND_TEST_SCHEMA_MISSING_ACK_URL="postgres://grind@127.0.0.1:$port/grind_schema_missing_ack?sslmode=disable" \
GRIND_TEST_SCHEMA_MISSING_ATTEMPT_SEQUENCE_URL="postgres://grind@127.0.0.1:$port/grind_schema_missing_attempt_sequence?sslmode=disable" \
GRIND_TEST_SCHEMA_MISSING_UNIQUE_SUBMISSIONS_URL="postgres://grind@127.0.0.1:$port/grind_schema_missing_unique_submissions?sslmode=disable" \
GRIND_TEST_SCHEMA_MISSING_FK_URL="postgres://grind@127.0.0.1:$port/grind_schema_missing_fk?sslmode=disable" \
GRIND_TEST_SCHEMA_ATOMIC_URL="postgres://grind@127.0.0.1:$port/grind_schema_atomic?sslmode=disable" \
GRIND_TEST_SCHEMA_CONCURRENT_URL="postgres://grind@127.0.0.1:$port/grind_schema_concurrent?sslmode=disable" \
GRIND_TEST_SCHEMA_PARTIAL_URL="postgres://grind@127.0.0.1:$port/grind_schema_partial?sslmode=disable" \
GRIND_TEST_SCHEMA_UPGRADE_URL="postgres://grind@127.0.0.1:$port/grind_schema_upgrade?sslmode=disable" \
GRIND_TEST_SCHEMA_UPGRADE_FRESH_URL="postgres://grind@127.0.0.1:$port/grind_schema_upgrade_fresh?sslmode=disable" \
GRIND_TEST_SCHEMA_FUTURE_FOREIGN_URL="postgres://grind@127.0.0.1:$port/grind_schema_future_foreign?sslmode=disable" \
GRIND_TEST_SCHEMA_SHAPE_URL="postgres://grind@127.0.0.1:$port/grind_schema_shape?sslmode=disable" \
GRIND_TEST_SCHEMA_MIXED_CASE_URL="postgres://grind@127.0.0.1:$port/grind_schema_mixed_case?sslmode=disable" \
GRIND_TEST_REPEATABLE_READ_URL="postgres://grind@127.0.0.1:$port/grind_repeatable_read_test?sslmode=disable" \
GRIND_TEST_QUARANTINE_URL="postgres://grind@127.0.0.1:$port/grind_quarantine_test?sslmode=disable" \
GRIND_TEST_FAULT_PROXY_URL="postgres://grind@127.0.0.1:$port/grind_fault_proxy?sslmode=disable" \
GRIND_TEST_MIGRATION_DEADLINE_URL="postgres://grind@127.0.0.1:$port/grind_migration_deadline?sslmode=disable" \
GRIND_TEST_MIGRATION_LOCK_URL="postgres://grind@127.0.0.1:$port/grind_migration_lock?sslmode=disable" \
GRIND_TEST_PRUNE_URL="postgres://grind@127.0.0.1:$port/grind_prune_test?sslmode=disable" \
GRIND_TEST_PRUNE_OWNER_B_URL="postgres://grind@127.0.0.1:$port/grind_prune_owner_b?sslmode=disable" \
GRIND_TEST_CIGOGNE_E2E_URL="postgres://grind@127.0.0.1:$port/grind_cigogne_e2e?sslmode=disable" \
GRIND_TEST_CIGOGNE_E2E_FRESH_URL="postgres://grind@127.0.0.1:$port/grind_cigogne_e2e_fresh?sslmode=disable" \
GRIND_TEST_CIGOGNE_CONCURRENT_URL="postgres://grind@127.0.0.1:$port/grind_cigogne_concurrent?sslmode=disable" \
GRIND_TEST_UPGRADE_LOST_REPLY_URL="postgres://grind@127.0.0.1:$port/grind_upgrade_lost_reply?sslmode=disable" \
GRIND_TEST_POSTGRES_LOG="$root/postgres.log" \
GRIND_TEST_MARKER="$root/database-test-ran" \
  gleam test


  (
  cd "$repo_root/oracle"
  # Hex is not preinstalled anywhere this gate's own toolchain provides —
  # `flake.nix`'s dev shell installs `elixir`/`rebar3` but no Hex archive,
  # and a bare CI runner has no pre-existing `~/.mix` either. `mix deps.get`
  # against a Hex dependency with no Hex client installed triggers Mix's own
  # interactive "install Hex?" prompt; on a non-interactive runner that
  # prompt reads EOF from stdin and Mix aborts instead of installing.
  # Installed into isolated MIX_HOME/HEX_HOME (the disposable "$root" locally,
  # or the explicit GRIND_CI_CACHE on CI) rather
  # than the invoking user's real `~/.mix`/`~/.hex`, and scoped to this one
  # subshell only. MIX_REBAR3 points Mix at the nix store's own `rebar3`
  # (already on PATH from `flake.nix`) instead of letting Mix download and
  # build its own copy over the network.
  # CI shares only its isolated client/archive cache across gate steps.
  export MIX_HOME="${GRIND_CI_CACHE:-$root}/mix"
  export HEX_HOME="${GRIND_CI_CACHE:-$root}/hex"
  export MIX_REBAR3
  MIX_REBAR3="$(command -v rebar3)"
  mix local.hex --force --if-missing
  mix deps.get --check-locked
  mix compile --force --warnings-as-errors
  GRIND_OBAN_TEST_DATABASE_URL="postgres://grind@127.0.0.1:$port/oban_test?sslmode=disable" \
  GRIND_ORACLE_MARKER="$root/oracle-test-ran" \
    mix run run.exs
  GRIND_ORACLE_DATABASE_URL="postgres://grind@127.0.0.1:$port/grind_oracle_test?sslmode=disable" \
  GRIND_OBAN_TEST_DATABASE_URL="postgres://grind@127.0.0.1:$port/oban_paired_test?sslmode=disable" \
  GRIND_ORACLE_RESULTS_ROOT="$oracle_results" \
    bash "$repo_root/scripts/run-paired-oracle.sh"
)

consumer_bad_url="postgres://grind@127.0.0.1:$port/grind_database_missing?sslmode=disable"
(
  cd "$repo_root/consumer"
  GRIND_CONSUMER_DATABASE_URL="postgres://grind@127.0.0.1:$port/grind_consumer_test?sslmode=disable" \
  GRIND_CONSUMER_STORAGE_FAILURE_URL="$consumer_bad_url" \
  GRIND_CONSUMER_TEST_MARKER="$root/consumer-test-ran" \
    gleam test
)

# The usage examples under examples/usage are compiled and run as a test suite.
# Every topic must reach its last assertion, so a renamed or removed API fails
# here instead of leaving a stale example behind.
(
  cd "$repo_root/examples/usage"
  GRIND_USAGE_DATABASE_URL="postgres://grind@127.0.0.1:$port/grind_usage_test?sslmode=disable" \
  GRIND_USAGE_CIGOGNE_DATABASE_URL="postgres://grind@127.0.0.1:$port/grind_usage_cigogne?sslmode=disable" \
  GRIND_USAGE_TEST_MARKER="$root/usage-test-ran" \
    gleam test
)
