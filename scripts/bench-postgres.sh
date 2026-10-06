#!/usr/bin/env bash
set -euo pipefail

# Disposable PostgreSQL cluster for the bench harness, modeled on
# scripts/test-postgres.sh's own shape but carrying the extra GUCs the bench
# planning notes call for (pg_stat_statements, lock-wait/deadlock logging,
# I/O timing) and BENCH_KEEP=1 to leave the cluster running for interactive
# follow-up (`psql`, another `gleam run -m grind_bench/load -- ...` call,
# re-reading `bench/results/*/raw/*.jsonl`) instead of tearing it down on
# exit.
#
# Item 10 (roles): two superuser roles beyond the cluster's own bootstrap
# user -- `grind_a` (Grind's own installation pool) and `grind_ctl` (the
# harness's own ledger/drain-poll/samplers pool) -- so `pg_stat_activity`/
# `pg_stat_statements` can attribute load to one side or the other.
# `max_connections` is raised well past the largest matrix point
# `scripts/bench-matrix.sh` drives (consumers x concurrency, e.g. 8x50=400)
# plus headroom for the ctl/ledger/drain/sampler connections.
#
# Runs: bench/'s own `gleam test` (the audit-checker and preload-guard
# mutation-discipline proofs -- see bench/test/grind_bench_audit_test.gleam
# and bench/test/grind_bench_preload_test.gleam), then the 1k-job smoke
# scenario (`gleam run -m grind_bench/load -- smoke 1000`), which is this
# script's own gate: it exits non-zero (via `grind_bench_ffi:halt/1`) if the
# audit fails. Deliberately separate from scripts/test-postgres.sh (per the
# plan's own decision) -- never wired into the root gate, and the bench
# project is never picked up by the root `gleam test` (a separate
# `gleam.toml`, exactly like `consumer/`).

port=${GRIND_BENCH_PGPORT:-$((20000 + RANDOM % 20000))}
root="$(mktemp -d "${TMPDIR:-/tmp}/grind-bench-postgres.XXXXXX")"
cluster="$root/data"
started=0
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
bench_root="$repo_root/bench"
results_dir="${GRIND_BENCH_RESULTS_DIR:-$repo_root/.ci-results/bench-smoke-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
mkdir -p "$(dirname "$results_dir")"
mkdir "$results_dir"

cleanup() {
  local status=$?
  if [[ -f "$root/postgres.log" ]]; then cp "$root/postgres.log" "$results_dir/postgres.log" || status=1; fi
  if [[ "${BENCH_KEEP:-0}" == "1" ]]; then
    echo "BENCH_KEEP=1: leaving cluster running at 127.0.0.1:$port (data dir: $cluster)"
    echo "  stop it later with: pg_ctl -D '$cluster' -m immediate stop"
    return "$status"
  fi
  if [[ "$started" == 1 ]]; then
    pg_ctl -D "$cluster" -m immediate stop >/dev/null || status=1
  fi
  rm -rf "$root"
  return "$status"
}
trap cleanup EXIT
python3 -B -m unittest discover -s "$bench_root/test" -p 'test_*.py'

if pg_isready -h 127.0.0.1 -p "$port" >/dev/null 2>&1; then
  echo "port $port is already in use; refusing to use a non-disposable database" >&2
  exit 1
fi

initdb -D "$cluster" --username=grind --auth-local=trust --auth-host=trust >/dev/null
pg_ctl -D "$cluster" -o "-h 127.0.0.1 -p $port -k $root \
  -c shared_preload_libraries=pg_stat_statements \
  -c log_lock_waits=on \
  -c deadlock_timeout=100 \
  -c track_io_timing=on \
  -c max_connections=600 \
  -c synchronous_standby_names=grind_bench_never_standby \
  -c synchronous_commit=local" \
  -l "$root/postgres.log" start >/dev/null
started=1

createuser -h 127.0.0.1 -p "$port" -U grind --superuser grind_a
createuser -h 127.0.0.1 -p "$port" -U grind --superuser grind_ctl

createdb -h 127.0.0.1 -p "$port" -U grind grind_bench
createdb -h 127.0.0.1 -p "$port" -U grind grind_bench_test
createdb -h 127.0.0.1 -p "$port" -U grind grind_bench_schema_drift

for db in grind_bench grind_bench_test grind_bench_schema_drift; do
  psql -h 127.0.0.1 -p "$port" -U grind -d "$db" -v ON_ERROR_STOP=1 \
    -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" >/dev/null
done

printf 'Disposable PostgreSQL %s at 127.0.0.1:%s (bench databases, roles grind_a/grind_ctl)\n' "$(postgres --version | awk '{print $3}')" "$port"

bench_database_url="postgres://grind_a@127.0.0.1:$port/grind_bench?sslmode=disable"
bench_ctl_database_url="postgres://grind_ctl@127.0.0.1:$port/grind_bench?sslmode=disable"
bench_test_database_url="postgres://grind@127.0.0.1:$port/grind_bench_test?sslmode=disable"
bench_schema_drift_url="postgres://grind@127.0.0.1:$port/grind_bench_schema_drift?sslmode=disable"

(cd "$bench_root" && gleam clean)

marker="$root/bench-test-ran"
: >"$marker"
(
  cd "$bench_root"
  GRIND_BENCH_TEST_DATABASE_URL="$bench_test_database_url" \
  GRIND_BENCH_TEST_SCHEMA_DRIFT_URL="$bench_schema_drift_url" \
  GRIND_BENCH_TEST_MARKER="$marker" \
    gleam test
)
for contract in \
  bench-audit-i1a-red-then-green-passed \
  bench-audit-extra-job-rows-red-then-green-passed \
  bench-audit-submission-count-red-then-green-passed \
  bench-audit-i1b-red-then-green-passed \
  bench-audit-i2-red-then-green-passed \
  bench-audit-i2-duplicate-effects-red-then-green-passed \
  bench-audit-i2-authorized-replay-red-then-green-passed \
  bench-audit-i3-red-then-green-passed \
  bench-audit-i3-quarantine-red-then-green-passed \
  bench-audit-i4-red-then-green-passed \
  bench-audit-i5-missing-ack-red-then-green-passed \
  bench-audit-i5-ack-without-terminal-red-then-green-passed \
  bench-audit-i5-duplicate-ack-red-then-green-passed \
  bench-audit-i5-state-mismatch-red-then-green-passed \
  bench-audit-i6-red-then-green-passed \
  bench-audit-i6-log-scan-red-then-green-passed \
  bench-audit-i7-red-then-green-passed \
  bench-audit-quarantine-real-wiring-passed \
  bench-audit-forwarder-dropped-real-wiring-passed \
  bench-preload-schema-matches-clean-passed \
  bench-preload-column-missing-red-then-green-passed \
  bench-preload-unmirrored-required-column-red-then-green-passed \
  bench-preload-matches-submit-shape-passed \
  bench-preload-column-by-column-passed \
  bench-instrumentation-lease-log-red-then-green-passed \
  bench-instrumentation-slow-ack-red-then-green-passed \
  bench-instrumentation-real-ack-delay-passed \
  bench-instrumentation-per-target-activation-passed \
  bench-instrumentation-unrenewed-expiry-visible-passed \
  bench-audit-t2-final-classification-passed; do
  if ! grep -q "$contract" "$marker"; then
    echo "bench contract did not execute: $contract" >&2
    exit 1
  fi
done

echo "bench: audit checker and preload guard mutation tests passed"

GRIND_BENCH_COMMIT="$(git -C "$repo_root" rev-parse HEAD)"
export GRIND_BENCH_COMMIT
export GRIND_BENCH_DIRTY=0
[[ -z "$(git -C "$repo_root" status --porcelain)" ]] || export GRIND_BENCH_DIRTY=1
GRIND_BENCH_SOURCE_SHA256="$(python3 "$repo_root/scripts/bench-provenance.py" --digest)"
export GRIND_BENCH_SOURCE_SHA256
python3 "$repo_root/scripts/bench-provenance.py" "$results_dir/provenance.json" "$@"
(
  cd "$bench_root"
  GRIND_BENCH_DATABASE_URL="$bench_database_url" \
  GRIND_BENCH_CTL_DATABASE_URL="$bench_ctl_database_url" \
  GRIND_BENCH_RESULTS_DIR="$results_dir" \
  GRIND_BENCH_POSTGRES_LOG="$root/postgres.log" \
  GRIND_BENCH_PG_DATA_DIR="$cluster" \
    gleam run -m grind_bench/load -- smoke 1000
)

echo "bench: smoke (1000 jobs) passed"

# Exercise repaired plan/arrival/pruning paths in the gate as well as their
# instrumentation units. These are activation checks, not a performance matrix.
for scenario in "l2 1 250 0 500" "l3 50 1000" "l5 0 3000" "l5 1 3000" "l7 40 1 4 1" "profile 40 1 4"; do
  read -r -a scenario_args <<< "$scenario"
  activation_log="$root/activation-${scenario// /-}.log"
  (
    cd "$bench_root"
    GRIND_BENCH_DATABASE_URL="$bench_database_url" \
    GRIND_BENCH_CTL_DATABASE_URL="$bench_ctl_database_url" \
    GRIND_BENCH_RESULTS_DIR="$results_dir" \
    GRIND_BENCH_POSTGRES_LOG="$root/postgres.log" \
    GRIND_BENCH_PG_DATA_DIR="$cluster" \
      gleam run -m grind_bench/load -- "${scenario_args[@]}"
  ) | tee "$activation_log"
  if [[ "$scenario" != l2* ]]; then
    grep -Eq '^completion_observer_stop_ack polls=([2-9]|[1-9][0-9]+) harness_pool_restart_retries=[0-9]+$' "$activation_log" || {
      echo "completion observer did not survive multiple queries and acknowledge stop" >&2
      exit 1
    }
    # A retried pgo pool restart keeps the run alive but makes it suspect as
    # evidence (bench/README.md, "Harness pools").
    grep -Eq ' harness_pool_restart_retries=0$' "$activation_log" || {
      echo "a harness pool restarted during $scenario; repeat the run" >&2
      exit 1
    }
  fi
done
# Exercise real completion-snapshot SQL and both shared sampler call sites.
python3 - "$results_dir" <<'PY_CHECK'
import json
from pathlib import Path
import sys
root = Path(sys.argv[1])
provenance = json.loads((root / "provenance.json").read_text())
for name in ("l7-1x4-r1.jsonl", "profile-1x4.jsonl"):
    data = json.loads((root / "raw" / (name + ".drain.json")).read_text())
    assert data["valid"] and data["outcome"] == "drained", data
    assert data["drain_timeout_ms"] == provenance["drain_timeout_ms"], data
    assert data["source_sha256"] == provenance["source_sha256"], data
    assert data["observer_stopped"] and data["sampler_covers_drain"], data
    assert data["sampler"]["ticks"] >= 2, data
    assert data["sampler"]["first_monotonic_ms"] <= data["drain_started_monotonic_ms"], data
    assert data["sampler"]["last_monotonic_ms"] >= data["drain_ended_monotonic_ms"], data
    counts = data["completion_snapshot"]
    for metric in ("submitted", "handler_started", "handler_completed", "durable_observed", "succeeded_receipts", "state:succeeded"):
        assert counts[metric] == data["expected_jobs"] == 40, (name, metric, data)
    assert sum(value for key, value in counts.items() if key.startswith("state:")) == 40, data
PY_CHECK
[[ "$(python3 "$repo_root/scripts/bench-provenance.py" --digest)" == "$GRIND_BENCH_SOURCE_SHA256" ]] || { echo "source changed during benchmark gate; evidence invalid" >&2; exit 1; }
echo "bench: query plans, open-loop, prune and full-drain sampling activation passed"
