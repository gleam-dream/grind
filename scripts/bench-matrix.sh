#!/usr/bin/env bash
set -euo pipefail

# Item 13: drives the full (reduced-representative, see the L1/L7 point
# lists below) L1 and L7 matrices reproducibly against a disposable
# PostgreSQL cluster, with item 10's grind_a/grind_ctl role split, item 7's
# DB-timestamp timing (handled inside `grind_bench/load` itself), item 8's
# "≥3 repeats, discard a warm-up, report mean/min/max", and item 9's DB CPU
# + pg_stat_statements bucket split (also handled inside `grind_bench/load`,
# via GRIND_BENCH_PG_DATA_DIR). Writes every CSV to
# `bench/results/<date>-<commit>/` (item 13's own provenance requirement);
# raw per-tick JSONL stays gitignored under that directory's own `raw/`.
#
# Not scripts/bench-postgres.sh: that script is bench's own gate (mutation
# tests + one 1k-job smoke run) and is never meant to run the full matrix.
# This script starts its own disposable cluster the same way, but never
# touches scripts/bench-postgres.sh or scripts/test-postgres.sh.
#
# Usage: scripts/bench-matrix.sh [l1|l7|all]  (default: all)

what="${1:-all}"

port=${GRIND_BENCH_PGPORT:-$((20000 + RANDOM % 20000))}
root="$(mktemp -d "${TMPDIR:-/tmp}/grind-bench-matrix.XXXXXX")"
cluster="$root/data"
started=0
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
bench_root="$repo_root/bench"

cleanup() {
  if [[ "${BENCH_KEEP:-0}" == "1" ]]; then
    echo "BENCH_KEEP=1: leaving cluster running at 127.0.0.1:$port (data dir: $cluster)"
    echo "  stop it later with: pg_ctl -D '$cluster' -m immediate stop"
    return
  fi
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
pg_ctl -D "$cluster" -o "-h 127.0.0.1 -p $port \
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
psql -h 127.0.0.1 -p "$port" -U grind -d grind_bench -v ON_ERROR_STOP=1 \
  -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" >/dev/null

printf 'Disposable PostgreSQL %s at 127.0.0.1:%s (matrix run)\n' "$(postgres --version | awk '{print $3}')" "$port"

export GRIND_BENCH_DATABASE_URL="postgres://grind_a@127.0.0.1:$port/grind_bench?sslmode=disable"
export GRIND_BENCH_CTL_DATABASE_URL="postgres://grind_ctl@127.0.0.1:$port/grind_bench?sslmode=disable"
export GRIND_BENCH_POSTGRES_LOG="$root/postgres.log"
export GRIND_BENCH_PG_DATA_DIR="$cluster"
export GRIND_BENCH_COMMIT="$(cd "$repo_root" && git rev-parse --short HEAD)"
if [[ -n "$(cd "$repo_root" && git status --porcelain)" ]]; then
  export GRIND_BENCH_DIRTY=1
else
  export GRIND_BENCH_DIRTY=0
fi

results_dir="$bench_root/results/$(date +%Y-%m-%d)-${GRIND_BENCH_COMMIT}"
warmup_dir="$root/warmup-results"
export GRIND_BENCH_RESULTS_DIR="$results_dir"
mkdir -p "$results_dir" "$warmup_dir"

echo "commit=$GRIND_BENCH_COMMIT dirty=$GRIND_BENCH_DIRTY results_dir=$results_dir"

(cd "$bench_root" && gleam build)

repeats=3

# consumers concurrency queues cost_ms job_count
l1_points=(
  "1 10 1 0 12000"
  "2 10 1 0 20000"
  "1 50 1 0 10000"
  "4 10 2 0 30000"
  "4 10 1 10 25000"
)

# consumers concurrency job_count
l7_points=(
  "1 50 9000"
  "5 10 33000"
  "10 5 46000"
)

run_l1_point() {
  local consumers=$1 concurrency=$2 queues=$3 cost_ms=$4 job_count=$5
  echo "== l1 ${consumers}x${concurrency}x${queues}xc${cost_ms} (job_count=$job_count) =="
  echo "  warm-up (discarded)"
  (
    cd "$bench_root"
    GRIND_BENCH_RESULTS_DIR="$warmup_dir" \
      gleam run -m grind_bench/load -- l1 "$job_count" "$consumers" "$concurrency" "$queues" "$cost_ms" 0
  )
  for repeat in $(seq 1 "$repeats"); do
    echo "  repeat $repeat"
    (cd "$bench_root" && gleam run -m grind_bench/load -- l1 "$job_count" "$consumers" "$concurrency" "$queues" "$cost_ms" "$repeat")
  done
}

run_l7_point() {
  local consumers=$1 concurrency=$2 job_count=$3
  echo "== l7 ${consumers}x${concurrency} (job_count=$job_count) =="
  echo "  warm-up (discarded)"
  (
    cd "$bench_root"
    GRIND_BENCH_RESULTS_DIR="$warmup_dir" \
      gleam run -m grind_bench/load -- l7 "$job_count" "$consumers" "$concurrency" 0
  )
  for repeat in $(seq 1 "$repeats"); do
    echo "  repeat $repeat"
    (cd "$bench_root" && gleam run -m grind_bench/load -- l7 "$job_count" "$consumers" "$concurrency" "$repeat")
  done
}

if [[ "$what" == "l1" || "$what" == "all" ]]; then
  for point in "${l1_points[@]}"; do
    run_l1_point $point
  done
fi

if [[ "$what" == "l7" || "$what" == "all" ]]; then
  for point in "${l7_points[@]}"; do
    run_l7_point $point
  done
  echo "== profile 1x50 (coordinator profiling, item 11) =="
  (cd "$bench_root" && gleam run -m grind_bench/load -- profile 9000 1 50)
  echo "== profile 5x10 (coordinator profiling, item 11) =="
  (cd "$bench_root" && gleam run -m grind_bench/load -- profile 33000 5 10)
fi

echo "matrix run complete: $results_dir"
