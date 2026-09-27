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
# Usage: scripts/bench-matrix.sh [l1|l7|l2|l3|l4|l5|l6|all]  (default: all)

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

# consumers interval_ms filler_rows duration_ms -- reduced from the plan's
# {1,8} x {10,50,250,1000}ms x {0, 1M} to a representative subset for
# wall-clock feasibility (documented in docs/PERFORMANCE-EVIDENCE.md).
l2_points=(
  "1 50 0 3000"
  "1 50 100000 3000"
  "1 250 0 3000"
  "1 250 100000 3000"
  "8 50 0 3000"
  "8 50 100000 3000"
  "8 250 0 3000"
  "8 250 100000 3000"
)

# arrival_per_sec duration_ms -- fixed 4xC10 shape, baked into run_l3 itself.
l3_points=(
  "50 5000"
  "200 5000"
  "1000 5000"
)

# submitters mode total_submissions -- reduced total_submissions per mode
# (documented): hot mode is deliberately small (10 keys means most calls
# serialize on the same advisory-lock domain); cold uses more since it is
# the near-zero-contention baseline and is cheap.
l4_points=(
  "4 hot 1500"
  "16 hot 1500"
  "64 hot 1500"
  "4 cold 4000"
  "16 cold 4000"
  "64 cold 4000"
)

# pruner_on duration_ms
l5_points=(
  "0 2000"
  "1 2000"
)

# concurrency job_count cost_ms -- real, unmodified defaults (L=30000,
# D=4000); cost_ms=25000 spans 2 renewal ticks (L/3=10000ms) per job.
# job_count is always concurrency*3 (3 claim waves).
l6t1_points=(
  "4 12 25000"
  "10 30 25000"
  "50 150 25000"
)

# k_slow_acks d_ms -- C=10 and L=6D/cost=3L are fixed inside run_l6t2
# itself. d_ms=2000 (scaled down from the real 4000ms default for
# wall-clock feasibility -- see run_l6t2's own doc comment; the smallest
# d_ms postgres.validate accepts through setup_with_deadline's own
# unique_lock_wait scaling is a bit under 1334ms, so 2000 keeps a
# comfortable margin).
l6t2_points=(
  "0 2000"
  "1 2000"
  "2 2000"
  "4 2000"
  "8 2000"
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

run_l2_point() {
  local consumers=$1 interval_ms=$2 filler_rows=$3 duration_ms=$4
  echo "== l2 ${consumers}c-i${interval_ms}-f${filler_rows} (duration_ms=$duration_ms) =="
  echo "  warm-up (discarded)"
  (
    cd "$bench_root"
    GRIND_BENCH_RESULTS_DIR="$warmup_dir" \
      gleam run -m grind_bench/load -- l2 "$consumers" "$interval_ms" "$filler_rows" "$duration_ms" 0
  )
  for repeat in $(seq 1 "$repeats"); do
    echo "  repeat $repeat"
    (cd "$bench_root" && gleam run -m grind_bench/load -- l2 "$consumers" "$interval_ms" "$filler_rows" "$duration_ms" "$repeat")
  done
}

run_l3_point() {
  local arrival_per_sec=$1 duration_ms=$2
  echo "== l3 arrival=${arrival_per_sec}/s (duration_ms=$duration_ms) =="
  echo "  warm-up (discarded)"
  (
    cd "$bench_root"
    GRIND_BENCH_RESULTS_DIR="$warmup_dir" \
      gleam run -m grind_bench/load -- l3 "$arrival_per_sec" "$duration_ms" 0
  )
  for repeat in $(seq 1 "$repeats"); do
    echo "  repeat $repeat"
    (cd "$bench_root" && gleam run -m grind_bench/load -- l3 "$arrival_per_sec" "$duration_ms" "$repeat")
  done
}

run_l4_point() {
  local submitters=$1 mode=$2 total_submissions=$3
  echo "== l4 ${submitters}x${mode} (total_submissions=$total_submissions) =="
  echo "  warm-up (discarded)"
  (
    cd "$bench_root"
    GRIND_BENCH_RESULTS_DIR="$warmup_dir" \
      gleam run -m grind_bench/load -- l4 "$submitters" "$mode" "$total_submissions" 0
  )
  for repeat in $(seq 1 "$repeats"); do
    echo "  repeat $repeat"
    (cd "$bench_root" && gleam run -m grind_bench/load -- l4 "$submitters" "$mode" "$total_submissions" "$repeat")
  done
}

run_l5_point() {
  local pruner_on=$1 duration_ms=$2
  echo "== l5 pruner_on=${pruner_on} (duration_ms=$duration_ms) =="
  echo "  warm-up (discarded)"
  (
    cd "$bench_root"
    GRIND_BENCH_RESULTS_DIR="$warmup_dir" \
      gleam run -m grind_bench/load -- l5 "$pruner_on" "$duration_ms" 0
  )
  for repeat in $(seq 1 "$repeats"); do
    echo "  repeat $repeat"
    (cd "$bench_root" && gleam run -m grind_bench/load -- l5 "$pruner_on" "$duration_ms" "$repeat")
  done
}

# L6's own points run 2 repeats, no discarded warm-up: each point is
# considerably more expensive (tens of seconds) than an L1-L5 point, and a
# threshold decision (T1/T2) needs its own real numbers more than it needs
# a warm-up run's caches primed -- see docs/PERFORMANCE-EVIDENCE.md for
# this documented reduction.
l6_repeats=2

run_l6t1_point() {
  local concurrency=$1 job_count=$2 cost_ms=$3
  echo "== l6t1 concurrency=${concurrency} (job_count=$job_count cost_ms=$cost_ms) =="
  for repeat in $(seq 1 "$l6_repeats"); do
    echo "  repeat $repeat"
    (cd "$bench_root" && gleam run -m grind_bench/load -- l6t1 "$concurrency" "$job_count" "$cost_ms" "$repeat")
  done
}

run_l6t2_point() {
  local k_slow_acks=$1 d_ms=$2
  echo "== l6t2 k=${k_slow_acks} (d_ms=$d_ms) =="
  for repeat in $(seq 1 "$l6_repeats"); do
    echo "  repeat $repeat"
    (cd "$bench_root" && gleam run -m grind_bench/load -- l6t2 "$k_slow_acks" "$d_ms" "$repeat")
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

if [[ "$what" == "l2" || "$what" == "all" ]]; then
  for point in "${l2_points[@]}"; do
    run_l2_point $point
  done
fi

if [[ "$what" == "l3" || "$what" == "all" ]]; then
  for point in "${l3_points[@]}"; do
    run_l3_point $point
  done
fi

if [[ "$what" == "l4" || "$what" == "all" ]]; then
  for point in "${l4_points[@]}"; do
    run_l4_point $point
  done
fi

if [[ "$what" == "l5" || "$what" == "all" ]]; then
  for point in "${l5_points[@]}"; do
    run_l5_point $point
  done
fi

if [[ "$what" == "l6" || "$what" == "all" ]]; then
  for point in "${l6t1_points[@]}"; do
    run_l6t1_point $point
  done
  for point in "${l6t2_points[@]}"; do
    run_l6t2_point $point
  done
fi

echo "matrix run complete: $results_dir"
