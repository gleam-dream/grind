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
case "$what" in
  l1|l2|l3|l4|l5|l6|l6t1|l6t2|l7|all) ;;
  *) echo "unknown matrix: $what" >&2; exit 2 ;;
esac

port=${GRIND_BENCH_PGPORT:-$((20000 + RANDOM % 20000))}
root="$(mktemp -d "${TMPDIR:-/tmp}/grind-bench-matrix.XXXXXX")"
cluster="$root/data"
started=0
results_reserved=0
delay_proxy_pid=""
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
bench_root="$repo_root/bench"

cleanup() {
  local status=$?
  # Retain warm-up context on any matrix failure. It may include earlier
  # successful warm-ups; logs and diagnostic outcomes identify the failed phase.
  if [[ "$status" != 0 && "$results_reserved" == 1 && -d "${warmup_dir:-}" ]]; then
    mkdir "$results_dir/warmup-results-on-failure" || status=1
    cp -R "$warmup_dir/." "$results_dir/warmup-results-on-failure/" || status=1
  fi
  if [[ -n "$delay_proxy_pid" ]]; then kill "$delay_proxy_pid" 2>/dev/null || true; fi
  if [[ "${BENCH_KEEP:-0}" == "1" ]]; then
    if [[ "$results_reserved" == 1 ]]; then
      cp "$root/postgres.log" "$results_dir/postgres.log" || status=1
    fi
    echo "BENCH_KEEP=1: leaving cluster running at 127.0.0.1:$port (data dir: $cluster)"
    echo "  stop it later with: pg_ctl -D '$cluster' -m immediate stop"
    return "$status"
  fi
  if [[ "$started" == 1 ]]; then
    pg_ctl -D "$cluster" -m immediate stop >/dev/null || status=1
  fi
  if [[ "$results_reserved" == 1 ]]; then
    cp "$root/postgres.log" "$results_dir/postgres.log" || status=1
  fi
  rm -rf "$root"
  return "$status"
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
export GRIND_BENCH_NETWORK_DELAY_MS="${GRIND_BENCH_NETWORK_DELAY_MS:-0}"
if [[ "$GRIND_BENCH_NETWORK_DELAY_MS" != "0" ]]; then
  proxy_port=$((port + 1))
  python3 "$bench_root/network_delay.py" --listen-port "$proxy_port" --upstream-port "$port" --delay-ms "$GRIND_BENCH_NETWORK_DELAY_MS" --ready "$root/proxy-ready" &
  delay_proxy_pid=$!
  for _ in $(seq 1 100); do
    [[ -f "$root/proxy-ready" ]] && break
    kill -0 "$delay_proxy_pid"
    sleep 0.05
  done
  [[ -f "$root/proxy-ready" ]] || { echo "delay proxy did not start" >&2; exit 1; }
  export GRIND_BENCH_DATABASE_URL="postgres://grind_a@127.0.0.1:$proxy_port/grind_bench?sslmode=disable"
fi
export GRIND_BENCH_POSTGRES_LOG="$root/postgres.log"
export GRIND_BENCH_PG_DATA_DIR="$cluster"
GRIND_BENCH_COMMIT="$(cd "$repo_root" && git rev-parse --short HEAD)"
export GRIND_BENCH_COMMIT
if [[ -n "$(cd "$repo_root" && git status --porcelain)" ]]; then
  export GRIND_BENCH_DIRTY=1
else
  export GRIND_BENCH_DIRTY=0
fi

results_dir="${GRIND_BENCH_RESULTS_DIR:-$bench_root/results/$(date +%Y-%m-%dT%H%M%S)-${GRIND_BENCH_COMMIT}}"
warmup_dir="$root/warmup-results"
export GRIND_BENCH_RESULTS_DIR="$results_dir"
python3 "$repo_root/scripts/bench-provenance.py" --reserve-dir "$results_dir"
results_reserved=1
mkdir -p "$warmup_dir"

GRIND_BENCH_SOURCE_SHA256="$(python3 "$repo_root/scripts/bench-provenance.py" --digest)"
export GRIND_BENCH_SOURCE_SHA256
python3 "$repo_root/scripts/bench-provenance.py" "$results_dir/provenance.json" "$@"
echo "commit=$GRIND_BENCH_COMMIT dirty=$GRIND_BENCH_DIRTY results_dir=$results_dir"

(cd "$bench_root" && gleam build)

repeats=${GRIND_BENCH_REPEATS:-3}
[[ "$repeats" -ge 3 ]] || { echo "at least three measured repeats required" >&2; exit 1; }

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

# Zero, 100k and one million retained rows; actual counts and plans are retained.
l2_points=(
  "1 50 0 10000"
  "1 50 100000 10000"
  "1 50 1000000 10000"
  "1 250 0 10000"
  "1 250 100000 10000"
  "1 250 1000000 10000"
  "8 50 0 10000"
  "8 50 100000 10000"
  "8 50 1000000 10000"
  "8 250 0 10000"
  "8 250 100000 10000"
  "8 250 1000000 10000"
)

# arrival_per_sec duration_ms -- fixed 4xC10 shape, baked into run_l3 itself.
l3_points=(
  "50 5000"
  "200 5000"
  "1000 5000"
)

# Fixed resource budget and equal work; enough duration for lock sampling.
l4_points=(
  "4 hot 40000"
  "16 hot 40000"
  "64 hot 40000"
  "4 cold 40000"
  "16 cold 40000"
  "64 cold 40000"
)

# pruner_on duration_ms
l5_points=(
  "0 10000"
  "1 10000"
)

# Minimum 3L handler duration, staggered across one further lease.
l6t1_points=(
  "4 4 90000"
  "10 10 90000"
  "50 50 90000"
)

# Targeted default-scale profiles: K sweep at the actual minimum 4D,
# representative boundary cases at the former minimum and default lease.
# Every tuple is k deadline lease delay concurrency main_pool; each consumer
# also reserves one renewal connection, reported separately in the CSV.
l6t2_points=()
for delay in 3200 4800; do
  for k in 0 3 4 5 6 7 8; do
    l6t2_points+=("$k 4000 16000 $delay 10 10")
  done
  for lease in 24000 30000; do
    for k in 0 5 8; do
      l6t2_points+=("$k 4000 $lease $delay 10 10")
    done
  done
done
# Opt-in selected stress points, not a cartesian expansion of every axis.
if [[ "${GRIND_BENCH_T2_STRESS:-0}" == "1" ]]; then
  l6t2_points+=(
    "8 4000 16000 3200 50 50"
    "8 4000 16000 4800 50 50"
    "8 4000 16000 3200 50 10"
    "8 4000 16000 4800 50 10"
  )
fi
# An explicit file can select a reproducible subset or extra resource shapes.
# Rows contain exactly: K D L ACK_delay concurrency main_pool (positive ints).
if [[ -n "${GRIND_BENCH_T2_PROFILES:-}" ]]; then
  cp "$GRIND_BENCH_T2_PROFILES" "$results_dir/t2-profiles.txt"
  l6t2_points=()
  while read -r k d lease delay concurrency pool extra; do
    [[ -z "${k:-}" || "$k" == \#* ]] && continue
    [[ -z "${extra:-}" && "$k $d $lease $delay $concurrency $pool" =~ ^[0-9]+\ [0-9]+\ [0-9]+\ [0-9]+\ [0-9]+\ [0-9]+$ ]] || { echo "invalid T2 profile row" >&2; exit 1; }
    l6t2_points+=("$k $d $lease $delay $concurrency $pool")
  done < "$results_dir/t2-profiles.txt"
  [[ "${#l6t2_points[@]}" -gt 0 ]] || { echo "empty T2 profile file" >&2; exit 1; }
fi

# Preserve the actual selected workload shapes so evidence validation does not
# maintain a second matrix configuration.
{
  printf 'suite=%s\nrepeats=%s\n' "$what" "$repeats"
  printf 'l1 %s\n' "${l1_points[@]}"
  printf 'l2 %s\n' "${l2_points[@]}"
  printf 'l3 %s\n' "${l3_points[@]}"
  printf 'l4 %s\n' "${l4_points[@]}"
  printf 'l5 %s\n' "${l5_points[@]}"
  printf 'l6_t1 %s\n' "${l6t1_points[@]}"
  printf 'l6_t2 %s\n' "${l6t2_points[@]}"
  printf 'l7 %s\n' "${l7_points[@]}"
} > "$results_dir/matrix-points.txt"

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

# Every point discards a warm-up and keeps at least three repeats.
l6_repeats=$repeats

run_l6t1_point() {
  local concurrency=$1 job_count=$2 cost_ms=$3
  echo "== l6t1 concurrency=${concurrency} (job_count=$job_count cost_ms=$cost_ms) =="
  (cd "$bench_root" && GRIND_BENCH_RESULTS_DIR="$warmup_dir" gleam run -m grind_bench/load -- l6t1 "$concurrency" "$job_count" "$cost_ms" 0)
  for repeat in $(seq 1 "$l6_repeats"); do
    echo "  repeat $repeat"
    (cd "$bench_root" && gleam run -m grind_bench/load -- l6t1 "$concurrency" "$job_count" "$cost_ms" "$repeat")
  done
}

run_l6t2_point() {
  local k_slow_acks=$1 d_ms=$2 lease=$3 delay=$4 concurrency=$5 pool=$6
  echo "== l6t2 k=$k_slow_acks D=$d_ms L=$lease delay=$delay C=$concurrency main_pool=$pool renewal_pool=1 =="
  (cd "$bench_root" && GRIND_BENCH_RESULTS_DIR="$warmup_dir" gleam run -m grind_bench/load -- l6t2 "$k_slow_acks" "$d_ms" "$lease" "$delay" "$concurrency" "$pool" 0)
  for repeat in $(seq 1 "$l6_repeats"); do
    echo "  repeat $repeat"
    (cd "$bench_root" && gleam run -m grind_bench/load -- l6t2 "$k_slow_acks" "$d_ms" "$lease" "$delay" "$concurrency" "$pool" "$repeat")
  done
}

if [[ "$what" == "l1" || "$what" == "all" ]]; then
  for point in "${l1_points[@]}"; do
    read -r -a point_args <<< "$point"
    run_l1_point "${point_args[@]}"
  done
fi

if [[ "$what" == "l7" || "$what" == "all" ]]; then
  for point in "${l7_points[@]}"; do
    read -r -a point_args <<< "$point"
    run_l7_point "${point_args[@]}"
  done
  echo "== profile 1x50 (coordinator profiling, item 11) =="
  (cd "$bench_root" && gleam run -m grind_bench/load -- profile 9000 1 50)
  echo "== profile 5x10 (coordinator profiling, item 11) =="
  (cd "$bench_root" && gleam run -m grind_bench/load -- profile 33000 5 10)
fi

if [[ "$what" == "l2" || "$what" == "all" ]]; then
  for point in "${l2_points[@]}"; do
    read -r -a point_args <<< "$point"
    run_l2_point "${point_args[@]}"
  done
fi

if [[ "$what" == "l3" || "$what" == "all" ]]; then
  for point in "${l3_points[@]}"; do
    read -r -a point_args <<< "$point"
    run_l3_point "${point_args[@]}"
  done
fi

if [[ "$what" == "l4" || "$what" == "all" ]]; then
  for point in "${l4_points[@]}"; do
    read -r -a point_args <<< "$point"
    run_l4_point "${point_args[@]}"
  done
fi

if [[ "$what" == "l5" || "$what" == "all" ]]; then
  for point in "${l5_points[@]}"; do
    read -r -a point_args <<< "$point"
    run_l5_point "${point_args[@]}"
  done
fi

if [[ "$what" == "l6" || "$what" == "l6t1" || "$what" == "all" ]]; then
  for point in "${l6t1_points[@]}"; do
    read -r -a point_args <<< "$point"
    run_l6t1_point "${point_args[@]}"
  done
fi
if [[ "$what" == "l6" || "$what" == "l6t2" || "$what" == "all" ]]; then
  for point in "${l6t2_points[@]}"; do
    read -r -a point_args <<< "$point"
    run_l6t2_point "${point_args[@]}"
  done
fi

[[ "$(python3 "$repo_root/scripts/bench-provenance.py" --digest)" == "$GRIND_BENCH_SOURCE_SHA256" ]] || { echo "source changed during matrix; evidence invalid" >&2; exit 1; }
echo "matrix run complete: $results_dir"
