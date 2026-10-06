#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"
profile=${1:-full}
artifacts=${CI_ARTIFACT_DIR:-"$repo_root/.ci-results/$(date -u +%Y%m%dT%H%M%SZ)-$$"}
mkdir -p "$(dirname "$artifacts")"
mkdir "$artifacts"
artifacts="$(cd "$artifacts" && pwd)"
exec > >(tee "$artifacts/gate.log") 2>&1
export PYTHONDONTWRITEBYTECODE=1
mix_root="$(mktemp -d "${TMPDIR:-/tmp}/grind-ci-mix.XXXXXX")"
trap 'rm -rf "$mix_root"' EXIT
export MIX_HOME="$mix_root/mix" HEX_HOME="$mix_root/hex" MIX_REBAR3
MIX_REBAR3="$(command -v rebar3)"

bootstrap_oracle() {
  (cd oracle && mix local.hex --force --if-missing && mix deps.get --check-locked)
}
fast() {
  actionlint
  mapfile -d '' shell_files < <(git ls-files -z -- '*.sh')
  shellcheck "${shell_files[@]}"
  ruff check scripts bench resilience oracle --exclude oracle/deps --exclude oracle/_build
  for project in . consumer bench resilience; do
    (cd "$project" && gleam build --warnings-as-errors)
  done
  bootstrap_oracle
  bash scripts/check-native.sh
  (
    # Fast tests cannot inherit caller database targets; full tests set their own
    # disposable URLs in scripts/test-postgres.sh.
    test_variables=$(python3 -c 'import os; print("\n".join(name for name in os.environ if name.startswith("GRIND_TEST_")))')
    while IFS= read -r name; do
      if [[ -n "$name" ]]; then unset "$name"; fi
    done <<< "$test_variables"
    gleam test
  )
  python3 -B -m unittest discover -s scripts -p 'test_*.py' -v
  python3 -B -m unittest discover -s resilience -p 'test_*.py' -v
  python3 -B -m unittest discover -s bench/test -p 'test_*.py' -v
  python3 -B -m unittest discover -s oracle -p 'test_*.py' -v
}
full() {
  fast
  GRIND_TEST_LOG_DIR="$artifacts/postgres" GRIND_ORACLE_RESULTS_ROOT="$artifacts/core-oracle"     bash scripts/test-postgres.sh
  # test-postgres deliberately cleans root/consumer builds; check native sources again.
  bash scripts/check-native.sh
  GRIND_BENCH_RESULTS_DIR="$artifacts/bench-smoke" bash scripts/bench-smoke.sh
  GRIND_RESILIENCE_OUTPUT="$artifacts/resilience" bash scripts/test-resilience.sh
  python3 scripts/ci-evidence.py resilience "$artifacts/resilience"
  GRIND_ORACLE_FAULT_OUTPUT="$artifacts/oban-faults" bash oracle/run-faults.sh
  python3 oracle/fault_compare.py --grind "$artifacts/resilience" --oban "$artifacts/oban-faults"     --output "$artifacts/fault-comparison.json"
}
case "$profile" in
  fast) fast ;;
  full) full ;;
  matrix)
    bootstrap_oracle
    export GRIND_BENCH_RESULTS_DIR="$artifacts/matrix"
    export GRIND_BENCH_REPEATS=3 GRIND_BENCH_T2_STRESS=1 GRIND_BENCH_DRAIN_TIMEOUT_MS=600000
    export GRIND_BENCH_RELEASE_EVIDENCE=1
    unset GRIND_BENCH_ALLOW_T2_FAILURE
    bash scripts/bench-matrix.sh all
    python3 scripts/ci-evidence.py matrix "$GRIND_BENCH_RESULTS_DIR"
    ;;
  soak)
    GRIND_RESILIENCE_OUTPUT="$artifacts/soak" bash scripts/test-resilience.sh       --release-evidence --deadline-ms 4000 --lease-ms 30000 --soak-seconds 7200
    python3 scripts/ci-evidence.py soak "$artifacts/soak"
    ;;
  *) echo 'Usage: scripts/ci.sh [fast|full|matrix|soak]' >&2; exit 2 ;;
esac
