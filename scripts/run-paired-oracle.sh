#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
: "${GRIND_ORACLE_DATABASE_URL:?dedicated empty Grind database required}"
: "${GRIND_OBAN_TEST_DATABASE_URL:?dedicated empty Oban database required}"
: "${GRIND_ORACLE_RESULTS_ROOT:?result directory required}"
GRIND_ORACLE_RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$-$RANDOM"
export GRIND_ORACLE_RUN_ID
mkdir -p "$(dirname "$GRIND_ORACLE_RESULTS_ROOT")"
mkdir "$GRIND_ORACLE_RESULTS_ROOT"
results_root="$(cd "$GRIND_ORACLE_RESULTS_ROOT" && pwd)"
python3 -m unittest discover -s "$repo_root/oracle" -p 'test_*.py' -v
cp "$repo_root/oracle/scenarios.json" "$results_root/catalog.json"
export GRIND_ORACLE_CATALOG="$results_root/catalog.json"

(
  cd "$repo_root"
  GRIND_ORACLE_RESULTS="$results_root/grind.jsonl" gleam run -m grind/oracle/paired 2>&1 | tee "$results_root/grind.log"
)
(
  cd "$repo_root/oracle"
  GRIND_ORACLE_RESULTS="$results_root/oban.jsonl" mix run paired.exs 2>&1 | tee "$results_root/oban.log"
  mix run --no-start comparison_test.exs
  mix run --no-start compare.exs "$GRIND_ORACLE_CATALOG" "$results_root/grind.jsonl" "$results_root/oban.jsonl" | tee "$results_root/comparison.log"
)
