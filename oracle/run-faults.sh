#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cluster_root="$(mktemp -d "${TMPDIR:-/tmp}/oban-faults.XXXXXX")"
port=${GRIND_ORACLE_FAULT_PGPORT:-$((20000 + RANDOM % 20000))}
output=${GRIND_ORACLE_FAULT_OUTPUT:-"$repo_root/oracle/results/$(date -u +%Y%m%dT%H%M%SZ)-$$"}
started=0
if [[ -e "$output" ]]; then
  echo "Refusing to overwrite existing evidence: $output" >&2
  rm -rf "$cluster_root"
  exit 1
fi
mkdir -p "$(dirname "$output")"
cleanup() {
  local status=$?
  if [[ "$started" == 1 ]]; then
    pg_ctl -D "$cluster_root/data" -m immediate stop >/dev/null 2>&1 || status=1
  fi
  if [[ -f "$cluster_root/postgres.log" ]]; then
    mkdir -p "$output" || status=1
    cp "$cluster_root/postgres.log" "$output/postgres.log" || status=1
  fi
  rm -rf "$cluster_root" || status=1
  return "$status"
}
trap cleanup EXIT
if pg_isready -h 127.0.0.1 -p "$port" >/dev/null 2>&1; then
  echo "Refusing to use occupied PostgreSQL port $port" >&2
  exit 1
fi
initdb -D "$cluster_root/data" --username=grind --auth-local=trust --auth-host=trust >/dev/null
pg_ctl -D "$cluster_root/data" -o "-h 127.0.0.1 -p $port -k $cluster_root -c synchronous_commit=on" -l "$cluster_root/postgres.log" start >/dev/null
started=1
createdb -h 127.0.0.1 -p "$port" -U grind oban_resilience
export GRIND_OBAN_TEST_DATABASE_URL="postgres://grind@127.0.0.1:$port/oban_resilience?sslmode=disable"
(
  cd "$repo_root/oracle"
  mix compile --warnings-as-errors
)
python3 -m unittest discover -s "$repo_root/oracle" -p 'test_*.py' -v
python3 "$repo_root/oracle/faults.py" --database-url "$GRIND_OBAN_TEST_DATABASE_URL" --output "$output" "$@"
echo "Oban independent-VM evidence: $output"
