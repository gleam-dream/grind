#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cluster_root="$(mktemp -d "${TMPDIR:-/tmp}/grind-resilience.XXXXXX")"
port=${GRIND_RESILIENCE_PGPORT:-$((20000 + RANDOM % 20000))}
output=${GRIND_RESILIENCE_OUTPUT:-"$repo_root/resilience/results/$(date -u +%Y%m%dT%H%M%SZ)-$$"}
started=0
cleanup() {
  if [[ "$started" == 1 ]]; then
    pg_ctl -D "$cluster_root/data" -m immediate stop >/dev/null 2>&1 || true
  fi
  if [[ -d "$output" ]]; then
    if [[ -f "$cluster_root/postgres.log" ]]; then
      cp "$cluster_root/postgres.log" "$output/postgres.log"
    fi
  fi
  rm -rf "$cluster_root"
}
trap cleanup EXIT
if pg_isready -h 127.0.0.1 -p "$port" >/dev/null 2>&1; then
  echo "Refusing to use occupied PostgreSQL port $port" >&2
  exit 1
fi
initdb -D "$cluster_root/data" --username=grind --auth-local=trust --auth-host=trust >/dev/null
pg_ctl -D "$cluster_root/data" -o "-h 127.0.0.1 -p $port -c synchronous_commit=on" -l "$cluster_root/postgres.log" start >/dev/null
started=1
createdb -h 127.0.0.1 -p "$port" -U grind grind_resilience
export RESILIENCE_PGDATA="$cluster_root/data"
(
  cd "$repo_root/resilience"
  gleam build
)
mkdir -p "$(dirname "$output")"
python3 "$repo_root/resilience/run.py" \
  --database-url "postgres://grind@127.0.0.1:$port/grind_resilience?sslmode=disable" \
  --output "$output" "$@"
echo "Resilience evidence: $output"
