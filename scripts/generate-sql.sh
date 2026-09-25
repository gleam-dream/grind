#!/usr/bin/env bash
set -euo pipefail

# Squirrel (src/**/sql/*.sql -> sibling sql.gleam) needs a live PostgreSQL
# server to inspect each query's real parameter and column types, both when
# generating (`gleam run -m squirrel`) and when checking freshness
# (`gleam run -m squirrel check`, run from scripts/test-postgres.sh). This
# script stands up the same kind of disposable, throwaway cluster
# scripts/test-postgres.sh uses, applies Grind's schema DDL with psql, points
# Squirrel at it via DATABASE_URL, and regenerates src/grind/internal/sql.gleam.

port=${GRIND_TEST_PGPORT:-$((20000 + RANDOM % 20000))}
root="$(mktemp -d "${TMPDIR:-/tmp}/grind-generate-sql.XXXXXX")"
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

if pg_isready -h 127.0.0.1 -p "$port" >/dev/null 2>&1; then
  echo "port $port is already in use; refusing to use a non-disposable database" >&2
  exit 1
fi

initdb -D "$cluster" --username=grind --auth-local=trust --auth-host=trust >/dev/null
pg_ctl -D "$cluster" -o "-h 127.0.0.1 -p $port" -l "$root/postgres.log" start >/dev/null
started=1
createdb -h 127.0.0.1 -p "$port" -U grind grind_codegen
psql -h 127.0.0.1 -p "$port" -U grind -d grind_codegen -v ON_ERROR_STOP=1 \
  -f "$repo_root/src/grind/internal/schema.sql" >/dev/null

printf 'Disposable PostgreSQL %s at 127.0.0.1:%s (Squirrel codegen database)\n' "$(postgres --version | awk '{print $3}')" "$port"

(
  cd "$repo_root"
  DATABASE_URL="postgres://grind@127.0.0.1:$port/grind_codegen?sslmode=disable" \
    gleam run -m squirrel
)
