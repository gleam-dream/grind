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
pg_ctl -D "$cluster" -o "-h 127.0.0.1 -p $port -k $root" -l "$root/postgres.log" start >/dev/null
started=1
createdb -h 127.0.0.1 -p "$port" -U grind grind_codegen

# Grind's own migrations.gleam (src/grind/internal/migrations.gleam) is the
# runtime source of truth; the cigogne-format files under priv/migrations
# are a byte-identical mirror kept only so an application can apply Grind's
# schema through cigogne (see README, "Migrations"). Apply their `up`
# sections here, in filename order, exactly as an application importing them
# via cigogne would, so Squirrel inspects the same schema either way.
up_sql="$root/schema-up.sql"
: >"$up_sql"
for migration_file in "$repo_root"/priv/migrations/*.sql; do
  # `tr -d '\r'` first: a migration file saved with CRLF line endings would
  # otherwise leave the guard lines' trailing `\r` inside the sed patterns
  # below, silently matching nothing and extracting an empty (or truncated)
  # up section instead of failing loudly.
  tr -d '\r' <"$migration_file" |
    sed -n '/^--- migration:up$/,/^--- migration:down$/p' |
    sed '1d;$d' >>"$up_sql"
done
if [[ ! -s "$up_sql" ]]; then
  echo "extracted migration up SQL is empty; check priv/migrations/*.sql's own --- migration:up/down guards" >&2
  exit 1
fi
psql -h 127.0.0.1 -p "$port" -U grind -d grind_codegen -v ON_ERROR_STOP=1 \
  -f "$up_sql" >/dev/null

printf 'Disposable PostgreSQL %s at 127.0.0.1:%s (Squirrel codegen database)\n' "$(postgres --version | awk '{print $3}')" "$port"

(
  cd "$repo_root"
  DATABASE_URL="postgres://grind@127.0.0.1:$port/grind_codegen?sslmode=disable" \
    gleam run -m squirrel
)

# Squirrel overwrites sql.gleam's own header wholesale each run, dropping the
# hand-written note explaining the generated/hand-written SQL split (see
# AGENTS.md). Re-prepend it right after Squirrel's own 5-line banner every
# time, so a regeneration never silently loses it.
generated_sql="$repo_root/src/grind/internal/sql.gleam"
if ! grep -q "do not hand-edit" "$generated_sql"; then
  note_file="$root/sql-header-note.txt"
  cat >"$note_file" <<'NOTE'
//// This file is regenerated wholesale by `scripts/generate-sql.sh` (which
//// wraps `gleam run -m squirrel` against a disposable database) from the
//// static `.sql` files under `./src/grind/internal/sql/`; do not hand-edit
//// it, and re-add this note if it is ever lost to a regeneration. This is
//// one half of a permanent split, not a migration in progress: a query
//// belongs here only when its SQL text is fixed at compile time. Dynamic
//// SQL — shared lease/period/lock predicate fragments spliced into more
//// than one query, per-disposition acknowledgement SQL (the proposed state
//// selects which columns/branches apply), nullable-parameter queries whose
//// bound value shape varies by call, and candidate selection (its `WHERE`/
//// `ORDER BY`/locking clause depends on scope, period, and conflict
//// action) — stays hand-written inline in `grind/postgres` and
//// `grind/internal/unique_admission`, where squirrel cannot generate it
//// from a single static string. See `AGENTS.md` for the same rule.
NOTE
  awk 'NR==5{print; while ((getline line < note) > 0) print line; next} {print}' \
    note="$note_file" "$generated_sql" >"$generated_sql.tmp"
  mv "$generated_sql.tmp" "$generated_sql"
fi
