# Agent Instructions

## About this repo

`grind` — A strongly-typed Oban for Gleam: durable, typed background jobs on OTP + Postgres.

Ports/wraps: Oban. Design: [gleam-dream/oversight](https://github.com/gleam-dream/oversight)/grind-design.md.

## Tooling

- `nix develop` (or direnv): dev shell with `gleam`, Erlang/OTP 28, `rebar3`, `lefthook`.
- `nix fmt`: formats the whole repo via treefmt (`gleam format`, `nixfmt`, `prettier`).
- `lefthook`: pre-commit hook formats staged files and re-stages them.
- `nix flake check`: fails iff the tree is not formatted (plus any existing checks).
- `gleam test`: runs the test suite.

## Generated vs. hand-written SQL

`src/grind/internal/sql.gleam` is regenerated wholesale by
`scripts/generate-sql.sh` (`gleam run -m squirrel` against a disposable
database) from the static `.sql` files under `src/grind/internal/sql/`. Never
hand-edit `sql.gleam`; add or change a static query by editing its `.sql`
file and rerunning the script. This is a permanent split, not a migration in
progress:

- **Squirrel-generated (static queries)**: any query whose SQL text is fixed
  at compile time, given its own `.sql` file.
- **Hand-written inline (`grind/postgres` and its `grind/internal/` collaborators)**:
  dynamic SQL — shared lease/period/lock predicate fragments spliced into
  more than one query, per-disposition acknowledgement SQL (branches on the
  proposed state), nullable-parameter queries whose bound value shape varies
  by call, and candidate selection (its `WHERE`/`ORDER BY`/locking clause
  depends on scope, period, and conflict action). Squirrel cannot generate
  these from a single static string, so they stay inline.

## Adding a migration

`src/grind/internal/migrations.gleam`'s `migrations()` is the single source
of truth `postgres.migrate` executes; `priv/migrations/*.sql` is a
cigogne-format mirror Grind itself never reads (see README, "Migrations").
Both must move together, and `migrate_with` requires `migrations()` to be
exactly the contiguous range `{11..latest}` with no gaps or duplicate
versions (checked with `let assert`, since a malformed list is a
programming error, not a runtime condition) — always true as long as you add
exactly one new highest version at a time:

1. Add a new file `priv/migrations/<14-digit UTC timestamp>-grind_v<N>.sql`
   (`N` = the next integer after the current highest version), following the
   existing baseline's shape: `--- migration:up`, a blank options line, the
   **advisory-lock statement first** (copy it verbatim from
   `migrations.advisory_lock_statement()` or the existing baseline file — this
   is what makes an application applying migrations directly through cigogne
   serialise against a concurrent `postgres.migrate` caller too), then the new
   version's own statements (each ending `;`, its own trailing
   `INSERT INTO grind_schema_migrations (version) VALUES (<N>)` last),
   `--- migration:down` with the real, reverse-order `DROP`s (delete the
   marker row first), `--- migration:end`.
2. Add the matching `Migration(<N>, [...], [...], [...])` entry to
   `migrations()` in `src/grind/internal/migrations.gleam`: its `statements`
   list is the exact same statement text (minus the trailing `;`, lock
   statement included) as the new file's `up` section; its `shape`
   **declares** the version's own _complete, cumulative_ set of
   `grind_`-prefixed relations — every table, sequence, and index this
   version's schema has, including every implicit object PostgreSQL itself
   creates (a `bigserial` column's own sequence, and the backing index behind
   every `PRIMARY KEY`/`UNIQUE` constraint) — plus any column-level
   `key_columns` a relation needs beyond its own existence; its
   `foreign_keys` list names every foreign-key constraint (by name, checked
   against `pg_constraint`, independently of `shape`'s own `pg_class` check —
   a plain foreign key backed by no index of its own never appears in
   `pg_class`) this version's schema cumulatively requires, empty for a
   version that adds none. `read_schema_generation` compares `shape` against
   the schema's _actual_ `grind_`-prefixed relations exactly: a relation this
   schema has that the shape does not list fails closed exactly like a
   missing one, so **confirm the real relation set empirically**
   (`SELECT relname, relkind FROM
pg_class WHERE relname LIKE 'grind\_%' ORDER BY relname` against a
   database this version was actually applied to) rather than deriving it
   from the DDL text alone — undercounting implicit objects here is the
   easiest way to get this wrong (it was, the first time `v11_shape` was
   written).
3. Once the migration is released (merged, not still under review), compute
   its file's sha256 (`shasum -a 256 <file> | awk '{print toupper($1)}'`) and
   add it to `released_migration_sha256` in
   `test/grind/migrations/conformance_test.gleam` — this is
   what makes `grind_migrations_conformance_test` catch an accidental
   post-release edit to a file that has already shipped. (The _newest_
   version does not need a pin yet, since it may still be edited in the same
   change — but nothing stops you from adding one early once it settles.)
4. Run `nix develop --command bash scripts/test-postgres.sh` — it applies
   every `priv/migrations/*.sql` file's `up` section with `psql` before
   Squirrel codegen/check, so a mismatch between the two sources surfaces
   there too, on top of the conformance test.
