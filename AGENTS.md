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
- **Hand-written inline (`grind/postgres`, `grind/internal/unique_admission`)**:
  dynamic SQL — shared lease/period/lock predicate fragments spliced into
  more than one query, per-disposition acknowledgement SQL (branches on the
  proposed state), nullable-parameter queries whose bound value shape varies
  by call, and candidate selection (its `WHERE`/`ORDER BY`/locking clause
  depends on scope, period, and conflict action). Squirrel cannot generate
  these from a single static string, so they stay inline.
