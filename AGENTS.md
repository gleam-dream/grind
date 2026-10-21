# Agent Instructions

## About this repo

`grind` — typed, durable background jobs on OTP and PostgreSQL. Oban is a scoped behavioral oracle, not a runtime wrapper. Read `docs/design/design.typ`, `docs/design/CONTEXT.typ`, `docs/COVERAGE.md` and the relevant `docs/adr/` record before changing behavior. Preserve caller-owned worker, codec and error boundaries and the SQL rules below. Telemetry and staged transaction admission do not establish business commit.

## Tooling and gates

- When updating dependency pins, reconcile affected `manifest.toml` files in examples, consumers and nested packages through Gleam, then run the full repository gate; updating the root lock alone is insufficient.

- `nix develop` (or direnv): dev shell with `gleam`, Erlang/OTP 29, `rebar3`, `lefthook`.
- Dependencies: the unpublished Sinal package is a Git dependency on `main`, and each `manifest.toml` is the lock. Update it by hand with `gleam update sinal` in each project that depends on it (root, `bench`, `consumer`, `examples/usage`, `resilience`). The Oban oracle source is fetched into the gitignored `oracle/deps`; no upstream source is committed.
- `nix fmt`: formats the repo through treefmt. `lefthook` formats and re-stages staged files on commit. `nix flake check` fails iff the tree is not formatted.
- `gleam test`: runs the suite and skips database cases without their test URL; it never replaces the database gate.
- `nix develop --command bash scripts/test-postgres.sh`: database gate on a disposable cluster. It runs the root, consumer and `examples/usage` suites, the twelve paired oracle scenarios and the Squirrel conformance check.
- `nix develop --command bash scripts/ci.sh fast`: edit-loop checks. `scripts/ci.sh full` runs every applicable check and runs before integration. `scripts/ci.sh matrix` and `scripts/ci.sh soak` are the weekly benchmark and endurance profiles. None of them repairs source files.
- Benchmarks: `scripts/bench-smoke.sh` and `scripts/bench-matrix.sh`. Resilience: `scripts/test-resilience.sh`. Oracle faults: `oracle/run-faults.sh`. Their flags are in `docs/BENCHMARKS.md`, `resilience/README.md` and `docs/ORACLE.md`.
- Run database gates, benchmarks and resilience runs serially in one checkout. Qualification rules are in the design, "Verification method".

## References

- Usage: compiled examples under `examples/usage`, one module per topic (`examples/usage/README.md`). Change an example when the API changes; the fast profile builds them and the database gate runs every topic.
- `docs/ORACLE.md` pins the Oban reference (OSS v2.24.1, commit `64b8481e5383f6bc46b7a5284e4ced3202dec9d5`), what is compared and how outcomes are classified. `oracle/ORACLE-LEDGER.md` maps Grind tests to upstream sources.
- `docs/BENCHMARKS.md` states the benchmark method, validity rules and the dated retained results. It and `docs/ORACLE.md` link each other; a pin or method change updates both.

## Generated and hand-written SQL

- `src/grind/internal/sql.gleam` is regenerated wholesale by `scripts/generate-sql.sh` from the static files under `src/grind/internal/sql/`. Never edit it by hand; change the `.sql` file and rerun the script.
- Dynamic SQL stays inline in `grind/internal/postgres` and its collaborators. The split is permanent (ADR-0014).

## Adding a migration

The migration contract is in the design, "Storage and migrations", with rationale in ADR-0015 and ADR-0016. The steps:

1. Add `priv/migrations/<14-digit UTC timestamp>-grind_v<N>.sql`, where `N` is the next integer after the highest version. Follow the baseline shape: the advisory-lock statement first, copied from `migrations.advisory_lock_statement()`, then the version statements ending with the marker insert, then a real reverse-order `down` section.
2. Add the matching `Migration(<N>, ...)` entry to `migrations()` in `src/grind/internal/migrations.gleam`. Its statements equal the file `up` section without trailing semicolons. Its `shape` and `foreign_keys` are complete and cumulative; confirm the relation set with `SELECT relname, relkind FROM pg_class WHERE relname LIKE 'grind\_%' ORDER BY relname` on a database where the version was applied.
3. After release, add the file sha256 (`shasum -a 256 <file> | awk '{print toupper($1)}'`) to `released_migration_sha256` in `test/grind/migrations/conformance_test.gleam`.
4. Run `scripts/test-postgres.sh`, which applies every mirror `up` section with `psql` before Squirrel generation and check.

<!-- agent-skills:begin -->
<!-- framework-commit: cab7c0590036edaa66d8430cc5016399a9fd2c71 origin: git@github.com:lostbean/skills.git -->

(machine-owned; do not edit inside this fence — re-run setup to refresh)

## Agent skills

**Design layer** — `docs/design/design.typ` describes the design,
`docs/design/CONTEXT.typ` defines its vocabulary, and `docs/adr/` records
decision rationale. The rendered document is `docs/design/design-layer.pdf`.
`docs/COVERAGE.md` maps repository parts to their design owners.

**Tracker** — local markdown under `issues/`: one directory per feature
(`issues/<feature-slug>/`), the PRD at `issues/<feature-slug>/PRD.md`, issue
files at `issues/<feature-slug>/issues/<NN>-<slug>.md` numbered from 01. Each
issue file carries two header lines near the top — `Status: <state>`,
`Category: <category>`. Conversation appends to the bottom
of the file under a `## Comments` heading. Labels (canonical strings, adopted
verbatim): states `needs-triage`, `needs-info`, `ready-for-agent`,
`ready-for-human`, `in-progress`, `done`, `wontfix`; categories `bug`,
`enhancement`.

**AI disclaimer** — AI-authored tracker comments start with
`AI-assisted contribution.`

**Design tools** — `nix run .#design-gate-render -- docs/design docs/design/design-layer.pdf`
renders the design. `nix run .#design-gate-context -- docs/design --estimate`
estimates context. CI calls the shared validator directly with
`nix run .#design-gate-check -- docs/design .`. Run `nix flake check` separately
for repository formatting. Local test scripts own application compilation and behavior.

<!-- agent-skills:end -->

## README snippets

Keep README Gleam examples in compiled example or consumer source. Use named
`snips-start` / `snips-end` regions (or include a whole example module), and retain
Prettier ignore ranges around generated README blocks. Format the source first,
then run `nix run .#snips -- --commands deny README.md`. CI runs
`nix run .#snips -- --check --commands deny README.md` using the shared pinned
package. Snips checks synchronization; existing example tests check behavior.
