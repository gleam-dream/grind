-- Bench-owned ledger schema. Deliberately never the Grind installation's own
-- schema (see bench/README-shaped module docs and AGENTS.md's SQL split):
-- this DDL lives entirely under a separate PostgreSQL schema (by default
-- `grind_bench`) so a bench run's own bookkeeping can never collide with, or
-- be mistaken for, Grind's own `grind_jobs` and receipt tables, which live
-- under their own separately configured schema (see
-- `grind_bench.default_config`).
--
-- Correlation model: every submitted bench job carries an application-owned
-- `bench_index` (a bench-run-local sequence number, distinct from Grind's own
-- `grind_jobs.id`) inside its JSON input. `bench_submissions` is the ledger's
-- own record of that mapping (`bench_index` -> the real `job_id` Grind
-- returned), written once at submission/preload time. `bench_effects` is one
-- row per actual handler invocation, keyed by `bench_index` alone (a worker's
-- `perform` callback never receives Grind's own job id — see
-- `docs/IMPLEMENTATION-SCOPE.md`, "Job lifecycle and attempt history" — so
-- the ledger has to use the identity the application itself controls). The
-- audit checker (`grind_bench/audit`) joins these two tables, plus Grind's
-- own `grind_jobs`/`grind_job_acknowledgements`/`grind_job_resolutions` in
-- its separately configured schema, to check invariants I1-I7.
--
-- Idempotent (`IF NOT EXISTS` throughout): `grind_bench.ensure_ledger_schema`
-- runs this on every harness start, matching how `postgres.migrate` is
-- always safe to call again.
CREATE SCHEMA IF NOT EXISTS grind_bench;

CREATE TABLE IF NOT EXISTS grind_bench.bench_submissions (
  bench_index bigint PRIMARY KEY,
  job_id bigint NOT NULL,
  queue text NOT NULL,
  submitted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS bench_submissions_job_id_idx
  ON grind_bench.bench_submissions (job_id);

CREATE TABLE IF NOT EXISTS grind_bench.bench_effects (
  id bigserial PRIMARY KEY,
  bench_index bigint NOT NULL,
  delivery_count integer NOT NULL,
  node text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS bench_effects_bench_index_idx
  ON grind_bench.bench_effects (bench_index);

-- Truncated (never dropped) between runs by `grind_bench.reset_ledger` so a
-- fresh smoke/load run starts from an empty ledger without re-creating the
-- schema.
