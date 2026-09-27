//// Shared bench-harness configuration and setup: the two independent
//// PostgreSQL schemas a bench run needs (Grind's own installation, and the
//// bench-owned ledger), plus starting each side's own connection pool.
////
//// Grind's own schema and the ledger schema are deliberately never the
//// same value (see `bench/priv/bench.sql`'s own doc comment and
//// AGENTS.md's SQL split) -- a bench run's bookkeeping must never live
//// inside the schema it is measuring.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/otp/actor
import gleam/result
import gleam/string
import grind/postgres
import pog
import simplifile

@external(erlang, "grind_bench_ffi", "fresh_run_id")
fn fresh_run_id() -> String

/// One bench run's full configuration: the database(s) it targets and the
/// two schema names it splits Grind's own installation from the bench
/// ledger.
///
/// `database_url` and `ctl_database_url` are deliberately allowed to differ
/// (item 10, "roles"): a hardened matrix run connects Grind's own pool as
/// one PostgreSQL role (e.g. `grind_a`) and the harness's own ledger/drain
/// poll/samplers as another (`grind_ctl`), so `pg_stat_activity`/
/// `pg_stat_statements` can attribute load to one side or the other.
/// `default_config` points both at the same URL (a single-role setup, e.g.
/// the plain `smoke` scenario), and `with_ctl_database_url` overrides the
/// ctl side alone.
pub type Config {
  Config(
    database_url: String,
    ctl_database_url: String,
    grind_schema: String,
    ledger_schema: String,
    grind_pool_size: Int,
    ledger_pool_size: Int,
  )
}

/// `grind_schema` defaults to a fresh, run-scoped name (`bench_jobs_<run_id>`
/// -- item 1: every bench run gets its own Grind schema, created and
/// migrated by `postgres.migrate`, never a schema shared with any other run
/// or left over from a crashed one -- see `drop_schema`/
/// `drop_stale_bench_schemas`, which callers use to keep this from
/// accumulating). `ledger_schema` stays the fixed `"grind_bench"` -- the
/// ledger is truncated (`reset_ledger`), not recreated, at the start of
/// every run.
pub fn default_config(database_url: String) -> Config {
  Config(
    database_url:,
    ctl_database_url: database_url,
    grind_schema: "bench_jobs_" <> fresh_run_id(),
    ledger_schema: "grind_bench",
    grind_pool_size: 10,
    ledger_pool_size: 4,
  )
}

pub fn with_grind_pool_size(config: Config, pool_size: Int) -> Config {
  Config(..config, grind_pool_size: pool_size)
}

pub fn with_ledger_pool_size(config: Config, pool_size: Int) -> Config {
  Config(..config, ledger_pool_size: pool_size)
}

/// Item 10: points the harness's own ledger/drain-poll/samplers connections
/// at a distinct role from Grind's own pool. Defaults to `database_url`
/// (same role) when never called.
pub fn with_ctl_database_url(
  config: Config,
  ctl_database_url: String,
) -> Config {
  Config(..config, ctl_database_url:)
}

/// Item 6: the ledger pool must never bottleneck drain detection or effect
/// recording -- size it to at least the run's total concurrency
/// (`consumers * concurrency`), the same rule the plan calls for.
pub fn ledger_pool_size_for_concurrency(total_concurrency: Int) -> Int {
  int.max(total_concurrency, 4)
}

/// `grind/postgres.Settings` for this bench run's own Grind installation,
/// scoped to `config.grind_schema` -- never `config.ledger_schema`.
pub fn grind_settings(config: Config) -> postgres.Settings {
  postgres.settings(config.database_url)
  |> postgres.with_pool_size(config.grind_pool_size)
  |> postgres.with_schema(config.grind_schema)
}

pub type LedgerStartError {
  LedgerUrlInvalid
  LedgerPoolStartFailed(actor.StartError)
  LedgerSchemaSetupFailed(pog.QueryError)
}

/// Starts a small, bench-owned PostgreSQL pool whose `search_path` resolves
/// to `config.ledger_schema` -- entirely independent of Grind's own pool
/// (started separately via `grind_settings`/`postgres.start`), matching the
/// plan's "bench-owned pool" for ledger writes: a stalled or contended
/// ledger write must never compete with, or be mistaken for, Grind's own
/// storage calls.
pub fn start_ledger_pool(
  config: Config,
) -> Result(pog.Connection, LedgerStartError) {
  use pog_config <- result.try(
    pog.url_config(
      process.new_name("grind_bench_ledger_pool"),
      config.ctl_database_url,
    )
    |> result.replace_error(LedgerUrlInvalid),
  )
  let pog_config =
    pog_config
    |> pog.pool_size(config.ledger_pool_size)
    |> pog.connection_parameter(
      name: "search_path",
      value: config.ledger_schema,
    )
  use started <- result.try(
    pog.start(pog_config) |> result.map_error(LedgerPoolStartFailed),
  )
  let connection = started.data
  use _ <- result.try(ensure_ledger_schema(connection))
  Ok(connection)
}

/// Runs `bench/priv/bench.sql` (idempotent `CREATE SCHEMA`/`CREATE TABLE ...
/// IF NOT EXISTS`) against `connection`. Split on `;` at statement
/// boundaries: `pog.query` runs exactly one statement per call, unlike
/// `psql -f`, which is why this is not just a single `pog.execute` call.
fn ensure_ledger_schema(
  connection: pog.Connection,
) -> Result(Nil, LedgerStartError) {
  let assert Ok(sql) = simplifile.read(bench_sql_path())
  let statements =
    sql
    |> string.split("\n")
    |> list.filter(fn(line) { !string.starts_with(string.trim(line), "--") })
    |> string.join("\n")
    |> string.split(";")
    |> list.map(string.trim)
    |> list.filter(fn(statement) { statement != "" })
  list.try_each(statements, fn(statement) {
    pog.query(statement)
    |> pog.execute(connection)
    |> result.map(fn(_) { Nil })
    |> result.map_error(LedgerSchemaSetupFailed)
  })
}

/// Truncates every ledger table (never drops them) so a fresh smoke/load run
/// starts from an empty ledger -- including the L6 instrumentation tables
/// (`grind_bench/instrumentation`), truncated unconditionally here so a
/// scenario that never installs either trigger still starts from a
/// guaranteed-empty table, not merely "whatever an unrelated earlier run
/// left behind." Grind's own schema is untouched -- a caller that wants a
/// fully clean Grind installation too calls `postgres.migrate` against a
/// fresh database instead.
pub fn reset_ledger(connection: pog.Connection) -> Result(Nil, pog.QueryError) {
  pog.query(
    "TRUNCATE grind_bench.bench_submissions, grind_bench.bench_effects, grind_bench.bench_lease_log, grind_bench.bench_slow_ack_targets",
  )
  |> pog.execute(connection)
  |> result.map(fn(_) { Nil })
}

/// `bench/priv/bench.sql`'s path, resolved relative to this module's own
/// source file at build time via `?MODULE`'s compiled beam path would be
/// fragile across `gleam run`/`gleam test`/an installed escript; resolving
/// from the current working directory instead matches how this project is
/// always invoked (`cd bench && gleam run ...` / `cd bench && gleam test`,
/// exactly like `consumer/`'s own test suite is always run from `consumer/`).
fn bench_sql_path() -> String {
  "priv/bench.sql"
}

/// Item 6: a single dedicated connection for drain polling -- never the
/// handler ledger pool (`start_ledger_pool`'s own connection, which
/// `bench_effects` writes contend for under load). A 1-connection pool is
/// deliberate: drain detection issues one query at a time in a poll loop, so
/// there is never more than one in-flight statement to serve.
pub fn start_drain_connection(
  config: Config,
) -> Result(pog.Connection, LedgerStartError) {
  use pog_config <- result.try(
    pog.url_config(
      process.new_name("grind_bench_drain_pool"),
      config.ctl_database_url,
    )
    |> result.replace_error(LedgerUrlInvalid),
  )
  let pog_config =
    pog_config
    |> pog.pool_size(1)
    // Every audit check (`grind_bench/audit`'s own SQL) references
    // `bench_submissions`/`bench_effects` unqualified, relying on
    // `search_path` resolving them -- exactly like `start_ledger_pool`'s own
    // connection. Missing this the first time this connection existed made
    // every drain-poll query fail with "relation bench_submissions does not
    // exist", which `grind_bench/load.poll_drain`'s own catch-all silently
    // treated as "not yet drained" until the poll timed out -- a real
    // 500-job run that finished in under a second still reported "timed out
    // waiting for drain" 30 seconds later.
    |> pog.connection_parameter(
      name: "search_path",
      value: config.ledger_schema,
    )
  use started <- result.try(
    pog.start(pog_config) |> result.map_error(LedgerPoolStartFailed),
  )
  Ok(started.data)
}

/// Item 1: `DROP SCHEMA IF EXISTS <schema> CASCADE` against `connection`
/// (any plain connection -- the ledger or drain connection both work, since
/// this is a fully-qualified, one-shot statement independent of
/// `search_path`). `schema` is always one this module itself generated
/// (`default_config`'s `"bench_jobs_" <> fresh_run_id()`) or read back from
/// `information_schema.schemata` by `drop_stale_bench_schemas` -- never
/// caller-supplied free text, so splicing it directly into `DROP SCHEMA` is
/// safe here the same way `postgres.with_schema`'s own doc comment reasons
/// about `CREATE SCHEMA`.
pub fn drop_schema(
  connection: pog.Connection,
  schema: String,
) -> Result(Nil, pog.QueryError) {
  pog.query("DROP SCHEMA IF EXISTS \"" <> schema <> "\" CASCADE")
  |> pog.execute(connection)
  |> result.map(fn(_) { Nil })
}

/// Item 1: "drop old ones, or keep only the last run with BENCH_KEEP" --
/// called once at the start of every scenario's own `setup`, before the
/// fresh schema for *this* run is created, so a schema orphaned by a
/// previous run that crashed (or was interrupted) before its own end-of-run
/// cleanup ran never accumulates. Every prior run's own end-of-run cleanup
/// (see `grind_bench/load.cleanup_schema`) already drops its own schema
/// unless `BENCH_KEEP=1`; this is the backstop for the runs that never got
/// that far, plus the case where `BENCH_KEEP=1` was used for more than one
/// run in a row.
pub fn drop_stale_bench_schemas(
  connection: pog.Connection,
) -> Result(Nil, pog.QueryError) {
  use schemas <- result.try(bench_schema_names(connection))
  list.try_each(schemas, drop_schema(connection, _))
}

fn bench_schema_names(
  connection: pog.Connection,
) -> Result(List(String), pog.QueryError) {
  let query =
    pog.query(
      "SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'bench\\_jobs\\_%'",
    )
    |> pog.returning({
      use name <- decode.field(0, decode.string)
      decode.success(name)
    })
  pog.execute(query, connection) |> result.map(fn(returned) { returned.rows })
}

/// Item 1: "assert the queue is empty before preload" -- guards against a
/// `fresh_run_id` collision (astronomically unlikely, but cheap to check)
/// or a caller that accidentally pointed `grind_schema` at a pre-existing,
/// non-empty installation instead of a freshly migrated one.
pub fn assert_queue_empty(database: postgres.Database) -> Nil {
  let connection = postgres.connection(database)
  let query =
    pog.query("SELECT count(*) FROM grind_jobs")
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
  let assert Ok(pog.Returned(rows: [0], ..)) = pog.execute(query, connection)
  Nil
}
