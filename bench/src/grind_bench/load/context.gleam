import gleam/int
import gleam/io
import gleam/string
import grind/job
import grind/postgres
import grind_bench
import grind_bench/load/runtime
import grind_bench/preload
import pog

/// Shared setup every scenario needs: a fresh, migrated Grind installation
/// (item 1: its own run-scoped schema) plus a reset ledger and a dedicated
/// drain-polling connection (item 6).
pub type Harness {
  Harness(
    database: postgres.Database,
    ledger: pog.Connection,
    drain: pog.Connection,
  )
}

pub fn setup(pool_size: Int, ledger_pool_size: Int) -> Harness {
  setup_with_settings(pool_size, ledger_pool_size, fn(settings) { settings })
}

/// Like `setup`, but overrides `postgres.Settings.statement_deadline_ms`
/// (`D`) before validating -- L6's T2 scenario scales `D` down from the
/// real 4000ms default for wall-clock feasibility while preserving the
/// exact `L = 6D` / `cost = 3L` relationships (see `run_l6t2`'s own doc
/// comment).
pub fn setup_with_deadline(
  pool_size: Int,
  ledger_pool_size: Int,
  deadline_ms: Int,
) -> Harness {
  // `postgres.validate` requires `unique_lock_wait_ms + 1000 <
  // statement_deadline_ms` (`docs/RISKS.md`-documented margin) -- the
  // default `unique_lock_wait_ms` (2000) only clears the real default
  // deadline (4000). A scaled-down `deadline_ms` (L6T2's own reduced `D`)
  // needs a proportionally scaled-down lock wait too, or `setup` itself
  // fails closed before this scenario ever starts.
  let lock_wait_ms = int.max(1, deadline_ms / 4)
  setup_with_settings(pool_size, ledger_pool_size, fn(settings) {
    settings
    |> postgres.with_statement_deadline(deadline_ms)
    |> postgres.with_unique_lock_wait(lock_wait_ms)
  })
}

fn setup_with_settings(
  pool_size: Int,
  ledger_pool_size: Int,
  adjust: fn(postgres.Settings) -> postgres.Settings,
) -> Harness {
  let config =
    grind_bench.default_config(runtime.database_url())
    |> grind_bench.with_ctl_database_url(runtime.ctl_database_url())
    |> grind_bench.with_grind_pool_size(pool_size)
    |> grind_bench.with_ledger_pool_size(ledger_pool_size)
  let assert Ok(ledger) = grind_bench.start_ledger_pool(config)
  // Item 1: drop any schema orphaned by a run that never reached its own
  // end-of-run cleanup (a crash, or an interrupted BENCH_KEEP run), before
  // this run creates its own fresh one.
  let assert Ok(Nil) = grind_bench.drop_stale_bench_schemas(ledger)
  let assert Ok(settings) =
    grind_bench.grind_settings(config) |> adjust |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  let assert Ok(Nil) = postgres.migrate(database)
  grind_bench.assert_queue_empty(database)
  let assert Ok(Nil) = grind_bench.reset_ledger(ledger)
  let assert Ok(drain) = grind_bench.start_drain_connection(config)
  runtime.reset_all_counters()
  case preload.check_schema_matches(database) {
    Ok(Nil) -> Nil
    Error(drift) -> {
      io.println(
        "grind_bench/preload: schema drift detected, refusing to preload: "
        <> string.inspect(drift),
      )
      runtime.halt(1)
    }
  }
  Harness(database:, ledger:, drain:)
}

pub fn schema_of(database: postgres.Database) -> String {
  job.installation_schema(postgres.installation(database))
}

/// Item 1: drops this run's own fresh schema unless `BENCH_KEEP=1`, so
/// schemas never accumulate across ordinary runs (see `setup`'s own
/// `drop_stale_bench_schemas` for the backstop on a run that skips this).
pub fn cleanup_schema(ledger: pog.Connection, grind_schema: String) -> Nil {
  case runtime.bench_keep() {
    True ->
      io.println(
        "BENCH_KEEP=1: leaving schema \"" <> grind_schema <> "\" in place",
      )
    False -> {
      let assert Ok(Nil) = grind_bench.drop_schema(ledger, grind_schema)
      Nil
    }
  }
}
