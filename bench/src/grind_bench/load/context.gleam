import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/option.{type Option, None, Some}
import gleam/string
import grind/internal/job
import grind/internal/postgres
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
    completion_observer: Option(CompletionObserver),
  )
}

pub opaque type CompletionObserver {
  CompletionObserver(
    pid: process.Pid,
    stop: process.Subject(process.Subject(Int)),
  )
}

/// Stop the run-owned observer before closing pools or dropping its schema.
/// The acknowledgement is sent after its last query completes.
pub fn stop_observer(harness: Harness) -> Nil {
  case harness.completion_observer {
    None -> Nil
    Some(CompletionObserver(pid:, stop:)) -> {
      let acknowledged = process.new_subject()
      process.send(stop, acknowledged)
      case process.receive(acknowledged, within: 5000) {
        Ok(polls) -> {
          let assert True = polls > 0
          let assert 0 =
            runtime.counter_value(runtime.completion_observer_error_counter)
          io.println(
            "completion_observer_stop_ack polls=" <> int.to_string(polls),
          )
        }
        Error(_) -> {
          process.kill(pid)
          panic as "completion observer did not acknowledge stop; run evidence is invalid"
        }
      }
    }
  }
}

pub fn setup(pool_size: Int, ledger_pool_size: Int) -> Harness {
  setup_with_settings(
    pool_size,
    ledger_pool_size,
    fn(settings) { settings },
    True,
  )
}

/// Idle/admission-only measurements do not need completion sampling.
pub fn setup_without_completion_observer(
  pool_size: Int,
  ledger_pool_size: Int,
) -> Harness {
  setup_with_settings(
    pool_size,
    ledger_pool_size,
    fn(settings) { settings },
    False,
  )
}

/// Overrides the statement deadline for an explicit fault profile. Matrix
/// validation uses the real D=4000ms default as well as caller-selected D.
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
  let lock_wait_ms = case deadline_ms >= 4000 {
    True -> 2000
    False -> int.max(1, deadline_ms / 4)
  }
  setup_with_settings(
    pool_size,
    ledger_pool_size,
    fn(settings) {
      settings
      |> postgres.with_statement_deadline(deadline_ms)
      |> postgres.with_unique_lock_wait(lock_wait_ms)
    },
    True,
  )
}

fn setup_with_settings(
  pool_size: Int,
  ledger_pool_size: Int,
  adjust: fn(postgres.Settings) -> postgres.Settings,
  observe: Bool,
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
  let completion_observer = case observe {
    True -> {
      let ready = process.new_subject()
      let pid =
        process.spawn_unlinked(fn() {
          // Only the receiving process may create this subject. The parent
          // receives the handle through its own ready subject before returning.
          let stop = process.new_subject()
          process.send(ready, stop)
          observe_completions(drain, schema_of(database), stop, 0)
        })
      let assert Ok(stop) = process.receive(ready, within: 5000)
      Some(CompletionObserver(pid:, stop:))
    }
    False -> None
  }
  Harness(database:, ledger:, drain:, completion_observer:)
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

// 10 ms sampling gives an explicitly bounded-by-observation upper estimate,
// not a claim that a SQL timestamp is the instant COMMIT became durable.
fn observe_completions(
  drain: pog.Connection,
  schema: String,
  stop: process.Subject(process.Subject(Int)),
  polls: Int,
) -> Nil {
  let sql =
    "INSERT INTO grind_bench.bench_durable_completions (job_id) SELECT a.job_id FROM \""
    <> schema
    <> "\".grind_job_acknowledgements a JOIN grind_bench.bench_submissions s ON s.job_id = a.job_id WHERE a.committed_state = 'succeeded' ON CONFLICT DO NOTHING"
  case pog.execute(pog.query(sql), drain) {
    Error(error) -> {
      let _ = runtime.bump(runtime.completion_observer_error_counter)
      io.println(
        "completion observer failed; run evidence is invalid: "
        <> string.inspect(error),
      )
    }
    Ok(_) ->
      case process.receive(stop, within: 10) {
        Ok(acknowledged) -> process.send(acknowledged, polls + 1)
        Error(_) -> observe_completions(drain, schema, stop, polls + 1)
      }
  }
}
