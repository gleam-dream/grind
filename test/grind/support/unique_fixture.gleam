import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import grind/postgres
import grind/support/concurrency.{ReleaseAttempt}
import grind/support/env.{unique_test_run_id}
import grind/support/queue_signals.{type LeaseSignal, FirstAttemptStarted}
import grind/support/worker_failure.{type LookupFailure, AccountMissing}
import grind/worker
import pog

// -- Uniqueness (grind/unique, submit_unique/reconcile_unique) --------------
//
// Shared helpers for the uniqueness and admission tests. `unique_test_suffix` gives each
// test run a fresh, per-process-unique numeric string; every fixed worker
// id, queue name, and submission id these tests use includes it, so
// re-running this suite against a persistent development database (not the
// gate's disposable per-run cluster) never collides with rows or receipts a
// previous run left behind.

pub fn unique_test_suffix() -> String {
  int.to_string(unique_test_run_id())
}

/// Starts a pool, migrates, hands the database and a raw connection to
/// `run`, and closes the pool afterwards — the setup many uniqueness tests need except `run_submit_unique_pre_storage_rejection_test`, which
/// deliberately closes its pool before migrating. `_label` is unused (the
/// pool's own name is created inside `postgres.start` and never exposed
/// back to the caller) — kept as a parameter purely so each call site
/// still reads as "which scenario this pool is for", not renumbered.
pub fn with_unique_database(
  database_url: String,
  _label: String,
  run: fn(postgres.Database, pog.Connection) -> Nil,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  run(database, postgres.connection(database))
}

/// The multi-pool counterpart to `with_unique_database` above: starts one
/// separate pool (a separate physical connection) per entry in `labels`
/// (the entries' own text is unused, purely a per-call-site count-and-name
/// the way `with_unique_database`'s own `_label` is), migrates via the
/// first, defers closing every one, and hands the whole list of
/// `#(Database, Connection)` pairs to `run` — for concurrent-admission
/// tests that need several independent connections to the same
/// database rather than one. Paired with its own connection (rather than
/// returning `Database` alone) because `postgres.connection` is `@internal`
/// — available to this test suite, but not part of the public API these
/// tests are meant to exercise through `run`'s own callback boundary.
pub fn with_unique_databases(
  database_url: String,
  labels: List(String),
  run: fn(List(#(postgres.Database, pog.Connection))) -> Nil,
) -> Nil {
  let entries =
    list.map(labels, fn(_label) {
      let assert Ok(validated) =
        postgres.settings(database_url) |> postgres.validate
      let assert Ok(database) = postgres.start(validated)
      #(database, postgres.connection(database))
    })
  use <- exception.defer(fn() {
    list.each(entries, fn(entry) { postgres.close(entry.0) })
  })
  let assert [#(first, _), ..] = entries
  let assert Ok(Nil) = postgres.migrate(first)
  run(entries)
}

/// Like `unique_test_worker`, but the handler blocks (reporting
/// `FirstAttemptStarted(release)` on `started` first) until explicitly
/// released, instead of returning immediately. Used by the reschedule/claim
/// race test in `grind/unique/reschedule_test`, which needs the claimed row to stay genuinely
/// `executing` for a controlled window — a worker that returns immediately
/// lets the coordinator's own subsequent acknowledgement race ahead to
/// `succeeded` before the concurrent reschedule submission's blocked row
/// lock is even granted, an environment-dependent race, not a deterministic
/// proof.
pub fn unique_test_blocking_worker(
  id: String,
  started: process.Subject(LeaseSignal),
) -> worker.Worker(Int, String, LookupFailure) {
  let assert Ok(input_codec) =
    worker.codec(id <> "-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      id <> "-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(worker_def) =
    worker.define(id, "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, FirstAttemptStarted(release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok(int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  worker_def
}
