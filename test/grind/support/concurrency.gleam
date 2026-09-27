import gleam/erlang/process
import gleam/int
import gleam/result
import grind/support/env.{unique_test_run_id}
import pog

pub type LeaseCommand {
  ReleaseAttempt
}

pub type LongHandlerSignal {
  LongHandlerStarted(process.Subject(LeaseCommand))
}

pub type ClaimGateSignal {
  ClaimGateAcquired(process.Subject(LeaseCommand))
  ClaimGateReleased(Bool)
}

/// Spawns a background process that runs `submit` (a zero-argument closure
/// so callers can partially apply `submit_keep_existing`/`submit_reschedule`/
/// `attempt.acknowledge`/etc. with whichever database/queue/submission
/// it needs) and sends the result to `result` — the small boilerplate concurrent tests in this suite otherwise repeat once per concurrent caller.
/// Generic over the result type so both the uniqueness admission tests and
/// the acknowledgement contention test share it.
pub fn spawn_submit(result: process.Subject(a), submit: fn() -> a) -> Nil {
  let _ = process.spawn_unlinked(fn() { process.send(result, submit()) })
  Nil
}

// -- Uniqueness (increments 8-9: forced concurrent overlap, contention) ----
//
// Shared helpers for the barrier-forced-overlap and lock-contention tests in this suite. These reuse `ClaimGateSignal`/`LeaseCommand` (already declared for
// `run_overlapping_claim_test`'s inline hold-then-release-on-cue shape) and
// generalize that same shape into `spawn_lock_holder`, rather than
// re-declaring it per test.

/// Spawns a background process that opens its own transaction on
/// `connection`, executes `acquire_query` (expected to run and hold some
/// PostgreSQL lock for the rest of that transaction — an advisory lock or a
/// row lock), signals `ClaimGateAcquired` once `acquire_query` has returned,
/// then waits (bounded) for a release before committing (which releases
/// whatever lock it holds). `run_overlapping_claim_test` uses this exact
/// shape inline for its own claim-`UPDATE` barrier; factored out here so
/// the uniqueness barrier/contention tests share one
/// implementation instead of re-declaring it.
pub fn spawn_lock_holder(
  connection: pog.Connection,
  acquire_query: pog.Query(a),
) -> #(process.Subject(ClaimGateSignal), process.Subject(ClaimGateSignal)) {
  let lock_ready = process.new_subject()
  let lock_finished = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      // `release_lock` must be created by this spawned process, not the
      // caller: `process.receive` only allows the subject's own creator to
      // receive on it, exactly like `run_overlapping_claim_test`'s inline
      // version in `grind/queue/claims_test` declares it inside the spawned
      // closure and hands it to the caller via `ClaimGateAcquired`.
      let release_lock = process.new_subject()
      let transaction_result =
        pog.transaction(connection, fn(transaction_connection) {
          case pog.execute(acquire_query, on: transaction_connection) {
            Error(_) -> Error(Nil)
            Ok(_) -> {
              process.send(lock_ready, ClaimGateAcquired(release_lock))
              case process.receive(release_lock, within: 10_000) {
                Ok(ReleaseAttempt) -> Ok(Nil)
                Error(Nil) -> Error(Nil)
              }
            }
          }
        })
      process.send(
        lock_finished,
        ClaimGateReleased(result.is_ok(transaction_result)),
      )
    })
  #(lock_ready, lock_finished)
}

/// Installs a `BEFORE INSERT` trigger on `grind_jobs`, scoped to
/// `worker_id`, that blocks any insert for that worker behind
/// `pg_advisory_xact_lock(lock_key)` — the same held-then-released-on-cue
/// barrier shape `run_overlapping_claim_test`'s `grind_test_claim_overlap`
/// trigger uses for a claim `UPDATE`, generalized here to an `INSERT` and
/// parameterized by worker id and lock key so the forced-overlap tests
/// in `grind/unique/concurrent_admission_test` (including the mixed-scope variant, which needs its own separate
/// lock key) share one trigger implementation. Returns a cleanup thunk for
/// `exception.defer`.
pub fn install_unique_insert_barrier(
  connection: pog.Connection,
  name: String,
  worker_id: String,
  lock_key: Int,
) -> fn() -> Nil {
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.worker_id = '"
      <> worker_id
      <> "' THEN PERFORM pg_advisory_xact_lock("
      <> int.to_string(lock_key)
      <> "); END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER "
      <> name
      <> " BEFORE INSERT ON grind_jobs FOR EACH ROW EXECUTE FUNCTION "
      <> name
      <> "()",
    )
    |> pog.execute(on: connection)
  fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS " <> name <> " ON grind_jobs")
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> name <> "()")
      |> pog.execute(on: connection)
    Nil
  }
}

/// A deterministic advisory-lock key, distinct per test run (derived from
/// `unique_test_run_id()`) and offset well clear of every other literal
/// advisory-lock key this suite hard-codes elsewhere (74126/31 in
/// `run_overlapping_claim_test`), for the forced-overlap barrier triggers
/// in the uniqueness tests. `salt` lets one test declare more than one distinct key (the
/// mixed-scope variant runs alongside the main overlap test, in the same
/// `gleam test` process, and must not share a lock key with it).
pub fn unique_test_lock_key(salt: Int) -> Int {
  let assert Ok(reduced) = int.modulo(unique_test_run_id(), by: 100_000_000)
  900_000_000 + reduced + salt
}
