import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/string
import gleeunit/should
import grind/internal/terminal
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/support/concurrency.{
  type LongHandlerSignal, LongHandlerStarted, ReleaseAttempt,
}
import grind/support/consumer.{manual_policy}
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/worker_failure.{
  AccountMissing, decode_lookup_failure, encode_lookup_failure,
}
import grind/worker
import pog

/// Whether a `job.State` is one of the six terminal states
/// `grind_jobs_finished_at_check` (`grind_v12`) requires `finished_at` to be
/// non-null for. The exhaustive `case` (no wildcard) is the point: adding a
/// new `job.State` variant fails this module to compile until it is placed
/// on one side or the other, rather than silently leaving
/// `terminal.states_sql()` behind.
fn is_terminal_job_state(state: job.State) -> Bool {
  case state {
    job.Succeeded
    | job.BusinessFailed
    | job.RuntimeFailed
    | job.ContractMismatch
    | job.Discarded
    | job.Cancelled -> True
    job.Queued
    | job.Scheduled
    | job.Retryable
    | job.Executing
    | job.Uncertain -> False
  }
}

/// Ties `terminal.states_sql()` to `job.state_to_stored`'s own terminal
/// variants directly, rather than trusting the two hand-maintained lists
/// (the SQL fragment's literal text, and `is_terminal_job_state`'s `case`
/// above) to stay in lockstep by inspection alone.
pub fn terminal_states_sql_matches_every_job_state_test() {
  let all_states = [
    job.Queued,
    job.Scheduled,
    job.Retryable,
    job.Executing,
    job.Succeeded,
    job.BusinessFailed,
    job.RuntimeFailed,
    job.ContractMismatch,
    job.Uncertain,
    job.Discarded,
    job.Cancelled,
  ]
  list.each(all_states, fn(state) {
    let quoted = "'" <> job.state_to_stored(state) <> "'"
    string.contains(terminal.states_sql(), quoted)
    |> should.equal(is_terminal_job_state(state))
  })
}

/// `grind_jobs_finished_at_check` (`grind_v12`) is PostgreSQL's own
/// enforcement of "terminal state iff `finished_at` is set" — this probes it
/// directly with raw SQL from both directions, independent of any Grind
/// write path, so a future write site that forgets to set `finished_at`
/// correctly fails loudly at the database layer rather than silently
/// drifting. `pog_ffi`'s `convert_error` reports a `pgsql_error` that
/// carries a `constraint` field (both a check and a unique violation do) as
/// `pog.ConstraintViolated(message, constraint, detail)` — SQLSTATE `23514`
/// (`check_violation`) is what PostgreSQL raises here, though `pog` itself
/// does not surface the raw code once it has already recognised the
/// constraint name.
pub fn postgres_finished_at_check_constraint_rejects_mismatch_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_finished_at_check_constraint_test(database_url)
  }
}

fn run_finished_at_check_constraint_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)

  // A non-terminal state (`queued`) with `finished_at` set.
  let assert Error(queued_error) =
    pog.query(
      "INSERT INTO grind_jobs (queue, worker_id, worker_version, input_version, input, output_version, state, available_at, finished_at) VALUES ('finished-at-check', 'finished-at-check.worker', 'v1', 'v1', '1'::jsonb, 'v1', 'queued', clock_timestamp(), clock_timestamp())",
    )
    |> pog.execute(on: connection)
  let assert pog.ConstraintViolated(constraint:, ..) = queued_error
  constraint |> should.equal("grind_jobs_finished_at_check")

  // A terminal state (`succeeded`) with `finished_at` left null.
  let assert Error(succeeded_error) =
    pog.query(
      "INSERT INTO grind_jobs (queue, worker_id, worker_version, input_version, input, output_version, state, available_at) VALUES ('finished-at-check', 'finished-at-check.worker', 'v1', 'v1', '1'::jsonb, 'v1', 'succeeded', clock_timestamp())",
    )
    |> pog.execute(on: connection)
  let assert pog.ConstraintViolated(constraint: succeeded_constraint, ..) =
    succeeded_error
  succeeded_constraint |> should.equal("grind_jobs_finished_at_check")

  mark_database_test_executed("finished-at-check-constraint-rejects-mismatch")
}

fn finished_at_is_set(connection: pog.Connection, id: Int) -> Bool {
  let assert Ok(returned) =
    pog.query("SELECT finished_at IS NOT NULL FROM grind_jobs WHERE id = $1")
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use finished <- decode.field(0, decode.bool)
      decode.success(finished)
    })
    |> pog.execute(on: connection)
  let assert [finished] = returned.rows
  finished
}

/// Drives one already-claimed, gated job (blocked on its own `started`
/// signal) through a concurrent `postgres.cancel` while it is genuinely
/// `executing`, then releases it and waits for the ack that follows —
/// exactly the "cancel a running attempt" sequence
/// `run_cancel_running_ack_test` uses, factored out so each of the three
/// cancel-overridable acknowledge branches can reuse it without repeating
/// the same process-spawn/handshake boilerplate.
fn drive_cancel_while_executing(
  database: postgres.Database,
  consumer: queue.Consumer,
  handle: job.JobHandle(input, output, error),
  started: process.Subject(LongHandlerSignal),
) -> Nil {
  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(reply, queue.process_one(consumer))
    })
  let assert Ok(LongHandlerStarted(release)) =
    process.receive(started, within: 5000)
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  postgres.cancel(database, handle)
  |> should.equal(Ok(postgres.CancellationRequested))
  process.send(release, ReleaseAttempt)
  process.receive(reply, within: 5000) |> should.equal(Ok(Ok(True)))
  Nil
}

/// Table-driven proof that `finished_at` ends up non-null after every
/// terminal write site `grind_v12` added it to — `attempt
/// .acknowledge_transaction`'s eight per-outcome branches (`succeeded`,
/// `business_failed`, `discarded`, `cancelled`, and `runtime_failed` are
/// unconditional, since a concurrent cancellation only ever overrides one
/// terminal outcome with another; `retryable`, `scheduled` (from a worker
/// snooze), and `uncertain` are non-terminal unless that same override
/// fires), `attempt.mark_contract_mismatch`, `sql.cancel_before_run`, and
/// `postgres.write_resolution`'s own `UPDATE` for both of its terminal
/// target states — and stays null after every non-terminal one, including
/// `AuthorizeReplay`, whose target state (`queued`) is the one non-terminal
/// outcome `resolve_uncertain` itself can produce, and the three
/// cancel-overridden branches' own ordinary (non-cancelled) path. Every job
/// below lives on its own queue and is driven through exactly the one path
/// under test, so no scenario's own claim can race another's.
pub fn postgres_finished_at_written_at_every_terminal_path_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_finished_at_paths_test(database_url)
  }
}

fn run_finished_at_paths_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)
  let run_queue = "finished-at-run"

  let assert Ok(plain_input) =
    worker.codec(
      "finished-at-plain-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(plain_output) =
    worker.codec(
      "finished-at-plain-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(succeeded_worker) =
    worker.define(
      "finished-at.succeeded",
      "v1",
      plain_input,
      plain_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(error_codec) =
    worker.codec(
      "finished-at-error-v1",
      worker.infallible(encode_lookup_failure),
      decode_lookup_failure(),
    )
  let assert Ok(business_failed_worker) =
    worker.define_with_error_codec(
      "finished-at.business-failed",
      "v1",
      plain_input,
      plain_output,
      error_codec,
      fn(account_id) { Error(AccountMissing(account_id)) },
    )
  let assert Ok(business_failed_worker) =
    worker.with_max_attempts(business_failed_worker, 1)
  let assert Ok(discard_base) =
    worker.define(
      "finished-at.discard-base",
      "v1",
      plain_input,
      plain_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let discard_worker =
    worker.with_queue_handler(discard_base, fn(_) {
      worker.WorkerDiscarded("finished-at probe")
    })
  let assert Ok(cancel_base) =
    worker.define(
      "finished-at.cancel-base",
      "v1",
      plain_input,
      plain_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let cancel_worker =
    worker.with_queue_handler(cancel_base, fn(_) {
      worker.WorkerCancelled("finished-at probe")
    })
  let assert Ok(snooze_delay) = worker.retry_delay(1000)
  let assert Ok(snooze_base) =
    worker.define(
      "finished-at.snooze-base",
      "v1",
      plain_input,
      plain_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let snooze_worker =
    worker.with_queue_handler(snooze_base, fn(_) {
      worker.WorkerSnoozed(snooze_delay, "finished-at probe")
    })
  let assert Ok(retry_worker) =
    worker.define("finished-at.retry", "v1", plain_input, plain_output, fn(_) {
      Error(AccountMissing(9))
    })

  // Gated variants of the three branches that are only non-terminal in the
  // *absence* of a concurrent cancellation (`retryable`, `scheduled`,
  // `uncertain`): each blocks mid-execution on its own `started`/`release`
  // handshake so a test can request cancellation while the row is genuinely
  // `executing`, then let the worker's own proposal run into the
  // already-cancelled row — proving the bare `CASE WHEN cancel_requested_at
  // IS NOT NULL THEN clock_timestamp() END` in each of those three branches,
  // not just their ordinary (uncancelled) path already covered above.
  let gated_retry_started = process.new_subject()
  let assert Ok(gated_retry_worker) =
    worker.define(
      "finished-at.retry-gated",
      "v1",
      plain_input,
      plain_output,
      fn(value) {
        let release = process.new_subject()
        process.send(gated_retry_started, LongHandlerStarted(release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Error(AccountMissing(value))
          Error(Nil) -> Ok(int.to_string(value))
        }
      },
    )
  let gated_snooze_started = process.new_subject()
  let assert Ok(gated_snooze_base) =
    worker.define(
      "finished-at.snooze-gated-base",
      "v1",
      plain_input,
      plain_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let gated_snooze_worker =
    worker.with_queue_handler(gated_snooze_base, fn(_) {
      let release = process.new_subject()
      process.send(gated_snooze_started, LongHandlerStarted(release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) ->
          worker.WorkerSnoozed(snooze_delay, "finished-at probe")
        Error(Nil) -> worker.WorkerDiscarded("timed out")
      }
    })
  let gated_uncertain_started = process.new_subject()
  let assert Ok(gated_uncertain_base) =
    worker.define(
      "finished-at.uncertain-gated-base",
      "v1",
      plain_input,
      plain_output,
      fn(value) { Ok(int.to_string(value)) },
    )
  let gated_uncertain_worker =
    worker.with_queue_handler(gated_uncertain_base, fn(_) {
      let release = process.new_subject()
      process.send(gated_uncertain_started, LongHandlerStarted(release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> worker.WorkerUncertain("finished-at probe")
        Error(Nil) -> worker.WorkerDiscarded("timed out")
      }
    })

  let assert Ok(workers) = registry.new(run_queue)
  let assert Ok(workers) = registry.register(workers, succeeded_worker)
  let assert Ok(workers) = registry.register(workers, business_failed_worker)
  let assert Ok(workers) = registry.register(workers, discard_worker)
  let assert Ok(workers) = registry.register(workers, cancel_worker)
  let assert Ok(workers) = registry.register(workers, snooze_worker)
  let assert Ok(workers) = registry.register(workers, retry_worker)
  let assert Ok(workers) = registry.register(workers, gated_retry_worker)
  let assert Ok(workers) = registry.register(workers, gated_snooze_worker)
  let assert Ok(workers) = registry.register(workers, gated_uncertain_worker)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  // -- Terminal write sites: finished_at ends up non-null -------------------

  // acknowledge_transaction, "succeeded" branch.
  let assert Ok(succeeded_handle) =
    postgres.submit(database, run_queue, succeeded_worker, 1)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, succeeded_handle) |> should.equal(Ok(job.Succeeded))
  finished_at_is_set(connection, job.id_value(succeeded_handle))
  |> should.equal(True)

  // acknowledge_transaction, "business_failed" branch (budget exhausted).
  let assert Ok(business_failed_handle) =
    postgres.submit(database, run_queue, business_failed_worker, 2)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, business_failed_handle)
  |> should.equal(Ok(job.BusinessFailed))
  finished_at_is_set(connection, job.id_value(business_failed_handle))
  |> should.equal(True)

  // acknowledge_transaction, "discarded" branch.
  let assert Ok(discarded_handle) =
    postgres.submit(database, run_queue, discard_worker, 3)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, discarded_handle) |> should.equal(Ok(job.Discarded))
  finished_at_is_set(connection, job.id_value(discarded_handle))
  |> should.equal(True)

  // acknowledge_transaction, "cancelled" branch (worker-initiated).
  let assert Ok(cancelled_handle) =
    postgres.submit(database, run_queue, cancel_worker, 4)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, cancelled_handle) |> should.equal(Ok(job.Cancelled))
  finished_at_is_set(connection, job.id_value(cancelled_handle))
  |> should.equal(True)

  // acknowledge_transaction, "retryable" branch overridden by a concurrent
  // cancellation: the bare `CASE WHEN cancel_requested_at IS NOT NULL THEN
  // clock_timestamp() END` fires because the row committed `cancelled`, not
  // its own ordinary (non-cancelled) `NULL` path already proven below.
  let assert Ok(retry_cancelled_handle) =
    postgres.submit(database, run_queue, gated_retry_worker, 14)
  drive_cancel_while_executing(
    database,
    consumer,
    retry_cancelled_handle,
    gated_retry_started,
  )
  postgres.state(database, retry_cancelled_handle)
  |> should.equal(Ok(job.Cancelled))
  finished_at_is_set(connection, job.id_value(retry_cancelled_handle))
  |> should.equal(True)

  // acknowledge_transaction, "snoozed" branch overridden by a concurrent
  // cancellation.
  let assert Ok(snooze_cancelled_handle) =
    postgres.submit(database, run_queue, gated_snooze_worker, 15)
  drive_cancel_while_executing(
    database,
    consumer,
    snooze_cancelled_handle,
    gated_snooze_started,
  )
  postgres.state(database, snooze_cancelled_handle)
  |> should.equal(Ok(job.Cancelled))
  finished_at_is_set(connection, job.id_value(snooze_cancelled_handle))
  |> should.equal(True)

  // acknowledge_transaction, "uncertain" branch overridden by a concurrent
  // cancellation.
  let assert Ok(uncertain_cancelled_handle) =
    postgres.submit(database, run_queue, gated_uncertain_worker, 16)
  drive_cancel_while_executing(
    database,
    consumer,
    uncertain_cancelled_handle,
    gated_uncertain_started,
  )
  postgres.state(database, uncertain_cancelled_handle)
  |> should.equal(Ok(job.Cancelled))
  finished_at_is_set(connection, job.id_value(uncertain_cancelled_handle))
  |> should.equal(True)

  // acknowledge_transaction, "runtime_failed" branch: input decode fails at
  // claim time even though the input_version still matches the registered
  // codec exactly, since the stored JSON itself is not a valid encoded
  // `Int`.
  let assert Ok(runtime_failed_handle) =
    postgres.submit(database, run_queue, succeeded_worker, 5)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET input = '\"not-an-int\"'::jsonb WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(runtime_failed_handle)))
    |> pog.execute(on: connection)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, runtime_failed_handle)
  |> should.equal(Ok(job.RuntimeFailed))
  finished_at_is_set(connection, job.id_value(runtime_failed_handle))
  |> should.equal(True)

  // attempt.mark_contract_mismatch: the claim itself never runs the worker.
  let assert Ok(contract_mismatch_handle) =
    postgres.submit(database, run_queue, succeeded_worker, 6)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET output_version = 'finished-at-drifted' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(contract_mismatch_handle)))
    |> pog.execute(on: connection)
  let assert Error(_) = queue.process_one(consumer)
  postgres.state(database, contract_mismatch_handle)
  |> should.equal(Ok(job.ContractMismatch))
  finished_at_is_set(connection, job.id_value(contract_mismatch_handle))
  |> should.equal(True)

  // sql.cancel_before_run: never claimed at all.
  let assert Ok(cancel_before_run_handle) =
    postgres.submit(database, "finished-at-static", succeeded_worker, 7)
  postgres.cancel(database, cancel_before_run_handle)
  |> should.equal(Ok(postgres.CancelledBeforeRun))
  finished_at_is_set(connection, job.id_value(cancel_before_run_handle))
  |> should.equal(True)

  // postgres.write_resolution, confirm_success target.
  let assert Ok(resolve_success_handle) =
    postgres.submit(database, "finished-at-resolve", succeeded_worker, 8)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 501, attempt_epoch = 1, attempt_owner = 'lost-owner', lease_expires_at = clock_timestamp(), uncertain_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(resolve_success_handle)))
    |> pog.execute(on: connection)
  postgres.resolve_uncertain(
    database,
    resolve_success_handle,
    postgres.ResolutionRequest(
      "finished-at-resolve-success",
      "on-call",
      "finished_at probe",
      postgres.ConfirmSuccess("approved"),
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Succeeded)))
  finished_at_is_set(connection, job.id_value(resolve_success_handle))
  |> should.equal(True)

  // postgres.write_resolution, confirm_business_failure target.
  let assert Ok(resolve_failure_handle) =
    postgres.submit(database, "finished-at-resolve", business_failed_worker, 9)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 502, attempt_epoch = 1, attempt_owner = 'lost-owner', lease_expires_at = clock_timestamp(), uncertain_at = clock_timestamp(), error_version = 'finished-at-error-v1' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(resolve_failure_handle)))
    |> pog.execute(on: connection)
  postgres.resolve_uncertain(
    database,
    resolve_failure_handle,
    postgres.ResolutionRequest(
      "finished-at-resolve-failure",
      "on-call",
      "finished_at probe",
      postgres.ConfirmBusinessFailure(AccountMissing(9)),
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.BusinessFailed)))
  finished_at_is_set(connection, job.id_value(resolve_failure_handle))
  |> should.equal(True)

  // -- Non-terminal write sites: finished_at stays null ----------------------

  // A freshly submitted job: never touched.
  let assert Ok(queued_handle) =
    postgres.submit(database, "finished-at-static", succeeded_worker, 10)
  postgres.state(database, queued_handle) |> should.equal(Ok(job.Queued))
  finished_at_is_set(connection, job.id_value(queued_handle))
  |> should.equal(False)

  // acknowledge_transaction, "snoozed" branch (no concurrent cancellation).
  let assert Ok(scheduled_handle) =
    postgres.submit(database, run_queue, snooze_worker, 11)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, scheduled_handle) |> should.equal(Ok(job.Scheduled))
  finished_at_is_set(connection, job.id_value(scheduled_handle))
  |> should.equal(False)

  // acknowledge_transaction, "retryable" branch (no concurrent cancellation).
  let assert Ok(retryable_handle) =
    postgres.submit(database, run_queue, retry_worker, 12)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, retryable_handle) |> should.equal(Ok(job.Retryable))
  finished_at_is_set(connection, job.id_value(retryable_handle))
  |> should.equal(False)

  // A row forced into `uncertain`, before any resolution.
  let assert Ok(uncertain_handle) =
    postgres.submit(database, "finished-at-resolve", succeeded_worker, 13)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 503, attempt_epoch = 1, attempt_owner = 'lost-owner', lease_expires_at = clock_timestamp(), uncertain_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(uncertain_handle)))
    |> pog.execute(on: connection)
  postgres.state(database, uncertain_handle) |> should.equal(Ok(job.Uncertain))
  finished_at_is_set(connection, job.id_value(uncertain_handle))
  |> should.equal(False)

  // postgres.write_resolution, authorize_replay target (`queued`, the one
  // non-terminal outcome a resolution can itself produce).
  postgres.resolve_uncertain(
    database,
    uncertain_handle,
    postgres.ResolutionRequest(
      "finished-at-resolve-replay",
      "on-call",
      "finished_at probe",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  finished_at_is_set(connection, job.id_value(uncertain_handle))
  |> should.equal(False)

  mark_database_test_executed("finished-at-written-at-every-terminal-path")
}
