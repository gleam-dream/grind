//// Holding an abandoned job uncertain writes its settlement mark; a
//// handler's own `Uncertain` writes none (docs/design/design.typ,
//// "Declared settlement of abandoned jobs", behavior "Holding an abandoned
//// job uncertain writes the mark").

import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/time/duration
import gleeunit/should
import grind/internal/attempt
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/support/consumer.{manual_policy}
import grind/support/env.{
  mark_database_test_executed, queue_database_url, unique_test_run_id,
}
import grind/worker
import pog

pub fn postgres_lease_expiry_hold_writes_the_mark_test() {
  use database <- with_database()
  let name = queue_name("mark-hold")
  let definition = int_worker(name, worker.HoldUncertain)
  let assert Ok(handle) = postgres.submit(database, name, definition, 1)
  let attempt_id = claim_and_expire(database, name, definition)

  let assert Ok(_) = postgres.quarantine_expired(database, limit: 100)

  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  due_mark(database, handle) |> should.equal(Some(attempt_id))
  mark_database_test_executed("settlement-mark-lease-expiry")
}

pub fn postgres_exhausted_replays_write_the_mark_of_the_last_attempt_test() {
  use database <- with_database()
  let name = queue_name("mark-replay")
  let definition = int_worker(name, worker.ReplayAfterLeaseExpiry(1))
  let assert Ok(handle) = postgres.submit(database, name, definition, 1)
  let _first = claim_and_expire(database, name, definition)
  let assert Ok(_) = postgres.quarantine_expired(database, limit: 100)
  postgres.state(database, handle) |> should.equal(Ok(job.Queued))
  due_mark(database, handle) |> should.equal(None)

  let second = claim_and_expire(database, name, definition)
  let assert Ok(_) = postgres.quarantine_expired(database, limit: 100)

  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  due_mark(database, handle) |> should.equal(Some(second))
  mark_database_test_executed("settlement-mark-exhausted-replays")
}

pub fn postgres_handler_uncertain_writes_no_mark_test() {
  use database <- with_database()
  let name = queue_name("mark-handler")
  let definition =
    worker.responding(
      "settlement.mark.handler",
      input: int_codec(),
      output: int_codec(),
      handle: fn(_context, _input) { worker.Uncertain("charge sent, no reply") },
    )
    |> worker.with_queue(name)
  let assert Ok(handle) = postgres.submit(database, name, definition, 1)
  let consumer = start_consumer(database, name, definition)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))

  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  due_mark(database, handle) |> should.equal(None)
  mark_database_test_executed("settlement-mark-handler-uncertain")
}

pub fn postgres_timeout_under_hold_uncertain_writes_the_mark_test() {
  use database <- with_database()
  let name = queue_name("mark-timeout")
  let definition =
    worker.new(
      "settlement.mark.timeout",
      input: int_codec(),
      output: int_codec(),
      perform: fn(value) {
        process.sleep(60_000)
        Ok(value)
      },
    )
    |> worker.with_queue(name)
    |> worker.with_timeout(worker.After(duration.milliseconds(50)))
  let assert Ok(handle) = postgres.submit(database, name, definition, 1)
  let consumer = start_consumer(database, name, definition)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))

  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  due_mark(database, handle)
  |> should.equal(Some(current_attempt(database, handle)))
  mark_database_test_executed("settlement-mark-timeout")
}

// -- Support -----------------------------------------------------------------

fn with_database(run: fn(postgres.Database) -> Nil) -> Nil {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(validated) = postgres.settings(url) |> postgres.validate
      let assert Ok(database) = postgres.start(validated)
      use <- exception.defer(fn() { postgres.close(database) })
      let assert Ok(Nil) = postgres.migrate(database)
      run(database)
    }
  }
}

fn queue_name(prefix: String) -> String {
  prefix <> "-" <> int.to_string(unique_test_run_id())
}

fn int_codec() -> worker.Codec(Int) {
  worker.codec(worker.infallible(json.int), decode.int)
}

fn int_worker(
  name: String,
  abandonment: worker.Abandonment,
) -> worker.Worker(Int, Int, Nil) {
  worker.new(
    "settlement.mark",
    input: int_codec(),
    output: int_codec(),
    perform: Ok,
  )
  |> worker.with_queue(name)
  |> worker.with_abandonment(abandonment)
}

fn start_consumer(
  database: postgres.Database,
  name: String,
  definition: worker.Worker(Int, Int, e),
) -> queue.Consumer {
  let assert Ok(workers) = registry.new(name)
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  consumer
}

/// Claims the job without running its handler, as a node that then died
/// would, and moves the lease to the database clock so it has expired.
fn claim_and_expire(
  database: postgres.Database,
  name: String,
  definition: worker.Worker(Int, Int, e),
) -> Int {
  let assert Ok(workers) = registry.new(name)
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(Some(claimed)) =
    attempt.claim_one(database, name, workers, "gone-node", 30_000)
  let #(id, attempt_id, _epoch) = attempt.claim_identity(claimed)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: postgres.connection(database))
  attempt_id
}

/// The attempt the job's mark names, when the mark is due now.
fn due_mark(
  database: postgres.Database,
  handle: job.JobHandle(a, b, c),
) -> Option(Int) {
  let assert Ok(returned) =
    pog.query(
      "SELECT settle_attempt_id FROM grind_jobs WHERE id = $1 AND settle_at <= clock_timestamp()",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning(decode.field(0, decode.int, decode.success))
    |> pog.execute(on: postgres.connection(database))
  case returned.rows {
    [attempt] -> Some(attempt)
    _ -> None
  }
}

fn current_attempt(
  database: postgres.Database,
  handle: job.JobHandle(a, b, c),
) -> Int {
  let assert Ok(returned) =
    pog.query("SELECT attempt_id FROM grind_jobs WHERE id = $1")
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning(decode.field(0, decode.int, decode.success))
    |> pog.execute(on: postgres.connection(database))
  let assert [attempt] = returned.rows
  attempt
}
