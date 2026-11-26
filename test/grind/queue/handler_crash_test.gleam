import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{Some}
import gleam/string
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/settlement
import grind/internal/worker
import grind/support/consumer.{manual_policy}
import grind/support/env.{
  mark_database_test_executed, queue_database_url, unique_test_run_id,
}

/// A handler that crashes abandons its attempt at once: under the default
/// `HoldUncertain` the job is held `Uncertain` without waiting for its lease
/// to expire, with a description of the crash, and a declared settlement
/// sees the cause `HandlerCrashed`.
pub fn postgres_handler_crash_holds_the_job_uncertain_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_handler_crash_test(database_url)
  }
}

fn run_handler_crash_test(database_url: String) -> Nil {
  let queue_name = "handler-crash-" <> int.to_string(unique_test_run_id())
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "handler-crash-input-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "handler-crash-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let causes = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "handler.crash",
      "v1",
      input_codec,
      output_codec,
      fn(secret: String) -> Result(String, Nil) {
        // The failed pattern carries the job's input. The stored reason
        // names the failure and where it happened, never that value.
        let assert Ok(reply) = refuse(secret)
        Ok(reply)
      },
    )
  let definition =
    worker.Worker(
      ..definition,
      settlement: Some(fn(_transaction, abandoned: worker.Abandoned(String)) {
        process.send(causes, abandoned.cause)
        Ok(worker.SettleLeaveUncertain("inspected"))
      }),
    )
  let assert Ok(workers) = registry.new(queue_name)
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, queue_name, definition, "card-4111-secret")
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  // No lease expiry is needed: the crash is acknowledged as an abandonment.
  let assert Ok(_) = queue.process_one(consumer)
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  let assert Some(recorded) =
    description_of(database, queue_name, job.Uncertain)
  string.starts_with(
    recorded,
    "handler exited: let_assert in grind/queue/handler_crash_test.run_handler_crash_test, line ",
  )
  |> should.be_true()
  string.ends_with(
    recorded,
    ": Pattern match failed, no pattern matched the value.",
  )
  |> should.be_true()
  string.contains(recorded, "card-4111-secret") |> should.be_false()

  let assert Ok(settlement.Declined(_)) =
    settlement.settle_next(database, workers, 30_000)
  process.receive(causes, within: 0)
  |> should.equal(Ok(worker.HandlerCrashed))
  mark_database_test_executed("handler-crash-held-uncertain")
}

fn description_of(
  database: postgres.Database,
  queue_name: String,
  state: job.State,
) -> option.Option(String) {
  let assert Ok(rows) =
    postgres.list_jobs(database, Some(queue_name), Some(state), 0, 10)
  let assert Ok(row) = list.first(rows)
  row.failure_description
}

fn refuse(secret: String) -> Result(String, String) {
  Error(secret)
}
