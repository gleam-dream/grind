import exception
import gleam/dynamic/decode
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
import grind/internal/worker
import grind/support/consumer.{manual_policy}
import grind/support/env.{
  mark_database_test_executed, queue_database_url, unique_test_run_id,
}
import pog

/// A handler that crashes leaves its reason on the job. The job stays
/// `Executing` until its lease expires, because a crash does not prove that
/// the handler had no effect, and the quarantine scan that then holds the job
/// `Uncertain` keeps the reason in front of its own text.
pub fn postgres_handler_crash_reason_is_kept_on_the_job_test() {
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
  let assert Ok(workers) = registry.new(queue_name)
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, queue_name, definition, "card-4111-secret")
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Error(queue.QueueWorkerExited))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  let assert Some(recorded) =
    description_of(database, queue_name, job.Executing)
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

  // The lease expires and the next claim's quarantine scan holds the job.
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1 AND state = 'executing'",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: postgres.connection(database))
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  let assert Some(held) = description_of(database, queue_name, job.Uncertain)
  held
  |> should.equal(
    recorded <> "; expired attempt requires outcome reconciliation",
  )
  string.contains(held, "card-4111-secret") |> should.be_false()
  mark_database_test_executed("handler-crash-reason-kept")
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
