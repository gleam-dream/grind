import exception
import gleam/dynamic/decode
import gleam/int
import gleam/json
import gleeunit/should
import grind/job
import grind/postgres
import grind/support/env.{database_url, mark_database_test_executed}
import grind/worker
import pog

pub fn postgres_admission_round_trips_typed_arguments_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_postgres_admission_test(database_url)
  }
}

fn run_postgres_admission_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("integer-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "text-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(counter) =
    worker.define(
      "counter.increment",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value + 1)) },
    )
  let assert Ok(handle) = postgres.submit(database, "default", counter, 41)

  postgres.arguments(database, handle)
  |> should.equal(Ok(41))

  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET worker_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("v2"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)
  postgres.arguments(database, handle)
  |> should.equal(
    Error(postgres.WorkerContractMismatch(
      expected_id: "counter.increment",
      expected_version: "v1",
      actual_id: "counter.increment",
      actual_version: "v2",
    )),
  )
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET worker_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("v1"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)

  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET input_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("integer-input-v2"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)
  postgres.arguments(database, handle)
  |> should.equal(
    Error(
      postgres.CodecFailed(worker.CodecVersionMismatch(
        expected: "integer-input-v1",
        got: "integer-input-v2",
      )),
    ),
  )
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET input_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("integer-input-v1"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)

  postgres.state(database, handle)
  |> should.equal(Ok(job.Queued))
  let _ = postgres.close(database)
  let assert Ok(reopened) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened) })
  postgres.state(reopened, handle)
  |> should.equal(Ok(job.Queued))
  mark_database_test_executed("admission-read-passed")
}
