//// Dedicated adapter for oracle/scenarios.json. Actions use public Grind APIs.
//// SQL only observes committed state or establishes the catalog's retention
//// ages, avoiding minute-long wall-clock sleeps. This module is explicitly run
//// by the database gate; missing configuration fails rather than skipping.

import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleam/list
import gleam/string
import grind/internal/consumer as queue
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/submission
import grind/internal/unique
import grind/internal/worker
import grind/support/consumer
import pog
import simplifile

type Kind {
  Success
  Failure
  Snooze
  Cancel
  Future
  Past
  Discard
  Unique
  Prune
}

type Scenario {
  Scenario(
    id: String,
    kind: Kind,
    value: Int,
    max_attempts: Int,
    delay_ms: Int,
    runs: Int,
    scheduled_age_ms: Int,
    finished_age_ms: Int,
    retention_ms: Int,
    verify_delay: Bool,
  )
}

@external(erlang, "grind_oracle_env", "fetch")
fn environment(name: String) -> Result(String, Nil)

pub fn main() {
  let assert Ok(catalog_path) = environment("GRIND_ORACLE_CATALOG")
  let assert Ok(results_path) = environment("GRIND_ORACLE_RESULTS")
  let assert Ok(database_url) = environment("GRIND_ORACLE_DATABASE_URL")
  let assert Ok(run_id) = environment("GRIND_ORACLE_RUN_ID")
  let assert Ok(catalog) = simplifile.read(catalog_path)
  let assert Ok(scenarios) = json.parse(catalog, catalog_decoder())
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  // A dedicated database is required: stale results must never masquerade as
  // this run, and scenario rows must not collide with the integration suite.
  let assert Ok(existing) =
    pog.query("SELECT count(*) FROM grind_jobs")
    |> pog.returning(integer_column())
    |> pog.execute(on: postgres.connection(database))
  let assert [0] = existing.rows
  let lines =
    list.map(scenarios, fn(scenario) {
      // Isolate initial conditions. In particular, a due-scheduled success
      // from an earlier case must not become an extra Oban prune candidate.
      let assert Ok(_) =
        pog.query("DELETE FROM grind_jobs")
        |> pog.execute(on: postgres.connection(database))
      json.object([
        #("catalog_version", json.int(1)),
        #("engine", json.string("grind")),
        #("run_id", json.string(run_id)),
        #("scenario", json.string(scenario.id)),
        #("result", run(database, scenario)),
      ])
      |> json.to_string
    })
  let assert Ok(Nil) =
    simplifile.write(results_path, string.join(lines, "\n") <> "\n")
}

fn catalog_decoder() -> decode.Decoder(List(Scenario)) {
  use version <- decode.field("version", decode.int)
  let assert 1 = version
  use scenarios <- decode.field("scenarios", decode.list(scenario_decoder()))
  decode.success(scenarios)
}

fn scenario_decoder() -> decode.Decoder(Scenario) {
  use id <- decode.field("id", decode.string)
  use kind <- decode.field("kind", kind_decoder())
  use parameters <- decode.field("parameters", {
    use value <- decode.field("value", decode.int)
    use max_attempts <- decode.field("max_attempts", decode.int)
    use delay_ms <- decode.field("delay_ms", decode.int)
    use runs <- decode.field("runs", decode.int)
    use scheduled_age_ms <- decode.optional_field(
      "scheduled_age_ms",
      0,
      decode.int,
    )
    use finished_age_ms <- decode.optional_field(
      "finished_age_ms",
      0,
      decode.int,
    )
    use retention_ms <- decode.optional_field("retention_ms", 0, decode.int)
    use verify_delay <- decode.optional_field(
      "verify_delay",
      False,
      decode.bool,
    )
    decode.success(#(
      value,
      max_attempts,
      delay_ms,
      runs,
      scheduled_age_ms,
      finished_age_ms,
      retention_ms,
      verify_delay,
    ))
  })
  let #(
    value,
    max_attempts,
    delay_ms,
    runs,
    scheduled_age_ms,
    finished_age_ms,
    retention_ms,
    verify_delay,
  ) = parameters
  decode.success(Scenario(
    id:,
    kind:,
    value:,
    max_attempts:,
    delay_ms:,
    runs:,
    scheduled_age_ms:,
    finished_age_ms:,
    retention_ms:,
    verify_delay:,
  ))
}

fn kind_decoder() -> decode.Decoder(Kind) {
  use text <- decode.then(decode.string)
  case text {
    "success" -> decode.success(Success)
    "failure" -> decode.success(Failure)
    "snooze" -> decode.success(Snooze)
    "cancel" -> decode.success(Cancel)
    "future" -> decode.success(Future)
    "past" -> decode.success(Past)
    "discard" -> decode.success(Discard)
    "unique" -> decode.success(Unique)
    "prune" -> decode.success(Prune)
    _ -> decode.failure(Success, "supported paired scenario kind")
  }
}

fn run(database: postgres.Database, scenario: Scenario) -> json.Json {
  let calls = process.new_subject()
  let assert Ok(codec) =
    worker.codec("paired-int-v1", worker.infallible(json.int), decode.int)
  let assert Ok(errors) =
    worker.codec(
      "paired-error-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(delay) = worker.retry_delay(scenario.delay_ms)
  let assert Ok(definition) =
    worker.define_with_error_codec(
      "paired." <> scenario.id,
      "v1",
      codec,
      codec,
      errors,
      fn(value) { Ok(value + 1) },
    )
  let definition =
    worker.with_queue_handler(definition, fn(value) {
      process.send(calls, value + 1)
      case scenario.kind {
        Failure -> worker.WorkerFailed("business failure")
        Snooze -> worker.WorkerSnoozed(delay, "paired snooze")
        Discard -> worker.WorkerDiscarded("paired discard")
        _ -> worker.WorkerSucceeded(value + 1)
      }
    })
  let assert Ok(definition) =
    worker.with_max_attempts(definition, scenario.max_attempts)
  let definition =
    worker.with_retry_policy(
      definition,
      worker.retry_policy(fn(_, _) { worker.RetryAfter(delay) }),
    )
  let queue_name = "paired-" <> scenario.id
  let assert Ok(workers) = registry.new(queue_name)
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(coordinator) =
    queue.start(database, workers, consumer.manual_policy())
  use <- exception.defer(fn() { queue.stop(coordinator) })
  case scenario.kind {
    Prune -> prune(database, scenario, definition, queue_name, coordinator)
    Unique -> unique_duplicate(database, scenario, definition, queue_name)
    _ -> {
      let before_ms = database_ms(database)
      let assert Ok(handle) = case scenario.kind {
        Future | Past -> {
          let offset = case scenario.kind {
            Future -> scenario.delay_ms
            _ -> 0 - scenario.delay_ms
          }
          let assert Ok(at) = job.available_at(database_ms(database) + offset)
          postgres.submit_at(
            database,
            queue_name,
            definition,
            scenario.value,
            at,
          )
        }
        _ -> postgres.submit(database, queue_name, definition, scenario.value)
      }
      case scenario.kind {
        Cancel -> {
          let assert Ok(postgres.CancelledBeforeRun) =
            postgres.cancel(database, handle)
          Nil
        }
        _ -> Nil
      }
      list.each(list.repeat(Nil, scenario.runs), fn(_) {
        let assert Ok(_) = queue.process_one(coordinator)
      })
      let executions = count_calls(calls, 0)
      let fields = snapshot(database, handle, executions)
      let fields = case scenario.verify_delay {
        True -> [
          #(
            "delay_matches",
            json.bool(delay_matches(
              database,
              handle,
              before_ms,
              scenario.delay_ms,
            )),
          ),
          ..fields
        ]
        False -> fields
      }
      let fields = case scenario.kind {
        Success -> {
          let assert Ok(job.SucceededWith(value)) =
            postgres.outcome(database, handle)
          [#("value", json.int(value)), ..fields]
        }
        _ -> fields
      }
      json.object(fields)
    }
  }
}

fn count_calls(calls: process.Subject(Int), count: Int) -> Int {
  case process.receive(calls, within: 0) {
    Ok(_) -> count_calls(calls, count + 1)
    Error(Nil) -> count
  }
}

fn snapshot(
  database: postgres.Database,
  handle: job.JobHandle(Int, Int, String),
  executions: Int,
) -> List(#(String, json.Json)) {
  let assert Ok(state) = postgres.state(database, handle)
  let assert Ok(rows) =
    pog.query(
      "SELECT state, attempt_count, snooze_count, state IN ('scheduled', 'retryable') AND available_at > clock_timestamp() FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning({
      use stored <- decode.field(0, decode.string)
      use attempt <- decode.field(1, decode.int)
      use snoozes <- decode.field(2, decode.int)
      use future <- decode.field(3, decode.bool)
      decode.success(#(stored, attempt, snoozes, future))
    })
    |> pog.execute(on: postgres.connection(database))
  let assert [#(stored, attempt, snoozes, future)] = rows.rows
  let assert Ok(parsed) = job.state_of_stored(stored)
  let assert True = state == parsed
  let state = case stored {
    "queued" -> "available"
    _ -> stored
  }
  [
    #("state", json.string(state)),
    #("attempt", json.int(attempt)),
    #("snoozes", json.int(snoozes)),
    #("future", json.bool(future)),
    #("executions", json.int(executions)),
  ]
}

fn database_ms(database: postgres.Database) -> Int {
  let assert Ok(rows) =
    pog.query(
      "SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint",
    )
    |> pog.returning(integer_column())
    |> pog.execute(on: postgres.connection(database))
  let assert [value] = rows.rows
  value
}

fn delay_matches(
  database: postgres.Database,
  handle: job.JobHandle(Int, Int, String),
  before_ms: Int,
  delay_ms: Int,
) -> Bool {
  let assert Ok(rows) =
    pog.query(
      "SELECT floor(extract(epoch FROM available_at) * 1000)::bigint FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning(integer_column())
    |> pog.execute(on: postgres.connection(database))
  let assert [scheduled_ms] = rows.rows
  let after_ms = database_ms(database)
  // Both clocks are PostgreSQL's. The shared normalization allows 20ms for
  // Oban's application-clock scheduling and refuses a >5s observation span.
  after_ms >= before_ms
  && after_ms - before_ms <= 5000
  && scheduled_ms >= before_ms + delay_ms - 20
  && scheduled_ms <= after_ms + delay_ms + 20
}

fn unique_duplicate(
  database: postgres.Database,
  scenario: Scenario,
  definition: worker.Worker(Int, Int, String),
  queue_name: String,
) -> json.Json {
  let policy =
    unique.policy(
      unique.full_input(),
      unique.WithinQueue,
      unique.while_retained(),
      unique.Incomplete,
    )
  let assert Ok(first_id) = submission.submission_id(scenario.id <> "-first")
  let assert Ok(second_id) = submission.submission_id(scenario.id <> "-second")
  let assert Ok(submission.Inserted(first)) =
    postgres.submit_unique(
      database,
      queue_name,
      first_id,
      definition,
      scenario.value,
      submission.Immediately,
      policy,
      unique.KeepExisting,
    )
  let assert Ok(submission.Existing(second)) =
    postgres.submit_unique(
      database,
      queue_name,
      second_id,
      definition,
      scenario.value,
      submission.Immediately,
      policy,
      unique.KeepExisting,
    )
  json.object([
    #(
      "reused",
      json.bool(job.id_value(first) == submission.conflict_job_id(second)),
    ),
    ..snapshot(database, first, 0)
  ])
}

fn prune(
  database: postgres.Database,
  scenario: Scenario,
  definition: worker.Worker(Int, Int, String),
  queue_name: String,
  coordinator: queue.Consumer,
) -> json.Json {
  let assert Ok(old) =
    postgres.submit(database, queue_name, definition, scenario.value)
  let assert Ok(young) =
    postgres.submit(database, queue_name, definition, scenario.value)
  let assert Ok(True) = queue.process_one(coordinator)
  let assert Ok(True) = queue.process_one(coordinator)
  // Only fixture timestamps are altered. Admission, execution and deletion
  // pass through the public runtime, and each original ACK is durable first.
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET available_at = clock_timestamp() - $2::bigint * interval '1 millisecond', finished_at = clock_timestamp() - $3::bigint * interval '1 millisecond' WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(old)))
    |> pog.parameter(pog.int(scenario.scheduled_age_ms))
    |> pog.parameter(pog.int(scenario.finished_age_ms))
    |> pog.execute(on: postgres.connection(database))
  let assert Ok(report) =
    postgres.prune_finished(
      database,
      older_than_ms: scenario.retention_ms,
      limit: 100,
    )
  json.object([
    #("old_deleted", json.bool(not_present(database, old))),
    #("young_survived", json.bool(!not_present(database, young))),
    #("deleted", json.int(report.jobs)),
  ])
}

fn not_present(
  database: postgres.Database,
  handle: job.JobHandle(Int, Int, String),
) -> Bool {
  let assert Ok(rows) =
    pog.query("SELECT count(*) FROM grind_jobs WHERE id = $1")
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.returning(integer_column())
    |> pog.execute(on: postgres.connection(database))
  rows.rows == [0]
}

fn integer_column() -> decode.Decoder(Int) {
  use value <- decode.field(0, decode.int)
  decode.success(value)
}
