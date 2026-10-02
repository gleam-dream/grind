//// Grind runs typed background jobs on PostgreSQL and Erlang/OTP.
////
//// This module holds only the package version. The work happens in these
//// modules:
////
//// - `grind/worker` defines a typed, versioned worker and the JSON codecs that
////   persist its input, output and error.
//// - `grind/registry` collects the workers that one queue runs.
//// - `grind/postgres` owns the database pool, runs migrations, submits jobs and
////   reads their state and outcome.
//// - `grind/queue` starts a supervised consumer that claims and runs jobs with
////   bounded concurrency.
//// - `grind/job` holds the job handle, its states and its outcomes.
//// - `grind/submission` and `grind/unique` describe admission results and
////   uniqueness policies.
//// - `grind/pruner` deletes finished jobs on a timer.
//// - `grind/observation` and `grind/diagnostic` describe the Sinal events
////   Grind emits.
////
//// ```gleam
//// import gleam/dynamic/decode
//// import gleam/json
//// import grind/postgres
//// import grind/queue
//// import grind/registry
//// import grind/worker
////
//// pub fn main() {
////   let assert Ok(text) = worker.codec("1", json.string, decode.string)
////   let assert Ok(greet) =
////     worker.define("greet", "1", text, text, fn(name) { Ok("Hello, " <> name) })
////
////   let assert Ok(settings) =
////     postgres.settings("postgres://app@localhost/app") |> postgres.validate
////   let assert Ok(database) = postgres.start(settings)
////   let assert Ok(Nil) = postgres.migrate(database)
////
////   let assert Ok(workers) = registry.new("default")
////   let assert Ok(workers) = registry.register(workers, greet)
////   let assert Ok(consumer) =
////     queue.start(database, workers, queue.default_policy_validated())
////
////   let assert Ok(handle) = postgres.submit(database, "default", greet, "Ada")
////   // After the consumer runs the job, `postgres.outcome(database, handle)`
////   // returns `Ok(job.SucceededWith("Hello, Ada"))`.
////   let _ = queue.stop(consumer)
////   handle
//// }
//// ```

pub fn version() -> String {
  "0.1.0"
}
