//// Testing a handler: `testing.perform` runs it without a database, and
//// `testing.drain` runs a queue's due jobs on demand.

import gleam/dynamic/decode
import gleam/io
import gleam/json
import gleam/time/duration
import grind
import grind/job
import grind/testing
import grind/worker
import grind_usage/support

fn text() -> worker.Codec(String) {
  worker.codec(worker.infallible(json.string), decode.string)
}

fn greeter(queue: String) -> worker.Worker(String, String, String) {
  worker.new(
    support.unique("testing.greet"),
    input: text(),
    output: text(),
    perform: fn(name) {
      case name {
        "" -> Error("a name is required")
        name -> Ok("hello " <> name)
      }
    },
  )
  |> worker.with_queue(queue)
  |> worker.with_error_codec(text())
}

fn attempt_reporter() -> worker.Worker(String, String, Nil) {
  worker.responding(
    support.unique("testing.attempt"),
    input: text(),
    output: text(),
    handle: fn(context, _name) {
      worker.Succeeded("attempt " <> int_to_string(worker.attempt(context)))
    },
  )
}

@external(erlang, "erlang", "integer_to_binary")
fn int_to_string(value: Int) -> String

/// No database: the input, output and error go through the worker's codecs,
/// exactly as a stored job's would.
pub fn offline() -> Nil {
  let greeter = greeter("testing")
  let assert Ok(worker.Succeeded("hello ada")) = testing.perform(greeter, "ada")
  let assert Ok(worker.Failed("a name is required")) =
    testing.perform(greeter, "")
  // The context is chosen by the test.
  let assert Ok(worker.Succeeded("attempt 3")) =
    testing.perform_with(
      attempt_reporter(),
      testing.context(job_id: 7, attempt: 3),
      "ada",
    )
  io.println("handler_testing: handlers performed without a database")
  support.mark("usage-handler-testing-offline-passed")
}

/// With a database: a runtime without consumers claims nothing, so `drain`
/// is the only thing that runs the queue.
pub fn run(url: String) -> Nil {
  offline()
  let greeter = greeter(support.unique("testing-drain"))
  use jobs <- support.with_grind(url, fn(config) {
    config |> grind.with_worker(greeter) |> grind.without_consumers
  })
  let assert Ok(grind.Inserted(handle)) =
    grind.submit(jobs, job.new(greeter, "grace"))
  let assert Ok(1) =
    testing.drain(
      jobs,
      queue: greeter.queue,
      limit: 10,
      within: duration.seconds(10),
    )
  let assert Ok(grind.Succeeded("hello grace")) = grind.outcome(jobs, handle)
  support.mark("usage-handler-testing-passed")
}
