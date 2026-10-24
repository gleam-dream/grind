//// The runtime: one supervised child with a stable name-based handle, an
//// unsupervised runtime for scripts, a submit-only node, and a configuration
//// that is checked before anything starts.

import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/io
import gleam/json
import gleam/otp/static_supervisor as supervisor
import gleam/time/duration
import grind
import grind/job
import grind/queue
import grind/worker
import grind_usage/support
import pog

fn amount() -> worker.Codec(Int) {
  worker.codec(worker.infallible(json.int), decode.int)
}

fn passthrough(queue: String) -> worker.Worker(Int, Int, Nil) {
  worker.new(
    support.unique("runtime.passthrough"),
    input: amount(),
    output: amount(),
    perform: fn(value) { Ok(value) },
  )
  |> worker.with_queue(queue)
}

/// `grind.supervised` is one child of the application's tree. `grind.named`
/// returns a handle that is valid before the runtime starts and across its
/// restarts, so a web handler reaches Grind without threading a value.
fn supervised(url: String) -> Nil {
  let work = passthrough(support.unique("runtime-supervised"))
  let name = process.new_name("usage_supervised")
  let jobs = grind.named(name)
  let assert Error(grind.SubmitNotRunning) =
    grind.submit(jobs, job.new(work, 1))

  let config =
    grind.new(support.pool(url))
    |> grind.with_worker(work)
    |> grind.with_startup_migration
  let assert Ok(started) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(grind.supervised(config, name))
    |> supervisor.start
  use <- exception.defer(fn() { support.stop_supervisor(started.pid) })

  let assert Ok(grind.Inserted(handle)) = grind.submit(jobs, job.new(work, 1))
  let assert Ok(grind.Succeeded(1)) =
    grind.await(jobs, handle, within: duration.seconds(10))
  // A supervised runtime is stopped by its supervisor, never by `grind.stop`.
  let assert Error(grind.OwnedBySupervisor) = grind.stop(jobs)
  Nil
}

/// A script owns its runtime and stops it when it is done.
fn script(url: String) -> Nil {
  let work = passthrough(support.unique("runtime-script"))
  let config =
    grind.new(support.pool(url))
    |> grind.with_worker(work)
    |> grind.with_startup_migration
  let assert Ok(jobs) = grind.start(config, process.new_name("usage_script"))
  let assert Ok(grind.Inserted(handle)) = grind.submit(jobs, job.new(work, 2))
  let assert Ok(grind.Succeeded(2)) =
    grind.await(jobs, handle, within: duration.seconds(10))
  let assert Ok(grind.StoppedCleanly) = grind.stop(jobs)
  Nil
}

/// A submit-only node admits jobs and runs none: nothing claims them here.
fn submit_only(url: String) -> Nil {
  let work = passthrough(support.unique("runtime-submit-only"))
  use jobs <- support.with_grind(url, fn(config) {
    config |> grind.with_worker(work) |> grind.without_consumers
  })
  let assert Ok(grind.Inserted(handle)) = grind.submit(jobs, job.new(work, 3))
  let assert Ok(grind.Pending(job.Queued)) = grind.outcome(jobs, handle)
  Nil
}

/// `start` and `supervised` validate the configuration; `grind.check` gives
/// the same typed error up front.
fn configuration() -> Nil {
  let pool = pog.default_config(process.new_name("usage_runtime_pool"))
  let assert Error(error) =
    grind.new(pool) |> grind.with_queue(queue.new("nobody")) |> grind.check
  let assert grind.QueueWithoutWorkers("nobody") = error
  io.println("runtime: " <> grind.describe_config_error(error))
}

pub fn run(url: String) -> Nil {
  supervised(url)
  script(url)
  submit_only(url)
  configuration()
  support.mark("usage-runtime-passed")
}
