//// Measures what a public call copies into the calling process. A call
//// asks the runtime's process for what it needs, and the reply is copied
//// into the caller. Registered workers stay with the runtime and the
//// processes that run them, so a large worker definition costs a `submit`,
//// a read, a `testing.drain` or a `stop` nothing. `admin.settle_due` runs
//// settlements in the caller, so it copies the workers of its queue that
//// declare one, and no other.

import exception
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/time/duration
import gleeunit/should
import grind
import grind/admin
import grind/facade/support.{pool_config, string_codec, unique}
import grind/job
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/testing
import grind/worker

@external(erlang, "grind_copy_size", "flat_bytes")
fn flat_bytes(term: a) -> Int

@external(erlang, "grind_copy_size", "received_bytes")
fn received_bytes(call: fn() -> a) -> #(a, Int)

/// Data built at run time, as an application builds a catalogue or an
/// environment when it starts.
fn built_catalogue(entries: Int) -> List(String) {
  int.range(from: 0, to: entries, with: [], run: fn(built, n) {
    ["built entry " <> int.to_string(n), ..built]
  })
}

const large_entries = 5000

const settling_entries = 1000

fn capturing_worker(
  id: String,
  queue: String,
  entries: Int,
) -> worker.Worker(String, String, Nil) {
  let catalogue = built_catalogue(entries)
  worker.new(
    id,
    input: string_codec(),
    output: string_codec(),
    perform: fn(key) { Ok(key <> int.to_string(list.length(catalogue))) },
  )
  |> worker.with_queue(queue)
}

/// What a caller may receive per call besides any worker it runs: the
/// database handle and the database's replies.
const call_budget_bytes = 16_384

pub fn postgres_public_calls_copy_no_worker_into_the_caller_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run(url)
  }
}

fn run(url: String) -> Nil {
  let large_bytes = flat_bytes(built_catalogue(large_entries))
  let settling_bytes = flat_bytes(built_catalogue(settling_entries))
  // A wrong worker in a reply exceeds every bound below.
  should.be_true(large_bytes > settling_bytes + 4 * call_budget_bytes)
  let large_queue = unique("caller-large")
  let large =
    capturing_worker(unique("caller.large"), large_queue, large_entries)
  // The settling queue holds a worker that declares a settlement and a
  // large one that does not.
  let settling_queue = unique("caller-settling")
  let idle =
    capturing_worker(unique("caller.idle"), settling_queue, large_entries)
  let settling =
    capturing_worker(
      unique("caller.settling"),
      settling_queue,
      settling_entries,
    )
    |> worker.with_settlement(fn(_transaction, _abandoned) {
      Ok(worker.AuthorizeReplay)
    })
  let name = process.new_name("caller_copy_grind")
  let assert Ok(jobs) =
    grind.new(pool_config(url))
    |> grind.with_worker(large)
    |> grind.with_worker(idle)
    |> grind.with_worker(settling)
    |> grind.with_startup_migration
    |> grind.start(name)
  // A failed assertion still stops the runtime; after the measured stop
  // this one finds nothing running.
  use <- exception.defer(fn() { grind.stop(jobs) })
  let submit = fn() {
    grind.submit(jobs, job.new(large, "a") |> job.after(duration.hours(1)))
  }
  let drain = fn() {
    testing.drain(
      jobs,
      queue: large_queue,
      limit: 1,
      within: duration.seconds(10),
    )
  }
  let settle = fn() { admin.settle_due(jobs, queue: settling_queue, limit: 1) }
  // The first calls load modules, and the code server's replies would
  // count; measure the second.
  let assert Ok(grind.Inserted(handle)) = submit()
  let assert Ok(_) = grind.state(jobs, handle)
  let assert Ok(0) = drain()
  let assert Ok(_) = settle()
  let #(submitted, submit_bytes) = received_bytes(submit)
  let assert Ok(grind.Inserted(handle)) = submitted
  let #(state, state_bytes) = received_bytes(fn() { grind.state(jobs, handle) })
  let assert Ok(_) = state
  let #(drained, drain_bytes) = received_bytes(drain)
  let assert Ok(0) = drained
  should.be_true(submit_bytes < call_budget_bytes)
  should.be_true(state_bytes < call_budget_bytes)
  should.be_true(drain_bytes < call_budget_bytes)
  // Settling copies the settling worker into the caller to run it, and
  // neither the idle worker beside it nor the other queue's. The lower
  // bound shows the trace sees a worker that does cross.
  let #(settled, settle_bytes) = received_bytes(settle)
  let assert Ok(_) = settled
  should.be_true(settle_bytes > settling_bytes)
  should.be_true(settle_bytes < settling_bytes + call_budget_bytes)
  let #(outcome, stop_bytes) = received_bytes(fn() { grind.stop(jobs) })
  let assert Ok(_) = outcome
  should.be_true(stop_bytes < call_budget_bytes)
  mark_database_test_executed("facade-caller-copy-passed")
}
