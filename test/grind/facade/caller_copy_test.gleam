//// Measures what a public call copies into the calling process. A call
//// asks the runtime's process for what it needs, and the reply is copied
//// into the caller. Registered workers stay with the runtime and the
//// consumers that run them, so a large worker definition costs a `submit`,
//// a read or a `stop` nothing; `admin.settle_due` and `testing.drain` run a
//// queue's workers in the caller and copy that queue's workers only.

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
import grind/worker

@external(erlang, "grind_copy_size", "flat_bytes")
fn flat_bytes(term: a) -> Int

@external(erlang, "grind_copy_size", "received_bytes")
fn received_bytes(call: fn() -> a) -> #(a, Int)

/// Data built at run time, as an application builds a catalogue or an
/// environment when it starts.
fn built_catalogue() -> List(String) {
  int.range(from: 0, to: 5000, with: [], run: fn(entries, n) {
    ["built entry " <> int.to_string(n), ..entries]
  })
}

fn capturing_worker(
  id: String,
  queue: String,
) -> worker.Worker(String, String, Nil) {
  let entries = built_catalogue()
  worker.new(
    id,
    input: string_codec(),
    output: string_codec(),
    perform: fn(key) { Ok(key <> int.to_string(list.length(entries))) },
  )
  |> worker.with_queue(queue)
}

/// What a caller may receive per call: the database handle and the
/// database's replies, never a worker.
const call_budget_bytes = 16_384

pub fn postgres_public_calls_copy_no_worker_into_the_caller_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> run(url)
  }
}

fn run(url: String) -> Nil {
  let captured = flat_bytes(built_catalogue())
  should.be_true(captured > 100_000)
  let large = capturing_worker(unique("caller.large"), unique("caller-large"))
  let other_queue = unique("caller-other")
  let other = capturing_worker(unique("caller.other"), other_queue)
  let name = process.new_name("caller_copy_grind")
  let assert Ok(jobs) =
    grind.new(pool_config(url))
    |> grind.with_worker(large)
    |> grind.with_worker(other)
    |> grind.with_startup_migration
    |> grind.start(name)
  // A failed assertion still stops the runtime; after the measured stop
  // this one finds nothing running.
  use <- exception.defer(fn() { grind.stop(jobs) })
  let submit = fn() {
    grind.submit(jobs, job.new(large, "a") |> job.after(duration.hours(1)))
  }
  // The first calls load modules, and the code server's replies would
  // count; measure the second.
  let assert Ok(grind.Inserted(handle)) = submit()
  let assert Ok(_) = grind.state(jobs, handle)
  let #(submitted, submit_bytes) = received_bytes(submit)
  let assert Ok(grind.Inserted(handle)) = submitted
  let #(state, state_bytes) = received_bytes(fn() { grind.state(jobs, handle) })
  let assert Ok(_) = state
  should.be_true(submit_bytes < call_budget_bytes)
  should.be_true(state_bytes < call_budget_bytes)
  // Settling a queue runs its workers' settlements in the caller, so it
  // copies that queue's workers, and only those.
  let assert Ok(_) = admin.settle_due(jobs, queue: other_queue, limit: 1)
  let #(settled, settle_bytes) =
    received_bytes(fn() { admin.settle_due(jobs, queue: other_queue, limit: 1) })
  let assert Ok(_) = settled
  should.be_true(settle_bytes > captured)
  should.be_true(settle_bytes < captured + call_budget_bytes)
  let #(outcome, stop_bytes) = received_bytes(fn() { grind.stop(jobs) })
  let assert Ok(_) = outcome
  should.be_true(stop_bytes < call_budget_bytes)
  mark_database_test_executed("facade-caller-copy-passed")
}
