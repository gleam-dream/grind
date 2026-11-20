//// Measures what Grind copies between processes for each job. A claimed
//// job carries a closure over its whole worker definition from the
//// coordinator through the attempt supervisor and the attempt actor into the
//// handler process, so everything the definition captures is copied with
//// every job. Only the handler
//// keeps it: the attempt actor drops it once the handler starts, and the
//// lease renewer gets only the attempt fence.

import exception
import gleam/dynamic
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{Some}
import gleam/otp/actor
import gleam/string
import gleeunit/should
import grind/internal/attempt
import grind/internal/postgres
import grind/internal/queue/worker as queue_worker
import grind/internal/registry
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/worker

/// Static data a handler reads. Gleam inlines a constant where it is used,
/// so a closure that refers to it does not capture it.
const catalogue = [
  "catalogue entry 000 with a description long enough to matter",
  "catalogue entry 001 with a description long enough to matter",
  "catalogue entry 002 with a description long enough to matter",
  "catalogue entry 003 with a description long enough to matter",
  "catalogue entry 004 with a description long enough to matter",
  "catalogue entry 005 with a description long enough to matter",
  "catalogue entry 006 with a description long enough to matter",
  "catalogue entry 007 with a description long enough to matter",
  "catalogue entry 008 with a description long enough to matter",
  "catalogue entry 009 with a description long enough to matter",
  "catalogue entry 010 with a description long enough to matter",
  "catalogue entry 011 with a description long enough to matter",
  "catalogue entry 012 with a description long enough to matter",
  "catalogue entry 013 with a description long enough to matter",
  "catalogue entry 014 with a description long enough to matter",
  "catalogue entry 015 with a description long enough to matter",
  "catalogue entry 016 with a description long enough to matter",
  "catalogue entry 017 with a description long enough to matter",
  "catalogue entry 018 with a description long enough to matter",
  "catalogue entry 019 with a description long enough to matter",
  "catalogue entry 020 with a description long enough to matter",
  "catalogue entry 021 with a description long enough to matter",
  "catalogue entry 022 with a description long enough to matter",
  "catalogue entry 023 with a description long enough to matter",
  "catalogue entry 024 with a description long enough to matter",
  "catalogue entry 025 with a description long enough to matter",
  "catalogue entry 026 with a description long enough to matter",
  "catalogue entry 027 with a description long enough to matter",
  "catalogue entry 028 with a description long enough to matter",
  "catalogue entry 029 with a description long enough to matter",
  "catalogue entry 030 with a description long enough to matter",
  "catalogue entry 031 with a description long enough to matter",
  "catalogue entry 032 with a description long enough to matter",
  "catalogue entry 033 with a description long enough to matter",
  "catalogue entry 034 with a description long enough to matter",
  "catalogue entry 035 with a description long enough to matter",
  "catalogue entry 036 with a description long enough to matter",
  "catalogue entry 037 with a description long enough to matter",
  "catalogue entry 038 with a description long enough to matter",
  "catalogue entry 039 with a description long enough to matter",
  "catalogue entry 040 with a description long enough to matter",
  "catalogue entry 041 with a description long enough to matter",
  "catalogue entry 042 with a description long enough to matter",
  "catalogue entry 043 with a description long enough to matter",
  "catalogue entry 044 with a description long enough to matter",
  "catalogue entry 045 with a description long enough to matter",
  "catalogue entry 046 with a description long enough to matter",
  "catalogue entry 047 with a description long enough to matter",
  "catalogue entry 048 with a description long enough to matter",
  "catalogue entry 049 with a description long enough to matter",
  "catalogue entry 050 with a description long enough to matter",
  "catalogue entry 051 with a description long enough to matter",
  "catalogue entry 052 with a description long enough to matter",
  "catalogue entry 053 with a description long enough to matter",
  "catalogue entry 054 with a description long enough to matter",
  "catalogue entry 055 with a description long enough to matter",
  "catalogue entry 056 with a description long enough to matter",
  "catalogue entry 057 with a description long enough to matter",
  "catalogue entry 058 with a description long enough to matter",
  "catalogue entry 059 with a description long enough to matter",
  "catalogue entry 060 with a description long enough to matter",
  "catalogue entry 061 with a description long enough to matter",
  "catalogue entry 062 with a description long enough to matter",
  "catalogue entry 063 with a description long enough to matter",
  "catalogue entry 064 with a description long enough to matter",
  "catalogue entry 065 with a description long enough to matter",
  "catalogue entry 066 with a description long enough to matter",
  "catalogue entry 067 with a description long enough to matter",
  "catalogue entry 068 with a description long enough to matter",
  "catalogue entry 069 with a description long enough to matter",
  "catalogue entry 070 with a description long enough to matter",
  "catalogue entry 071 with a description long enough to matter",
  "catalogue entry 072 with a description long enough to matter",
  "catalogue entry 073 with a description long enough to matter",
  "catalogue entry 074 with a description long enough to matter",
  "catalogue entry 075 with a description long enough to matter",
  "catalogue entry 076 with a description long enough to matter",
  "catalogue entry 077 with a description long enough to matter",
  "catalogue entry 078 with a description long enough to matter",
  "catalogue entry 079 with a description long enough to matter",
  "catalogue entry 080 with a description long enough to matter",
  "catalogue entry 081 with a description long enough to matter",
  "catalogue entry 082 with a description long enough to matter",
  "catalogue entry 083 with a description long enough to matter",
  "catalogue entry 084 with a description long enough to matter",
  "catalogue entry 085 with a description long enough to matter",
  "catalogue entry 086 with a description long enough to matter",
  "catalogue entry 087 with a description long enough to matter",
  "catalogue entry 088 with a description long enough to matter",
  "catalogue entry 089 with a description long enough to matter",
  "catalogue entry 090 with a description long enough to matter",
  "catalogue entry 091 with a description long enough to matter",
  "catalogue entry 092 with a description long enough to matter",
  "catalogue entry 093 with a description long enough to matter",
  "catalogue entry 094 with a description long enough to matter",
  "catalogue entry 095 with a description long enough to matter",
  "catalogue entry 096 with a description long enough to matter",
  "catalogue entry 097 with a description long enough to matter",
  "catalogue entry 098 with a description long enough to matter",
  "catalogue entry 099 with a description long enough to matter",
  "catalogue entry 100 with a description long enough to matter",
  "catalogue entry 101 with a description long enough to matter",
  "catalogue entry 102 with a description long enough to matter",
  "catalogue entry 103 with a description long enough to matter",
  "catalogue entry 104 with a description long enough to matter",
  "catalogue entry 105 with a description long enough to matter",
  "catalogue entry 106 with a description long enough to matter",
  "catalogue entry 107 with a description long enough to matter",
  "catalogue entry 108 with a description long enough to matter",
  "catalogue entry 109 with a description long enough to matter",
  "catalogue entry 110 with a description long enough to matter",
  "catalogue entry 111 with a description long enough to matter",
  "catalogue entry 112 with a description long enough to matter",
  "catalogue entry 113 with a description long enough to matter",
  "catalogue entry 114 with a description long enough to matter",
  "catalogue entry 115 with a description long enough to matter",
  "catalogue entry 116 with a description long enough to matter",
  "catalogue entry 117 with a description long enough to matter",
  "catalogue entry 118 with a description long enough to matter",
  "catalogue entry 119 with a description long enough to matter",
]

@external(erlang, "grind_copy_size", "flat_bytes")
fn flat_bytes(term: a) -> Int

@external(erlang, "sys", "get_state")
fn actor_state(pid: process.Pid) -> dynamic.Dynamic

@external(erlang, "grind_copy_size", "heap_bytes_after_gc")
fn heap_bytes_after_gc(pid: process.Pid) -> Int

/// Data built at run time, as an application builds a catalogue or an
/// environment when it starts.
fn built_catalogue() -> List(String) {
  int.range(from: 0, to: 5000, with: [], run: fn(entries, n) {
    ["built entry " <> int.to_string(n), ..entries]
  })
}

fn text_codec() -> worker.Codec(String) {
  worker.codec(worker.infallible(json.string), decode.string)
}

fn capturing_worker(id: String) -> worker.Worker(String, String, Nil) {
  let entries = built_catalogue()
  worker.new(id, input: text_codec(), output: text_codec(), perform: fn(key) {
    Ok(key <> int.to_string(list.length(entries)))
  })
}

fn constant_worker(id: String) -> worker.Worker(String, String, Nil) {
  worker.new(id, input: text_codec(), output: text_codec(), perform: fn(key) {
    Ok(key <> int.to_string(list.length(catalogue)))
  })
}

pub fn a_worker_value_carries_what_its_handler_captures_test() {
  let captured = flat_bytes(built_catalogue())
  should.be_true(captured > 100_000)
  should.be_true(flat_bytes(capturing_worker("copy.capturing")) > captured)
}

pub fn a_handler_that_reads_a_module_constant_does_not_capture_it_test() {
  should.be_true(flat_bytes(catalogue) > 10_000)
  should.be_true(flat_bytes(constant_worker("copy.constant")) < 2048)
}

pub fn selecting_a_worker_carries_that_worker_only_test() {
  let assert Ok(workers) = registry.new("copy-cost")
  let assert Ok(workers) =
    registry.register(workers, capturing_worker("copy.large"))
  let assert Ok(workers) =
    registry.register(workers, constant_worker("copy.small"))
  let assert Ok(registry.Selected(run: small_run, ..)) =
    registry.select(workers, "copy-cost", "copy.small", "1")
  let assert Ok(registry.Selected(run: large_run, ..)) =
    registry.select(workers, "copy-cost", "copy.large", "1")
  should.be_true(flat_bytes(small_run) < 2048)
  should.be_true(flat_bytes(large_run) > 100_000)
}

pub fn postgres_attempt_actor_holds_no_worker_after_start_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_attempt_actor_test(database_url)
  }
}

type AttemptEvent {
  Acknowledging
  Completed
}

/// A worker that captures a large runtime value, reports that its handler
/// started, then blocks until the test kills its attempt.
fn blocking_worker(
  id: String,
  queue: String,
  started: process.Subject(Nil),
) -> worker.Worker(String, String, Nil) {
  let entries = built_catalogue()
  worker.new(id, input: text_codec(), output: text_codec(), perform: fn(key) {
    process.send(started, Nil)
    process.sleep_forever()
    Ok(key <> int.to_string(list.length(entries)))
  })
  |> worker.with_queue(queue)
}

fn run_attempt_actor_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let queue = "copy-cost-actor"
  let owner = "copy-cost-owner"
  let started = process.new_subject()
  let large = blocking_worker("copy.actor.large", queue, started)
  let assert Ok(workers) = registry.new(queue)
  let assert Ok(workers) = registry.register(workers, large)
  let assert Ok(_) =
    postgres.submit(database, queue, large, string.repeat("x", 200))
  let assert Ok(Some(claimed)) =
    attempt.claim_one(database, queue, workers, owner, 30_000)
  // The claim carries the worker into the attempt supervisor, the attempt
  // actor and the handler process.
  let captured = flat_bytes(built_catalogue())
  should.be_true(flat_bytes(claimed) > captured)
  let renewals = process.new_subject()
  let request =
    queue_worker.WorkerRequest(
      queue_subject: process.new_subject(),
      claimed:,
      on_acknowledging: fn(_, _, _) { Acknowledging },
      on_complete: fn(_, _, _, _) { Completed },
      database:,
      queue:,
      owner:,
      renewer: renewals,
      retry_interval_ms: 1000,
      automatic: True,
    )
  let assert Ok(attempt_actor) = actor.start(queue_worker.worker_actor(request))
  let pid = attempt_actor.pid
  use <- exception.defer(fn() {
    process.unlink(pid)
    process.kill(pid)
  })
  process.send(attempt_actor.data, queue_worker.StartAttempt)
  let assert Ok(Nil) = process.receive(started, 5000)
  // The message the renewer receives carries no worker, its cancellation
  // callback included.
  let assert Ok(track) = process.receive(renewals, 1000)
  should.be_true(flat_bytes(track) < 1024)
  // Once the handler runs, the attempt actor keeps no worker, in its state
  // or its heap.
  should.be_true(flat_bytes(actor_state(pid)) < 8192)
  should.be_true(heap_bytes_after_gc(pid) < captured / 2)
  mark_database_test_executed("copy-cost-attempt-actor-passed")
}
