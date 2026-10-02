//// Benchmark worker with separate handler-start and handler-finish times.
//// The ledger uses an independent pool; it never borrows Grind capacity.
//// Durable completion is measured by a separate observer after the ACK is
//// visible, so sleeping, returning, and committing remain distinct events.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import grind/worker.{type Worker}
import pog

/// `bench_index`: this run's own submission sequence number (assigned by the
/// driver, not by Grind). `cost_ms`: how long this job's handler sleeps
/// before completing, simulating real work.
pub type BenchJob {
  BenchJob(bench_index: Int, cost_ms: Int)
}

pub fn encode(job: BenchJob) -> json.Json {
  let BenchJob(bench_index:, cost_ms:) = job
  json.object([
    #("bench_index", json.int(bench_index)),
    #("cost_ms", json.int(cost_ms)),
  ])
}

pub fn decoder() -> decode.Decoder(BenchJob) {
  use bench_index <- decode.field("bench_index", decode.int)
  use cost_ms <- decode.field("cost_ms", decode.int)
  decode.success(BenchJob(bench_index:, cost_ms:))
}

@external(erlang, "grind_bench_ffi", "node_name")
fn node_name() -> String

@external(erlang, "grind_bench_counter_ffi", "next")
fn next_delivery_count(bench_index: Int) -> Int

/// Records one delivery of `bench_index` in the ledger's `bench_effects`
/// table. A failed ledger write is swallowed here (never turned into a
/// worker failure -- that would change Grind's own retry/delivery-count
/// semantics for a bench-harness-only concern) but bumps
/// `grind_bench_counter_ffi`'s own `"ledger_write_errors"` counter, which
/// `grind_bench/audit`'s partial I6 check reads.
fn record_effect(
  ledger: pog.Connection,
  bench_index: Int,
  delivery_count: Int,
) -> Nil {
  let query =
    pog.query(
      "INSERT INTO bench_effects (bench_index, delivery_count, node) VALUES ($1, $2, $3)",
    )
    |> pog.parameter(pog.int(bench_index))
    |> pog.parameter(pog.int(delivery_count))
    |> pog.parameter(pog.text(node_name()))
  case pog.execute(query, ledger) {
    Ok(_) -> Nil
    Error(_) -> {
      let _ = next_delivery_count(-1)
      Nil
    }
  }
}

/// Builds the bench worker, bound to `ledger` (the bench-owned pool -- see
/// `grind_bench.start_ledger_pool`). `id` lets a scenario register several
/// distinct worker identities sharing this same behavior (useful for
/// multi-queue matrices) without a version collision.
pub fn build(
  ledger: pog.Connection,
  id: String,
) -> Result(Worker(BenchJob, Int, Nil), worker.DefinitionError) {
  let assert Ok(input) =
    worker.codec(id <> "-input-v1", worker.infallible(encode), decoder())
  let assert Ok(output) =
    worker.codec(id <> "-output-v1", worker.infallible(json.int), decode.int)
  worker.define(id, "v1", input, output, fn(job) {
    let BenchJob(bench_index:, cost_ms:) = job
    let delivery_count = next_delivery_count(bench_index)
    record_effect(ledger, bench_index, delivery_count)
    case cost_ms > 0 {
      True -> process.sleep(cost_ms)
      False -> Nil
    }
    let query =
      pog.query(
        "UPDATE bench_effects SET finished_at = clock_timestamp() WHERE bench_index = $1 AND delivery_count = $2",
      )
      |> pog.parameter(pog.int(bench_index))
      |> pog.parameter(pog.int(delivery_count))
    case pog.execute(query, ledger) {
      Ok(_) -> Nil
      Error(_) -> {
        let _ = next_delivery_count(-1)
        Nil
      }
    }
    Ok(bench_index)
  })
}
