//// Admission throughput: `count` jobs submitted one `submit` at a time
//// against the same jobs in one `submit_many`, alternating, `runs` times.
////
//// ```sh
//// GRIND_BENCH_DATABASE_URL=postgres://... \
////   gleam run -m grind_bench/submit_many -- 1000 5
//// ```
////
//// Prints one line per run and arm with the elapsed milliseconds and jobs
//// per second. The runtime runs without consumers, so only admission is
//// timed; every arm writes `count` new jobs.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import grind
import grind/job
import grind/worker
import grind_bench/load/runtime
import pog

pub fn main() -> Nil {
  let assert Ok(url) = runtime.getenv("GRIND_BENCH_DATABASE_URL")
  let #(count, runs) = case runtime.plain_arguments() {
    [count, runs, ..] -> {
      let assert Ok(count) = int.parse(count)
      let assert Ok(runs) = int.parse(runs)
      #(count, runs)
    }
    _ -> #(1000, 5)
  }
  let codec = worker.codec(worker.infallible(json.int), decode.int)
  let bench_worker =
    worker.new("bench.submit_many", input: codec, output: codec, perform: fn(n) {
      Ok(n)
    })
    |> worker.with_queue("bench-submit-many")
  let assert Ok(config) =
    pog.url_config(process.new_name("grind_bench_submit_many_pool"), url)
  let assert Ok(jobs) =
    grind.start(
      grind.new(config)
        |> grind.with_worker(bench_worker)
        |> grind.without_consumers
        |> grind.with_startup_migration,
      process.new_name("grind_bench_submit_many"),
    )
  let batch =
    list.map(list.repeat(Nil, count), fn(_) { job.new(bench_worker, 1) })
  // One discarded warm-up of each arm.
  let _ = single(jobs, batch)
  let _ = bulk(jobs, batch)
  list.each(list.repeat(Nil, runs), fn(_) {
    report("submit", count, single(jobs, batch))
    report("submit_many", count, bulk(jobs, batch))
  })
  let assert Ok(_) = grind.stop(jobs)
  Nil
}

fn single(jobs: grind.Grind, batch: List(job.Job(Int, Int, e))) -> Int {
  let started = runtime.monotonic_ms()
  list.each(batch, fn(next) {
    let assert Ok(_) = grind.submit(jobs, next)
  })
  runtime.monotonic_ms() - started
}

fn bulk(jobs: grind.Grind, batch: List(job.Job(Int, Int, e))) -> Int {
  let started = runtime.monotonic_ms()
  let assert Ok(_) = grind.submit_many(jobs, batch)
  runtime.monotonic_ms() - started
}

fn report(arm: String, count: Int, elapsed_ms: Int) -> Nil {
  io.println(
    arm
    <> " "
    <> int.to_string(count)
    <> " jobs: "
    <> int.to_string(elapsed_ms)
    <> " ms, "
    <> int.to_string(count * 1000 / int.max(elapsed_ms, 1))
    <> " jobs/s",
  )
}
