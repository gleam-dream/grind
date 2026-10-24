import gleam/erlang/process
import gleeunit/should
import grind_bench/load/arrivals
import grind_bench/load/report
import grind_bench/summarize

pub fn bounded_generator_accounts_for_exhausted_slots_test() {
  let run =
    arrivals.generate(1000, 30, 1, fn(_) {
      process.sleep(60)
      Ok(Nil)
    })
  run.scheduled |> should.equal(30)
  run.max_outstanding |> should.equal(1)
  { run.capacity_limited > 0 } |> should.be_true
  run.dispatched + run.capacity_limited |> should.equal(run.scheduled)
  run.admitted + run.failed + run.unfinished |> should.equal(run.dispatched)
  run.failed |> should.equal(0)
  run.unfinished |> should.equal(0)
}

pub fn failed_admissions_remain_in_the_denominator_test() {
  let run = arrivals.generate(100, 50, 8, fn(_) { Error(Nil) })
  run.scheduled |> should.equal(5)
  run.failed |> should.equal(run.dispatched)
  run.admitted |> should.equal(0)
  run.unfinished |> should.equal(0)
  run.failed + run.capacity_limited |> should.equal(run.scheduled)
}

pub fn durable_latency_p95_is_a_reported_percentile_test() {
  let stats = summarize.percentiles([1.0, 2.0, 3.0, 4.0, 5.0])
  report.percentile_field(stats, "p95") |> should.equal("5.0")
}
