//// `gleam run -m grind_bench/load -- <scenario> <args>` -- the bench CLI
//// entry point. Scenarios:
////
////   smoke [job_count]
////     Preload `job_count` jobs (default 1000), drain them, run the full
////     I1-I7 audit, and exit non-zero if it fails. This is
////     `scripts/bench-postgres.sh`'s own gate step.
////
////   l1 <job_count> <consumers> <concurrency> <queues> <cost_ms> [repeat]
////     One point of the drain-throughput matrix: `consumers` independent
////     `Consumer`s (round-robin over `queues` distinct queue names, each
////     with `queue.with_maximum_concurrency(concurrency)`) draining
////     `job_count` preloaded jobs whose handler sleeps `cost_ms` before
////     completing. Appends one row to `<results_dir>/l1.csv` (and one to
////     `<results_dir>/statements.csv` -- item 9's `pg_stat_statements`
////     bucket split). `repeat` (default `0`) is a caller-supplied repeat
////     index, carried through to the CSV row only -- see
////     `scripts/bench-matrix.sh`, item 8's "≥3 repeats, discard warm-up".
////
////   l7 <job_count> <consumers> <concurrency> [repeat]
////     One point of the coordinator-bottleneck matrix: like `l1` but always
////     one queue and `cost_ms` fixed at 1. This is handler cost; network
////     delay is a separate transport axis and must never be inferred from it. Each
////     consumer's own coordinator `Pid` (`@internal
////     queue.coordinator_pid`) is sampled for `message_queue_len` every
////     20ms for the run's own duration. Appends one row to
////     `<results_dir>/l7.csv`.
////
////   profile <job_count> <consumers> <concurrency>
////     Item 11: a statistical profiler for the coordinator-bottleneck
////     question -- samples every consumer's own coordinator `current_function`
////     and total `reductions` every 2ms for the run's own duration (a
////     `tools`-application-free stand-in for `eprof`/`fprof`; see
////     `grind_bench_sampler_ffi:current_function_and_reductions/1`'s own
////     doc comment), then reports the sampled function distribution --
////     "where the serial time per job goes" -- to `<results_dir>/profile.csv`
////     and stdout. Same one-queue, `cost_ms = 1` shape as `l7`.
////
//// Every scenario reads `GRIND_BENCH_DATABASE_URL` (required, Grind's own
//// pool -- item 10's `grind_a` role) and `GRIND_BENCH_RESULTS_DIR` (default
//// `"results/adhoc"`, relative to the bench project's own working
//// directory -- this module is always run via `cd bench && gleam run -m
//// grind_bench/load -- ...`, matching how `consumer/`'s own test suite is
//// always run from `consumer/`). `GRIND_BENCH_CTL_DATABASE_URL` (item 10's
//// `grind_ctl` role -- ledger, drain polling, samplers) defaults to
//// `GRIND_BENCH_DATABASE_URL` when unset. `GRIND_BENCH_POSTGRES_LOG` (item
//// 6/I6, the disposable cluster's own log file) and
//// `GRIND_BENCH_PG_DATA_DIR` (item 9, DB CPU via `ps`) are both optional --
//// their checks/measurements are skipped, not failed, when unset.
//// `GRIND_BENCH_COMMIT`/`GRIND_BENCH_DIRTY` (item 13 provenance) default to
//// `"unknown"`/`"0"`.

import gleam/int
import gleam/io
import gleam/string
import grind_bench/load/admission
import grind_bench/load/baseline
import grind_bench/load/maintenance
import grind_bench/load/observers
import grind_bench/load/open_loop
import grind_bench/load/profile
import grind_bench/load/runtime

pub fn main() -> Nil {
  case runtime.plain_arguments() {
    ["smoke"] -> baseline.run_smoke(1000)
    ["smoke", job_count] -> baseline.run_smoke(parse_or_panic(job_count))
    ["l1", job_count, consumers, concurrency, queues, cost_ms] ->
      baseline.run_l1(
        parse_or_panic(job_count),
        parse_or_panic(consumers),
        parse_or_panic(concurrency),
        parse_or_panic(queues),
        parse_or_panic(cost_ms),
        0,
      )
    ["l1", job_count, consumers, concurrency, queues, cost_ms, repeat] ->
      baseline.run_l1(
        parse_or_panic(job_count),
        parse_or_panic(consumers),
        parse_or_panic(concurrency),
        parse_or_panic(queues),
        parse_or_panic(cost_ms),
        parse_or_panic(repeat),
      )
    ["l7", job_count, consumers, concurrency] ->
      baseline.run_l7(
        parse_or_panic(job_count),
        parse_or_panic(consumers),
        parse_or_panic(concurrency),
        0,
      )
    ["l7", job_count, consumers, concurrency, repeat] ->
      baseline.run_l7(
        parse_or_panic(job_count),
        parse_or_panic(consumers),
        parse_or_panic(concurrency),
        parse_or_panic(repeat),
      )
    ["profile", job_count, consumers, concurrency] ->
      profile.run_profile(
        parse_or_panic(job_count),
        parse_or_panic(consumers),
        parse_or_panic(concurrency),
      )
    ["l2", consumers, interval_ms, filler_rows, duration_ms] ->
      open_loop.run_l2(
        parse_or_panic(consumers),
        parse_or_panic(interval_ms),
        parse_or_panic(filler_rows),
        parse_or_panic(duration_ms),
        0,
      )
    ["l2", consumers, interval_ms, filler_rows, duration_ms, repeat] ->
      open_loop.run_l2(
        parse_or_panic(consumers),
        parse_or_panic(interval_ms),
        parse_or_panic(filler_rows),
        parse_or_panic(duration_ms),
        parse_or_panic(repeat),
      )
    ["l3", arrival_per_sec, duration_ms] ->
      open_loop.run_l3(
        parse_or_panic(arrival_per_sec),
        parse_or_panic(duration_ms),
        0,
      )
    ["l3", arrival_per_sec, duration_ms, repeat] ->
      open_loop.run_l3(
        parse_or_panic(arrival_per_sec),
        parse_or_panic(duration_ms),
        parse_or_panic(repeat),
      )
    ["l4", submitters, mode, total_submissions] ->
      admission.run_l4(
        parse_or_panic(submitters),
        mode,
        parse_or_panic(total_submissions),
        0,
      )
    ["l4", submitters, mode, total_submissions, repeat] ->
      admission.run_l4(
        parse_or_panic(submitters),
        mode,
        parse_or_panic(total_submissions),
        parse_or_panic(repeat),
      )
    ["l5", pruner_on, duration_ms] ->
      maintenance.run_l5(
        parse_or_panic(pruner_on),
        parse_or_panic(duration_ms),
        0,
      )
    ["l5", pruner_on, duration_ms, repeat] ->
      maintenance.run_l5(
        parse_or_panic(pruner_on),
        parse_or_panic(duration_ms),
        parse_or_panic(repeat),
      )
    ["l6t1", concurrency, job_count, cost_ms] ->
      maintenance.run_l6t1(
        parse_or_panic(concurrency),
        parse_or_panic(job_count),
        parse_or_panic(cost_ms),
        0,
      )
    ["l6t1", concurrency, job_count, cost_ms, repeat] ->
      maintenance.run_l6t1(
        parse_or_panic(concurrency),
        parse_or_panic(job_count),
        parse_or_panic(cost_ms),
        parse_or_panic(repeat),
      )
    ["l6t2", k_slow_acks, d_ms] ->
      maintenance.run_l6t2(parse_or_panic(k_slow_acks), parse_or_panic(d_ms), 0)
    ["l6t2", k_slow_acks, d_ms, repeat] ->
      maintenance.run_l6t2(
        parse_or_panic(k_slow_acks),
        parse_or_panic(d_ms),
        parse_or_panic(repeat),
      )
    ["l6t2", k, d, lease, delay, repeat] ->
      maintenance.run_l6t2_profile(
        parse_or_panic(k),
        parse_or_panic(d),
        parse_or_panic(lease),
        parse_or_panic(delay),
        parse_or_panic(repeat),
      )
    ["l6t2", k, d, lease, delay, concurrency, pool, repeat] ->
      maintenance.run_l6t2_resources(
        parse_or_panic(k),
        parse_or_panic(d),
        parse_or_panic(lease),
        parse_or_panic(delay),
        parse_or_panic(concurrency),
        parse_or_panic(pool),
        parse_or_panic(repeat),
      )
    other -> {
      io.println(
        "unknown grind_bench/load invocation: " <> string.inspect(other),
      )
      io.println(
        "usage: gleam run -m grind_bench/load -- smoke [job_count] | l1 <job_count> <consumers> <concurrency> <queues> <cost_ms> [repeat] | l7 <job_count> <consumers> <concurrency> [repeat] | profile <job_count> <consumers> <concurrency> | l2 <consumers> <interval_ms> <filler_rows> <duration_ms> [repeat] | l3 <arrival_per_sec> <duration_ms> [repeat] | l4 <submitters> <hot|cold> <total_submissions> [repeat] | l5 <0|1 pruner_on> <duration_ms> [repeat] | l6t1 <concurrency> <job_count> <cost_ms> [repeat] | l6t2 <k_slow_acks> <d_ms> [repeat]",
      )
      runtime.halt(2)
    }
  }
}

fn parse_or_panic(value: String) -> Int {
  case int.parse(value) {
    Ok(n) -> n
    Error(Nil) -> panic as { "not an integer: " <> value }
  }
}

pub const ledger_error_counter = runtime.ledger_error_counter

pub const quarantine_counter = runtime.quarantine_counter

pub const forwarder_drop_counter = runtime.forwarder_drop_counter

pub const l4_submission_counter = runtime.l4_submission_counter

pub fn counter_value(key: Int) -> Int {
  runtime.counter_value(key)
}

pub fn attach_audit_observers(id_suffix: String) -> Nil {
  observers.attach_audit_observers(id_suffix)
}
