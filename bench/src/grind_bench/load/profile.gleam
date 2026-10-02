import gleam/dict
import gleam/dynamic/decode
import gleam/float
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/option

import gleam/result
import gleam/string
import grind/queue
import grind/registry
import grind_bench
import grind_bench/load/context
import grind_bench/load/drain as drain_measurement
import grind_bench/load/observers
import grind_bench/load/report
import grind_bench/load/runtime
import grind_bench/load/workload
import grind_bench/worker as bench_worker
import simplifile

// -- profile: item 11, statistical coordinator profiling -------------------

type FunctionSample {
  FunctionSample(module: String, function: String, arity: Int)
}

fn function_sample_decoder() -> decode.Decoder(FunctionSample) {
  use module <- decode.field("module", decode.string)
  use function <- decode.field("function", decode.string)
  use arity <- decode.field("arity", decode.int)
  decode.success(FunctionSample(module:, function:, arity:))
}

type ProfileTick {
  ProfileTick(total_reductions: Int, functions: List(FunctionSample))
}

fn profile_tick_decoder() -> decode.Decoder(ProfileTick) {
  use total_reductions <- decode.field("total_reductions", decode.int)
  use functions <- decode.field(
    "functions",
    decode.list(function_sample_decoder()),
  )
  decode.success(ProfileTick(total_reductions:, functions:))
}

pub fn run_profile(job_count: Int, consumers: Int, concurrency: Int) -> Nil {
  let drain_timeout_ms = runtime.drain_timeout_ms()
  let total_concurrency = consumers * concurrency
  let harness =
    context.setup(
      int.max(total_concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(total_concurrency),
    )
  let context.Harness(database:, ledger:, ..) = harness
  let queue_name = "profile-q0"
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.profile.echo")
  let assert Ok(r) = registry.new(queue_name)
  let assert Ok(r) = registry.register(r, worker_def)
  observers.attach_audit_observers()
  let log_lines_before = report.postgres_log_lines_before()
  workload.preload_and_track(
    database,
    ledger,
    worker_def,
    [queue_name],
    job_count,
    1,
  )
  workload.assert_submission_count(ledger, job_count)
  workload.analyze_and_checkpoint(database, ledger)

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(5)
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy
  let startup_started_ms = runtime.monotonic_ms()
  let consumers_list =
    runtime.int_range(consumers)
    |> list.map(fn(_i) {
      let assert Ok(c) = queue.start(database, r, policy)
      c
    })
  let coordinator_pids = list.filter_map(consumers_list, queue.coordinator_pid)

  let label = int.to_string(consumers) <> "x" <> int.to_string(concurrency)
  let raw_path = runtime.results_dir() <> "/raw/profile-" <> label <> ".jsonl"
  let grind_schema = context.schema_of(database)
  let drained =
    drain_measurement.run(
      harness,
      consumers_list,
      drain_measurement.Config(
        raw_path:,
        interval_ms: 2,
        timeout_ms: drain_timeout_ms,
        expected_jobs: job_count,
        startup_started_ms:,
        sample: fn() {
          let samples =
            list.filter_map(
              coordinator_pids,
              runtime.current_function_and_reductions,
            )
          let total_reductions =
            list.fold(samples, 0, fn(acc, sample) { acc + sample.3 })
          [
            #("total_reductions", json.int(total_reductions)),
            #(
              "functions",
              json.array(samples, fn(sample) {
                let #(module, function, arity, _) = sample
                json.object([
                  #("module", json.string(module)),
                  #("function", json.string(function)),
                  #("arity", json.int(arity)),
                ])
              }),
            ),
          ]
        },
      ),
    )

  case drained {
    Error(Nil) -> {
      io.println("profile: drain failed; see " <> raw_path <> ".drain.json")
      runtime.halt(1)
    }
    Ok(Nil) -> {
      let elapsed_ms = case report.timestamps_ms(ledger, grind_schema) {
        Ok(#(t0, t_end)) -> t_end - t0
        Error(Nil) -> -1
      }
      report_profile(raw_path, "profile-" <> label, elapsed_ms)
      report.run_audit_and_report(
        ledger,
        database,
        "profile",
        job_count,
        log_lines_before,
      )
    }
  }
}

fn report_profile(path: String, label: String, elapsed_ms: Int) -> Nil {
  case simplifile.read(path) {
    Error(_) -> io.println("profile: no samples captured at " <> path)
    Ok(content) -> {
      let ticks =
        content
        |> string.split("\n")
        |> list.map(string.trim)
        |> list.filter(fn(line) { line != "" })
        |> list.filter_map(fn(line) {
          json.parse(line, profile_tick_decoder()) |> result.replace_error(Nil)
        })
      case ticks {
        [] -> io.println("profile: no decodable samples at " <> path)
        _ -> {
          let all_samples = list.flat_map(ticks, fn(tick) { tick.functions })
          let total_samples = list.length(all_samples)
          let tally =
            list.fold(all_samples, dict.new(), fn(acc, sample) {
              let key =
                sample.module
                <> ":"
                <> sample.function
                <> "/"
                <> int.to_string(sample.arity)
              dict.upsert(acc, key, fn(existing) {
                case existing {
                  option.Some(n) -> n + 1
                  option.None -> 1
                }
              })
            })
          let reduction_delta = case ticks {
            [first, ..] ->
              case list.last(ticks) {
                Ok(last) -> last.total_reductions - first.total_reductions
                Error(Nil) -> 0
              }
            [] -> 0
          }
          let reduction_rate_per_sec = case elapsed_ms > 0 {
            True ->
              int.to_float(reduction_delta)
              /. { int.to_float(elapsed_ms) /. 1000.0 }
            False -> 0.0
          }
          let rows =
            dict.to_list(tally)
            |> list.sort(fn(a, b) { int.compare(b.1, a.1) })
            |> list.map(fn(entry) {
              let #(key, count) = entry
              let percent = case total_samples > 0 {
                True ->
                  int.to_float(count) /. int.to_float(total_samples) *. 100.0
                False -> 0.0
              }
              string.join(
                [
                  runtime.provenance_prefix(),
                  label,
                  key,
                  int.to_string(count),
                  float.to_string(percent),
                  float.to_string(reduction_rate_per_sec),
                ],
                ",",
              )
            })
          report.write_rows(
            runtime.results_dir() <> "/profile.csv",
            runtime.provenance_header_prefix()
              <> ",label,function,sample_count,percent,reduction_rate_per_sec",
            rows,
          )
          io.println(
            "profile "
            <> label
            <> ": "
            <> int.to_string(total_samples)
            <> " samples, "
            <> int.to_string(reduction_delta)
            <> " reductions ("
            <> float.to_string(reduction_rate_per_sec)
            <> "/s), top functions:",
          )
          rows
          |> list.take(12)
          |> list.each(fn(row) { io.println("  " <> row) })
        }
      }
    }
  }
}
