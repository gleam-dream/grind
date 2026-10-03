//// Shared L7/profile drain measurement. Sampling spans the drain interval,
//// which is deliberately separate from the existing throughput denominator.

import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/result
import gleam/string
import grind/internal/consumer as queue
import grind_bench/load/context
import grind_bench/load/runtime
import grind_bench/load/workload
import pog
import simplifile

pub type Config {
  Config(
    raw_path: String,
    interval_ms: Int,
    timeout_ms: Int,
    expected_jobs: Int,
    startup_started_ms: Int,
    sample: fn() -> List(#(String, json.Json)),
  )
}

type Coverage {
  Coverage(first_ms: Int, last_ms: Int, ticks: Int, max_gap_ms: Int)
}

type Sampler {
  Sampler(pid: process.Pid, stop: process.Subject(process.Subject(Coverage)))
}

type Message {
  Stop(process.Subject(Coverage))
  OwnerExited
}

/// Always attempts consumer shutdown, including after diagnostic I/O failure.
/// No second drain is performed after the first decision.
pub fn run(
  harness: context.Harness,
  consumers: List(queue.Consumer),
  config: Config,
) -> Result(Nil, Nil) {
  use <- exception.defer(fn() {
    list.each(consumers, fn(consumer) {
      let _ = queue.stop(consumer)
      Nil
    })
  })
  let schema = context.schema_of(harness.database)
  measure(
    config,
    fn(timeout) { workload.wait_for_drain(harness.drain, schema, timeout) },
    fn() { context.stop_observer(harness) },
    fn() { completion_snapshot(harness.drain, schema) },
  )
}

/// The same path is used by timeout/error tests with an injected wait and
/// snapshot. Startup, the drain decision and post-decision diagnostics are
/// separate intervals; a slow diagnostic query cannot inflate drain time.
pub fn measure(
  config: Config,
  wait: fn(Int) -> Result(Nil, Nil),
  finish_observation: fn() -> Nil,
  snapshot: fn() -> Result(json.Json, String),
) -> Result(Nil, Nil) {
  let sampler = start_sampler(config)
  let start_ms = runtime.monotonic_ms()
  let waited = case sampler {
    Ok(_) -> exception.rescue(fn() { wait(config.timeout_ms) })
    Error(_) -> Ok(Error(Nil))
  }
  let end_ms = runtime.monotonic_ms()
  let coverage = result.try(sampler, stop_sampler)
  // Stop the independent observer before recording its final completion count.
  // A failure here is retained and invalidates the run, rather than bypassing
  // diagnostics or the consumer cleanup in run().
  let observer = exception.rescue(finish_observation)
  let counts = case exception.rescue(snapshot) {
    Ok(value) -> value
    Error(error) -> Error(string.inspect(error))
  }
  let snapshot_ms = runtime.monotonic_ms()
  let covered = case coverage {
    Ok(c) -> c.ticks >= 2 && c.first_ms <= start_ms && c.last_ms >= end_ms
    Error(_) -> False
  }
  let outcome = case sampler, waited {
    Error(_), _ -> "sampler_start_failed"
    Ok(_), Ok(Ok(Nil)) -> "drained"
    Ok(_), Ok(Error(Nil)) -> "timed_out"
    Ok(_), Error(_) -> "drain_query_failed"
  }
  let valid =
    outcome == "drained"
    && covered
    && result.is_ok(observer)
    && result.is_ok(counts)
  let document =
    json.object([
      #(
        "source_sha256",
        json.string(result.unwrap(
          runtime.getenv("GRIND_BENCH_SOURCE_SHA256"),
          "unknown",
        )),
      ),
      #(
        "commit",
        json.string(result.unwrap(
          runtime.getenv("GRIND_BENCH_COMMIT"),
          "unknown",
        )),
      ),
      #(
        "dirty",
        json.string(result.unwrap(
          runtime.getenv("GRIND_BENCH_DIRTY"),
          "unknown",
        )),
      ),
      #(
        "network_delay_ms",
        json.string(result.unwrap(
          runtime.getenv("GRIND_BENCH_NETWORK_DELAY_MS"),
          "0",
        )),
      ),
      #("raw_path", json.string(config.raw_path)),
      #("expected_jobs", json.int(config.expected_jobs)),
      #("drain_timeout_ms", json.int(config.timeout_ms)),
      #("startup_started_monotonic_ms", json.int(config.startup_started_ms)),
      #("pre_drain_elapsed_ms", json.int(start_ms - config.startup_started_ms)),
      #("drain_started_monotonic_ms", json.int(start_ms)),
      #("drain_ended_monotonic_ms", json.int(end_ms)),
      #("drain_elapsed_ms", json.int(end_ms - start_ms)),
      #("snapshot_observed_monotonic_ms", json.int(snapshot_ms)),
      #("outcome", json.string(outcome)),
      #("valid", json.bool(valid)),
      #("drain_error", case waited {
        Error(error) -> json.string(string.inspect(error))
        _ -> json.null()
      }),
      #("observer_stopped", json.bool(result.is_ok(observer))),
      #("observer_error", case observer {
        Error(error) -> json.string(string.inspect(error))
        Ok(_) -> json.null()
      }),
      #("sampling_scope", json.string("drain_interval_only")),
      #("sampler_interval_ms", json.int(config.interval_ms)),
      #("sampler_covers_drain", json.bool(covered)),
      #("sampler", case coverage {
        Ok(c) ->
          json.object([
            #("first_monotonic_ms", json.int(c.first_ms)),
            #("last_monotonic_ms", json.int(c.last_ms)),
            #("ticks", json.int(c.ticks)),
            #("max_observed_gap_ms", json.int(c.max_gap_ms)),
          ])
        Error(error) -> json.object([#("error", json.string(error))])
      }),
      #("completion_snapshot", case counts {
        Ok(values) -> values
        Error(error) -> json.object([#("error", json.string(error))])
      }),
    ])
  let assert Ok(Nil) =
    simplifile.create_directory_all(runtime.parent_dir(config.raw_path))
  let assert Ok(Nil) =
    simplifile.write(
      config.raw_path <> ".drain.json",
      json.to_string(document) <> "\n",
    )
  case valid {
    True -> Ok(Nil)
    False -> Error(Nil)
  }
}

fn start_sampler(config: Config) -> Result(Sampler, String) {
  let ready = process.new_subject()
  let owner = process.self()
  let pid =
    process.spawn_unlinked(fn() {
      let stop = process.new_subject()
      let monitor = process.monitor(owner)
      let selector =
        process.new_selector()
        |> process.select_map(stop, Stop)
        |> process.select_specific_monitor(monitor, fn(_) { OwnerExited })
      let assert Ok(Nil) =
        simplifile.create_directory_all(runtime.parent_dir(config.raw_path))
      let assert Ok(Nil) = simplifile.write(config.raw_path, "")
      let first = tick(config)
      process.send(ready, stop)
      sample_loop(config, selector, Coverage(first, first, 1, 0))
    })
  case process.receive(ready, within: 5000) {
    Ok(stop) -> Ok(Sampler(pid:, stop:))
    Error(_) -> {
      process.kill(pid)
      Error("sampler did not acknowledge its first persisted sample")
    }
  }
}

fn stop_sampler(sampler: Sampler) -> Result(Coverage, String) {
  let acknowledged = process.new_subject()
  process.send(sampler.stop, acknowledged)
  case process.receive(acknowledged, within: 5000) {
    Ok(coverage) -> Ok(coverage)
    Error(_) -> {
      process.kill(sampler.pid)
      Error("sampler did not persist its final sample and acknowledge stop")
    }
  }
}

fn tick(config: Config) -> Int {
  let now = runtime.monotonic_ms()
  // Keep the existing unix_ms field for raw-reader compatibility. Its value
  // has always been monotonic; the explicit field removes ambiguity.
  let sample =
    json.object([
      #("unix_ms", json.int(now)),
      #("monotonic_ms", json.int(now)),
      ..config.sample()
    ])
  let assert Ok(Nil) =
    simplifile.append(config.raw_path, json.to_string(sample) <> "\n")
  now
}

fn sample_loop(
  config: Config,
  selector: process.Selector(Message),
  coverage: Coverage,
) -> Nil {
  case process.selector_receive(selector, within: config.interval_ms) {
    Ok(OwnerExited) -> Nil
    received -> {
      let now = tick(config)
      let next =
        Coverage(
          coverage.first_ms,
          now,
          coverage.ticks + 1,
          int.max(coverage.max_gap_ms, now - coverage.last_ms),
        )
      case received {
        Ok(Stop(acknowledged)) -> process.send(acknowledged, next)
        Error(_) -> sample_loop(config, selector, next)
        Ok(OwnerExited) -> Nil
      }
    }
  }
}

/// One SQL snapshot after the drain decision and observer stop. Consumers may
/// still finish between the decision and this query; these are observation
/// counts, not a second acceptance check and not exact deadline-time counts.
fn completion_snapshot(
  connection: pog.Connection,
  schema: String,
) -> Result(json.Json, String) {
  let sql =
    "WITH tracked AS (SELECT s.bench_index,s.job_id,coalesce(j.state,'missing') AS state FROM grind_bench.bench_submissions s LEFT JOIN \""
    <> schema
    <> "\".grind_jobs j ON j.id=s.job_id) "
    <> "SELECT 'submitted' AS metric,count(*) AS value FROM tracked "
    <> "UNION ALL SELECT 'handler_started',count(DISTINCT e.bench_index) FROM grind_bench.bench_effects e JOIN tracked t USING(bench_index) "
    <> "UNION ALL SELECT 'handler_completed',count(DISTINCT e.bench_index) FROM grind_bench.bench_effects e JOIN tracked t USING(bench_index) WHERE e.finished_at IS NOT NULL "
    <> "UNION ALL SELECT 'durable_observed',count(*) FROM grind_bench.bench_durable_completions d JOIN tracked t USING(job_id) "
    <> "UNION ALL SELECT 'succeeded_receipts',count(*) FROM \""
    <> schema
    <> "\".grind_job_acknowledgements a JOIN tracked t USING(job_id) WHERE a.committed_state='succeeded' "
    <> "UNION ALL SELECT 'state:' || state,count(*) FROM tracked GROUP BY state"
  let query =
    pog.query(sql)
    |> pog.returning({
      use name <- decode.field(0, decode.string)
      use value <- decode.field(1, decode.int)
      decode.success(#(name, json.int(value)))
    })
  pog.execute(query, connection)
  |> result.map(fn(returned) { json.object(returned.rows) })
  |> result.map_error(string.inspect)
}
