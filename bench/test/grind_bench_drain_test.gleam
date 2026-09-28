import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleeunit/should
import grind_bench/load/drain
import grind_bench/load/runtime
import simplifile

pub fn drain_timeout_default_and_invalid_configuration_test() {
  runtime.parse_drain_timeout_ms(Error(Nil)) |> should.equal(Ok(60_000))
  runtime.parse_drain_timeout_ms(Ok("600000")) |> should.equal(Ok(600_000))
  runtime.parse_drain_timeout_ms(Ok("060000")) |> should.equal(Ok(60_000))
  list.each(
    ["", "0", "000", "-1", "+1", " 10", "10 ", "1.5", "1e6", "1_000", "１"],
    fn(raw) { runtime.parse_drain_timeout_ms(Ok(raw)) |> should.be_error },
  )
}

fn config(label: String) -> drain.Config {
  drain.Config(
    raw_path: "/tmp/grind-drain-"
      <> label
      <> "-"
      <> int.to_string(runtime.monotonic_ms())
      <> ".jsonl",
    interval_ms: 2,
    timeout_ms: 20,
    expected_jobs: 2,
    startup_started_ms: runtime.monotonic_ms(),
    sample: fn() { [#("message_queue_len_max", json.int(0))] },
  )
}

fn cleanup(config: drain.Config) -> Nil {
  let _ = simplifile.delete(config.raw_path)
  let _ = simplifile.delete(config.raw_path <> ".drain.json")
  Nil
}

fn read_diagnostic(config: drain.Config, decoder: decode.Decoder(a)) -> a {
  let assert Ok(contents) = simplifile.read(config.raw_path <> ".drain.json")
  let assert Ok(value) = json.parse(contents, decoder)
  value
}

pub fn full_drain_sampling_and_observer_stop_are_acknowledged_test() {
  let config = config("success")
  use <- exception.defer(fn() { cleanup(config) })
  let observer_stopped = process.new_subject()
  drain.measure(
    config,
    fn(budget) {
      budget |> should.equal(20)
      process.sleep(25)
      Ok(Nil)
    },
    fn() { process.send(observer_stopped, Nil) },
    fn() {
      // The completion snapshot must happen after the observer stops.
      process.receive(observer_stopped, within: 0) |> should.equal(Ok(Nil))
      Ok(json.object([#("durable_observed", json.int(2))]))
    },
  )
  |> should.equal(Ok(Nil))
  read_diagnostic(config, decode.at(["valid"], decode.bool)) |> should.be_true
  read_diagnostic(config, decode.at(["sampler_covers_drain"], decode.bool))
  |> should.be_true
  let ticks =
    read_diagnostic(config, decode.at(["sampler", "ticks"], decode.int))
  { ticks >= 2 } |> should.be_true
  let started =
    read_diagnostic(
      config,
      decode.at(["drain_started_monotonic_ms"], decode.int),
    )
  let ended =
    read_diagnostic(config, decode.at(["drain_ended_monotonic_ms"], decode.int))
  let first =
    read_diagnostic(
      config,
      decode.at(["sampler", "first_monotonic_ms"], decode.int),
    )
  let last =
    read_diagnostic(
      config,
      decode.at(["sampler", "last_monotonic_ms"], decode.int),
    )
  { first <= started && last >= ended } |> should.be_true
}

pub fn timeout_retains_partial_counts_and_does_not_repeat_the_wait_test() {
  let config = config("timeout")
  use <- exception.defer(fn() { cleanup(config) })
  let waits = process.new_subject()
  drain.measure(
    config,
    fn(_) {
      process.send(waits, Nil)
      process.sleep(25)
      Error(Nil)
    },
    fn() { Nil },
    fn() { Ok(json.object([#("durable_observed", json.int(1))])) },
  )
  |> should.be_error
  process.receive(waits, within: 0) |> should.equal(Ok(Nil))
  process.receive(waits, within: 0) |> should.be_error
  read_diagnostic(config, decode.at(["outcome"], decode.string))
  |> should.equal("timed_out")
  read_diagnostic(config, decode.at(["valid"], decode.bool)) |> should.be_false
  read_diagnostic(config, decode.at(["expected_jobs"], decode.int))
  |> should.equal(2)
  read_diagnostic(
    config,
    decode.at(["completion_snapshot", "durable_observed"], decode.int),
  )
  |> should.equal(1)
  read_diagnostic(config, decode.at(["sampler_covers_drain"], decode.bool))
  |> should.be_true
}

pub fn drain_query_crash_and_observer_failure_leave_invalid_diagnostics_test() {
  let config = config("query-error")
  use <- exception.defer(fn() { cleanup(config) })
  let stopped = process.new_subject()
  drain.measure(
    config,
    fn(_) { panic as "injected drain query failure" },
    fn() {
      process.send(stopped, Nil)
      panic as "injected observer failure"
    },
    fn() { Ok(json.object([#("durable_observed", json.int(0))])) },
  )
  |> should.be_error
  process.receive(stopped, within: 0) |> should.equal(Ok(Nil))
  read_diagnostic(config, decode.at(["outcome"], decode.string))
  |> should.equal("drain_query_failed")
  read_diagnostic(config, decode.at(["valid"], decode.bool)) |> should.be_false
  read_diagnostic(config, decode.at(["observer_stopped"], decode.bool))
  |> should.be_false
}
