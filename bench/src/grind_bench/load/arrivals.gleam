//// A bounded open-loop scheduler. A slot has an absolute due time; if all
//// submitters are occupied then that slot is recorded as generator-limited.
//// It is never shifted into a closed-loop backlog or silently discarded.

import gleam/erlang/process
import gleam/int
import gleam/list
import grind_bench/load/runtime

pub type Run {
  Run(
    scheduled: Int,
    dispatched: Int,
    admitted: Int,
    failed: Int,
    unfinished: Int,
    capacity_limited: Int,
    max_outstanding: Int,
    offered_elapsed_ms: Int,
    lags_ms: List(Float),
  )
}

type State {
  State(
    dispatched: Int,
    admitted: Int,
    failed: Int,
    outstanding: Int,
    capacity_limited: Int,
    peak: Int,
    lags: List(Float),
  )
}

pub fn generate(
  rate_per_sec: Int,
  duration_ms: Int,
  max_inflight: Int,
  submit: fn(Int) -> Result(Nil, Nil),
) -> Run {
  let assert True = rate_per_sec > 0 && duration_ms > 0 && max_inflight > 0
  let scheduled = rate_per_sec * duration_ms / 1000
  let assert True = scheduled > 0
  let done = process.new_subject()
  let started = runtime.monotonic_ms()
  let initial = State(0, 0, 0, 0, 0, 0, [])
  let offered =
    schedule(
      0,
      scheduled,
      rate_per_sec,
      max_inflight,
      started,
      done,
      submit,
      initial,
    )
  let offered_elapsed_ms = runtime.monotonic_ms() - started
  let finished = collect(done, offered, runtime.monotonic_ms() + 120_000)
  Run(
    scheduled:,
    dispatched: finished.dispatched,
    admitted: finished.admitted,
    failed: finished.failed,
    unfinished: finished.outstanding,
    capacity_limited: finished.capacity_limited,
    max_outstanding: finished.peak,
    offered_elapsed_ms:,
    lags_ms: list.reverse(finished.lags),
  )
}

fn schedule(
  index: Int,
  count: Int,
  rate: Int,
  limit: Int,
  started: Int,
  done: process.Subject(Result(Nil, Nil)),
  submit: fn(Int) -> Result(Nil, Nil),
  state: State,
) -> State {
  case index >= count {
    True -> state
    False -> {
      let due = started + index * 1000 / rate
      let wait = due - runtime.monotonic_ms()
      case wait > 0 {
        True -> process.sleep(wait)
        False -> Nil
      }
      let lag = int.to_float(runtime.monotonic_ms() - due)
      let state = drain_ready(done, state)
      let state = State(..state, lags: [lag, ..state.lags])
      let next = case state.outstanding < limit {
        True -> {
          let _ =
            process.spawn_unlinked(fn() { process.send(done, submit(index)) })
          State(
            ..state,
            dispatched: state.dispatched + 1,
            outstanding: state.outstanding + 1,
            peak: int.max(state.peak, state.outstanding + 1),
          )
        }
        False -> State(..state, capacity_limited: state.capacity_limited + 1)
      }
      schedule(index + 1, count, rate, limit, started, done, submit, next)
    }
  }
}

fn observe(state: State, outcome: Result(Nil, Nil)) -> State {
  case outcome {
    Ok(Nil) ->
      State(
        ..state,
        outstanding: state.outstanding - 1,
        admitted: state.admitted + 1,
      )
    Error(Nil) ->
      State(
        ..state,
        outstanding: state.outstanding - 1,
        failed: state.failed + 1,
      )
  }
}

fn drain_ready(done: process.Subject(Result(Nil, Nil)), state: State) -> State {
  case process.receive(done, within: 0) {
    Ok(outcome) -> drain_ready(done, observe(state, outcome))
    Error(_) -> state
  }
}

fn collect(
  done: process.Subject(Result(Nil, Nil)),
  state: State,
  deadline: Int,
) -> State {
  case state.outstanding == 0 {
    True -> state
    False -> {
      case
        process.receive(
          done,
          within: int.max(0, deadline - runtime.monotonic_ms()),
        )
      {
        Ok(outcome) -> collect(done, observe(state, outcome), deadline)
        Error(_) -> state
      }
    }
  }
}
