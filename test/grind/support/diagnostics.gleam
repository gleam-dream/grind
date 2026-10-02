//// Observes the public Sinal contracts with bounded waits and explicit filters.

import gleam/erlang/process
import gleam/int
import grind/support/env
import sinal

pub fn capture(
  event: sinal.Event(measurements, metadata),
  accepts: fn(metadata) -> Bool,
) -> #(process.Subject(#(measurements, metadata)), sinal.Attachment) {
  let signal = process.new_subject()
  let attachment =
    sinal.observe(event, fn(measurements, metadata) {
      case accepts(metadata) {
        True -> process.send(signal, #(measurements, metadata))
        False -> Nil
      }
    })
  #(signal, attachment)
}

pub fn await(
  signal: process.Subject(a),
  accepts: fn(a) -> Bool,
  within_ms: Int,
) -> Result(a, Nil) {
  await_until(signal, accepts, env.monotonic_ms() + within_ms)
}

fn await_until(
  signal: process.Subject(a),
  accepts: fn(a) -> Bool,
  deadline: Int,
) -> Result(a, Nil) {
  case process.receive(signal, int.max(0, deadline - env.monotonic_ms())) {
    Error(Nil) -> Error(Nil)
    Ok(value) ->
      case accepts(value) {
        True -> Ok(value)
        False -> await_until(signal, accepts, deadline)
      }
  }
}
