//// Shared Sinal wire primitives for Grind observations.
//// Public event records and their domain projections remain in observation.

import gleam/dynamic
import gleam/dynamic/decode
import gleam/erlang/atom
import sinal.{type Event}
import sinal/fields

/// Declares a field whose wire representation is a native string but whose
/// Gleam representation is a closed enum, via an explicit total
/// `to_string`/partial `from_string` pair. Built directly on
/// `sinal/fields.field` (the same construction `fields.string` itself
/// uses), so this stays inside Sinal's public field API with no extra FFI.
/// Mirrors `saga/observation`'s identical helper.
pub fn closed_string_field(
  key: String,
  to_string: fn(a) -> String,
  from_string: fn(String) -> Result(a, Nil),
) -> fields.Fields(a) {
  fields.field(
    atom.create(key),
    fn(value) { Ok(dynamic.string(to_string(value))) },
    fn(raw) {
      case decode.run(raw, decode.string) {
        Error(_) ->
          Error(fields.FieldDecodeError("Expected a native BEAM string"))
        Ok(str) ->
          case from_string(str) {
            Ok(value) -> Ok(value)
            Error(Nil) ->
              Error(fields.FieldDecodeError(
                "Unrecognized " <> key <> " kind: " <> str,
              ))
          }
      }
    },
  )
}

/// A single-field `Fields(Int)` for the `count` measurement every
/// `[grind, job, *]` event in this module carries (always `1`).
pub fn count_fields() -> fields.Fields(Int) {
  fields.int(atom.create("count"))
}

/// The shared `JobRef` codec: which job, queue, and worker contract an event
/// is about. Built once here and embedded (via `fields.pair`) into every
/// event introduced after `acknowledged`, rather than re-declaring the same
/// four fields per event.
pub fn job_ref_fields(
  from_tuple: fn(#(#(#(Int, String), String), String)) -> reference,
  to_tuple: fn(reference) -> #(#(#(Int, String), String), String),
) -> fields.Fields(reference) {
  let assert Ok(p1) =
    fields.pair(
      fields.int(atom.create("job_id")),
      fields.string(atom.create("queue")),
    )
  let assert Ok(p2) = fields.pair(p1, fields.string(atom.create("worker_id")))
  let assert Ok(p3) =
    fields.pair(p2, fields.string(atom.create("worker_version")))
  fields.imap(p3, from_tuple, to_tuple)
}

/// The shared `AttemptRef` codec: `attempt_id`, `epoch`, and the attempt
/// number, embedded into every event about one claimed attempt.
pub fn attempt_ref_fields(
  from_tuple: fn(#(#(Int, Int), Int)) -> reference,
  to_tuple: fn(reference) -> #(#(Int, Int), Int),
) -> fields.Fields(reference) {
  let assert Ok(p1) =
    fields.pair(
      fields.int(atom.create("attempt_id")),
      fields.int(atom.create("epoch")),
    )
  let assert Ok(p2) = fields.pair(p1, fields.int(atom.create("attempt")))
  fields.imap(p2, from_tuple, to_tuple)
}

/// Builds a `[grind, job, <parts>]` name from its final component (the shared
/// `[grind, job, ...]` prefix is fixed for every event this module exposes).
pub fn job_event_name(part: String) -> List(atom.Atom) {
  [atom.create("grind"), atom.create("job"), atom.create(part)]
}

pub fn event(
  name: List(atom.Atom),
  measurements: fields.Fields(measurement),
  metadata: fields.Fields(meta),
) -> Event(measurement, meta) {
  let assert Ok(event) = sinal.event(name, measurements, metadata)
  event
}
