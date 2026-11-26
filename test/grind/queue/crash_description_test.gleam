import exception
import gleam/dynamic.{type Dynamic}
import gleam/string

/// A crash description is bounded in bytes as well as in characters: one
/// grapheme can carry any number of combining marks, so a character limit
/// alone does not bound the text written to the job.
pub fn crash_description_is_bounded_in_bytes_test() {
  let message = "e" <> string.repeat("\u{0301}", 5000)
  let assert Error(exception.Errored(reason)) =
    exception.rescue(fn() { panic as message })
  let description = describe_exit(reason)
  assert string.byte_size(description) <= 2000
  assert string.starts_with(description, "panic in ")
}

@external(erlang, "grind_queue_ffi", "describe_exit")
fn describe_exit(reason: Dynamic) -> String
