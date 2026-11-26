import exception
import gleam/erlang/process
import gleam/string
import grind/internal/queue/worker

/// The whole description stored on a crashed handler's job, its prefix
/// included, is bounded in bytes as well as in characters: one grapheme can
/// carry any number of combining marks, so a character limit alone does not
/// bound the text written to the job.
pub fn crash_description_is_bounded_in_bytes_test() {
  let message = "e" <> string.repeat("\u{0301}", 5000)
  let assert Error(exception.Errored(reason)) =
    exception.rescue(fn() { panic as message })
  let description = worker.crash_description(process.Abnormal(reason))
  assert string.byte_size(description) <= 2000
  assert string.starts_with(description, "handler exited: panic in ")
}

/// The character bound covers the prefix too.
pub fn crash_description_is_bounded_in_characters_test() {
  let message = string.repeat("x", 1000)
  let assert Error(exception.Errored(reason)) =
    exception.rescue(fn() { panic as message })
  let description = worker.crash_description(process.Abnormal(reason))
  assert string.length(description) <= 500
}
