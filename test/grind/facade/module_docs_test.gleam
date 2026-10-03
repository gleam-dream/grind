import gleam/list
import gleam/string
import gleeunit/should

@external(erlang, "grind_module_docs_ffi", "public_sources")
fn public_sources() -> List(#(String, String))

/// `gleam docs` renders a module doc only from `////` lines.
pub fn every_public_module_starts_with_a_module_doc_test() {
  let sources = public_sources()
  sources
  |> list.map(fn(source) { source.0 })
  |> should.equal([
    "src/grind.gleam", "src/grind/admin.gleam", "src/grind/job.gleam",
    "src/grind/queue.gleam", "src/grind/telemetry.gleam",
    "src/grind/testing.gleam", "src/grind/unique.gleam",
    "src/grind/worker.gleam",
  ])
  sources
  |> list.filter(fn(source) { !string.starts_with(source.1, "//// ") })
  |> list.map(fn(source) { source.0 })
  |> should.equal([])
}

/// Machinery lives under `grind/internal`; no public module exports an
/// `@internal` function.
pub fn no_public_module_exports_internal_functions_test() {
  public_sources()
  |> list.filter(fn(source) { string.contains(source.1, "@internal") })
  |> list.map(fn(source) { source.0 })
  |> should.equal([])
}
