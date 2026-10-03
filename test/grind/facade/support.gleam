//// Shared setup for the facade tests: a runtime started through the public
//// `grind` API against the queue test database.

import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/time/duration
import grind
import grind/support/env.{unique_test_run_id}
import grind/worker
import pog

pub fn int_codec() -> worker.Codec(Int) {
  worker.codec(worker.infallible(json.int), decode.int)
}

pub fn string_codec() -> worker.Codec(String) {
  worker.codec(worker.infallible(json.string), decode.string)
}

/// A name unique to this test run.
pub fn unique(prefix: String) -> String {
  prefix <> "-" <> int.to_string(unique_test_run_id())
}

pub fn pool_config(url: String) -> pog.Config {
  let assert Ok(config) = pog.url_config(process.new_name("facade_pool"), url)
  config |> pog.pool_size(4)
}

/// A configuration with short deadlines, so leases (and so renewal-borne
/// cancellation) are a few seconds long.
pub fn fast(config: grind.Config) -> grind.Config {
  config
  |> grind.with_statement_deadline(duration.milliseconds(1500))
  |> grind.with_unique_lock_wait(duration.milliseconds(400))
}

/// Starts a runtime, migrates it, runs `body`, then stops it.
pub fn with_runtime(
  url: String,
  configure: fn(grind.Config) -> grind.Config,
  body: fn(grind.Grind) -> a,
) -> a {
  let name = process.new_name("facade_grind")
  let assert Ok(jobs) =
    grind.start(configure(grind.new(pool_config(url))), name)
  use <- exception.defer(fn() { grind.stop(jobs) })
  let assert Ok(Nil) = grind.migrate(jobs)
  body(jobs)
}
