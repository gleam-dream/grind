import exception
import gleam/erlang/process
import gleam/int
import grind
import pog

@external(erlang, "consumer_test_env", "database_url")
pub fn database_url() -> Result(String, Nil)

@external(erlang, "consumer_test_env", "storage_failure_url")
pub fn storage_failure_url() -> Result(String, Nil)

@external(erlang, "consumer_test_env", "mark")
pub fn mark(name: String) -> Nil

@external(erlang, "consumer_effect", "reset")
pub fn reset_effects() -> Nil

@external(erlang, "consumer_effect", "apply")
pub fn apply_synthetic_effect(key: String, amount: Int) -> #(String, Int)

@external(erlang, "consumer_effect", "count")
pub fn synthetic_effect_count(key: String) -> Int

@external(erlang, "consumer_effect", "receipt")
pub fn synthetic_effect_receipt(key: String) -> Result(String, Nil)

@external(erlang, "consumer_effect", "arm_crash_after_effect")
pub fn arm_crash_after_effect(key: String) -> Nil

@external(erlang, "consumer_counter", "reset")
pub fn reset_counter(key: String) -> Nil

@external(erlang, "consumer_counter", "next")
pub fn next_counter(key: String) -> Int

@external(erlang, "erlang", "unique_integer")
fn unique_integer() -> Int

@external(erlang, "os", "system_time")
fn system_time() -> Int

/// A name unique across test runs.
pub fn unique(prefix: String) -> String {
  prefix
  <> "-"
  <> int.to_string(system_time())
  <> "-"
  <> int.to_string(int.absolute_value(unique_integer()))
}

pub fn pool(url: String) -> pog.Config {
  let assert Ok(config) = pog.url_config(process.new_name("consumer_pool"), url)
  config |> pog.pool_size(4)
}

/// Starts a runtime over the consumer database that migrates its schema
/// first, runs `body`, and stops it.
pub fn with_grind(
  url: String,
  configure: fn(grind.Config) -> grind.Config,
  body: fn(grind.Grind) -> a,
) -> a {
  let assert Ok(jobs) =
    grind.start(
      configure(grind.new(pool(url))) |> grind.with_startup_migration,
      process.new_name("consumer_grind"),
    )
  use <- exception.defer(fn() { grind.stop(jobs) })
  body(jobs)
}
