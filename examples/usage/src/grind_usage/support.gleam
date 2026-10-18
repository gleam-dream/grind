//// Plumbing shared by the usage examples: the database URL from the
//// environment, names that are unique across runs, and a runtime that is
//// stopped when its body returns. Application code needs none of this.

import exception
import gleam/erlang/process
import gleam/int
import grind
import pog

@external(erlang, "grind_usage_env", "database_url")
pub fn database_url() -> Result(String, Nil)

@external(erlang, "grind_usage_env", "cigogne_url")
pub fn cigogne_url() -> Result(String, Nil)

@external(erlang, "grind_usage_env", "mark")
pub fn mark(name: String) -> Nil

@external(erlang, "grind_usage_env", "stop_supervisor")
pub fn stop_supervisor(pid: process.Pid) -> Nil

@external(erlang, "erlang", "unique_integer")
fn unique_integer() -> Int

@external(erlang, "os", "system_time")
fn system_time() -> Int

/// A name that is unique across runs, so examples never meet each other's rows.
pub fn unique(prefix: String) -> String {
  prefix
  <> "-"
  <> int.to_string(system_time())
  <> "-"
  <> int.to_string(int.absolute_value(unique_integer()))
}

/// A schema or table name: `unique` with underscores only.
pub fn identifier(prefix: String) -> String {
  prefix
  <> "_"
  <> int.to_string(system_time())
  <> "_"
  <> int.to_string(int.absolute_value(unique_integer()))
}

pub fn pool(url: String) -> pog.Config {
  let assert Ok(config) = pog.url_config(process.new_name("usage_pool"), url)
  config |> pog.pool_size(4)
}

/// Starts a runtime that migrates its schema first, runs `body`, and stops it.
pub fn with_grind(
  url: String,
  configure: fn(grind.Config) -> grind.Config,
  body: fn(grind.Grind) -> a,
) -> a {
  let assert Ok(jobs) =
    grind.start(
      configure(grind.new(pool(url))) |> grind.with_startup_migration,
      process.new_name("usage_grind"),
    )
  use <- exception.defer(fn() { grind.stop(jobs) })
  body(jobs)
}
