//// An unreachable database fails `start` with a typed error within the
//// connect timeout.

import gleam/erlang/process
import gleam/time/duration
import grind
import grind_consumer/support/env

pub fn an_unreachable_database_fails_start_test() {
  case env.storage_failure_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Error(grind.Unavailable(_)) =
        grind.new(env.pool(url))
        |> grind.with_connect_timeout(duration.seconds(1))
        |> grind.start(process.new_name("consumer_unreachable"))
      env.mark("consumer-storage-failure-passed")
    }
  }
}
