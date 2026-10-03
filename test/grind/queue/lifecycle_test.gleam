import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleam/list
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/postgres
import grind/internal/registry
import grind/internal/worker
import grind/support/env.{mark_database_test_executed, queue_database_url}

pub fn postgres_consumer_stop_cleans_only_its_own_normal_exit_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let reply = process.new_subject()
      let _ =
        process.spawn_unlinked(fn() {
          process.trap_exits(True)
          stop_cycles(url)
          process.send(reply, Nil)
        })
      process.receive(reply, 10_000) |> should.equal(Ok(Nil))
      mark_database_test_executed("consumer-stop-normal-exit-cleaned")
    }
  }
}

fn stop_cycles(url: String) -> Nil {
  let assert Ok(settings) = postgres.settings(url) |> postgres.validate
  let assert Ok(database) = postgres.start(settings)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(codec) =
    worker.codec("lifecycle-int", worker.infallible(json.int), decode.int)
  let assert Ok(definition) =
    worker.define("lifecycle.echo", "v1", codec, codec, Ok)
  let assert Ok(workers) = registry.new("lifecycle")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_manual_polling
    |> queue.validate_policy
  let exits =
    process.new_selector()
    |> process.select_trapped_exits(fn(exit) { exit })
  list.each([1, 2, 3], fn(_) {
    let assert Ok(consumer) = queue.start(database, workers, policy)
    let ready = process.new_subject()
    let other =
      process.spawn(fn() {
        let release = process.new_subject()
        process.send(ready, release)
        let assert Ok(Nil) = process.receive(release, 1000)
        Nil
      })
    let monitor = process.monitor(other)
    let down =
      process.new_selector()
      |> process.select_specific_monitor(monitor, fn(down) { down })
    let assert Ok(release) = process.receive(ready, 1000)
    process.send(release, Nil)
    let assert Ok(process.ProcessDown(reason: process.Normal, ..)) =
      process.selector_receive(down, 1000)
    queue.stop(consumer) |> should.equal(Ok(queue.StoppedCleanly))
    process.selector_receive(exits, 1000)
    |> should.equal(Ok(process.ExitMessage(other, process.Normal)))
    process.selector_receive(exits, 20) |> should.equal(Error(Nil))
  })
}
