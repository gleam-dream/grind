import exception
import gleam/erlang/process
import gleeunit/should
import grind/internal/queue/renewer
import pog

pub fn renewer_stops_when_coordinator_exits_normally_test() {
  let ready = process.new_subject()
  let parent =
    process.spawn_unlinked(fn() {
      let finish = process.new_subject()
      let connection =
        pog.named_connection(process.new_name("unused_renewal_test_pool"))
      let assert Ok(child) =
        renewer.start(
          connection,
          "lifetime-test",
          "owner",
          3000,
          1000,
          fn(_, _, _) { Nil },
        )
      process.send(ready, #(child.pid, finish))
      let assert Ok(Nil) = process.receive(finish, 5000)
      // Returning exits normally. A link alone ignores this reason.
      Nil
    })
  let assert Ok(#(pid, finish)) = process.receive(ready, 1000)
  use <- exception.defer(fn() { process.kill(pid) })
  let parent_monitor = process.monitor(parent)
  let parent_selector =
    process.new_selector()
    |> process.select_specific_monitor(parent_monitor, fn(down) { down })
  let monitor = process.monitor(pid)
  let selector =
    process.new_selector()
    |> process.select_specific_monitor(monitor, fn(_) { Nil })
  process.send(finish, Nil)
  let assert Ok(process.ProcessDown(reason: process.Normal, ..)) =
    process.selector_receive(parent_selector, 1000)
  process.selector_receive(selector, 1000) |> should.equal(Ok(Nil))
}
