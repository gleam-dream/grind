//// A pool and its checkout deadline share one supervised lifetime. The
//// deadline owner starts first and stops last, including failed child starts.

import gleam/dynamic.{type Dynamic}
import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import pog

pub fn supervised(
  config: pog.Config,
  deadline_ms: Int,
) -> supervision.ChildSpecification(static_supervisor.Supervisor) {
  static_supervisor.new(static_supervisor.OneForAll)
  |> static_supervisor.add(
    // This lifecycle owner must outlive every admitted application caller and
    // pgo descendant. A public bounded close may report StopTimedOut while it
    // continues draining; the parent must not force-kill it after five seconds.
    supervision.supervisor(fn() {
      case start_deadline_owner(config.pool_name, deadline_ms) {
        Ok(pid) -> Ok(actor.Started(pid, Nil))
        Error(reason) -> Error(actor.InitExited(process.Abnormal(reason)))
      }
    }),
  )
  |> static_supervisor.add(
    supervision.supervisor(fn() {
      managed_start(config.pool_name, fn() { pog.start(config) })
    }),
  )
  |> static_supervisor.supervised
}

@external(erlang, "grind_pool_ffi", "start_deadline_owner")
fn start_deadline_owner(
  name: process.Name(pog.Message),
  deadline_ms: Int,
) -> Result(process.Pid, Dynamic)

/// A public Database owns an unlinked root. Isolate failed OTP start links too:
/// a child initialization failure must return its error, never kill the caller.
@external(erlang, "grind_pool_ffi", "start_unlinked")
pub fn start_unlinked(
  start: fn() -> Result(actor.Started(a), actor.StartError),
) -> Result(actor.Started(a), actor.StartError)

/// An unsuccessful start returns no handle that can later close its tree.
/// Therefore it waits for shutdown, preserving the original startup error.
@external(erlang, "grind_pool_ffi", "abort_start")
pub fn abort_start(pid: process.Pid) -> Nil

@external(erlang, "grind_pool_ffi", "managed_start")
fn managed_start(
  name: process.Name(pog.Message),
  start: fn() -> actor.StartResult(pog.Connection),
) -> actor.StartResult(pog.Connection)
