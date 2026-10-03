//// A Grind runtime's registered state: the `Database` its consumers and the
//// facade use, found by the runtime's name.

import exception
import gleam/erlang/process
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import gleam/string
import grind/internal/consumer
import grind/internal/postgres
import grind/internal/registry
import pog

/// A handle on a runtime, found by name.
pub type Grind {
  Grind(name: process.Name(Message))
}

pub opaque type Message {
  GetRuntime(reply: process.Subject(Runtime))
}

/// One queue a node runs: its registry and validated consumer policies.
pub type QueuePlan {
  QueuePlan(
    name: String,
    workers: registry.Registry,
    policy: consumer.ValidatedPolicy,
    manual: consumer.ValidatedPolicy,
    grace_ms: Int,
  )
}

pub type Runtime {
  Runtime(
    database: postgres.Database,
    root: process.Pid,
    supervised: Bool,
    /// The queues this node consumes, with their coordinators' names.
    queues: List(#(QueuePlan, process.Name(consumer.Message))),
    /// Every queue the node's workers use, consumed or not.
    plans: List(QueuePlan),
  )
}

/// The runtime as a supervised child registered under `name`. It reads the
/// installation identity over the already-running pool, waiting up to the
/// connect timeout, and sends a failure to `failures` when given one.
pub fn child(
  validated: postgres.ValidatedSettings,
  name: process.Name(Message),
  supervised: Bool,
  queues: List(#(QueuePlan, process.Name(consumer.Message))),
  plans: List(QueuePlan),
  failures: Option(process.Subject(pog.QueryError)),
  connect_timeout_ms: Int,
) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() {
    actor.new_with_initialiser(connect_timeout_ms + 5000, fn(subject) {
      let root = parent_pid()
      case postgres.attach(validated, root) {
        Error(postgres.InstallationQueryFailed(error)) -> {
          case failures {
            Some(failures) -> process.send(failures, error)
            None -> Nil
          }
          Error("grind: the database is unavailable: " <> string.inspect(error))
        }
        Error(postgres.PoolStartFailed(_)) ->
          Error("grind: the database pool did not start")
        Ok(database) ->
          Ok(
            actor.initialised(Runtime(
              database:,
              root:,
              supervised:,
              queues:,
              plans:,
            ))
            |> actor.returning(subject),
          )
      }
    })
    |> actor.named(name)
    |> actor.on_message(fn(state, message) {
      case message {
        GetRuntime(reply) -> {
          process.send(reply, state)
          actor.continue(state)
        }
      }
    })
    |> actor.start
    |> result.map(fn(started) { actor.Started(started.pid, Nil) })
  })
}

/// The running runtime's state, or `Error(Nil)` when none is registered
/// under the handle's name.
pub fn lookup(grind: Grind) -> Result(Runtime, Nil) {
  case process.named(grind.name) {
    Error(Nil) -> Error(Nil)
    Ok(pid) -> {
      let monitor = process.monitor(pid)
      let reply = process.new_subject()
      let sent =
        exception.rescue(fn() {
          process.send(process.named_subject(grind.name), GetRuntime(reply))
        })
      let result = case sent {
        Error(_) -> Error(Nil)
        Ok(Nil) ->
          process.new_selector()
          |> process.select_map(reply, Ok)
          |> process.select_specific_monitor(monitor, fn(_) { Error(Nil) })
          |> process.selector_receive(within: 5000)
          |> result.flatten
      }
      process.demonitor_process(monitor)
      result
    }
  }
}

/// The running runtime's database.
pub fn database(grind: Grind) -> Result(postgres.Database, Nil) {
  lookup(grind) |> result.map(fn(runtime) { runtime.database })
}

@external(erlang, "grind_runtime_ffi", "put_pool")
pub fn record_pool(
  name: process.Name(Message),
  pool: process.Name(pog.Message),
) -> Nil

@external(erlang, "grind_runtime_ffi", "get_pool")
pub fn recorded_pool(
  name: process.Name(Message),
) -> Result(process.Name(pog.Message), Nil)

@external(erlang, "grind_runtime_ffi", "parent_pid")
fn parent_pid() -> process.Pid
