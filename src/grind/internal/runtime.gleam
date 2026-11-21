//// A Grind runtime's registered state: the `Database` its consumers and the
//// facade use, found by the runtime's name.
////
//// A reply is copied into the process that asks, so each request returns
//// only what its caller needs. The queue plans hold every registered
//// worker; they stay in this process, and only `queue` hands one plan out.

import exception
import gleam/erlang/process
import gleam/int
import gleam/list
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
  GetDatabase(reply: process.Subject(postgres.Database))
  GetStopPlan(reply: process.Subject(StopPlan))
  GetQueue(queue: String, reply: process.Subject(QueueLookup))
}

/// What `grind.stop` needs: the root supervisor and each consumed queue's
/// coordinator with its grace, without the queue's workers.
pub type StopPlan {
  StopPlan(
    root: process.Pid,
    supervised: Bool,
    coordinators: List(#(process.Name(consumer.Message), Int)),
  )
}

/// The database with one queue's plan, when a node's worker uses that queue.
pub type QueueLookup {
  QueueLookup(database: postgres.Database, plan: Option(QueuePlan))
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

/// What the runtime does about the schema before its consumers start.
pub type Startup {
  /// Apply missing migrations first (`grind.with_startup_migration`).
  MigrateFirst
  /// Refuse to start consumers on a schema behind this Grind's migrations.
  RequireCurrentSchema
  /// No consumer starts on this node, so nothing polls the schema early.
  SkipSchemaCheck
}

/// Why the runtime did not start, for `grind.start`'s typed error.
pub type Failure {
  DatabaseUnavailable(pog.QueryError)
  /// The schema's applied version is `found` (`None`: not installed), below
  /// the `required` version this Grind's consumers run against.
  SchemaBehind(found: Option(Int), required: Int)
  StartupMigrationFailed(postgres.StorageError)
}

/// The runtime as a supervised child registered under `name`. It reads the
/// installation identity over the already-running pool, waiting up to the
/// connect timeout, then settles the schema as `startup` says, before any
/// consumer (a later child) starts. It sends a failure to `failures` when
/// given one.
pub fn child(
  validated: postgres.ValidatedSettings,
  name: process.Name(Message),
  supervised: Bool,
  queues: List(#(QueuePlan, process.Name(consumer.Message))),
  plans: List(QueuePlan),
  failures: Option(process.Subject(Failure)),
  connect_timeout_ms: Int,
  startup: Startup,
  migration_deadline_ms: Int,
) -> supervision.ChildSpecification(Nil) {
  let init_timeout_ms = case startup {
    MigrateFirst ->
      // One bounded transaction per version, plus the schema creation.
      connect_timeout_ms
      + 5000
      + migration_deadline_ms
      * { postgres.latest_schema_version() + 1 }
    RequireCurrentSchema | SkipSchemaCheck -> connect_timeout_ms + 5000
  }
  let fail = fn(failure: Failure) {
    case failures {
      Some(failures) -> process.send(failures, failure)
      None -> Nil
    }
    Error(describe_failure(failure))
  }
  supervision.worker(fn() {
    actor.new_with_initialiser(init_timeout_ms, fn(subject) {
      let root = parent_pid()
      case postgres.attach(validated, root) {
        Error(postgres.InstallationQueryFailed(error)) ->
          fail(DatabaseUnavailable(error))
        Error(postgres.PoolStartFailed(_)) ->
          Error("grind: the database pool did not start")
        Ok(database) ->
          case settle_schema(database, startup) {
            Error(failure) -> fail(failure)
            Ok(Nil) ->
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
      }
    })
    |> actor.named(name)
    |> actor.on_message(fn(state, message) {
      case message {
        GetDatabase(reply) -> {
          process.send(reply, state.database)
          actor.continue(state)
        }
        GetStopPlan(reply) -> {
          let Runtime(root:, supervised:, queues:, ..) = state
          let coordinators =
            list.map(queues, fn(entry) {
              let #(plan, coordinator) = entry
              #(coordinator, plan.grace_ms)
            })
          process.send(reply, StopPlan(root:, supervised:, coordinators:))
          actor.continue(state)
        }
        GetQueue(queue, reply) -> {
          let plan =
            list.find(state.plans, fn(plan) { plan.name == queue })
            |> option.from_result
          process.send(reply, QueueLookup(database: state.database, plan:))
          actor.continue(state)
        }
      }
    })
    |> actor.start
    |> result.map(fn(started) { actor.Started(started.pid, Nil) })
  })
}

fn settle_schema(
  database: postgres.Database,
  startup: Startup,
) -> Result(Nil, Failure) {
  case startup {
    SkipSchemaCheck -> Ok(Nil)
    MigrateFirst ->
      postgres.migrate(database) |> result.map_error(StartupMigrationFailed)
    RequireCurrentSchema -> {
      let required = postgres.latest_schema_version()
      case postgres.schema_version(database) {
        Error(error) -> Error(DatabaseUnavailable(error))
        // A newer Grind may have migrated first in a rolling deploy.
        Ok(Some(found)) if found >= required -> Ok(Nil)
        Ok(found) -> Error(SchemaBehind(found:, required:))
      }
    }
  }
}

/// The failure as the child's start error, which a supervisor reports.
pub fn describe_failure(failure: Failure) -> String {
  case failure {
    DatabaseUnavailable(error) ->
      "grind: the database is unavailable: " <> string.inspect(error)
    SchemaBehind(found:, required:) -> describe_schema_behind(found, required)
    StartupMigrationFailed(error) ->
      "grind: the startup migration failed: " <> string.inspect(error)
  }
}

pub fn describe_schema_behind(found: Option(Int), required: Int) -> String {
  let found = case found {
    None -> "not installed"
    Some(version) -> "at version " <> int.to_string(version)
  }
  "grind: the schema is "
  <> found
  <> ", but this node's consumers need version "
  <> int.to_string(required)
  <> "; configure grind.with_startup_migration, or run grind.migrate from a runtime without consumers (or apply priv/migrations) before starting this one"
}

/// The running runtime's database, or `Error(Nil)` when none is registered
/// under the handle's name.
pub fn database(grind: Grind) -> Result(postgres.Database, Nil) {
  request(grind, GetDatabase)
}

/// What stopping the runtime needs, without any queue's workers.
pub fn stop_plan(grind: Grind) -> Result(StopPlan, Nil) {
  request(grind, GetStopPlan)
}

/// The database and the plan of `queue`. The plan carries that queue's
/// workers, which are copied into the caller; use it only where the caller
/// runs them.
pub fn queue(grind: Grind, queue: String) -> Result(QueueLookup, Nil) {
  request(grind, GetQueue(queue, _))
}

fn request(
  grind: Grind,
  make: fn(process.Subject(reply)) -> Message,
) -> Result(reply, Nil) {
  case process.named(grind.name) {
    Error(Nil) -> Error(Nil)
    Ok(pid) -> {
      let monitor = process.monitor(pid)
      let reply = process.new_subject()
      let sent =
        exception.rescue(fn() {
          process.send(process.named_subject(grind.name), make(reply))
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
