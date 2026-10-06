//// Grind runs typed background jobs on PostgreSQL and Erlang/OTP.
////
//// ```gleam
//// import gleam/otp/static_supervisor as supervisor
//// import gleam/time/duration
//// import grind
//// import grind/job
////
//// pub fn children(pool: pog.Config, name: process.Name(grind.Message)) {
////   let config =
////     grind.new(pool)
////     |> grind.with_worker(mailer())
////     |> grind.with_startup_migration
////   supervisor.new(supervisor.OneForOne)
////   |> supervisor.add(grind.supervised(config, name))
//// }
////
//// pub fn send(name: process.Name(grind.Message), email: Email) {
////   let jobs = grind.named(name)
////   let assert Ok(admission) = grind.submit(jobs, job.new(mailer(), email))
////   grind.await(jobs, grind.handle(admission), within: duration.seconds(5))
//// }
//// ```
////
//// One child runs everything a node needs: the PostgreSQL pool built from
//// the application's `pog.Config`, an observation forwarder, one consumer
//// per queue its workers use, and the pruner. `grind.connection` (and
//// `worker.connection` inside a handler) returns that pool, so the
//// application, Grind and other libraries share it; `submit_in` enqueues
//// inside the application's own transaction. A runtime that runs
//// consumers starts only on a current schema: `with_startup_migration`
//// applies the migrations first, or migrate at deploy time with `migrate`
//// from a runtime `without_consumers`, or apply `priv/migrations` through
//// cigogne.
////
//// | Operation                      | Default                        | Setter                          |
//// | ------------------------------ | ------------------------------ | ------------------------------- |
//// | storage call                   | 4 s; queued checkout may exceed it | `with_statement_deadline`    |
//// | initial connect at start       | 15 s                           | `with_connect_timeout`          |
//// | uniqueness lock wait           | 2 s                            | `with_unique_lock_wait`         |
//// | migration step                 | 30 s                           | `with_migration_deadline`       |
//// | encoded input, output, error   | 1 MiB, then `PayloadTooLarge`  | `with_max_payload_bytes`        |
//// | job retention                  | pruner on, 7 days              | `with_pruner`, `without_pruner` |
//// | observation forwarder          | 1,024 events in flight         | `with_observation_capacity`     |
//// | waiting for a result           | `await(within:)` takes a bound |                                 |
////
//// Worker and queue defaults are in `grind/worker` and `grind/queue`.
//// Configuration is checked by `start` and `supervised` (`check` returns
//// the same typed error up front). The errors and outcomes here may gain
//// variants: branch on `submit_error_kind`, `read_error_kind` and
//// `cancel_error_kind` where a new variant should not break your code.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result
import gleam/set
import gleam/string
import gleam/time/duration.{type Duration}
import grind/internal/admission
import grind/internal/consumer
import grind/internal/convert
import grind/internal/job as internal_job
import grind/internal/postgres
import grind/internal/pruner
import grind/internal/queue_config
import grind/internal/registry
import grind/internal/runtime
import grind/internal/submission
import grind/internal/unique as internal_unique
import grind/internal/unique_admission
import grind/internal/worker as definition
import grind/job.{type JobHandle, type State}
import grind/queue.{type Queue}
import grind/worker.{type Worker}
import pog
import sinal/correlation

// -- Configuration ------------------------------------------------------------

/// A node's Grind configuration. Build it with `new` and the `with_*`
/// setters; `start` and `supervised` check it.
pub opaque type Config {
  Config(
    pool: fn() -> pog.Config,
    workers: List(Registration),
    queues: List(Queue),
    schema: String,
    statement_deadline_ms: Int,
    unique_lock_wait_ms: Int,
    migration_deadline_ms: Int,
    connect_timeout_ms: Int,
    max_payload_bytes: Int,
    observation_capacity: Int,
    pruner: Option(Int),
    consumers: Bool,
    startup_migration: Bool,
  )
}

/// One registered worker, erased to what the runtime needs.
type Registration {
  Registration(
    id: String,
    version: String,
    queue: String,
    register: fn(registry.Registry) ->
      Result(registry.Registry, registry.RegisterError),
  )
}

/// The default job retention: 7 days.
const default_max_age_ms = 604_800_000

/// A configuration that builds Grind's pool from the application's
/// `pog.Config`, keeping its pool name, size and `search_path`. Grind adds
/// two connection parameters: `READ COMMITTED` isolation, and an
/// idle-in-transaction timeout of twice the statement deadline. Grind's own
/// statements run under its schema (`with_schema`), set for each storage
/// call and restored before the connection returns to the pool, so the
/// application's unqualified tables keep resolving through its own
/// `search_path`. The configuration is held in a closure, so its password
/// does not print.
pub fn new(pool: pog.Config) -> Config {
  Config(
    pool: fn() { pool },
    workers: [],
    queues: [],
    schema: "public",
    statement_deadline_ms: 4000,
    unique_lock_wait_ms: 2000,
    migration_deadline_ms: 30_000,
    connect_timeout_ms: postgres.default_connect_timeout_ms,
    max_payload_bytes: postgres.default_max_payload_bytes,
    observation_capacity: 1024,
    pruner: Some(default_max_age_ms),
    consumers: True,
    startup_migration: False,
  )
}

/// Registers a worker. Its queue runs on this node unless consumers are
/// off; a submit-only node registers no workers.
pub fn with_worker(
  config: Config,
  worker: Worker(input, output, error),
) -> Config {
  let registration =
    Registration(
      id: worker.id,
      version: worker.version,
      queue: worker.queue,
      register: fn(registry) { registry.register(registry, worker) },
    )
  Config(..config, workers: list.append(config.workers, [registration]))
}

/// Tunes one queue. The queue must be one a registered worker uses.
pub fn with_queue(config: Config, queue: Queue) -> Config {
  Config(..config, queues: list.append(config.queues, [queue]))
}

/// The PostgreSQL schema Grind reads and writes, `public` by default. The
/// schema is the unit of isolation between Grind installations in one
/// database. It scopes only Grind's own statements: queries the
/// application runs on `connection` keep the application's `search_path`.
pub fn with_schema(config: Config, schema: String) -> Config {
  Config(..config, schema:)
}

/// Sets the absolute storage deadline before checkout. A connection obtained
/// after expiry sends nothing; the pinned pool can wait past it while queued.
/// See docs/adr/0008-state-driver-deadline-and-installation-limits.md.
pub fn with_statement_deadline(config: Config, deadline: Duration) -> Config {
  Config(..config, statement_deadline_ms: duration.to_milliseconds(deadline))
}

/// Bounds the wait for a uniqueness admission's lock; it must end at least
/// one second before the statement deadline.
pub fn with_unique_lock_wait(config: Config, wait: Duration) -> Config {
  Config(..config, unique_lock_wait_ms: duration.to_milliseconds(wait))
}

/// Bounds each migration step.
pub fn with_migration_deadline(config: Config, deadline: Duration) -> Config {
  Config(..config, migration_deadline_ms: duration.to_milliseconds(deadline))
}

/// How long `start` waits for a first connection before it fails with
/// `Unavailable`.
pub fn with_connect_timeout(config: Config, timeout: Duration) -> Config {
  Config(..config, connect_timeout_ms: duration.to_milliseconds(timeout))
}

/// The largest encoded input, output or error, in bytes. A larger input
/// fails the submit with `PayloadTooLarge`; a larger output or error ends
/// the job as a runtime failure.
pub fn with_max_payload_bytes(config: Config, bytes: Int) -> Config {
  Config(..config, max_payload_bytes: bytes)
}

/// How many events the observation forwarder holds in flight before it
/// drops and counts them.
pub fn with_observation_capacity(config: Config, capacity: Int) -> Config {
  Config(..config, observation_capacity: capacity)
}

/// Deletes finished jobs, with their receipts, once they are older than
/// `max_age`: every 30 seconds, up to 10,000 at a time. It is on by default
/// with a `max_age` of 7 days, and safe to run on every node at once.
pub fn with_pruner(config: Config, max_age max_age: Duration) -> Config {
  Config(..config, pruner: Some(duration.to_milliseconds(max_age)))
}

/// Keeps finished jobs forever.
pub fn without_pruner(config: Config) -> Config {
  Config(..config, pruner: None)
}

/// Runs no consumers on this node: it submits and reads jobs only, like
/// Oban's `queues: false`.
pub fn without_consumers(config: Config) -> Config {
  Config(..config, consumers: False)
}

/// Applies missing schema migrations when the runtime starts, before any
/// consumer polls, exactly as `migrate` does; a failure fails the start
/// with `StartupMigrationFailed`. Without it, a runtime that would start
/// consumers refuses a schema behind this Grind's with `SchemaNotMigrated`.
/// Leave it off when the runtime's role may not run DDL, and migrate at
/// deploy time instead.
pub fn with_startup_migration(config: Config) -> Config {
  Config(..config, startup_migration: True)
}

/// A configuration value `start` or `supervised` rejected.
pub type ConfigError {
  /// A deadline, wait, timeout, limit, capacity or the pruner age is not
  /// positive. `setting` names it.
  NotPositive(setting: String, value: Int)
  /// The uniqueness lock wait must end at least `margin_ms` before the
  /// statement deadline.
  UniqueLockWaitTooCloseToDeadline(
    lock_wait_ms: Int,
    margin_ms: Int,
    deadline_ms: Int,
  )
  /// The migration deadline must exceed the migration lock timeout by at
  /// least `margin_ms`.
  MigrationDeadlineTooCloseToLockTimeout(
    deadline_ms: Int,
    lock_timeout_ms: Int,
    margin_ms: Int,
  )
  /// The schema is empty, longer than 63 bytes, contains a NUL, is
  /// `$user` or starts with `pg_`.
  InvalidSchema(schema: String)
  /// Two registered workers share an id and version.
  DuplicateWorker(id: String, version: String)
  /// A configured queue has no registered worker.
  QueueWithoutWorkers(queue: String)
  /// The same queue was configured twice.
  DuplicateQueue(queue: String)
  /// A queue setting is out of range. `setting` names it.
  InvalidQueue(queue: String, setting: String, value: Int)
  /// The queue's lease is shorter than four statement deadlines.
  LeaseTooShort(queue: String, lease_ms: Int, minimum_ms: Int)
}

/// Checks a configuration without starting anything. `start` and
/// `supervised` run the same checks.
pub fn check(config: Config) -> Result(Nil, ConfigError) {
  use _ <- result.try(settings(config))
  use _ <- result.try(queue_plans(config))
  Ok(Nil)
}

fn settings(config: Config) -> Result(postgres.ValidatedSettings, ConfigError) {
  use _ <- result.try(positive(
    "statement deadline",
    config.statement_deadline_ms,
  ))
  use _ <- result.try(positive(
    "uniqueness lock wait",
    config.unique_lock_wait_ms,
  ))
  use _ <- result.try(positive(
    "migration deadline",
    config.migration_deadline_ms,
  ))
  use _ <- result.try(positive("connect timeout", config.connect_timeout_ms))
  use _ <- result.try(positive("payload limit", config.max_payload_bytes))
  use _ <- result.try(positive(
    "observation capacity",
    config.observation_capacity,
  ))
  use _ <- result.try(case config.pruner {
    Some(max_age) -> positive("pruner max age", max_age)
    None -> Ok(Nil)
  })
  let pool = config.pool()
  use _ <- result.try(positive("pool size", pool.pool_size))
  postgres.settings_from_config(pool)
  |> postgres.with_schema(config.schema)
  |> postgres.with_statement_deadline(config.statement_deadline_ms)
  |> postgres.with_unique_lock_wait(config.unique_lock_wait_ms)
  |> postgres.with_migration_deadline(config.migration_deadline_ms)
  |> postgres.with_connect_timeout(config.connect_timeout_ms)
  |> postgres.with_max_payload_bytes(config.max_payload_bytes)
  |> postgres.with_observation_capacity(config.observation_capacity)
  |> postgres.validate
  |> result.map_error(fn(error) {
    case error {
      postgres.UniqueLockWaitTooCloseToDeadline ->
        UniqueLockWaitTooCloseToDeadline(
          lock_wait_ms: config.unique_lock_wait_ms,
          margin_ms: 1000,
          deadline_ms: config.statement_deadline_ms,
        )
      postgres.MigrationDeadlineTooCloseToLockTimeout ->
        MigrationDeadlineTooCloseToLockTimeout(
          deadline_ms: config.migration_deadline_ms,
          lock_timeout_ms: 2000,
          margin_ms: 1000,
        )
      // Every other setting was checked above.
      _ -> InvalidSchema(config.schema)
    }
  })
}

fn positive(setting: String, value: Int) -> Result(Nil, ConfigError) {
  case value > 0 {
    True -> Ok(Nil)
    False -> Error(NotPositive(setting:, value:))
  }
}

fn queue_plans(config: Config) -> Result(List(runtime.QueuePlan), ConfigError) {
  use _ <- result.try(unique_workers(config.workers, set.new()))
  let worker_queues =
    list.fold(config.workers, [], fn(queues, registration) {
      case list.contains(queues, registration.queue) {
        True -> queues
        False -> list.append(queues, [registration.queue])
      }
    })
  use _ <- result.try(
    list.try_fold(config.queues, set.new(), fn(seen, queue) {
      case
        set.contains(seen, queue.name),
        list.contains(worker_queues, queue.name)
      {
        True, _ -> Error(DuplicateQueue(queue.name))
        False, False -> Error(QueueWithoutWorkers(queue.name))
        False, True -> Ok(set.insert(seen, queue.name))
      }
    }),
  )
  list.try_map(worker_queues, fn(name) {
    let settings =
      list.find(config.queues, fn(queue) { queue.name == name })
      |> result.unwrap(queue_config.new(name))
    queue_plan(config, settings)
  })
}

fn unique_workers(
  workers: List(Registration),
  seen: set.Set(#(String, String)),
) -> Result(Nil, ConfigError) {
  case workers {
    [] -> Ok(Nil)
    [registration, ..rest] -> {
      let key = #(registration.id, registration.version)
      case set.contains(seen, key) {
        True -> Error(DuplicateWorker(id: key.0, version: key.1))
        False -> unique_workers(rest, set.insert(seen, key))
      }
    }
  }
}

fn queue_plan(
  config: Config,
  settings: Queue,
) -> Result(runtime.QueuePlan, ConfigError) {
  let queue_config.Queue(
    name:,
    concurrency:,
    poll_interval_ms:,
    lease_ms:,
    shutdown_grace_ms:,
  ) = settings
  use _ <- result.try(queue_positive(name, "concurrency", concurrency))
  use _ <- result.try(queue_positive(name, "poll interval", poll_interval_ms))
  use _ <- result.try(queue_positive(name, "lease", lease_ms))
  use _ <- result.try(case shutdown_grace_ms >= 0 {
    True -> Ok(Nil)
    False -> Error(InvalidQueue(name, "shutdown grace", shutdown_grace_ms))
  })
  let minimum_ms = 4 * config.statement_deadline_ms
  use _ <- result.try(case lease_ms < minimum_ms {
    True -> Error(LeaseTooShort(queue: name, lease_ms:, minimum_ms:))
    False -> Ok(Nil)
  })
  // The name is not empty (`queue.new` and `worker.with_queue` panic on
  // one) and duplicate workers were rejected above.
  let assert Ok(empty) = registry.new(name)
  let workers =
    list.fold(config.workers, empty, fn(registry, registration) {
      case registration.queue == name {
        False -> registry
        True -> {
          let assert Ok(registry) = registration.register(registry)
          registry
        }
      }
    })
  let base =
    consumer.default_policy()
    |> consumer.with_maximum_concurrency(concurrency)
    |> consumer.with_lease_duration(lease_ms)
    |> consumer.with_shutdown_grace(shutdown_grace_ms)
  // Every value was checked above.
  let assert Ok(policy) =
    base
    |> consumer.with_poll_interval(poll_interval_ms)
    |> consumer.validate_policy
  let assert Ok(manual) =
    base
    |> consumer.with_manual_polling
    |> consumer.with_maximum_concurrency(1)
    |> consumer.validate_policy
  Ok(runtime.QueuePlan(
    name:,
    workers:,
    policy:,
    manual:,
    grace_ms: shutdown_grace_ms,
  ))
}

fn queue_positive(
  queue: String,
  setting: String,
  value: Int,
) -> Result(Nil, ConfigError) {
  case value > 0 {
    True -> Ok(Nil)
    False -> Error(InvalidQueue(queue:, setting:, value:))
  }
}

// -- Runtime ------------------------------------------------------------------

/// A handle on a node's Grind runtime, found by name. It stays valid before
/// the runtime starts and across its restarts.
pub type Grind =
  runtime.Grind

/// The runtime's messages. Create the name `start` or `supervised`
/// registers with `process.new_name`, once.
pub type Message =
  runtime.Message

/// Why `start` failed.
pub type StartError {
  InvalidConfig(ConfigError)
  /// No connection could be made within the connect timeout, or the first
  /// query failed.
  Unavailable(pog.QueryError)
  /// The runtime would start consumers, but the schema is at `found`
  /// (`None`: not installed), below the `required` version. Configure
  /// `with_startup_migration`, or run `migrate` from a runtime started
  /// `without_consumers` (or apply `priv/migrations`) first.
  SchemaNotMigrated(found: Option(Int), required: Int)
  /// `with_startup_migration` was set and the migration failed.
  StartupMigrationFailed(MigrateError)
  /// The runtime's processes could not start, for example because another
  /// runtime already uses this name or pool name. The description never
  /// includes an exit reason, which could carry the pool configuration.
  StartFailed(description: String)
}

/// The handle for the runtime registered under `name`.
pub fn named(name: process.Name(Message)) -> Grind {
  runtime.Grind(name)
}

/// Starts a runtime that no supervisor owns, registered under `name`. Use
/// `supervised` in an application; `start` suits scripts and tests. Stop it
/// with `stop`.
pub fn start(
  config: Config,
  name: process.Name(Message),
) -> Result(Grind, StartError) {
  use validated <- result.try(
    settings(config) |> result.map_error(InvalidConfig),
  )
  use plans <- result.try(
    queue_plans(config) |> result.map_error(InvalidConfig),
  )
  use Nil <- result.try(case process.named(name) {
    Ok(_) -> Error(StartFailed("a runtime is already running under this name"))
    Error(Nil) -> Ok(Nil)
  })
  runtime.record_pool(name, postgres.pool_name(validated))
  let failures = process.new_subject()
  let root =
    root_supervisor(config, validated, plans, name, False, Some(failures))
  case start_unlinked(fn() { static_supervisor.start(root) }) {
    Ok(_) -> Ok(runtime.Grind(name))
    Error(error) ->
      case process.receive(failures, within: 0) {
        Ok(runtime.DatabaseUnavailable(failure)) -> Error(Unavailable(failure))
        Ok(runtime.SchemaBehind(found:, required:)) ->
          Error(SchemaNotMigrated(found:, required:))
        Ok(runtime.StartupMigrationFailed(failure)) ->
          Error(StartupMigrationFailed(migrate_error(failure)))
        Error(Nil) -> Error(StartFailed(start_failure(error)))
      }
  }
}

/// The runtime as one child of the application's supervision tree,
/// registered under `name`. A configuration error, an unreachable
/// database, or a schema behind this Grind's when the node runs consumers
/// (see `with_startup_migration`) fails the child's start with a
/// description; call `check` first for a typed configuration error. On shutdown each queue stops claiming
/// and waits up to its grace for running jobs.
pub fn supervised(
  config: Config,
  name: process.Name(Message),
) -> supervision.ChildSpecification(Grind) {
  let checked = {
    use validated <- result.try(settings(config))
    use plans <- result.try(queue_plans(config))
    runtime.record_pool(name, postgres.pool_name(validated))
    Ok(root_supervisor(config, validated, plans, name, True, None))
  }
  supervision.supervisor(fn() {
    case checked {
      Ok(root) ->
        static_supervisor.start(root)
        |> result.map(fn(started) {
          actor.Started(started.pid, runtime.Grind(name))
        })
      Error(error) -> Error(actor.InitFailed(describe_config_error(error)))
    }
  })
}

fn start_failure(error: actor.StartError) -> String {
  case error {
    actor.InitTimeout -> "a runtime process did not start in time"
    actor.InitFailed(reason) -> "a runtime process failed to start: " <> reason
    actor.InitExited(_) -> "a runtime process exited while starting"
  }
}

@external(erlang, "grind_pool_ffi", "start_unlinked")
fn start_unlinked(
  start: fn() -> Result(actor.Started(a), actor.StartError),
) -> Result(actor.Started(a), actor.StartError)

/// The pool, the forwarder, the runtime, one consumer per queue and the
/// pruner, in start order. A child that restarts restarts every child after
/// it, so consumers always run against the current runtime.
fn root_supervisor(
  config: Config,
  validated: postgres.ValidatedSettings,
  plans: List(runtime.QueuePlan),
  name: process.Name(Message),
  is_supervised: Bool,
  failures: Option(process.Subject(runtime.Failure)),
) -> static_supervisor.Builder {
  let queues = case config.consumers {
    True ->
      list.map(plans, fn(plan) { #(plan, process.new_name("grind_consumer")) })
    False -> []
  }
  let startup = case config.startup_migration, queues {
    True, _ -> runtime.MigrateFirst
    False, [] -> runtime.SkipSchemaCheck
    False, _ -> runtime.RequireCurrentSchema
  }
  let database = fn() {
    runtime.database(runtime.Grind(name))
    |> result.replace_error("grind: the runtime is not running")
  }
  let root =
    static_supervisor.new(static_supervisor.RestForOne)
    |> static_supervisor.add(postgres.pool_child(validated))
    |> static_supervisor.add(postgres.forwarder_child(validated))
    |> static_supervisor.add(runtime.child(
      validated,
      name,
      is_supervised,
      queues,
      plans,
      failures,
      config.connect_timeout_ms,
      startup,
      config.migration_deadline_ms,
    ))
  let root =
    list.fold(queues, root, fn(root, entry) {
      let #(plan, coordinator) = entry
      static_supervisor.add(
        root,
        consumer.supervised(
          database,
          plan.workers,
          plan.policy,
          coordinator,
          process.new_name("grind_renewals"),
        ),
      )
    })
  case config.pruner {
    Some(max_age_ms) -> {
      // The age was checked by `settings`; the other values are defaults.
      let assert Ok(policy) =
        pruner.default_policy()
        |> pruner.with_max_age(max_age_ms)
        |> pruner.validate_policy
      static_supervisor.add(
        root,
        pruner.supervised_from(
          database,
          policy,
          process.new_name("grind_pruner"),
        ),
      )
    }
    None -> root
  }
}

/// The runtime's pool, shared with the application: run your own queries
/// on it, or open the transaction that `submit_in` enqueues inside. It is
/// `pog.named_connection` of the configuration's pool name, and its
/// `search_path` is the application's: Grind sets its own schema for its
/// own statements only. A handler reaches the same pool with
/// `worker.connection(context)`. Panics when no runtime was configured
/// under this name on this node with `start` or `supervised`.
pub fn connection(grind: Grind) -> pog.Connection {
  case runtime.recorded_pool(grind.name) {
    Ok(pool) -> pog.named_connection(pool)
    Error(Nil) ->
      panic as "grind.connection: no Grind runtime was configured under this name on this node"
  }
}

/// How a `stop` ended.
pub type StopOutcome {
  /// Every queue finished its running jobs within its grace.
  StoppedCleanly
  /// These attempts were still running when their grace expired. Their
  /// claims are recovered when their leases expire.
  StoppedWithActiveWork(active_attempts: Int)
}

pub type StopError {
  /// No runtime is running under this name.
  NotStarted
  /// The runtime belongs to an application supervisor; stop it there.
  OwnedBySupervisor
  /// The runtime's processes did not confirm their shutdown in time.
  StopTimedOut
}

/// Stops a runtime started with `start`, from any process: each queue
/// stops claiming and waits up to its grace for running jobs, then every
/// process stops, the pool last.
pub fn stop(grind: Grind) -> Result(StopOutcome, StopError) {
  case runtime.lookup(grind) {
    Error(Nil) -> Error(NotStarted)
    Ok(runtime.Runtime(supervised: True, ..)) -> Error(OwnedBySupervisor)
    Ok(runtime.Runtime(root:, queues:, ..)) -> {
      let reply = process.new_subject()
      list.each(queues, fn(entry) {
        let #(plan, coordinator) = entry
        process.spawn(fn() {
          process.send(reply, consumer.drain(coordinator, plan.grace_ms))
        })
      })
      let active =
        list.fold(queues, 0, fn(total, entry) {
          let #(plan, _) = entry
          case process.receive(reply, within: plan.grace_ms + 2000) {
            Ok(consumer.DrainedWithActiveWork(active)) -> total + active
            _ -> total
          }
        })
      case stop_supervisor(root) {
        Error(Nil) -> Error(StopTimedOut)
        Ok(_) ->
          case active {
            0 -> Ok(StoppedCleanly)
            _ -> Ok(StoppedWithActiveWork(active))
          }
      }
    }
  }
}

@external(erlang, "grind_postgres_ffi", "stop_supervisor")
fn stop_supervisor(pid: process.Pid) -> Result(Bool, Nil)

/// Why `migrate` failed.
pub type MigrateError {
  /// The runtime is not running.
  MigrateNotRunning
  /// The schema belongs to an unknown or newer Grind, or was altered.
  IncompatibleSchema
  /// The schema is at a version this Grind does not know.
  UnsupportedSchemaVersion(version: Int)
  /// A migration step failed and rolled back; earlier steps stay applied.
  MigrationStepFailed(version: Int, reason: pog.QueryError)
  /// A step's lock wait expired; retry.
  MigrationLockUnavailable(version: Int)
  /// A step may or may not have committed; retrying is always safe.
  MigrationCommitUnknown(version: Int)
  /// The database could not be reached, or the schema could not be created.
  MigrationUnavailable(pog.QueryError)
}

/// Applies every missing schema migration, each in its own transaction
/// under an advisory lock. Running it again, or on many nodes at once, is
/// safe. A runtime that runs consumers needs a current schema to start, so
/// call this from a runtime started `without_consumers` (a deploy step), or
/// configure `with_startup_migration` instead.
pub fn migrate(grind: Grind) -> Result(Nil, MigrateError) {
  case runtime.database(grind) {
    Error(Nil) -> Error(MigrateNotRunning)
    Ok(database) ->
      postgres.migrate(database) |> result.map_error(migrate_error)
  }
}

fn migrate_error(error: postgres.StorageError) -> MigrateError {
  case error {
    postgres.MigrationQueryFailed(reason) -> MigrationUnavailable(reason)
    postgres.SchemaCreationFailed(reason) -> MigrationUnavailable(reason)
    postgres.IncompatibleSchema -> IncompatibleSchema
    postgres.UnsupportedSchemaVersion(version) ->
      UnsupportedSchemaVersion(version)
    postgres.MigrationStepFailed(version, reason) ->
      MigrationStepFailed(version, reason)
    postgres.MigrationLockUnavailable(version) ->
      MigrationLockUnavailable(version)
    postgres.MigrationCommitUnknown(version) -> MigrationCommitUnknown(version)
  }
}

// -- Submission ---------------------------------------------------------------

/// What a submit admitted. A job without `job.unique` is always
/// `Inserted`; `handle` returns the job either way, so a plain submit needs
/// no `case`:
///
/// ```gleam
/// use admission <- result.try(grind.submit(jobs, job.new(mailer(), email)))
/// grind.await(jobs, grind.handle(admission), within: duration.seconds(5))
/// ```
pub type Admission(input, output, error) {
  /// A new job.
  Inserted(JobHandle(input, output, error))
  /// A job already occupies the uniqueness key; nothing was inserted.
  Existing(Conflict(input, output, error))
  /// The occupying scheduled job was moved to the policy's time.
  Rescheduled(Conflict(input, output, error))
}

/// The stored job a uniqueness conflict found. `state` is its state when
/// the decision was made. `handle` reads it with the submitted worker's
/// codecs: the occupying job has the same worker id and version, and a
/// read still checks its stored codec versions.
pub type Conflict(input, output, error) {
  Conflict(
    job_id: Int,
    queue: String,
    state: State,
    handle: JobHandle(input, output, error),
  )
}

/// The admitted job: the new one, or the one occupying the uniqueness key.
pub fn handle(
  admission: Admission(input, output, error),
) -> JobHandle(input, output, error) {
  case admission {
    Inserted(handle) -> handle
    Existing(conflict) | Rescheduled(conflict) -> conflict.handle
  }
}

/// A submit whose commit could not be confirmed. Pass it to
/// `reconcile_submission`.
pub type PendingSubmission(input, output, error) =
  submission.PendingSubmission(input, output, error)

pub type SubmitError(input, output, error) {
  /// The input codec, or a uniqueness key's codec, rejected the value.
  /// Nothing was written.
  InvalidInput(reason: String)
  /// The encoded input is larger than the payload limit. Nothing was
  /// written.
  PayloadTooLarge(bytes: Int, limit: Int)
  /// `job.with_id` was given an empty id. Nothing was written.
  EmptyJobId
  /// The job's id was already used for a different job.
  IdConflict
  /// The uniqueness lock wait expired. Nothing was written; retry.
  UniquenessContended
  /// The admission did not commit. Retry, under the same id when it has
  /// one.
  NotCommitted(reason: pog.QueryError)
  /// The admission may or may not have committed. Retry the same job, or
  /// pass the pending submission to `reconcile_submission`.
  CommitUnknown(PendingSubmission(input, output, error))
  /// `submit_in` was given a pool, not a transaction.
  NotInTransaction
  /// `submit_in`'s transaction is not `READ COMMITTED`.
  TransactionIsolationUnsupported(isolation: String)
  /// The transaction or pending submission belongs to another database or
  /// schema.
  WrongDatabase
  /// No runtime is running under this name on this node.
  SubmitNotRunning
}

/// Admits a job in its own transaction. Every admission records a receipt,
/// under the caller's submission key or a generated key. Retain the pending
/// command after `CommitUnknown`; reconciliation depends on receipt retention.
/// Emits `[grind, job, admitted]` once the commit is proven.
pub fn submit(
  grind: Grind,
  job: job.Job(input, output, error),
) -> Result(Admission(input, output, error), SubmitError(input, output, error)) {
  use database <- result.try(
    runtime.database(grind) |> result.replace_error(SubmitNotRunning),
  )
  use spec <- result.try(spec_for(job))
  postgres.admit(database, spec)
  |> result.map(admission_of(_, postgres.installation(database), spec.worker))
  |> result.map_error(submit_error)
}

/// Admits a job inside the application's open transaction `tx`, so it
/// commits or rolls back with the application's own writes. Grind sends no
/// `BEGIN` or `COMMIT`. The transaction must be `READ COMMITTED` and on
/// Grind's database; Grind sets `search_path` and `lock_timeout` for its
/// own statements, then restores yours. When your commit's outcome is
/// unknown, resubmit the same job under the same id (`job.with_id`).
///
/// ```gleam
/// pog.transaction(grind.connection(jobs), fn(tx) {
///   use _ <- result.try(orders.confirm(tx, order))
///   grind.submit_in(jobs, tx, job.new(receipt(), order.id))
///   |> result.map_error(ReceiptNotQueued)
/// })
/// ```
///
/// Grind emits no `[grind, job, admitted]` event here, because it cannot
/// see whether your commit succeeded; the job's first event is `claimed`.
/// To trace the admission, record it yourself once `pog.transaction`
/// returns `Ok`: the admission's `job.id(grind.handle(admission))`, and
/// the correlation you gave the job with `job.with_correlation`, join it to
/// the job's later events.
pub fn submit_in(
  grind: Grind,
  tx: pog.Connection,
  job: job.Job(input, output, error),
) -> Result(Admission(input, output, error), SubmitError(input, output, error)) {
  use database <- result.try(
    runtime.database(grind) |> result.replace_error(SubmitNotRunning),
  )
  use spec <- result.try(spec_for(job))
  postgres.admit_in(database, tx, spec)
  |> result.map(admission_of(_, postgres.installation(database), spec.worker))
  |> result.map_error(submit_error)
}

/// Settles a `CommitUnknown` by reading the receipt the submission would
/// have written. A matching receipt returns the original admission; no
/// receipt yet is another `CommitUnknown`.
pub fn reconcile_submission(
  grind: Grind,
  pending: PendingSubmission(input, output, error),
) -> Result(Admission(input, output, error), SubmitError(input, output, error)) {
  use database <- result.try(
    runtime.database(grind) |> result.replace_error(SubmitNotRunning),
  )
  postgres.reconcile_unique(database, pending)
  |> result.map(admission_of(
    _,
    submission.pending_submission_installation(pending),
    submission.pending_submission_worker(pending),
  ))
  |> result.map_error(submit_error)
}

fn spec_for(
  job: job.Job(input, output, error),
) -> Result(
  unique_admission.Spec(input, output, error),
  SubmitError(input, output, error),
) {
  let admission.Job(
    worker:,
    input:,
    id:,
    when:,
    unique:,
    correlation:,
    queue:,
    max_attempts:,
  ) = job
  use submission_id <- result.try(case id {
    None -> Ok(submission.generated_submission_id())
    Some(id) -> submission.submission_id(id) |> result.replace_error(EmptyJobId)
  })
  let worker = case max_attempts {
    Some(max_attempts) -> definition.Worker(..worker, max_attempts:)
    None -> worker
  }
  let availability = case when {
    admission.Now -> submission.Immediately
    admission.AtUnixMs(ms) ->
      submission.At(internal_job.AvailableAt(int.max(ms, 0)))
    admission.AfterMs(ms) -> submission.Delayed(ms)
  }
  let correlation = case correlation {
    Some(correlation) -> correlation
    None -> correlation.unique()
  }
  Ok(unique_admission.Spec(
    queue: option.unwrap(queue, worker.queue),
    submission_id:,
    worker:,
    input:,
    availability:,
    policy: option.map(unique, fn(uniqueness) {
      let internal_unique.Uniqueness(policy:, on_conflict:) = uniqueness
      #(policy, on_conflict)
    }),
    correlation: Some(correlation.to_string(correlation)),
  ))
}

fn admission_of(
  admission: submission.Admission(input, output, error),
  installation: internal_job.Installation,
  worker: Worker(input, output, error),
) -> Admission(input, output, error) {
  case admission {
    submission.Inserted(handle) -> Inserted(handle)
    submission.Existing(conflict) ->
      Existing(conflict_of(conflict, installation, worker))
    submission.Rescheduled(conflict) ->
      Rescheduled(conflict_of(conflict, installation, worker))
  }
}

fn conflict_of(
  conflict: submission.Conflict,
  installation: internal_job.Installation,
  worker: Worker(input, output, error),
) -> Conflict(input, output, error) {
  let job_id = submission.conflict_job_id(conflict)
  let queue = submission.conflict_queue(conflict)
  Conflict(
    job_id:,
    queue:,
    state: convert.state(submission.conflict_state(conflict)),
    handle: internal_job.JobHandle(
      id: job_id,
      installation:,
      queue:,
      worker_id: worker.id,
      worker_version: worker.version,
      input: worker.input,
      output: worker.output,
      error: worker.error,
    ),
  )
}

fn submit_error(
  error: submission.SubmitError(input, output, error),
) -> SubmitError(input, output, error) {
  case error {
    submission.EmptyQueueName -> InvalidInput("the job's queue is empty")
    submission.InvalidInput(reason) -> InvalidInput(reason)
    submission.PayloadTooLarge(bytes, limit) -> PayloadTooLarge(bytes, limit)
    submission.AdmissionContended -> UniquenessContended
    submission.SubmissionConflict -> IdConflict
    submission.NotCommitted(reason) -> NotCommitted(reason)
    submission.CommitUnknown(pending) -> CommitUnknown(pending)
    submission.NotInTransaction -> NotInTransaction
    submission.TransactionIsolationUnsupported(isolation) ->
      TransactionIsolationUnsupported(isolation)
    submission.HandleFromAnotherInstallation -> WrongDatabase
  }
}

// -- Reads --------------------------------------------------------------------

/// A job's last committed result.
pub type Outcome(output, error) {
  /// Not finished yet.
  Pending(state: State)
  Succeeded(output: output)
  /// Failed terminally. `cause` says why a business failure is terminal.
  Failed(
    failure: Failure(error),
    cause: Option(job.TerminalCause),
    description: String,
  )
  Discarded(reason: String)
  Cancelled(reason: String)
  /// The attempt's effect is unknown; resolve it with
  /// `admin.resolve_uncertain`.
  Uncertain(evidence: String)
}

/// What kind of failure ended a job.
pub type Failure(error) {
  /// The handler's typed error, stored by the worker's error codec.
  Business(error)
  /// A business failure with no stored error: the worker has no error
  /// codec, or the snooze limit ended the job.
  BusinessUnrecorded
  /// The input could not be decoded, or the output or error was rejected
  /// by its codec or exceeded the payload limit.
  RuntimeFailure
  /// The stored codec versions no longer match the registered worker.
  ContractMismatch
}

pub type ReadError {
  /// No stored job has this id.
  JobNotFound
  /// The stored job is in another queue than the handle says.
  QueueMismatch(expected: String, actual: String)
  /// The stored job belongs to another worker id or version.
  WorkerMismatch(
    expected_id: String,
    expected_version: String,
    actual_id: String,
    actual_version: String,
  )
  /// A stored codec version differs from the worker's.
  CodecMismatch(kind: job.CodecKind, expected: String, actual: String)
  /// A stored value did not decode with its codec.
  UndecodableValue(reason: String)
  /// A stored column holds a value Grind does not recognize.
  CorruptRecord(value: String)
  /// The handle belongs to another database or schema.
  HandleFromAnotherDatabase
  ReadUnavailable(pog.QueryError)
  /// No runtime is running under this name on this node.
  ReadNotRunning
}

/// Rebinds a stored job by its id to `worker`, checking that the stored
/// worker and codec versions match. Bind against the same database and
/// schema the id came from.
pub fn bind(
  grind: Grind,
  worker: Worker(input, output, error),
  id: Int,
) -> Result(JobHandle(input, output, error), ReadError) {
  use database <- result.try(read_database(grind))
  postgres.bind_handle(database, worker, id)
  |> result.map_error(read_error)
}

/// The job's typed input.
pub fn arguments(
  grind: Grind,
  handle: JobHandle(input, output, error),
) -> Result(input, ReadError) {
  use database <- result.try(read_database(grind))
  postgres.arguments(database, handle) |> result.map_error(read_error)
}

/// The job's stored state.
pub fn state(
  grind: Grind,
  handle: JobHandle(input, output, error),
) -> Result(State, ReadError) {
  use database <- result.try(read_database(grind))
  postgres.state(database, handle)
  |> result.map(convert.state)
  |> result.map_error(read_error)
}

/// The job's last committed result.
pub fn outcome(
  grind: Grind,
  handle: JobHandle(input, output, error),
) -> Result(Outcome(output, error), ReadError) {
  use database <- result.try(read_database(grind))
  read_outcome(database, handle)
}

/// Waits up to `within` for the job to leave the pending states, and
/// returns its outcome then: the finished result, `Uncertain`, or
/// `Pending(state)` when the time ran out. The budget is checked after each
/// storage read, so the last read can finish after `within`.
pub fn await(
  grind: Grind,
  handle: JobHandle(input, output, error),
  within within: Duration,
) -> Result(Outcome(output, error), ReadError) {
  use database <- result.try(read_database(grind))
  let give_up_at = monotonic_ms() + duration.to_milliseconds(within)
  await_loop(database, handle, give_up_at, 20)
}

fn await_loop(
  database: postgres.Database,
  handle: JobHandle(input, output, error),
  give_up_at: Int,
  pause_ms: Int,
) -> Result(Outcome(output, error), ReadError) {
  case read_outcome(database, handle) {
    Ok(Pending(_)) as pending -> {
      let now = monotonic_ms()
      case now >= give_up_at {
        True -> pending
        False -> {
          process.sleep(int.min(pause_ms, give_up_at - now))
          await_loop(database, handle, give_up_at, int.min(pause_ms * 2, 500))
        }
      }
    }
    other -> other
  }
}

@external(erlang, "grind_queue_ffi", "monotonic_ms")
fn monotonic_ms() -> Int

fn read_outcome(
  database: postgres.Database,
  handle: JobHandle(input, output, error),
) -> Result(Outcome(output, error), ReadError) {
  postgres.outcome(database, handle)
  |> result.map(fn(outcome) {
    case outcome {
      internal_job.Pending(state) -> Pending(convert.state(state))
      internal_job.SucceededWith(output) -> Succeeded(output)
      internal_job.BusinessFailedWith(error) ->
        Failed(Business(error), None, "worker returned an application error")
      internal_job.BusinessFailedWithCause(error, cause) ->
        Failed(
          Business(error),
          Some(convert.terminal_cause(cause)),
          "worker returned an application error",
        )
      internal_job.FailedOperationally(description) ->
        Failed(BusinessUnrecorded, None, description)
      internal_job.FailedOperationallyWithCause(description, cause) ->
        Failed(
          BusinessUnrecorded,
          Some(convert.terminal_cause(cause)),
          description,
        )
      internal_job.RuntimeFailedWith(description) ->
        Failed(RuntimeFailure, None, description)
      internal_job.ContractMismatchWith(description) ->
        Failed(ContractMismatch, None, description)
      internal_job.DiscardedWithReason(reason) -> Discarded(reason)
      internal_job.CancelledWithReason(reason) -> Cancelled(reason)
      internal_job.ReconciliationRequired(evidence) -> Uncertain(evidence)
    }
  })
  |> result.map_error(read_error)
}

fn read_database(grind: Grind) -> Result(postgres.Database, ReadError) {
  runtime.database(grind) |> result.replace_error(ReadNotRunning)
}

fn read_error(error: postgres.JobReadError) -> ReadError {
  case error {
    postgres.JobReadQueryFailed(reason) -> ReadUnavailable(reason)
    postgres.JobNotFound -> JobNotFound
    postgres.QueueRouteMismatch(expected:, actual:) ->
      QueueMismatch(expected:, actual:)
    postgres.WorkerContractMismatch(
      expected_id:,
      expected_version:,
      actual_id:,
      actual_version:,
    ) ->
      WorkerMismatch(
        expected_id:,
        expected_version:,
        actual_id:,
        actual_version:,
      )
    postgres.CodecContractMismatch(kind:, expected:, actual:) ->
      CodecMismatch(kind: convert.codec_kind(kind), expected:, actual:)
    postgres.CodecFailed(definition.CodecVersionMismatch(expected:, got:)) ->
      UndecodableValue(
        "stored codec version " <> got <> " is not the worker's " <> expected,
      )
    postgres.CodecFailed(definition.InvalidStoredJson(reason)) ->
      UndecodableValue(describe_decode_error(reason))
    postgres.InvalidStoredState(value) -> CorruptRecord(value)
    postgres.SucceededOutputMissing -> CorruptRecord("succeeded without output")
    postgres.ReceiptNotFound | postgres.ReceiptJobMismatch(..) -> JobNotFound
    postgres.HandleFromAnotherInstallation -> HandleFromAnotherDatabase
  }
}

fn describe_decode_error(error: json.DecodeError) -> String {
  case error {
    json.UnexpectedEndOfInput -> "unexpected end of input"
    json.UnexpectedByte(byte) -> "unexpected byte " <> byte
    json.UnexpectedSequence(sequence) -> "unexpected sequence " <> sequence
    json.UnableToDecode(errors) ->
      errors
      |> list.map(fn(error) {
        let decode.DecodeError(expected:, found:, path:) = error
        "expected "
        <> expected
        <> ", found "
        <> found
        <> " at "
        <> string.join(path, ".")
      })
      |> string.join("; ")
  }
}

// -- Cancellation -------------------------------------------------------------

/// What `cancel` did.
pub type CancelResult {
  /// The job had not started; it is cancelled.
  CancelledBeforeRun
  /// The job is running. Its handler's `worker.cancellation` fires at the
  /// attempt's next lease renewal, and its outcome becomes `Cancelled`
  /// unless it finishes first.
  CancellationRequested
  AlreadyCancelled
  /// The job is uncertain; resolve it with `grind/admin`.
  AlreadyUncertain
  AlreadyFinished(State)
}

pub type CancelError {
  CancelJobNotFound
  /// The stored job does not match the handle's queue or worker.
  CancelMismatch
  /// The cancellation may or may not have committed; cancelling again is
  /// safe.
  CancelCommitUnknown
  CancelUnavailable(pog.QueryError)
  CancelFromAnotherDatabase
  CancelNotRunning
}

/// Cancels a job. A job that has not started is cancelled at once; a
/// running job's handler is told through `worker.cancellation`.
pub fn cancel(
  grind: Grind,
  handle: JobHandle(input, output, error),
) -> Result(CancelResult, CancelError) {
  use database <- result.try(
    runtime.database(grind) |> result.replace_error(CancelNotRunning),
  )
  postgres.cancel(database, handle)
  |> result.map(fn(result) {
    case result {
      postgres.CancelledBeforeRun -> CancelledBeforeRun
      postgres.CancellationRequested -> CancellationRequested
      postgres.AlreadyCancelled -> AlreadyCancelled
      postgres.AlreadyUncertain -> AlreadyUncertain
      postgres.AlreadyFinished(state) -> AlreadyFinished(convert.state(state))
    }
  })
  |> result.map_error(fn(error) {
    case error {
      postgres.CancellationQueryFailed(reason) -> CancelUnavailable(reason)
      postgres.CancellationCommitUnknown | postgres.CancellationWriteRejected ->
        CancelCommitUnknown
      postgres.CancellationQueueMismatch
      | postgres.CancellationWorkerContractMismatch
      | postgres.CancellationInvalidStoredState(_) -> CancelMismatch
      postgres.CancellationJobNotFound -> CancelJobNotFound
      postgres.CancellationFromAnotherInstallation -> CancelFromAnotherDatabase
    }
  })
}

// -- Classification -----------------------------------------------------------

/// A stable classification of this module's errors, for callers that
/// should not match a union that may gain variants.
pub type ErrorKind {
  /// The request was wrong; retrying it unchanged fails the same way.
  Invalid
  NotFound
  /// The stored record does not match the request: another worker, queue,
  /// codec, id or database.
  Mismatch
  /// A lock was busy; retry.
  Contended
  /// The database or the runtime could not be reached; retry.
  Unreachable
  /// The write may or may not have committed; reconcile, or retry under the
  /// same id.
  Unknown
}

pub fn submit_error_kind(
  error: SubmitError(input, output, error),
) -> ErrorKind {
  case error {
    InvalidInput(_)
    | PayloadTooLarge(_, _)
    | EmptyJobId
    | NotInTransaction
    | TransactionIsolationUnsupported(_) -> Invalid
    IdConflict | WrongDatabase -> Mismatch
    UniquenessContended -> Contended
    NotCommitted(_) | SubmitNotRunning -> Unreachable
    CommitUnknown(_) -> Unknown
  }
}

pub fn read_error_kind(error: ReadError) -> ErrorKind {
  case error {
    JobNotFound -> NotFound
    QueueMismatch(..)
    | WorkerMismatch(..)
    | CodecMismatch(..)
    | UndecodableValue(_)
    | CorruptRecord(_)
    | HandleFromAnotherDatabase -> Mismatch
    ReadUnavailable(_) | ReadNotRunning -> Unreachable
  }
}

pub fn cancel_error_kind(error: CancelError) -> ErrorKind {
  case error {
    CancelJobNotFound -> NotFound
    CancelMismatch | CancelFromAnotherDatabase -> Mismatch
    CancelCommitUnknown -> Unknown
    CancelUnavailable(_) | CancelNotRunning -> Unreachable
  }
}

pub fn describe_config_error(error: ConfigError) -> String {
  case error {
    NotPositive(setting:, value:) ->
      "grind: the "
      <> setting
      <> " must be positive, got "
      <> int.to_string(value)
    UniqueLockWaitTooCloseToDeadline(lock_wait_ms:, margin_ms:, deadline_ms:) ->
      "grind: the uniqueness lock wait of "
      <> int.to_string(lock_wait_ms)
      <> " ms must end at least "
      <> int.to_string(margin_ms)
      <> " ms before the statement deadline of "
      <> int.to_string(deadline_ms)
      <> " ms; lower it with with_unique_lock_wait or raise the deadline with with_statement_deadline"
    MigrationDeadlineTooCloseToLockTimeout(
      deadline_ms:,
      lock_timeout_ms:,
      margin_ms:,
    ) ->
      "grind: the migration deadline of "
      <> int.to_string(deadline_ms)
      <> " ms must exceed the "
      <> int.to_string(lock_timeout_ms)
      <> " ms migration lock timeout by "
      <> int.to_string(margin_ms)
      <> " ms; raise it with with_migration_deadline"
    InvalidSchema(schema:) ->
      "grind: invalid schema name " <> string.inspect(schema)
    DuplicateWorker(id:, version:) ->
      "grind: worker " <> id <> " version " <> version <> " is registered twice"
    QueueWithoutWorkers(queue:) ->
      "grind: queue " <> queue <> " is configured but no worker uses it"
    DuplicateQueue(queue:) -> "grind: queue " <> queue <> " is configured twice"
    InvalidQueue(queue:, setting:, value:) ->
      "grind: queue "
      <> queue
      <> " has an invalid "
      <> setting
      <> " of "
      <> int.to_string(value)
    LeaseTooShort(queue:, lease_ms:, minimum_ms:) ->
      "grind: queue "
      <> queue
      <> " has a lease of "
      <> int.to_string(lease_ms)
      <> " ms, below the minimum of "
      <> int.to_string(minimum_ms)
      <> " ms (four statement deadlines); raise it with queue.with_lease"
  }
}

pub fn describe_start_error(error: StartError) -> String {
  case error {
    InvalidConfig(error) -> describe_config_error(error)
    Unavailable(reason) ->
      "grind: the database is unavailable: " <> string.inspect(reason)
    SchemaNotMigrated(found:, required:) ->
      runtime.describe_schema_behind(found, required)
    StartupMigrationFailed(error) ->
      "grind: the startup migration failed: " <> string.inspect(error)
    StartFailed(description:) ->
      "grind: the runtime did not start: " <> description
  }
}

pub fn describe_submit_error(
  error: SubmitError(input, output, error),
) -> String {
  case error {
    InvalidInput(reason:) -> "grind: the job's input was rejected: " <> reason
    PayloadTooLarge(bytes:, limit:) ->
      "grind: the encoded input of "
      <> int.to_string(bytes)
      <> " bytes exceeds the limit of "
      <> int.to_string(limit)
      <> " bytes; raise it with with_max_payload_bytes"
    EmptyJobId -> "grind: the job id is empty"
    IdConflict -> "grind: the job id was already used for a different job"
    UniquenessContended -> "grind: the uniqueness lock wait expired; retry"
    NotCommitted(reason:) ->
      "grind: the submit did not commit: " <> string.inspect(reason)
    CommitUnknown(_) ->
      "grind: the submit may have committed; reconcile it with reconcile_submission"
    NotInTransaction -> "grind: submit_in needs a transaction connection"
    TransactionIsolationUnsupported(isolation:) ->
      "grind: submit_in needs a READ COMMITTED transaction, not " <> isolation
    WrongDatabase ->
      "grind: the submission belongs to another database or schema"
    SubmitNotRunning -> "grind: no runtime is running under this name"
  }
}

pub fn describe_read_error(error: ReadError) -> String {
  case error {
    JobNotFound -> "grind: no such job"
    QueueMismatch(expected:, actual:) ->
      "grind: the job is in queue " <> actual <> ", not " <> expected
    WorkerMismatch(expected_id:, expected_version:, actual_id:, actual_version:) ->
      "grind: the job belongs to worker "
      <> actual_id
      <> " "
      <> actual_version
      <> ", not "
      <> expected_id
      <> " "
      <> expected_version
    CodecMismatch(kind:, expected:, actual:) ->
      "grind: the stored "
      <> codec_kind_name(kind)
      <> " codec version is "
      <> actual
      <> ", not "
      <> expected
    UndecodableValue(reason:) ->
      "grind: a stored value did not decode: " <> reason
    CorruptRecord(value:) -> "grind: unrecognized stored value " <> value
    HandleFromAnotherDatabase ->
      "grind: the handle belongs to another database or schema"
    ReadUnavailable(reason) ->
      "grind: the database is unavailable: " <> string.inspect(reason)
    ReadNotRunning -> "grind: no runtime is running under this name"
  }
}

pub fn describe_cancel_error(error: CancelError) -> String {
  case error {
    CancelJobNotFound -> "grind: no such job"
    CancelMismatch -> "grind: the stored job does not match the handle"
    CancelCommitUnknown ->
      "grind: the cancellation may have committed; cancel again"
    CancelUnavailable(reason) ->
      "grind: the database is unavailable: " <> string.inspect(reason)
    CancelFromAnotherDatabase ->
      "grind: the handle belongs to another database or schema"
    CancelNotRunning -> "grind: no runtime is running under this name"
  }
}

fn codec_kind_name(kind: job.CodecKind) -> String {
  case kind {
    job.InputCodec -> "input"
    job.OutputCodec -> "output"
    job.ErrorCodec -> "error"
  }
}
