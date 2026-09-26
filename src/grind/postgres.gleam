import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some, unwrap}
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result
import gleam/string
import grind/internal/lease
import grind/internal/migrations
import grind/internal/sql
import grind/internal/store
import grind/internal/unique_admission
import grind/job.{type JobHandle, type State, Queued, Scheduled}
import grind/observation
import grind/submission
import grind/unique
import grind/worker.{type Worker}
import pog
import sinal/forwarder.{type Forwarder}

/// Pure PostgreSQL pool settings. Validation does not acquire a resource.
/// Carries no pog type of its own — not `process.Name(pog.Message)` or
/// anything else pog-specific — so a caller never has to reach into pog's
/// own API just to configure Grind. The pool's own name is created once,
/// inside `validate`, and stays stable for that resulting `ValidatedSettings`
/// value's entire lifetime, across every `start`/`close` cycle; see
/// `validate` and `start`.
pub type Settings {
  Settings(
    database_url: String,
    pool_size: Int,
    unique_lock_wait_ms: Int,
    observation_capacity: Int,
    statement_deadline_ms: Int,
    migration_deadline_ms: Int,
  )
}

/// `statement_deadline_ms` defaults to 4000, not pog's own hardcoded
/// 5000 — chosen so the default `unique_lock_wait_ms` (2000) clears
/// `validate`'s own margin against it, and so `queue.start`'s lease rule
/// (`6 * statement_deadline_ms` at `maximum_concurrency > 1`) stays
/// comfortably under `queue.default_policy`'s 30 000 ms lease without
/// having to raise that default. See docs/RELEASE-READINESS.md,
/// "Acknowledgement deadline".
pub fn settings(database_url: String) -> Settings {
  Settings(
    database_url:,
    pool_size: 10,
    unique_lock_wait_ms: 2000,
    observation_capacity: 1024,
    statement_deadline_ms: 4000,
    migration_deadline_ms: 30_000,
  )
}

pub fn with_pool_size(settings: Settings, pool_size: Int) -> Settings {
  Settings(..settings, pool_size:)
}

/// Sets the bounded wait for `submit_unique`'s admission lock, in
/// milliseconds. Exceeding it surfaces as `AdmissionContended` rather
/// than blocking indefinitely. Also bounds `submit_with_id`'s internal wait
/// on the `grind_unique_submissions` primary key when a concurrent same-id
/// writer's insert is still uncommitted — `submit_with_id` has no
/// domain-wide advisory lock of its own (see
/// `docs/UNIQUENESS-CONTRACT.md`, "Admission receipts"), so this same
/// setting is what keeps that wait bounded too. Validated positive by
/// `validate`, before any pool starts.
pub fn with_unique_lock_wait(
  settings: Settings,
  milliseconds: Int,
) -> Settings {
  Settings(..settings, unique_lock_wait_ms: milliseconds)
}

/// Sets the Grind-owned checkout deadline, in milliseconds, that bounds
/// every Grind storage call against this pool — inline SQL, every
/// Squirrel-generated call, and a whole `pog.transaction` including its own
/// `BEGIN`/`COMMIT` (`src/grind_postgres_ffi.erl`). A lost reply on a
/// half-open socket (`docs/RECOVERY-EVIDENCE.md`, "Acknowledgement
/// deadline") surfaces as `QueueAckUnknown`/`pog.QueryTimeout` within
/// roughly this bound instead of hanging indefinitely. Validated positive,
/// and against `unique_lock_wait_ms`, by `validate`. Also sets the pooled
/// connection's own `idle_in_transaction_session_timeout` startup parameter
/// to twice this value, so a request whose *reply* is lost but which never
/// even reached PostgreSQL (the request itself dropped) does not leave a
/// real server session idle in transaction, holding row locks, forever —
/// see DEFECT 3 in docs/RELEASE-READINESS.md.
pub fn with_statement_deadline(
  settings: Settings,
  milliseconds: Int,
) -> Settings {
  Settings(..settings, statement_deadline_ms: milliseconds)
}

/// Sets the checkout deadline used only by `migrate`'s own transactions, in
/// milliseconds — separate from `statement_deadline_ms` because a schema
/// migration's DDL step can legitimately need longer than an ordinary
/// job-lifecycle statement. Applies *per step*, not to the whole
/// `migrate`/`migrate_with` call: each version in `migrations()` runs in its
/// own transaction (see `migrate_with`'s own doc comment), and this deadline
/// bounds each one independently rather than the sum across every version
/// applied in one call. Validated positive by `validate`.
pub fn with_migration_deadline(
  settings: Settings,
  milliseconds: Int,
) -> Settings {
  Settings(..settings, migration_deadline_ms: milliseconds)
}

/// Sets the bounded number of `[grind, job, *]` observations the package's
/// own `sinal/forwarder.Forwarder` holds in flight at once. Exceeding it
/// drops the observation and is reported once per drain via
/// `sinal/forwarder.dropped_event` (`[sinal, forwarder, dropped]`); it never
/// affects job outcomes. Validated positive by `validate`, before any
/// process starts.
pub fn with_observation_capacity(
  settings: Settings,
  capacity: Int,
) -> Settings {
  Settings(..settings, observation_capacity: capacity)
}

pub type ConfigError {
  InvalidDatabaseUrl
  InvalidPoolSize
  InvalidUniqueLockWait
  InvalidObservationCapacity
  InvalidStatementDeadline
  InvalidMigrationDeadline
  /// `unique_lock_wait_ms` is within roughly one second of
  /// `statement_deadline_ms` (DEFECT 1, docs/RELEASE-READINESS.md): default
  /// PostgreSQL's own `55P03 lock_not_available` (raised once
  /// `unique_lock_wait_ms` elapses, mapped to `submission.AdmissionContended`)
  /// would otherwise race the checkout deadline itself force-closing the
  /// connection first, surfacing as `NotCommitted(QueryTimeout)` or a
  /// commit-unknown outcome instead of the typed contention outcome.
  UniqueLockWaitTooCloseToDeadline
}

pub opaque type ValidatedSettings {
  ValidatedSettings(
    pog.Config,
    storage_owner: String,
    unique_lock_wait_ms: Int,
    observation_capacity: Int,
    statement_deadline_ms: Int,
    migration_deadline_ms: Int,
  )
}

/// The margin `validate` requires between `unique_lock_wait_ms` and
/// `statement_deadline_ms` (DEFECT 1). Not itself configurable: it exists so
/// PostgreSQL's own lock-wait error has time to actually surface and be
/// mapped to `submission.AdmissionContended` before the checkout deadline would
/// otherwise force-close the connection first.
const unique_lock_wait_margin_ms = 1000

/// Checks the URL and pool bound before any PostgreSQL process is started.
/// Each successful call creates one new Erlang atom for the pool's own name
/// (`process.new_name`, below) — atoms are never garbage-collected or
/// reclaimed by the runtime, so calling `validate` in a hot loop (rather than
/// once per logical database and reusing the resulting `ValidatedSettings`
/// across `start`/`close` cycles) leaks one atom per call for the life of
/// the node.
///
/// Every pooled connection is also given a `default_transaction_isolation
/// = 'read committed'` startup parameter (`pog.connection_parameter`),
/// overriding whatever the connecting role or database's own
/// `default_transaction_isolation` is configured to. This is not defensive
/// decoration: several of Grind's transactions depend on `READ COMMITTED`
/// semantics — a plain read after a wait must see what committed during
/// that wait (the uniqueness admission transaction's domain lock), and a
/// fenced `UPDATE` racing a concurrent retry of the exact same command must
/// not surface PostgreSQL's `REPEATABLE READ`/`SERIALIZABLE` conflict
/// handling (`40001 serialization_failure`) in place of the idempotent
/// result that retry is supposed to get (the acknowledgement path) — and
/// neither depends on a caller never configuring their role or database
/// with a non-default isolation level. See `docs/UNIQUENESS-CONTRACT.md`,
/// "Admission transaction" step 1, and `docs/RECOVERY-EVIDENCE.md`,
/// "Isolation-level pinning", for the full rationale and mutation evidence.
/// `grind/internal/unique_admission`'s own `pin_read_committed` (`SET
/// TRANSACTION ISOLATION LEVEL READ COMMITTED` as that transaction's own
/// first statement) is kept as defense in depth on top of this — a
/// connection pooler between Grind and PostgreSQL could drop or ignore a
/// startup parameter, where an in-transaction `SET TRANSACTION` cannot be
/// silently dropped the same way.
pub fn validate(settings: Settings) -> Result(ValidatedSettings, ConfigError) {
  case
    settings.pool_size > 0,
    settings.statement_deadline_ms > 0,
    settings.migration_deadline_ms > 0,
    settings.unique_lock_wait_ms > 0,
    settings.unique_lock_wait_ms + unique_lock_wait_margin_ms
    < settings.statement_deadline_ms,
    settings.observation_capacity > 0
  {
    False, _, _, _, _, _ -> Error(InvalidPoolSize)
    True, False, _, _, _, _ -> Error(InvalidStatementDeadline)
    True, True, False, _, _, _ -> Error(InvalidMigrationDeadline)
    True, True, True, False, _, _ -> Error(InvalidUniqueLockWait)
    True, True, True, True, False, _ -> Error(UniqueLockWaitTooCloseToDeadline)
    True, True, True, True, True, False -> Error(InvalidObservationCapacity)
    True, True, True, True, True, True ->
      // The pool's own name, created here rather than accepted from the
      // caller (`Settings` carries no pog type of its own) — and created
      // once, here, rather than fresh on every `start`: a `ValidatedSettings`
      // value that is `start`ed more than once (closing and reopening
      // "the same" database — see e.g.
      // `postgres_closed_pool_renewal_recovers_without_rerun_test`) must
      // reopen under the *same* name each time, because a `Consumer`
      // captures its `Database`'s `connection` (`{pool, Name}`) once, at
      // `queue.start`, and never learns about a later, separate `Database`
      // value — its own renewal/claim/ack calls keep addressing that one
      // captured name for as long as the consumer runs. A fresh name per
      // `start` would silently orphan any consumer across a close/reopen
      // cycle instead of letting it recover once the same name is
      // re-registered.
      case
        pog.url_config(
          process.new_name("grind_postgres_pool"),
          settings.database_url,
        )
      {
        Error(_) -> Error(InvalidDatabaseUrl)
        Ok(config) -> {
          let storage_owner =
            config.host
            <> ":"
            <> int.to_string(config.port)
            <> "/"
            <> config.database
          Ok(ValidatedSettings(
            config
              |> pog.pool_size(settings.pool_size)
              |> pog.connection_parameter(
                name: "default_transaction_isolation",
                value: "read committed",
              )
              |> pog.connection_parameter(
                name: "idle_in_transaction_session_timeout",
                value: int.to_string(2 * settings.statement_deadline_ms),
              ),
            storage_owner:,
            unique_lock_wait_ms: settings.unique_lock_wait_ms,
            observation_capacity: settings.observation_capacity,
            statement_deadline_ms: settings.statement_deadline_ms,
            migration_deadline_ms: settings.migration_deadline_ms,
          ))
        }
      }
  }
}

pub opaque type Database {
  Database(
    connection: pog.Connection,
    supervisor_pid: process.Pid,
    storage_owner: String,
    unique_lock_wait_ms: Int,
    statement_deadline_ms: Int,
    migration_deadline_ms: Int,
    forwarder: Forwarder,
  )
}

/// The Grind-owned checkout deadline (milliseconds) bounding every storage
/// call made against this `Database` — see `statement_deadline` and
/// `src/grind_postgres_ffi.erl`. `queue.start` reads this to enforce the
/// lease rule (`LeaseTooShortForDeadline`).
pub fn statement_deadline_ms(database: Database) -> Int {
  let Database(statement_deadline_ms:, ..) = database
  statement_deadline_ms
}

pub type StartError {
  PoolStartFailed(actor.StartError)
}

/// Starts a package-owned PostgreSQL pool after settings have been
/// validated. Reuses `validate`'s own pool name (`ValidatedSettings` never
/// exposes it — `Settings` carries no pog type of its own, and this
/// function's own caller never needs to know or choose it either) rather
/// than creating a new one here: a `Consumer` captures its `Database`'s
/// `connection` (`{pool, Name}`) once, at `queue.start`, and never learns
/// about a later, separate `Database` value, so reopening "the same"
/// database — closing, then calling `start` again with the *same*
/// `ValidatedSettings` — must reuse the same name for an existing consumer
/// to ever recover; see `validate`'s own doc comment. Starting two
/// independent databases still means calling `settings`/`validate` twice,
/// each producing its own distinct name. Starting the *same*
/// `ValidatedSettings` a second time without an intervening `close` fails
/// with `PoolStartFailed`, since the pool's own atom name is already
/// registered to the still-running first pool. Also starts this `Database`'s own
/// `sinal/forwarder.Forwarder`
/// — see `grind/observation` for the events it carries — nested under its
/// own dedicated supervisor, added to the root as a `Temporary` child.
/// `observation_capacity` was already checked positive by `validate`, so
/// `forwarder.new` cannot fail here.
///
/// The nesting matters: a handler attached to an observation event that
/// itself exits or is killed (rather than merely raising, which native
/// `:telemetry` does isolate) can take the forwarder process down — see
/// `sinal/forwarder`'s own module documentation. If the forwarder were a
/// plain sibling of the PostgreSQL pool under one shared supervisor,
/// repeatedly crashing it would exhaust *that* supervisor's own restart
/// intensity (2 restarts / 5 seconds by default) and terminate every child,
/// including the pool. Nesting the forwarder under its own supervisor and
/// marking that nested supervisor `Temporary` under the root means: on
/// ordinary crashes the nested supervisor keeps restarting the forwarder
/// exactly as before; if the forwarder crashes so persistently that the
/// *nested* supervisor exhausts its own restart intensity and terminates
/// itself, the root supervisor — per `Temporary`'s contract — never
/// restarts it and never counts that termination against its own restart
/// budget, so the pool is never affected. In that degraded state, further
/// observations are simply unavailable: `forwarder.emit` reports
/// `ForwarderUnavailable`, which `grind/postgres` already ignores (an
/// observation is a diagnostic side channel, never a policy decision) —
/// jobs keep being admitted, claimed, and acknowledged normally. See
/// `postgres_forwarder_crash_loop_does_not_stop_the_pool_test`
/// (`test/grind_test.gleam`) and `docs/RECOVERY-EVIDENCE.md`, "Acknowledged
/// observation".
pub fn start(settings: ValidatedSettings) -> Result(Database, StartError) {
  let ValidatedSettings(
    config,
    storage_owner:,
    unique_lock_wait_ms:,
    observation_capacity:,
    statement_deadline_ms:,
    migration_deadline_ms:,
  ) = settings
  let pog.Config(pool_name:, ..) = config
  let forwarder_name = process.new_name("grind_postgres_observation_forwarder")
  let assert Ok(fwd) = forwarder.new(forwarder_name, observation_capacity)
  let forwarder_supervisor =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(forwarder.supervised(fwd))
    |> static_supervisor.supervised()
    |> supervision.restart(supervision.Temporary)
  let supervisor =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(pog.supervised(config))
    |> static_supervisor.add(forwarder_supervisor)
  let connection = pog.named_connection(pool_name)
  // Attached before the pool itself starts accepting checkouts, so no
  // in-flight checkout against this pool's name can ever run before a
  // deadline is attached and silently fall back to the FFI's own hardcoded
  // default (5000ms).
  store.set_deadline(connection, statement_deadline_ms)
  case static_supervisor.start(supervisor) {
    Ok(started) -> {
      process.unlink(started.pid)
      Ok(Database(
        connection,
        started.pid,
        storage_owner,
        unique_lock_wait_ms,
        statement_deadline_ms,
        migration_deadline_ms,
        fwd,
      ))
    }
    Error(error) -> Error(PoolStartFailed(error))
  }
}

pub type CloseError {
  /// The pool's supervisor did not confirm its own shutdown within the FFI's
  /// own bounded wait (6000ms, `src/grind_postgres_ffi.erl`). The pool
  /// process may still be terminating; this `Database` value's own deadline
  /// entry is left in place rather than risking erasing a name a fresh
  /// `start` may since have reused (see `close`'s own doc comment).
  StopTimedOut
}

/// Stops the pool process owned by this `Database` value. `Ok(True)` means
/// this call actually stopped a still-live process; `Ok(False)` means it was
/// already gone. `close` only erases the pool's own deadline entry in the
/// first case — see `close`'s own doc comment for why.
@external(erlang, "grind_postgres_ffi", "stop_supervisor")
fn stop_supervisor(pid: process.Pid) -> Result(Bool, Nil)

/// Stops this `Database`'s own PostgreSQL pool and observation forwarder,
/// and erases the checkout deadline `start` attached to the pool's name —
/// but only when this call itself is the one that actually stopped a live
/// process. `ValidatedSettings` reuses the same pool name across every
/// `start`/`close` cycle (see `validate`), so calling `close` on a stale
/// `Database` handle whose supervisor has already stopped (kept around
/// after an intervening `start` reopened the same `ValidatedSettings` under
/// a fresh supervisor) is a no-op here rather than erasing the *live*
/// pool's deadline entry out from under it. Before this, that erasure ran
/// unconditionally: a stray double-`close` on an old handle silently
/// downgraded a still-running pool's every subsequent storage call to the
/// FFI's own hardcoded 5000ms default until that pool was itself closed.
/// Returns `Error(StopTimedOut)` if the supervisor does not confirm its own
/// shutdown in time; the deadline is left in place in that case too, since
/// the pool may still be alive.
pub fn close(database: Database) -> Result(Nil, CloseError) {
  let Database(supervisor_pid:, connection:, ..) = database
  case stop_supervisor(supervisor_pid) {
    Error(Nil) -> Error(StopTimedOut)
    Ok(True) -> {
      store.clear_deadline(connection)
      Ok(Nil)
    }
    Ok(False) -> Ok(Nil)
  }
}

pub type StorageError {
  /// A storage call made while reading the schema's current generation, or
  /// while acquiring the migration advisory lock or pinning `READ
  /// COMMITTED` — never one of a migration step's own DDL/DML statements,
  /// see `MigrationStepFailed` for those — failed or its reply was lost.
  MigrationQueryFailed(pog.QueryError)
  IncompatibleSchema
  UnsupportedSchemaVersion(Int)
  /// One migration step's own statements failed and that step's transaction
  /// was rolled back cleanly — every earlier step already committed its own
  /// transaction and stays applied; this step and every later one did not
  /// run. Safe to fix the underlying cause and re-run `migrate`.
  MigrationStepFailed(Int, pog.QueryError)
  /// A migration step may or may not have committed. Covers three distinct
  /// shapes, all safe to resolve the same way: a checkout failure before
  /// `BEGIN` ever ran (definitely not committed, reported here anyway since
  /// retrying is equally safe either way); `BEGIN` itself failing or losing
  /// its reply; and a failed `ROLLBACK` after a statement error (the
  /// deadline force-closing the connection, so the rollback's own outcome
  /// is unknown too). Re-running `migrate` is always safe regardless of
  /// which shape occurred: a step that did commit is detected by its own
  /// re-read of the schema generation and skipped; one that did not is
  /// simply retried.
  MigrationCommitUnknown(Int)
}

pub type Resolution(output, error) {
  ConfirmSuccess(output)
  ConfirmBusinessFailure(error)
  AuthorizeReplay
}

pub type ResolutionResult {
  ResolutionApplied(State)
  ResolutionAlreadyApplied(State)
}

pub type ResolutionError {
  EmptyResolutionId
  EmptyResolver
  EmptyResolutionDetails
  ReconciliationQueryFailed(pog.QueryError)
  ReconciliationNotRequired
  ResolutionCommandConflict
  ResolutionRouteMismatch
  ResolutionWorkerContractMismatch
  ResolutionCodecMismatch
  ResolutionRequiresErrorCodec
  ResolutionCancellationPending
  ResolutionAttemptMetadataMissing
  ResolutionWriteRejected
  ResolutionCommitUnknown(resolution_id: String)
}

/// One audited operator decision: `resolution_id` identifies this exact
/// decision for idempotent replay after a lost reply (see
/// `ResolutionCommitUnknown`); `resolved_by` and `details` are attributed,
/// non-empty audit fields; `decision` is the outcome itself.
pub type ResolutionRequest(output, error) {
  ResolutionRequest(
    resolution_id: String,
    resolved_by: String,
    details: String,
    decision: Resolution(output, error),
  )
}

/// Applies an audited operator decision to an uncertain job.
pub fn resolve_uncertain(
  database: Database,
  handle: JobHandle(input, output, error),
  request: ResolutionRequest(output, error),
) -> Result(ResolutionResult, ResolutionError) {
  let ResolutionRequest(resolution_id:, resolved_by:, details:, decision:) =
    request
  case resolution_id, resolved_by, details {
    "", _, _ -> Error(EmptyResolutionId)
    _, "", _ -> Error(EmptyResolver)
    _, _, "" -> Error(EmptyResolutionDetails)
    _, _, _ -> {
      let #(_, _, _, _, _, bound_output_version, _) =
        job.reconciliation_fields(handle)
      use
        #(
          decision,
          state,
          output_version,
          encoded_output,
          error_version,
          encoded_error,
          failure_description,
        )
      <- result.try(case decision {
        ConfirmSuccess(value) -> {
          let #(version, encoded) = job.encode_reconciled_success(handle, value)
          Ok(#(
            "confirm_success",
            "succeeded",
            version,
            Some(encoded),
            None,
            None,
            None,
          ))
        }
        ConfirmBusinessFailure(value) ->
          case job.encode_reconciled_error(handle, value) {
            None -> Error(ResolutionRequiresErrorCodec)
            Some(#(version, encoded)) ->
              Ok(#(
                "confirm_business_failure",
                "business_failed",
                bound_output_version,
                None,
                Some(version),
                Some(encoded),
                Some(details),
              ))
          }
        AuthorizeReplay ->
          Ok(#(
            "authorize_replay",
            "queued",
            bound_output_version,
            None,
            None,
            None,
            None,
          ))
      })
      let #(
        id,
        handle_owner,
        queue,
        worker_id,
        worker_version,
        expected_output_version,
        expected_error_version,
      ) = job.reconciliation_fields(handle)
      let Database(connection:, storage_owner: database_owner, forwarder:, ..) =
        database
      let command =
        ResolutionCommand(
          id:,
          database_owner:,
          handle_owner:,
          queue:,
          worker_id:,
          worker_version:,
          expected_output_version:,
          expected_error_version:,
          resolution_id:,
          resolved_by:,
          details:,
          decision:,
          target_state: state,
          output_version:,
          encoded_output:,
          error_version:,
          encoded_error:,
          failure_description:,
        )
      case
        store.transaction_safely(connection, fn(transaction) {
          reconcile_transaction(transaction, command)
        })
      {
        Ok(result) -> {
          case resolution_decision_of_stored(decision) {
            Error(Nil) -> Nil
            Ok(decision) -> {
              let #(committed_state, confirmation) = case result {
                ResolutionApplied(state) -> #(state, observation.Replied)
                ResolutionAlreadyApplied(state) -> #(
                  state,
                  observation.Reconciled,
                )
              }
              emit_resolved(
                forwarder,
                queue,
                id,
                worker_id,
                worker_version,
                decision,
                committed_state,
                resolution_id,
                resolved_by,
                confirmation,
              )
            }
          }
          Ok(result)
        }
        Error(pog.TransactionQueryError(_)) ->
          Error(ResolutionCommitUnknown(resolution_id))
        Error(pog.TransactionRolledBack(error)) -> Error(error)
      }
    }
  }
}

/// Builds and forwards `[grind, job, resolved]` from a proven-committed
/// `ResolutionResult`. Called only from `resolve_uncertain`, strictly after
/// `transaction_safely` has already returned — never from inside a
/// transaction callback. `ResolutionApplied` is this call's own fresh commit
/// (`Replied`); `ResolutionAlreadyApplied` is a prior commit of this exact
/// `resolution_id` proven by a receipt read (`Reconciled`) — see
/// `resolution_receipt_outcome`.
fn emit_resolved(
  fwd: Forwarder,
  queue: String,
  job_id: Int,
  worker_id: String,
  worker_version: String,
  decision: observation.ResolutionDecision,
  committed_state: State,
  resolution_id: String,
  resolved_by: String,
  confirmation: observation.Confirmation,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      observation.resolved(),
      observation.ResolvedMeasurements(count: 1),
      observation.ResolvedMetadata(
        ref: observation.JobRef(job_id:, queue:, worker_id:, worker_version:),
        decision:,
        committed_state:,
        resolution_id:,
        resolved_by:,
        confirmation:,
      ),
    )
  Nil
}

fn resolution_decision_of_stored(
  decision: String,
) -> Result(observation.ResolutionDecision, Nil) {
  case decision {
    "confirm_success" -> Ok(observation.DecisionConfirmSuccess)
    "confirm_business_failure" -> Ok(observation.DecisionConfirmBusinessFailure)
    "authorize_replay" -> Ok(observation.DecisionAuthorizeReplay)
    _ -> Error(Nil)
  }
}

fn reconcile_transaction(
  connection: pog.Connection,
  command: ResolutionCommand,
) -> Result(ResolutionResult, ResolutionError) {
  let ResolutionCommand(database_owner:, handle_owner:, ..) = command
  case database_owner == handle_owner {
    False -> Error(ResolutionRouteMismatch)
    True -> reconcile_matching_owner(connection, command)
  }
}

fn reconcile_matching_owner(
  connection: pog.Connection,
  command: ResolutionCommand,
) -> Result(ResolutionResult, ResolutionError) {
  case resolution_receipt_outcome(connection, command) {
    Error(error) -> Error(error)
    Ok(Some(result)) -> Ok(result)
    Ok(None) -> apply_uncertain_resolution(connection, command)
  }
}

/// Looks up an existing resolution receipt for `command`'s `resolution_id`
/// and, if one exists, checks it matches this exact command. `Ok(None)`
/// means no receipt exists yet — the caller decides what to do (apply a
/// fresh resolution, or — the second, post-lock call site in
/// `apply_uncertain_resolution` below — report that reconciliation is
/// genuinely not required). Shared by two call sites deliberately: this is
/// the exact "re-read the receipt instead of misreporting a concurrent
/// retry as stale" pattern the acknowledgement path already uses
/// (`acknowledge_transaction`'s re-read of `matching_acknowledgement` after
/// a 0-row fenced `UPDATE`), applied here to `resolve_uncertain`'s
/// analogous race — see `docs/RECOVERY-EVIDENCE.md`, "Concurrent audited
/// resolution".
fn resolution_receipt_outcome(
  connection: pog.Connection,
  command: ResolutionCommand,
) -> Result(Option(ResolutionResult), ResolutionError) {
  let ResolutionCommand(
    id:,
    database_owner:,
    queue:,
    worker_id:,
    worker_version:,
    resolution_id:,
    resolved_by:,
    details:,
    decision:,
    target_state:,
    output_version:,
    encoded_output:,
    error_version:,
    encoded_error:,
    ..,
  ) = command
  let #(payload_version, payload) = case decision {
    "confirm_success" -> #(Some(output_version), encoded_output)
    "confirm_business_failure" -> #(error_version, encoded_error)
    _ -> #(None, None)
  }
  case find_resolution(connection, database_owner, resolution_id, payload) {
    Error(error) -> Error(error)
    Ok(None) -> Ok(None)
    Ok(Some(#(
      job_id,
      old_queue,
      stored_worker_id,
      stored_worker_version,
      old_decision,
      old_resolver,
      old_details,
      old_target_state,
      old_payload_version,
      payload_match,
    ))) ->
      case
        job_id == id
        && old_queue == queue
        && stored_worker_id == Some(worker_id)
        && stored_worker_version == Some(worker_version)
        && old_decision == decision
        && old_resolver == resolved_by
        && old_details == details
        && old_target_state == target_state
        && old_payload_version == payload_version
        && payload_match == "same"
      {
        False -> Error(ResolutionCommandConflict)
        True ->
          resolution_state(old_target_state)
          |> result.map(fn(state) { Some(ResolutionAlreadyApplied(state)) })
      }
  }
}

fn find_resolution(
  connection: pog.Connection,
  storage_owner: String,
  resolution_id: String,
  payload: Option(String),
) -> Result(
  Option(
    #(
      Int,
      String,
      Option(String),
      Option(String),
      String,
      String,
      String,
      String,
      Option(String),
      String,
    ),
  ),
  ResolutionError,
) {
  let query =
    pog.query(
      "SELECT job_id, queue, worker_id, worker_version, decision, resolved_by, details, target_state, payload_version, CASE WHEN payload IS NOT DISTINCT FROM $3::jsonb THEN 'same' ELSE 'different' END FROM grind_job_resolutions WHERE storage_owner = $1 AND resolution_id = $2",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(resolution_id))
    |> pog.parameter(pog.nullable(pog.text, payload))
    |> pog.returning({
      use job_id <- decode.field(0, decode.int)
      use queue <- decode.field(1, decode.string)
      use worker_id <- decode.field(2, decode.optional(decode.string))
      use worker_version <- decode.field(3, decode.optional(decode.string))
      use decision <- decode.field(4, decode.string)
      use resolved_by <- decode.field(5, decode.string)
      use details <- decode.field(6, decode.string)
      use target_state <- decode.field(7, decode.string)
      use payload_version <- decode.field(8, decode.optional(decode.string))
      use payload_match <- decode.field(9, decode.string)
      decode.success(#(
        job_id,
        queue,
        worker_id,
        worker_version,
        decision,
        resolved_by,
        details,
        target_state,
        payload_version,
        payload_match,
      ))
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(ReconciliationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Ok(None)
        [resolution] -> Ok(Some(resolution))
        _ -> Error(ResolutionCommandConflict)
      }
  }
}

fn resolution_state(state: String) -> Result(State, ResolutionError) {
  case state {
    "queued" -> Ok(Queued)
    "scheduled" -> Ok(Scheduled)
    "executing" -> Ok(job.Executing)
    "succeeded" -> Ok(job.Succeeded)
    "business_failed" -> Ok(job.BusinessFailed)
    "runtime_failed" -> Ok(job.RuntimeFailed)
    "contract_mismatch" -> Ok(job.ContractMismatch)
    "uncertain" -> Ok(job.Uncertain)
    "discarded" -> Ok(job.Discarded)
    "cancelled" -> Ok(job.Cancelled)
    _ -> Error(ReconciliationNotRequired)
  }
}

fn apply_uncertain_resolution(
  connection: pog.Connection,
  command: ResolutionCommand,
) -> Result(ResolutionResult, ResolutionError) {
  let ResolutionCommand(
    id:,
    database_owner:,
    handle_owner:,
    queue:,
    worker_id:,
    worker_version:,
    expected_output_version:,
    expected_error_version:,
    decision:,
    ..,
  ) = command
  case database_owner == handle_owner {
    False -> Error(ResolutionRouteMismatch)
    True -> {
      let select =
        pog.query(
          "SELECT storage_owner, queue, worker_id, worker_version, state, attempt_id, attempt_epoch, attempt_owner, lease_expires_at::text, output_version, error_version, cancel_requested_at IS NOT NULL FROM grind_jobs WHERE id = $1 FOR UPDATE",
        )
        |> pog.parameter(pog.int(id))
        |> pog.returning({
          use stored_owner <- decode.field(0, decode.string)
          use stored_queue <- decode.field(1, decode.string)
          use stored_worker <- decode.field(2, decode.string)
          use stored_worker_version <- decode.field(3, decode.string)
          use state <- decode.field(4, decode.string)
          use attempt_id <- decode.field(5, decode.optional(decode.int))
          use attempt_epoch <- decode.field(6, decode.int)
          use attempt_owner <- decode.field(7, decode.optional(decode.string))
          use lease_expires_at <- decode.field(
            8,
            decode.optional(decode.string),
          )
          use stored_output_version <- decode.field(9, decode.string)
          use stored_error_version <- decode.field(
            10,
            decode.optional(decode.string),
          )
          use cancel_requested <- decode.field(11, decode.bool)
          decode.success(#(
            stored_owner,
            stored_queue,
            stored_worker,
            stored_worker_version,
            state,
            attempt_id,
            attempt_epoch,
            attempt_owner,
            lease_expires_at,
            stored_output_version,
            stored_error_version,
            cancel_requested,
          ))
        })
      use stored <- result.try(
        case store.execute_safely(select, on: connection) {
          Error(error) -> Error(ReconciliationQueryFailed(error))
          Ok(returned) ->
            case returned.rows {
              [stored] -> Ok(stored)
              _ -> Error(ReconciliationNotRequired)
            }
        },
      )
      let #(
        stored_owner,
        stored_queue,
        stored_worker,
        stored_worker_version,
        stored_state,
        attempt_id,
        attempt_epoch,
        attempt_owner,
        lease_expires_at,
        stored_output_version,
        stored_error_version,
        cancel_requested,
      ) = stored
      let codec_matches = case decision {
        "authorize_replay" -> True
        "confirm_success" -> stored_output_version == expected_output_version
        "confirm_business_failure" ->
          stored_output_version == expected_output_version
          && stored_error_version == expected_error_version
        _ -> False
      }
      case stored_owner == database_owner && stored_queue == queue {
        False -> Error(ResolutionRouteMismatch)
        True ->
          case
            stored_worker == worker_id
            && stored_worker_version == worker_version
          {
            False -> Error(ResolutionWorkerContractMismatch)
            True ->
              case stored_state == "uncertain" {
                // The row is no longer `uncertain` — either genuinely no
                // reconciliation is needed, or (the race this re-check
                // exists for) a concurrent call for this exact command won
                // and already committed while this call waited on the row
                // lock just above. Re-reading the receipt here, rather
                // than assuming the former, is the same "re-read instead
                // of misreporting a concurrent retry as stale" pattern
                // `acknowledge_transaction` already uses.
                False ->
                  case resolution_receipt_outcome(connection, command) {
                    Error(error) -> Error(error)
                    Ok(Some(result)) -> Ok(result)
                    Ok(None) -> Error(ReconciliationNotRequired)
                  }
                True ->
                  case codec_matches {
                    False -> Error(ResolutionCodecMismatch)
                    True ->
                      case decision == "authorize_replay" && cancel_requested {
                        True -> Error(ResolutionCancellationPending)
                        False ->
                          case attempt_id, attempt_owner, lease_expires_at {
                            Some(attempt_id), Some(attempt_owner), Some(_) ->
                              write_resolution(
                                connection,
                                command,
                                attempt_id,
                                attempt_epoch,
                                attempt_owner,
                              )
                            _, _, _ -> Error(ResolutionAttemptMetadataMissing)
                          }
                      }
                  }
              }
          }
      }
    }
  }
}

fn write_resolution(
  connection: pog.Connection,
  command: ResolutionCommand,
  attempt_id: Int,
  attempt_epoch: Int,
  attempt_owner: String,
) -> Result(ResolutionResult, ResolutionError) {
  let ResolutionCommand(
    id:,
    database_owner:,
    queue:,
    worker_id:,
    worker_version:,
    resolution_id:,
    resolved_by:,
    details:,
    decision:,
    target_state:,
    output_version:,
    encoded_output:,
    error_version:,
    encoded_error:,
    failure_description:,
    ..,
  ) = command
  let insert =
    pog.query(
      "INSERT INTO grind_job_resolutions (storage_owner, queue, job_id, worker_id, worker_version, resolution_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, decision, resolved_by, details, target_state, payload_version, payload) SELECT job.storage_owner, job.queue, job.id, $14, $15, $4, job.attempt_id, job.attempt_epoch, job.attempt_owner, job.lease_expires_at, $8, $9, $10, $11, $12, $13::jsonb FROM grind_jobs AS job WHERE job.id = $3 AND job.storage_owner = $1 AND job.queue = $2 AND job.worker_id = $16 AND job.worker_version = $17 AND job.state = 'uncertain' AND job.attempt_id = $5 AND job.attempt_epoch = $6 AND job.attempt_owner = $7 AND job.lease_expires_at IS NOT NULL RETURNING resolution_id",
    )
    |> pog.parameter(pog.text(database_owner))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(resolution_id))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(attempt_epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.parameter(pog.text(decision))
    |> pog.parameter(pog.text(resolved_by))
    |> pog.parameter(pog.text(details))
    |> pog.parameter(pog.text(target_state))
    |> pog.parameter(
      pog.nullable(pog.text, case decision {
        "confirm_success" -> Some(output_version)
        "confirm_business_failure" -> error_version
        _ -> None
      }),
    )
    |> pog.parameter(
      pog.nullable(pog.text, case decision {
        "confirm_success" -> encoded_output
        "confirm_business_failure" -> encoded_error
        _ -> None
      }),
    )
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.returning({
      use resolution_id <- decode.field(0, decode.string)
      decode.success(resolution_id)
    })
  use _ <- result.try(case store.execute_safely(insert, on: connection) {
    Error(error) -> Error(ReconciliationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [_] -> Ok(Nil)
        _ -> Error(ResolutionWriteRejected)
      }
  })
  let update =
    pog.query(
      "UPDATE grind_jobs SET state = $1, output = $2::jsonb, output_version = $3, error = $4::jsonb, error_version = $5, failure_description = $6, attempt_id = CASE WHEN $1 = 'queued' THEN NULL ELSE attempt_id END, attempt_owner = NULL, lease_expires_at = NULL, available_at = CASE WHEN $1 = 'queued' THEN clock_timestamp() ELSE available_at END WHERE id = $7 AND storage_owner = $8 AND queue = $9 AND worker_id = $10 AND worker_version = $11 AND state = 'uncertain' AND attempt_id = $12 AND attempt_epoch = $13 AND attempt_owner = $14 RETURNING state",
    )
    |> pog.parameter(pog.text(target_state))
    |> pog.parameter(pog.nullable(pog.text, encoded_output))
    |> pog.parameter(pog.text(output_version))
    |> pog.parameter(pog.nullable(pog.text, encoded_error))
    |> pog.parameter(pog.nullable(pog.text, error_version))
    |> pog.parameter(pog.nullable(pog.text, failure_description))
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(database_owner))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(attempt_epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      decode.success(state)
    })
  use state <- result.try(case store.execute_safely(update, on: connection) {
    Error(error) -> Error(ReconciliationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [state] -> Ok(state)
        _ -> Error(ResolutionWriteRejected)
      }
  })
  case resolution_state(state) {
    Error(error) -> Error(error)
    Ok(state) -> Ok(ResolutionApplied(state))
  }
}

/// Applies every not-yet-applied step of `grind/internal/migrations.migrations()`
/// in ascending version order. Repeated calls are safe; older experimental
/// schema versions and any tampered or foreign schema fail closed. See
/// `migrate_with` for the mechanism.
pub fn migrate(database: Database) -> Result(Nil, StorageError) {
  migrate_with(database, migrations.migrations())
}

/// Grind's migration runner, generalised over an explicit step list so the
/// test suite can exercise it against synthetic steps appended after the
/// real ones. `migrate` is exactly `migrate_with(database, migrations())`.
///
/// `steps` must be exactly the contiguous range
/// `{baseline_schema_version..latest}` with no gaps or duplicate versions —
/// this is `migrate_with`'s own precondition on its caller (always satisfied
/// by `migrations()` itself, proven by `grind_migrations_conformance_test`;
/// the test suite's own synthetic step lists must satisfy it too), checked
/// with `let assert` rather than a typed error: a malformed step list is a
/// programming error in the caller, never a runtime schema condition.
///
/// Runs each step in its own transaction, in ascending version order, under
/// `Settings.migration_deadline_ms`:
///
/// 1. Pin this transaction to `READ COMMITTED` as its own literal first
///    statement (defence in depth, exactly like the uniqueness admission
///    transaction — see `grind/internal/unique_admission`'s
///    `pin_read_committed`), then acquire a transaction-scoped PostgreSQL
///    advisory lock (`pg_advisory_xact_lock`, keyed by a fixed Grind string
///    and the current schema) so two concurrent `migrate`/`migrate_with`
///    callers — in this process or another — serialise instead of racing
///    the same step. The identical lock statement is also this step's own
///    first statement in `migrations()`/`priv/migrations/*.sql`, so an
///    application applying migrations directly through cigogne serialises
///    against a concurrent `postgres.migrate` caller too; re-acquiring the
///    same transaction-scoped lock a second time in the same transaction is
///    a no-op, never a self-deadlock.
/// 2. Re-read the schema's current generation (see `read_schema_generation`)
///    now that the lock is held, and skip this step if it is already
///    applied — necessary both for plain idempotent re-runs and for a
///    concurrent migrator that was waiting on the lock behind this one.
/// 3. Otherwise run this step's own statements, in order, including its
///    trailing `grind_schema_migrations` marker insert.
/// 4. Re-read the schema's generation once more, in the same transaction,
///    and require it now reports exactly `AtVersion(this step's version)`
///    before committing — catches a step whose statements ran without error
///    but produced the wrong marker or the wrong physical shape (a missing
///    object, a missing key column) that a later step or caller would
///    otherwise silently trust.
///
/// `@internal`: exposed only for the test suite (concurrent migrators,
/// injected partial failures, and the upgrade harness), never part of the
/// public API.
@internal
pub fn migrate_with(
  database: Database,
  steps: List(migrations.Migration),
) -> Result(Nil, StorageError) {
  let assert True = steps_are_contiguous_from_baseline(steps)
    as "migrate_with's steps must be exactly the contiguous range {baseline_schema_version..latest}, with no gaps or duplicate versions"
  let Database(connection:, migration_deadline_ms:, ..) = database
  steps
  |> list.sort(fn(a, b) { int.compare(a.version, b.version) })
  |> list.try_each(fn(step) {
    run_migration_step(connection, migration_deadline_ms, steps, step)
  })
}

fn steps_are_contiguous_from_baseline(
  steps: List(migrations.Migration),
) -> Bool {
  let versions =
    steps
    |> list.map(fn(step) { step.version })
    |> list.sort(int.compare)
  case versions {
    [] -> False
    [first, ..] if first != baseline_schema_version -> False
    _ ->
      versions
      |> list.index_map(fn(version, index) {
        version == baseline_schema_version + index
      })
      |> list.all(fn(matches) { matches })
  }
}

fn run_migration_step(
  connection: pog.Connection,
  migration_deadline_ms: Int,
  steps: List(migrations.Migration),
  step: migrations.Migration,
) -> Result(Nil, StorageError) {
  case
    store.migration_transaction_safely(
      connection,
      migration_deadline_ms,
      fn(transaction) {
        run_migration_step_transaction(transaction, steps, step)
      },
    )
  {
    Ok(Nil) -> Ok(Nil)
    // `BEGIN`/`COMMIT` itself failing or losing its reply is one of three
    // shapes this covers — see `MigrationCommitUnknown`'s own doc comment
    // for the other two (a checkout failure before `BEGIN` ever ran, and a
    // failed `ROLLBACK` after a statement error). Re-running `migrate` is
    // always safe regardless of which one occurred.
    Error(pog.TransactionQueryError(_)) ->
      Error(MigrationCommitUnknown(step.version))
    Error(pog.TransactionRolledBack(error)) -> Error(error)
  }
}

fn run_migration_step_transaction(
  connection: pog.Connection,
  steps: List(migrations.Migration),
  step: migrations.Migration,
) -> Result(Nil, StorageError) {
  use _ <- result.try(pin_read_committed(connection))
  use _ <- result.try(acquire_migration_lock(connection))
  use generation <- result.try(read_schema_generation(connection, steps))
  case generation_at_least(generation, step.version) {
    True -> Ok(Nil)
    False -> {
      use _ <- result.try(
        list.try_each(step.statements, fn(statement) {
          case store.execute_safely(pog.query(statement), on: connection) {
            Ok(_) -> Ok(Nil)
            Error(query_error) ->
              Error(MigrationStepFailed(step.version, query_error))
          }
        }),
      )
      use applied <- result.try(read_schema_generation(connection, steps))
      case applied {
        AtVersion(version) if version == step.version -> Ok(Nil)
        _ -> Error(IncompatibleSchema)
      }
    }
  }
}

/// Pins this transaction to `READ COMMITTED`, as its own literal first
/// statement — reusing the same Squirrel-generated query and rationale as
/// `grind/internal/unique_admission`'s own `pin_read_committed`. Not
/// load-bearing the same way there (a migration step's later reads have no
/// concurrent-commit-visibility requirement `READ COMMITTED` specifically
/// satisfies), but kept as the same defence in depth against a role or
/// database whose own `default_transaction_isolation` is not already
/// `READ COMMITTED`, for consistency and because the pool-level pin
/// (`postgres.validate`) — the two connections' first line of defence — can
/// still be silently dropped by a pooler between Grind and PostgreSQL.
fn pin_read_committed(connection: pog.Connection) -> Result(Nil, StorageError) {
  case
    store.call_safely(connection, fn(connection) {
      sql.pin_read_committed(connection)
    })
  {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(_) -> Ok(Nil)
  }
}

/// Serialises concurrent migrators (this process or another) against one
/// another: an ordinary PostgreSQL transaction-scoped advisory lock, held
/// until this step's own transaction commits or rolls back, keyed by a
/// fixed Grind string plus the current schema so unrelated schemas (or a
/// non-Grind advisory lock user) never collide with it. The identical
/// statement text is also embedded as every version's own first entry in
/// `migrations()`/`priv/migrations/*.sql`
/// (`migrations.advisory_lock_statement()`) so cigogne serialises the same
/// way; running it here too, unconditionally, is what makes `migrate_with`
/// itself safe even for a caller that only ever uses `postgres.migrate`.
fn acquire_migration_lock(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
  let query =
    pog.query(migrations.advisory_lock_statement())
    |> pog.returning({
      use locked <- decode.field(0, decode.bool)
      decode.success(locked)
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(_) -> Ok(Nil)
  }
}

/// The lowest schema version `migrate`/`migrate_with` ever accepts as an
/// already-installed generation. Anything below it (including the legacy,
/// never-migrated v10 experimental marker) fails closed as
/// `UnsupportedSchemaVersion`; there is no migration path onto v11.
const baseline_schema_version = 11

type SchemaGeneration {
  Fresh
  AtVersion(Int)
}

fn generation_at_least(generation: SchemaGeneration, version: Int) -> Bool {
  case generation {
    Fresh -> False
    AtVersion(current) -> current >= version
  }
}

/// Reads the schema's current generation against `steps` (the full step
/// list this `migrate_with` call was given — `migrate` itself always passes
/// `migrations()`; `latest` is the highest version among them):
///
/// - No `grind_schema_migrations` marker table: `Fresh` if no other
///   `grind_`-prefixed object exists either, else `IncompatibleSchema` (a
///   foreign or partial schema that never reached Grind's own bookkeeping).
/// - A marker table whose rows are not exactly the contiguous range
///   `{baseline_schema_version..max}` (a gap, an empty table, or a marker
///   starting below the baseline while also reaching at or above it):
///   `IncompatibleSchema`.
/// - `max` above `latest`: `UnsupportedSchemaVersion(max)` — a schema newer
///   than this call knows how to run against. Checked *before* any physical
///   shape check runs, so a schema tagged with a foreign or future marker
///   is rejected correctly even alongside unrelated foreign objects or a
///   broken shape at an earlier, otherwise-valid version.
/// - `max` below `baseline_schema_version` (the legacy, never-migrated v10
///   experimental marker, or any older one): `UnsupportedSchemaVersion(max)`.
/// - Otherwise, `max` is a version present in `steps`: `AtVersion(max)` once
///   the actual `grind_`-prefixed relation set (name and kind) is *exactly*
///   (no fewer, no more) that version's own `Migration.shape`, and every
///   `key_columns` entry in it is present — a defensive, fail-closed check
///   against a tampered or partially-repaired schema, never trusted from
///   the marker alone. A version absent from `steps` at this point cannot
///   occur: `max <= latest` and `max >= baseline_schema_version` were just
///   checked, and `migrate_with`'s own precondition guarantees `steps`
///   covers every version in that range — but is still handled as
///   `IncompatibleSchema` rather than a crash, defensively.
fn read_schema_generation(
  connection: pog.Connection,
  steps: List(migrations.Migration),
) -> Result(SchemaGeneration, StorageError) {
  let latest =
    list.fold(steps, 0, fn(highest, step) { int.max(highest, step.version) })
  use marker_table_exists <- result.try(schema_migrations_table_exists(
    connection,
  ))
  case marker_table_exists {
    False ->
      case read_grind_relations(connection) {
        Error(error) -> Error(error)
        Ok([]) -> Ok(Fresh)
        Ok(_) -> Error(IncompatibleSchema)
      }
    True -> {
      use #(count, minimum, maximum) <- result.try(read_schema_marker(
        connection,
      ))
      case count, minimum, maximum {
        0, _, _ -> Error(IncompatibleSchema)
        count, minimum, maximum if count != maximum - minimum + 1 ->
          Error(IncompatibleSchema)
        _, _, maximum if maximum > latest ->
          Error(UnsupportedSchemaVersion(maximum))
        _, _, maximum if maximum < baseline_schema_version ->
          Error(UnsupportedSchemaVersion(maximum))
        _, minimum, _ if minimum < baseline_schema_version ->
          Error(IncompatibleSchema)
        _, minimum, _ if minimum > baseline_schema_version ->
          Error(IncompatibleSchema)
        _, _, maximum -> validate_expected_shape(connection, maximum, steps)
      }
    }
  }
}

/// Quotes the schema name (`quote_ident`) before embedding it in the
/// textual argument `to_regclass` parses — a bare `current_schema() || '.'
/// || ...` would silently fold a mixed-case or otherwise identifier-quoted
/// schema name to lower case, `to_regclass` would then look up a schema
/// that does not exist, and this would wrongly report the marker table
/// absent even when installed and fully functional.
fn schema_migrations_table_exists(
  connection: pog.Connection,
) -> Result(Bool, StorageError) {
  let query =
    pog.query(
      "SELECT to_regclass(quote_ident(current_schema()) || '.grind_schema_migrations') IS NOT NULL",
    )
    |> pog.returning({
      use present <- decode.field(0, decode.bool)
      decode.success(present)
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [present] -> Ok(present)
        _ -> Error(IncompatibleSchema)
      }
  }
}

/// Every `grind_`-prefixed relation (table, sequence, index, ...) in the
/// current schema, as `#(relname, relkind)` — `relkind` is PostgreSQL's own
/// single-character code (`r`/`S`/`i`/...), cast to `text` explicitly since
/// its native `"char"` pseudo-type has no unique `||` overload against
/// `text` (`42725 ambiguous_function`) should a caller ever concatenate it.
/// Backs both the fresh-schema check (an empty result) and the per-version
/// exact-shape check (`relation_shape_matches`).
fn read_grind_relations(
  connection: pog.Connection,
) -> Result(List(#(String, String)), StorageError) {
  let query =
    pog.query(
      "SELECT c.relname, c.relkind::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND left(c.relname, 6) = 'grind_'",
    )
    |> pog.returning({
      use name <- decode.field(0, decode.string)
      use kind <- decode.field(1, decode.string)
      decode.success(#(name, kind))
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(returned) -> Ok(returned.rows)
  }
}

fn read_schema_marker(
  connection: pog.Connection,
) -> Result(#(Int, Int, Int), StorageError) {
  let query =
    pog.query(
      "SELECT count(*)::bigint, COALESCE(min(version), 0)::bigint, COALESCE(max(version), 0)::bigint FROM grind_schema_migrations",
    )
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      use minimum <- decode.field(1, decode.int)
      use maximum <- decode.field(2, decode.int)
      decode.success(#(count, minimum, maximum))
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [version] -> Ok(version)
        _ -> Error(IncompatibleSchema)
      }
  }
}

/// The per-version physical-shape check backing `read_schema_generation`'s
/// fail-closed guarantee: `version`'s own `Migration.shape`, looked up from
/// `steps`, must match the current schema's actual `grind_`-prefixed
/// relation set *exactly* (a stray relation this shape does not list — a
/// leftover from a version this schema was never fully repaired from, or
/// genuinely foreign clutter — fails closed exactly like a missing one).
fn validate_expected_shape(
  connection: pog.Connection,
  version: Int,
  steps: List(migrations.Migration),
) -> Result(SchemaGeneration, StorageError) {
  case list.find(steps, fn(step) { step.version == version }) {
    Error(Nil) -> Error(IncompatibleSchema)
    Ok(step) -> {
      use shape_ok <- result.try(relation_shape_matches(connection, step.shape))
      case shape_ok {
        False -> Error(IncompatibleSchema)
        True -> Ok(AtVersion(version))
      }
    }
  }
}

fn relation_kind_code(kind: migrations.RelationKind) -> String {
  case kind {
    migrations.Table -> "r"
    migrations.Sequence -> "S"
    migrations.Index -> "i"
  }
}

fn relation_shape_matches(
  connection: pog.Connection,
  shape: List(migrations.ExpectedRelation),
) -> Result(Bool, StorageError) {
  use actual <- result.try(read_grind_relations(connection))
  let expected =
    shape
    |> list.map(fn(relation) {
      #(relation.name, relation_kind_code(relation.kind))
    })
    |> list.sort(fn(a, b) { string.compare(a.0, b.0) })
  case list.sort(actual, fn(a, b) { string.compare(a.0, b.0) }) == expected {
    False -> Ok(False)
    True -> relation_key_columns_match(connection, shape)
  }
}

fn relation_key_columns_match(
  connection: pog.Connection,
  shape: List(migrations.ExpectedRelation),
) -> Result(Bool, StorageError) {
  shape
  |> list.filter(fn(relation) { relation.key_columns != [] })
  |> list.try_fold(True, fn(all_matched_so_far, relation) {
    case all_matched_so_far {
      False -> Ok(False)
      True ->
        relation_has_columns(connection, relation.name, relation.key_columns)
    }
  })
}

fn relation_has_columns(
  connection: pog.Connection,
  table_name: String,
  columns: List(String),
) -> Result(Bool, StorageError) {
  let query =
    pog.query(
      "SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1 AND column_name = ANY($2)",
    )
    |> pog.parameter(pog.text(table_name))
    |> pog.parameter(pog.array(pog.text, columns))
    |> pog.returning({
      use present <- decode.field(0, decode.int)
      decode.success(present)
    })
  case store.execute_safely(query, on: connection) {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [present] -> Ok(present == list.length(columns))
        _ -> Error(IncompatibleSchema)
      }
  }
}

/// Persists an immediate job without invoking its worker. May return
/// `submission.EmptyQueueName` (an empty `queue`) or
/// `submission.CommitUnknownWithoutId` (the insert's own query failed, its
/// reply was lost, or its checkout failed outright — see that variant's doc
/// comment); never `submission.AdmissionContended`, `submission.SubmissionConflict`,
/// `submission.NotCommitted`, or `submission.CommitUnknown`, which only ever come
/// from `submit_unique`/`submit_with_id`'s admission transaction.
pub fn submit(
  database: Database,
  queue: String,
  worker: Worker(input, output, error),
  input: input,
) -> Result(
  JobHandle(input, output, error),
  submission.SubmitError(input, output, error),
) {
  submit_with_availability(database, queue, worker, input, None)
}

/// Persists a job at an absolute Unix-millisecond availability time. May
/// return `submission.EmptyQueueName` or `submission.CommitUnknownWithoutId`; never
/// `submission.AdmissionContended`, `submission.SubmissionConflict`,
/// `submission.NotCommitted`, or `submission.CommitUnknown` — the same possible and
/// impossible variants as `submit`.
pub fn submit_at(
  database: Database,
  queue: String,
  worker: Worker(input, output, error),
  input: input,
  available_at: job.AvailableAt,
) -> Result(
  JobHandle(input, output, error),
  submission.SubmitError(input, output, error),
) {
  submit_with_availability(
    database,
    queue,
    worker,
    input,
    Some(job.available_at_unix_milliseconds(available_at)),
  )
}

fn submit_with_availability(
  database: Database,
  queue: String,
  worker: Worker(input, output, error),
  input: input,
  available_at_unix_ms: Option(Int),
) -> Result(
  JobHandle(input, output, error),
  submission.SubmitError(input, output, error),
) {
  case queue {
    "" -> Error(submission.EmptyQueueName)
    _ -> {
      let Database(connection:, storage_owner:, forwarder:, ..) = database
      let worker.Metadata(
        id: worker_id,
        worker_version:,
        input_version:,
        output_version:,
        error_version:,
        max_attempts:,
      ) = worker.metadata(worker)
      let error_parameter = case error_version {
        Some(version) -> pog.text(version)
        None -> pog.null()
      }
      let availability_parameter = case available_at_unix_ms {
        Some(availability) -> pog.int(availability)
        None -> pog.null()
      }
      let sql =
        "INSERT INTO grind_jobs (storage_owner, queue, worker_id, worker_version, input_version, input, output_version, error_version, max_attempts, state, available_at) "
        <> "VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, "
        <> "CASE WHEN $10::bigint IS NULL OR $10::bigint <= (extract(epoch FROM clock_timestamp()) * 1000)::bigint THEN 'queued' ELSE 'scheduled' END, "
        <> "CASE WHEN $10::bigint IS NULL THEN clock_timestamp() ELSE to_timestamp($10::double precision / 1000.0) END) RETURNING id, state, (extract(epoch FROM available_at) * 1000)::bigint"
      let query =
        pog.query(sql)
        |> pog.parameter(pog.text(storage_owner))
        |> pog.parameter(pog.text(queue))
        |> pog.parameter(pog.text(worker_id))
        |> pog.parameter(pog.text(worker_version))
        |> pog.parameter(pog.text(input_version))
        |> pog.parameter(pog.text(worker.encode_input(worker, input)))
        |> pog.parameter(pog.text(output_version))
        |> pog.parameter(error_parameter)
        |> pog.parameter(pog.int(max_attempts))
        |> pog.parameter(availability_parameter)
        |> pog.returning({
          use id <- decode.field(0, decode.int)
          use state <- decode.field(1, decode.string)
          use available_at_ms <- decode.field(2, decode.int)
          decode.success(#(id, state, available_at_ms))
        })
      case store.execute_safely(query, on: connection) {
        Error(error) -> Error(submission.CommitUnknownWithoutId(error))
        Ok(returned) ->
          case returned.rows {
            [#(id, state, available_at_ms)] -> {
              case job.state_of_stored(state) {
                Error(Nil) -> Nil
                Ok(committed_state) ->
                  emit_admitted(
                    forwarder,
                    queue,
                    id,
                    worker_id,
                    worker_version,
                    committed_state,
                    Some(available_at_ms),
                    None,
                    observation.Replied,
                  )
              }
              Ok(job.new_handle(id, storage_owner, queue, worker))
            }
            _ ->
              panic as "submit: unconditional single-row INSERT ... RETURNING returned a row count other than one"
          }
      }
    }
  }
}

/// Builds and forwards `[grind, job, admitted]`, shared by a plain
/// `submit`/`submit_at` admission and every `submit_unique` decision.
/// `submission_id`/`confirmation` are `None`/always `Replied` for a plain
/// submission (there is no receipt concept there — every plain submission is
/// its own fresh commit); see `AdmittedMetadata` for the unique case.
fn emit_admitted(
  fwd: Forwarder,
  queue: String,
  job_id: Int,
  worker_id: String,
  worker_version: String,
  committed_state: State,
  available_at_unix_ms: Option(Int),
  submission_id: Option(String),
  confirmation: observation.Confirmation,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      observation.admitted(),
      observation.AdmittedMeasurements(count: 1),
      observation.AdmittedMetadata(
        ref: observation.JobRef(job_id:, queue:, worker_id:, worker_version:),
        committed_state:,
        available_at_unix_ms:,
        submission_id:,
        confirmation:,
      ),
    )
  Nil
}

/// The shared read-path error space for `bind_handle`, `arguments`, `state`,
/// `outcome`, and `reconcile_acknowledgement` — every function that reads an
/// already-admitted job by its durable identity. Not every variant is
/// reachable from every function; each function's own doc comment says
/// which.
pub type JobReadError {
  JobReadQueryFailed(pog.QueryError)
  /// No row matches this id. For `bind_handle` this also covers a row that
  /// exists under a different storage owner: its own lookup filters by
  /// owner in the same query, so the two cases cannot be told apart from
  /// the read alone — the other four functions already hold a handle bound
  /// to a specific owner to compare against, so they report
  /// `StorageOwnerMismatch` instead in that case.
  JobNotFound
  /// The row's storage owner does not match this handle's own owner (a
  /// handle bound against a different `Database`, or read back after the
  /// database was reopened against a different storage owner).
  StorageOwnerMismatch
  QueueRouteMismatch(expected: String, actual: String)
  WorkerContractMismatch(
    expected_id: String,
    expected_version: String,
    actual_id: String,
    actual_version: String,
  )
  /// `bind_handle` only: the stored input/output/error codec version for
  /// `kind` does not match the worker definition's own (the first of the
  /// three that disagrees, checked input/output/error in that order). No
  /// decode is attempted — `bind_handle` never reads a codec'd value, only
  /// checks that a future read through the returned handle would be
  /// compatible.
  CodecContractMismatch(
    kind: worker.CodecKind,
    expected: String,
    actual: String,
  )
  /// A codec'd value's own version matched, but its stored JSON failed to
  /// decode under it — or its version did not match after all (see
  /// `worker.StoredCodecError`).
  CodecFailed(worker.StoredCodecError)
  /// The stored job state, or another stored enum-shaped column (a
  /// `business_failed` row's `failure_cause`), is not one of the values this
  /// code recognizes — unreachable without direct tampering, since these
  /// columns carry a `CHECK` constraint against the same closed vocabulary.
  /// The payload is always the raw stored value that failed to parse.
  InvalidStoredState(String)
  /// `outcome` only: a `succeeded` row has no stored output at all —
  /// unreachable without direct tampering, since every `succeeded` write
  /// site also writes the output in the same statement.
  SucceededOutputMissing
  /// `reconcile_acknowledgement` only: no acknowledgement receipt exists
  /// under this command ID — the acknowledgement it names never committed
  /// (or has since been pruned). Distinct from `JobNotFound`: a missing
  /// receipt says nothing about whether the job row itself exists.
  ReceiptNotFound
  /// `reconcile_acknowledgement` only: a receipt exists under this command
  /// ID, but its own recorded job ID does not match this handle's job ID —
  /// the command ID was read back against the wrong handle.
  ReceiptJobMismatch(expected: Int, actual: Int)
}

/// Reconstructs a typed handle from durable identity after an application
/// restart. `id` is scoped to this database's storage owner, and the current
/// worker definition must exactly match the stored worker and codec
/// contract. May return `JobReadQueryFailed`, `JobNotFound` (also covers a
/// row under a different storage owner — the lookup filters by owner in the
/// same query), `WorkerContractMismatch`, or `CodecContractMismatch`; never
/// `StorageOwnerMismatch`, `QueueRouteMismatch`, `CodecFailed`,
/// `InvalidStoredState`, `SucceededOutputMissing`, `ReceiptNotFound`, or
/// `ReceiptJobMismatch`, which only ever come from the other four
/// `JobReadError`-returning functions.
pub fn bind_handle(
  database: Database,
  worker: Worker(input, output, error),
  id: Int,
) -> Result(JobHandle(input, output, error), JobReadError) {
  let Database(connection:, storage_owner:, ..) = database
  let worker.Metadata(
    id: expected_worker_id,
    worker_version: expected_worker_version,
    input_version: expected_input_version,
    output_version: expected_output_version,
    error_version: expected_error_version,
    ..,
  ) = worker.metadata(worker)
  case
    store.call_safely(connection, fn(connection) {
      sql.bind_handle(connection, id, storage_owner)
    })
  {
    Error(error) -> Error(JobReadQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Error(JobNotFound)
        [
          sql.BindHandleRow(
            queue:,
            worker_id: stored_worker_id,
            worker_version: stored_worker_version,
            input_version: stored_input_version,
            output_version: stored_output_version,
            error_version: stored_error_version,
          ),
        ] ->
          case
            stored_worker_id == expected_worker_id
            && stored_worker_version == expected_worker_version
          {
            False ->
              Error(WorkerContractMismatch(
                expected_id: expected_worker_id,
                expected_version: expected_worker_version,
                actual_id: stored_worker_id,
                actual_version: stored_worker_version,
              ))
            True ->
              case stored_input_version == expected_input_version {
                False ->
                  Error(CodecContractMismatch(
                    kind: worker.InputCodec,
                    expected: expected_input_version,
                    actual: stored_input_version,
                  ))
                True ->
                  case stored_output_version == expected_output_version {
                    False ->
                      Error(CodecContractMismatch(
                        kind: worker.OutputCodec,
                        expected: expected_output_version,
                        actual: stored_output_version,
                      ))
                    True ->
                      case stored_error_version == expected_error_version {
                        False ->
                          Error(CodecContractMismatch(
                            kind: worker.ErrorCodec,
                            expected: unwrap(expected_error_version, "none"),
                            actual: unwrap(stored_error_version, "none"),
                          ))
                        True ->
                          Ok(job.new_handle(id, storage_owner, queue, worker))
                      }
                  }
              }
          }
        _ -> Error(JobNotFound)
      }
  }
}

/// Reloads the typed, version-checked input from PostgreSQL. May return
/// `JobReadQueryFailed`, `JobNotFound`, `StorageOwnerMismatch`,
/// `QueueRouteMismatch`, `WorkerContractMismatch`, or `CodecFailed`; never
/// `CodecContractMismatch` (`bind_handle`'s own pre-decode check),
/// `InvalidStoredState`, `SucceededOutputMissing`, `ReceiptNotFound`, or
/// `ReceiptJobMismatch` — `arguments` never parses the stored job state or a
/// receipt.
pub fn arguments(
  database: Database,
  handle: JobHandle(input, output, error),
) -> Result(input, JobReadError) {
  let Database(connection:, storage_owner:, ..) = database
  let #(id, handle_owner, handle_queue, worker_id, worker_version, input_codec) =
    job.storage_fields(handle)
  case
    store.call_safely(connection, fn(connection) {
      sql.arguments(connection, id)
    })
  {
    Error(error) -> Error(JobReadQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Error(JobNotFound)
        [
          sql.ArgumentsRow(
            input: encoded,
            input_version: codec_version,
            storage_owner: stored_owner,
            queue: stored_queue,
            worker_id: stored_worker,
            worker_version: stored_worker_version,
          ),
        ] ->
          case stored_owner == storage_owner && handle_owner == storage_owner {
            False -> Error(StorageOwnerMismatch)
            True ->
              case stored_queue == handle_queue {
                False ->
                  Error(QueueRouteMismatch(
                    expected: handle_queue,
                    actual: stored_queue,
                  ))
                True ->
                  case
                    stored_worker == worker_id
                    && stored_worker_version == worker_version
                  {
                    False ->
                      Error(WorkerContractMismatch(
                        expected_id: worker_id,
                        expected_version: worker_version,
                        actual_id: stored_worker,
                        actual_version: stored_worker_version,
                      ))
                    True ->
                      worker.decode_codec(input_codec, codec_version, encoded)
                      |> result.map_error(CodecFailed)
                  }
              }
          }
        _ -> Error(JobNotFound)
      }
  }
}

pub type CancellationResult {
  CancelledBeforeRun
  CancellationRequested
  AlreadyCancelled
  AlreadyUncertain
  AlreadyFinished(job.State)
}

pub type CancellationError {
  CancellationQueryFailed(pog.QueryError)
  CancellationCommitUnknown
  CancellationWriteRejected
  CancellationStorageOwnerMismatch
  CancellationQueueMismatch
  CancellationWorkerContractMismatch
  CancellationJobNotFound
  CancellationInvalidStoredState(String)
}

/// Request cancellation without conflating a running worker's proposal with
/// the state committed by its later acknowledgement.
pub fn cancel(
  database: Database,
  handle: JobHandle(input, output, error),
) -> Result(CancellationResult, CancellationError) {
  let Database(connection:, storage_owner:, forwarder:, ..) = database
  let #(id, handle_owner, queue, worker_id, worker_version, _) =
    job.storage_fields(handle)
  case storage_owner == handle_owner {
    False -> Error(CancellationStorageOwnerMismatch)
    True ->
      case
        store.transaction_safely(connection, fn(transaction) {
          cancel_transaction(
            transaction,
            storage_owner,
            id,
            queue,
            worker_id,
            worker_version,
          )
        })
      {
        Ok(#(result, previous_state)) -> {
          case
            cancellation_outcome_of(result),
            job.state_of_stored(previous_state)
          {
            Some(outcome), Ok(previous_state) ->
              emit_cancellation(
                forwarder,
                queue,
                id,
                worker_id,
                worker_version,
                previous_state,
                outcome,
              )
            _, _ -> Nil
          }
          Ok(result)
        }
        Error(pog.TransactionQueryError(_)) -> Error(CancellationCommitUnknown)
        Error(pog.TransactionRolledBack(error)) -> Error(error)
      }
  }
}

/// Builds and forwards `[grind, job, cancellation_decided]` from a proven-committed
/// `CancellationResult`. Called only from `cancel`, strictly after
/// `transaction_safely` has already returned — never from inside a
/// transaction callback (the same discipline `acknowledge` and
/// `resolve_uncertain` follow). Only `CancelledBeforeRun` and
/// `CancellationRequested` are genuine writes; every read-only outcome emits
/// nothing.
/// Which `[grind, job, cancellation_decided]` outcome, if any, a `CancellationResult`
/// reports. Only `CancelledBeforeRun`/`CancellationRequested` are genuine
/// writes; every read-only outcome (`AlreadyCancelled`, `AlreadyUncertain`,
/// `AlreadyFinished`) maps to `None` and must never emit.
fn cancellation_outcome_of(
  result: CancellationResult,
) -> Option(observation.CancellationOutcome) {
  case result {
    CancelledBeforeRun -> Some(observation.CancellationDecidedBeforeRun)
    CancellationRequested -> Some(observation.CancellationDecidedWhileRunning)
    AlreadyCancelled | AlreadyUncertain | AlreadyFinished(_) -> None
  }
}

fn emit_cancellation(
  fwd: Forwarder,
  queue: String,
  job_id: Int,
  worker_id: String,
  worker_version: String,
  previous_state: State,
  outcome: observation.CancellationOutcome,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      observation.cancellation_decided(),
      observation.CancellationMeasurements(count: 1),
      observation.CancellationMetadata(
        ref: observation.JobRef(job_id:, queue:, worker_id:, worker_version:),
        previous_state:,
        outcome:,
      ),
    )
  Nil
}

fn cancel_transaction(
  connection: pog.Connection,
  storage_owner: String,
  id: Int,
  queue: String,
  worker_id: String,
  worker_version: String,
) -> Result(#(CancellationResult, String), CancellationError) {
  use returned <- result.try(
    case
      store.call_safely(connection, fn(connection) {
        sql.cancel_lock(connection, id)
      })
    {
      Error(error) -> Error(CancellationQueryFailed(error))
      Ok(returned) -> Ok(returned)
    },
  )
  case returned.rows {
    [] -> Error(CancellationJobNotFound)
    [
      sql.CancelLockRow(
        storage_owner: stored_owner,
        queue: stored_queue,
        worker_id: stored_worker,
        worker_version: stored_version,
        state:,
      ),
    ] ->
      case stored_owner == storage_owner {
        False -> Error(CancellationStorageOwnerMismatch)
        True ->
          case stored_queue == queue {
            False -> Error(CancellationQueueMismatch)
            True ->
              case
                stored_worker == worker_id && stored_version == worker_version
              {
                False -> Error(CancellationWorkerContractMismatch)
                True -> {
                  use result <- result.try(cancel_locked_state(
                    connection,
                    id,
                    state,
                  ))
                  Ok(#(result, state))
                }
              }
          }
      }
    _ -> Error(CancellationJobNotFound)
  }
}

fn cancel_locked_state(
  connection: pog.Connection,
  id: Int,
  state: String,
) -> Result(CancellationResult, CancellationError) {
  case state {
    "queued" | "scheduled" | "retryable" -> {
      use returned <- result.try(
        case
          store.call_safely(connection, fn(connection) {
            sql.cancel_before_run(connection, id)
          })
        {
          Error(error) -> Error(CancellationQueryFailed(error))
          Ok(returned) -> Ok(returned)
        },
      )
      case returned.rows {
        [_] -> Ok(CancelledBeforeRun)
        _ -> Error(CancellationWriteRejected)
      }
    }
    "executing" -> {
      use returned <- result.try(
        case
          store.call_safely(connection, fn(connection) {
            sql.cancel_executing(connection, id)
          })
        {
          Error(error) -> Error(CancellationQueryFailed(error))
          Ok(returned) -> Ok(returned)
        },
      )
      case returned.rows {
        [_] -> Ok(CancellationRequested)
        _ -> Error(CancellationWriteRejected)
      }
    }
    "cancelled" -> Ok(AlreadyCancelled)
    "succeeded" -> Ok(AlreadyFinished(job.Succeeded))
    "business_failed" -> Ok(AlreadyFinished(job.BusinessFailed))
    "runtime_failed" -> Ok(AlreadyFinished(job.RuntimeFailed))
    "contract_mismatch" -> Ok(AlreadyFinished(job.ContractMismatch))
    "uncertain" -> Ok(AlreadyUncertain)
    "discarded" -> Ok(AlreadyFinished(job.Discarded))
    other -> Error(CancellationInvalidStoredState(other))
  }
}

/// Failures returned while running one claimed job. `QueueCodecMismatch` is
/// returned after the row is durably marked `ContractMismatch`; it is an error
/// to the batch caller but a committed job disposition.
pub type QueueRunError {
  QueueClaimFailed(pog.QueryError)
  QueueClaimReleaseFailed(pog.QueryError)
  QueueClaimReleaseRejected
  /// A query inside the acknowledgement transaction itself failed, so the
  /// callback returned `Error` and `COMMIT` was never sent — pog reports
  /// this as a controlled `TransactionRolledBack`, never routed through the
  /// `QueueAckUnknown` reconciliation path. Genuinely did not commit, the
  /// same "callback failed, therefore no commit was ever attempted"
  /// reasoning `submission.NotCommitted`'s doc comment gives for the
  /// uniqueness-admission path. Safe to retry the same acknowledgement.
  QueueAckFailed(pog.QueryError)
  /// The ACK transaction may have committed although its reply was lost.
  /// Reconcile this stable command ID before considering any replay.
  QueueAckUnknown(command_id: String, proposed: worker.Execution)
  /// The exact worker proposal was not committed because the claim no longer
  /// owned a live, executing row. The attached reason is a database snapshot.
  QueueAckStale(proposed: worker.Execution, reason: AckRejection)
  /// A fenced query returned a row count this module does not treat as
  /// ordinary contention: an unregistered worker at claim time (candidate
  /// selection should have excluded it) or more than one row matched by a
  /// primary-key-fenced update (impossible, since `id` is a primary key) —
  /// both a storage-layer contract this module itself relies on being
  /// violated. `mark_contract_mismatch` also reports this for zero rows:
  /// the claim's own fence no longer matched between claiming the row and
  /// recording the mismatch — a lost-fence race, not an impossible state,
  /// just one this path has no more specific recovery for today.
  QueueStorageInvariantViolated
  QueueAckCommandConflict
  QueueAckProposalCodecMismatch(
    kind: worker.CodecKind,
    expected: String,
    actual: String,
  )
  QueueCodecMismatch(kind: worker.CodecKind, expected: String, actual: String)
}

pub type AckRejection {
  /// No row at all was found for this job id — there is no row to read a
  /// state, fence, or lease from.
  AckRecordMissing
  AckLeaseExpired(attempt_id: Int, epoch: Int, owner: String)
  /// The row was still `executing`, but its own fencing fields no longer
  /// match this attempt. Every other variant but `AckStateChanged` is only
  /// ever constructed while the row is `executing` too, so `state` is not
  /// repeated on any of them — it would carry no information.
  AckOwnershipChanged(
    attempt_id: Option(Int),
    epoch: Option(Int),
    owner: Option(String),
  )
  AckStateChanged(state: job.State)
  /// The row was still `executing`, still fenced to this exact attempt,
  /// and its lease was still live — every check this module can make
  /// passed — yet the acknowledgement's own fenced write still matched no
  /// row. Genuinely unexplained by anything this re-read can see. Carries
  /// no fields: by construction, the re-read's own attempt_id/epoch/owner
  /// already equal the caller's own claim, so repeating them back would
  /// carry no information.
  AckRecordChanged
}

/// Durable attribution for one acknowledged attempt. The typed value is read
/// from the job's current outcome; the receipt retains only the committed
/// disposition needed to reconcile an ACK after its reply is lost.
pub type AcknowledgementReceipt {
  AcknowledgementReceipt(
    command_id: String,
    attempt_id: Int,
    attempt_epoch: Int,
    committed_state: job.State,
    business_failure_cause: Option(worker.BusinessFailureCause),
    committed_at_unix_ms: Int,
  )
}

type ResolutionCommand {
  ResolutionCommand(
    id: Int,
    database_owner: String,
    handle_owner: String,
    queue: String,
    worker_id: String,
    worker_version: String,
    expected_output_version: String,
    expected_error_version: Option(String),
    resolution_id: String,
    resolved_by: String,
    details: String,
    decision: String,
    target_state: String,
    output_version: String,
    encoded_output: Option(String),
    error_version: Option(String),
    encoded_error: Option(String),
    failure_description: Option(String),
  )
}

@internal
pub fn storage_owner(database: Database) -> String {
  let Database(storage_owner:, ..) = database
  storage_owner
}

/// Exposed only so `grind/internal/attempt` can read a connection out of an
/// opaque `Database` — that module cannot pattern-match `Database`'s own
/// constructor, which stays private to this module.
@internal
pub fn connection(database: Database) -> pog.Connection {
  let Database(connection:, ..) = database
  connection
}

/// Exposed only so `grind/internal/attempt` can read a forwarder out of an
/// opaque `Database`; see `connection`.
@internal
pub fn forwarder(database: Database) -> Forwarder {
  let Database(forwarder:, ..) = database
  forwarder
}

pub type QuarantineError {
  /// `limit` was not positive; nothing was touched.
  NonPositiveLimit
  QuarantineQueryFailed(pog.QueryError)
}

/// Quarantines up to `limit` expired `executing` rows across every queue in
/// this database's storage owner — not only the queues some running
/// consumer happens to poll. Intended for queues no consumer currently
/// polls (an idle queue, a worker retired without a replacement consumer,
/// or an operational sweep run outside any consumer's own poll loop); a
/// polled queue's own consumer already quarantines its expired rows as part
/// of `claim_one`, so calling this for a polled queue too is safe (it is the
/// same fenced, idempotent `UPDATE ... WHERE state = 'executing' AND
/// <expired>`, `SKIP LOCKED` against a concurrent claim or another
/// quarantine scan) but redundant. Like every other quarantine scan, this
/// never decodes or runs any worker code and is not restricted to any
/// registered worker identity or version — see
/// `grind/internal/lease.quarantine_expired_in_queue`, which shares this
/// function's `expired_lease_predicate` and `UPDATE` fragment
/// (`quarantine_update_sql`). Returns the number of rows actually
/// quarantined, which can be fewer than `limit` (nothing else expired) —
/// never more.
pub fn quarantine_expired(
  database: Database,
  limit limit: Int,
) -> Result(Int, QuarantineError) {
  case limit > 0 {
    False -> Error(NonPositiveLimit)
    True -> {
      let Database(connection:, storage_owner:, forwarder:, ..) = database
      let sql =
        lease.quarantine_update_sql(
          "SELECT id FROM grind_jobs WHERE storage_owner = $1 AND state = 'executing' AND "
          <> lease.expired_lease_predicate("clock_timestamp()")
          <> " ORDER BY id FOR UPDATE SKIP LOCKED LIMIT $2",
        )
      let query =
        pog.query(sql)
        |> pog.parameter(pog.text(storage_owner))
        |> pog.parameter(pog.int(limit))
        |> pog.returning(lease.quarantine_row_decoder())
      case store.execute_safely(query, on: connection) {
        Error(error) -> Error(QuarantineQueryFailed(error))
        Ok(returned) -> {
          list.each(returned.rows, fn(row) {
            lease.emit_quarantined(forwarder, row)
          })
          Ok(list.length(returned.rows))
        }
      }
    }
  }
}

/// May return `JobReadQueryFailed`, `JobNotFound`, `StorageOwnerMismatch`,
/// `QueueRouteMismatch`, `WorkerContractMismatch`, or `InvalidStoredState`;
/// never `CodecContractMismatch` or `CodecFailed` (`state` never touches a
/// codec'd value), `SucceededOutputMissing`, `ReceiptNotFound`, or
/// `ReceiptJobMismatch`.
pub fn state(
  database: Database,
  handle: JobHandle(input, output, error),
) -> Result(State, JobReadError) {
  let Database(connection:, storage_owner:, ..) = database
  let #(id, handle_owner, handle_queue, worker_id, worker_version, _) =
    job.storage_fields(handle)
  case
    store.call_safely(connection, fn(connection) { sql.state(connection, id) })
  {
    Error(error) -> Error(JobReadQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Error(JobNotFound)
        [
          sql.StateRow(
            storage_owner: stored_owner,
            queue: stored_queue,
            worker_id: stored_worker,
            worker_version: stored_worker_version,
            state:,
          ),
        ] ->
          case stored_owner == storage_owner && handle_owner == storage_owner {
            False -> Error(StorageOwnerMismatch)
            True ->
              case stored_queue == handle_queue {
                False ->
                  Error(QueueRouteMismatch(
                    expected: handle_queue,
                    actual: stored_queue,
                  ))
                True ->
                  case
                    stored_worker == worker_id
                    && stored_worker_version == worker_version
                  {
                    False ->
                      Error(WorkerContractMismatch(
                        expected_id: worker_id,
                        expected_version: worker_version,
                        actual_id: stored_worker,
                        actual_version: stored_worker_version,
                      ))
                    True ->
                      job.state_of_stored(state)
                      |> result.replace_error(InvalidStoredState(state))
                  }
              }
          }
        _ -> Error(JobNotFound)
      }
  }
}

/// Reads the last committed result without confusing it with a handler
/// return. May return `JobReadQueryFailed`, `JobNotFound`,
/// `StorageOwnerMismatch`, `QueueRouteMismatch`, `WorkerContractMismatch`,
/// `CodecFailed`, `InvalidStoredState`, or `SucceededOutputMissing`; never
/// `CodecContractMismatch` (`bind_handle`'s own pre-decode check),
/// `ReceiptNotFound`, or `ReceiptJobMismatch`.
pub fn outcome(
  database: Database,
  handle: JobHandle(input, output, error),
) -> Result(job.Outcome(output, error), JobReadError) {
  let Database(connection:, storage_owner:, ..) = database
  let #(
    id,
    handle_owner,
    handle_queue,
    worker_id,
    worker_version,
    output_codec,
    error_codec,
  ) = job.result_fields(handle)
  case
    store.call_safely(connection, fn(connection) { sql.outcome(connection, id) })
  {
    Error(error) -> Error(JobReadQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Error(JobNotFound)
        [
          sql.OutcomeRow(
            storage_owner: stored_owner,
            queue: stored_queue,
            worker_id: stored_worker,
            worker_version: stored_worker_version,
            state:,
            output: encoded_output,
            output_version:,
            error: encoded_error,
            error_version:,
            failure_description:,
            failure_cause:,
          ),
        ] ->
          outcome_from_row(
            storage_owner,
            handle_owner,
            handle_queue,
            worker_id,
            worker_version,
            output_codec,
            error_codec,
            #(
              stored_owner,
              stored_queue,
              stored_worker,
              stored_worker_version,
              state,
              encoded_output,
              output_version,
              encoded_error,
              error_version,
              failure_description,
              failure_cause,
            ),
          )
        _ -> Error(JobNotFound)
      }
  }
}

/// Reads a compact acknowledgement receipt by its stable command ID.
/// Historical typed values are not retained here; `outcome` reads the job's
/// current typed result independently. May return `JobReadQueryFailed`,
/// `StorageOwnerMismatch`, `QueueRouteMismatch`, `WorkerContractMismatch`,
/// `InvalidStoredState`, `ReceiptNotFound` (no receipt exists under this
/// command ID), or `ReceiptJobMismatch` (a receipt exists but names a
/// different job); never `JobNotFound`, `CodecContractMismatch`,
/// `CodecFailed`, or `SucceededOutputMissing`, since a receipt carries no
/// codec'd value of its own and is looked up by command ID, not job ID.
pub fn reconcile_acknowledgement(
  database: Database,
  handle: JobHandle(input, output, error),
  command_id: String,
) -> Result(AcknowledgementReceipt, JobReadError) {
  let Database(connection:, storage_owner: database_owner, ..) = database
  let #(id, handle_owner, queue, worker_id, worker_version, _) =
    job.storage_fields(handle)
  case handle_owner == database_owner {
    False -> Error(StorageOwnerMismatch)
    True -> {
      case
        store.call_safely(connection, fn(connection) {
          sql.reconcile_acknowledgement(connection, database_owner, command_id)
        })
      {
        Error(error) -> Error(JobReadQueryFailed(error))
        Ok(returned) ->
          case returned.rows {
            [] -> Error(ReceiptNotFound)
            [receipt] -> {
              let sql.ReconcileAcknowledgementRow(
                storage_owner: stored_owner,
                queue: stored_queue,
                job_id: stored_id,
                worker_id: stored_worker,
                worker_version: stored_worker_version,
                attempt_id:,
                attempt_epoch:,
                committed_state:,
                failure_cause:,
                committed_at_unix_ms:,
              ) = receipt
              case stored_owner == database_owner {
                False -> Error(StorageOwnerMismatch)
                True ->
                  case stored_id == id {
                    False ->
                      Error(ReceiptJobMismatch(expected: id, actual: stored_id))
                    True ->
                      case stored_queue == queue {
                        False ->
                          Error(QueueRouteMismatch(
                            expected: queue,
                            actual: stored_queue,
                          ))
                        True ->
                          case
                            stored_worker == worker_id
                            && stored_worker_version == worker_version
                          {
                            False ->
                              Error(WorkerContractMismatch(
                                expected_id: worker_id,
                                expected_version: worker_version,
                                actual_id: stored_worker,
                                actual_version: stored_worker_version,
                              ))
                            True -> {
                              use committed_state <- result.try(
                                acknowledgement_state(committed_state),
                              )
                              use failure_cause <- result.try(
                                acknowledgement_failure_cause(failure_cause),
                              )
                              Ok(AcknowledgementReceipt(
                                command_id:,
                                attempt_id:,
                                attempt_epoch:,
                                committed_state:,
                                business_failure_cause: failure_cause,
                                committed_at_unix_ms:,
                              ))
                            }
                          }
                      }
                  }
              }
            }
            _ -> Error(ReceiptNotFound)
          }
      }
    }
  }
}

fn acknowledgement_state(state: String) -> Result(job.State, JobReadError) {
  case state {
    "queued" -> Ok(job.Queued)
    "scheduled" -> Ok(job.Scheduled)
    "retryable" -> Ok(job.Retryable)
    "executing" -> Ok(job.Executing)
    "succeeded" -> Ok(job.Succeeded)
    "business_failed" -> Ok(job.BusinessFailed)
    "runtime_failed" -> Ok(job.RuntimeFailed)
    "contract_mismatch" -> Ok(job.ContractMismatch)
    "uncertain" -> Ok(job.Uncertain)
    "discarded" -> Ok(job.Discarded)
    "cancelled" -> Ok(job.Cancelled)
    other -> Error(InvalidStoredState(other))
  }
}

fn acknowledgement_failure_cause(
  cause: Option(String),
) -> Result(Option(worker.BusinessFailureCause), JobReadError) {
  case cause {
    None -> Ok(None)
    Some(raw) ->
      worker.business_failure_cause_from_string(raw)
      |> result.map(Some)
      |> result.replace_error(InvalidStoredState(raw))
  }
}

fn outcome_from_row(
  database_owner: String,
  handle_owner: String,
  handle_queue: String,
  handle_worker: String,
  handle_worker_version: String,
  output_codec: worker.Codec(output),
  error_codec: Option(worker.Codec(error)),
  stored: #(
    String,
    String,
    String,
    String,
    String,
    Option(String),
    String,
    Option(String),
    Option(String),
    Option(String),
    Option(String),
  ),
) -> Result(job.Outcome(output, error), JobReadError) {
  let #(
    stored_owner,
    stored_queue,
    stored_worker,
    stored_worker_version,
    state,
    encoded_output,
    output_version,
    encoded_error,
    error_version,
    failure_description,
    failure_cause,
  ) = stored
  case stored_owner == database_owner && handle_owner == database_owner {
    False -> Error(StorageOwnerMismatch)
    True ->
      case stored_queue == handle_queue {
        False ->
          Error(QueueRouteMismatch(expected: handle_queue, actual: stored_queue))
        True ->
          case
            stored_worker == handle_worker
            && stored_worker_version == handle_worker_version
          {
            False ->
              Error(WorkerContractMismatch(
                expected_id: handle_worker,
                expected_version: handle_worker_version,
                actual_id: stored_worker,
                actual_version: stored_worker_version,
              ))
            True ->
              outcome_value(
                state,
                output_codec,
                error_codec,
                encoded_output,
                output_version,
                encoded_error,
                error_version,
                failure_description,
                failure_cause,
              )
          }
      }
  }
}

fn outcome_value(
  state: String,
  output_codec: worker.Codec(output),
  error_codec: Option(worker.Codec(error)),
  encoded_output: Option(String),
  output_version: String,
  encoded_error: Option(String),
  error_version: Option(String),
  failure_description: Option(String),
  failure_cause: Option(String),
) -> Result(job.Outcome(output, error), JobReadError) {
  case state {
    "queued" -> Ok(job.Pending(Queued))
    "scheduled" -> Ok(job.Pending(Scheduled))
    "retryable" -> Ok(job.Pending(job.Retryable))
    "executing" -> Ok(job.Pending(job.Executing))
    "succeeded" ->
      case encoded_output {
        Some(encoded) ->
          worker.decode_codec(output_codec, output_version, encoded)
          |> result.map(job.SucceededWith)
          |> result.map_error(CodecFailed)
        None -> Error(SucceededOutputMissing)
      }
    "business_failed" -> {
      let cause = case failure_cause {
        Some(raw) ->
          option.from_result(worker.business_failure_cause_from_string(raw))
        None -> None
      }
      case error_codec, encoded_error, error_version {
        Some(codec), Some(encoded), Some(version) ->
          worker.decode_codec(codec, version, encoded)
          |> result.map(fn(error) {
            case cause {
              Some(terminal_cause) ->
                job.BusinessFailedWithCause(error, terminal_cause)
              None -> job.BusinessFailedWith(error)
            }
          })
          |> result.map_error(CodecFailed)
        _, _, _ ->
          case cause {
            Some(terminal_cause) ->
              Ok(job.FailedOperationallyWithCause(
                failure_description
                  |> unwrap("worker returned an application error"),
                terminal_cause,
              ))
            None ->
              Ok(job.FailedOperationally(
                failure_description
                |> unwrap("worker returned an application error"),
              ))
          }
      }
    }
    "runtime_failed" ->
      Ok(job.FailedOperationally(
        failure_description |> unwrap("worker runtime failed"),
      ))
    "contract_mismatch" ->
      Ok(job.FailedOperationally(
        failure_description |> unwrap("worker codec contract mismatch"),
      ))
    "uncertain" ->
      Ok(job.ReconciliationRequired(
        failure_description
        |> unwrap("attempt outcome requires reconciliation"),
      ))
    "discarded" ->
      Ok(job.DiscardedWithReason(failure_description |> unwrap("job discarded")))
    "cancelled" ->
      Ok(job.CancelledWithReason(failure_description |> unwrap("job cancelled")))
    other -> Error(InvalidStoredState(other))
  }
}

// -- Uniqueness admission ----------------------------------------------------
//
// The admission transaction itself lives in `grind/internal/unique_admission`
// (which has no dependency on this module, so `grind/postgres` depends on it
// instead). That module returns `grind/unique`'s public
// `Admission`/`Conflict`/`SubmitError`/`PendingSubmission` types
// directly, so `submit_unique`/`reconcile_unique` below are thin entry
// points, not a second translating layer. See `docs/UNIQUENESS-CONTRACT.md`
// for the full contract.

/// Admits one job under a uniqueness policy. Rejects an empty queue name
/// before acquiring any resource. See `docs/UNIQUENESS-CONTRACT.md` for the
/// full admission transaction. May return `submission.EmptyQueueName`,
/// `submission.AdmissionContended`, `submission.SubmissionConflict`,
/// `submission.NotCommitted`, or `submission.CommitUnknown`; never
/// `submission.CommitUnknownWithoutId`, which is reserved for `submit`/
/// `submit_at`'s own no-identity uncertain case.
pub fn submit_unique(
  database: Database,
  queue: String,
  submission_id: submission.SubmissionId,
  worker: Worker(input, output, error),
  input: input,
  availability: submission.Availability,
  policy: unique.Policy(input),
  on_conflict: unique.ConflictAction,
) -> Result(
  submission.Admission(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let Database(
    connection:,
    storage_owner:,
    unique_lock_wait_ms:,
    forwarder:,
    ..,
  ) = database
  let worker.Metadata(id: worker_id, worker_version:, ..) =
    worker.metadata(worker)
  case
    unique_admission.submit(
      connection,
      storage_owner,
      unique_lock_wait_ms,
      queue,
      submission_id,
      worker,
      input,
      availability,
      policy,
      on_conflict,
    )
  {
    Ok(unique_admission.Commit(
      outcome:,
      committed_state:,
      available_at_unix_ms:,
      via_receipt_match:,
    )) -> {
      let confirmation = case via_receipt_match {
        True -> observation.Reconciled
        False -> observation.Replied
      }
      emit_admitted(
        forwarder,
        queue,
        admission_job_id(outcome),
        worker_id,
        worker_version,
        committed_state,
        available_at_unix_ms,
        Some(submission.submission_id_value(submission_id)),
        confirmation,
      )
      Ok(outcome)
    }
    Error(error) -> Error(error)
  }
}

/// Persists a job (immediately, or at an absolute availability time — see
/// `availability`) with a caller-supplied `SubmissionId`, no uniqueness
/// policy. Unlike `submit`/`submit_at`, a retry of this exact call (same id,
/// same request) converges on the original commit instead of risking a
/// duplicate row: it reuses `submit_unique`'s own admission receipt, request
/// fingerprint, and reconciliation machinery — the same unified admission
/// transaction in `grind/internal/unique_admission`, taken with no policy
/// (`Request.policy: None`), not a second implementation — see
/// `docs/UNIQUENESS-CONTRACT.md`, "Admission receipts", for the full
/// contract, including what lock (if any) protects a concurrent same-id
/// retry with no uniqueness key to serialize on. Returns `submission.Inserted`
/// on this call's own first commit, or on a matching replay;
/// `submit_unique`'s `Existing`/`Rescheduled` variants never occur here —
/// there is no policy to conflict against, so every resolved call is
/// `Inserted`. May return `submission.EmptyQueueName`,
/// `submission.AdmissionContended`, `submission.SubmissionConflict`,
/// `submission.NotCommitted`, or `submission.CommitUnknown` (recoverable with
/// `reconcile_unique`, or a plain retry of the same `SubmissionId`); never
/// `submission.CommitUnknownWithoutId`, which is reserved for `submit`/
/// `submit_at`'s own no-identity uncertain case — use this function (or
/// `submit_unique`) instead of `submit`/`submit_at` for a job that might
/// need to be resubmitted safely.
pub fn submit_with_id(
  database: Database,
  queue: String,
  submission_id: submission.SubmissionId,
  worker: Worker(input, output, error),
  input: input,
  availability: submission.Availability,
) -> Result(
  submission.Admission(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let Database(
    connection:,
    storage_owner:,
    unique_lock_wait_ms:,
    forwarder:,
    ..,
  ) = database
  let worker.Metadata(id: worker_id, worker_version:, ..) =
    worker.metadata(worker)
  case
    unique_admission.submit_plain(
      connection,
      storage_owner,
      unique_lock_wait_ms,
      queue,
      submission_id,
      worker,
      input,
      availability,
    )
  {
    Ok(unique_admission.Commit(
      outcome:,
      committed_state:,
      available_at_unix_ms:,
      via_receipt_match:,
    )) -> {
      let confirmation = case via_receipt_match {
        True -> observation.Reconciled
        False -> observation.Replied
      }
      emit_admitted(
        forwarder,
        queue,
        admission_job_id(outcome),
        worker_id,
        worker_version,
        committed_state,
        available_at_unix_ms,
        Some(submission.submission_id_value(submission_id)),
        confirmation,
      )
      Ok(outcome)
    }
    Error(error) -> Error(error)
  }
}

/// The persisted job id an `Admission` decision is about, whichever variant
/// it is — `Inserted` carries a full `JobHandle`, `Existing`/`Rescheduled`
/// carry a `Conflict`, both identify exactly one row.
fn admission_job_id(
  admission: submission.Admission(input, output, error),
) -> Int {
  case admission {
    submission.Inserted(handle) -> job.id_value(handle)
    submission.Existing(conflict) -> submission.conflict_job_id(conflict)
    submission.Rescheduled(conflict) -> submission.conflict_job_id(conflict)
  }
}

/// Re-reads the receipt a `CommitUnknown` command would have written,
/// without repeating the admission transaction. A receipt matching the
/// retained request returns its recorded decision; a receipt that does not
/// match, or no receipt yet, both surface the same way a fresh `submit_unique`
/// call would (`SubmissionConflict` / another `CommitUnknown`).
pub fn reconcile_unique(
  database: Database,
  pending: submission.PendingSubmission(input, output, error),
) -> Result(
  submission.Admission(input, output, error),
  submission.SubmitError(input, output, error),
) {
  let Database(connection:, ..) = database
  unique_admission.reconcile(connection, pending)
}
