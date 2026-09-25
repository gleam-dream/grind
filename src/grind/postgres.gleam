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
import grind/internal/sql
import grind/internal/unique_admission
import grind/job.{type JobHandle, type State, Queued, Scheduled}
import grind/observation
import grind/registry.{type Registry}
import grind/unique
import grind/worker.{type Worker}
import pog
import sinal/forwarder.{type Forwarder}

/// Pure PostgreSQL pool settings. Validation does not acquire a resource.
pub type Settings {
  Settings(
    database_url: String,
    pool_name: process.Name(pog.Message),
    pool_size: Int,
    unique_lock_wait_ms: Int,
    observation_capacity: Int,
  )
}

pub fn settings(
  database_url: String,
  pool_name: process.Name(pog.Message),
) -> Settings {
  Settings(
    database_url:,
    pool_name:,
    pool_size: 10,
    unique_lock_wait_ms: 5000,
    observation_capacity: 1024,
  )
}

pub fn pool_size(settings: Settings, pool_size: Int) -> Settings {
  Settings(..settings, pool_size:)
}

/// Sets the bounded wait for `submit_unique`'s admission lock, in
/// milliseconds. Exceeding it surfaces as `AdmissionContended` rather
/// than blocking indefinitely. Validated positive by `validate`, before any
/// pool starts.
pub fn unique_lock_wait(settings: Settings, milliseconds: Int) -> Settings {
  Settings(..settings, unique_lock_wait_ms: milliseconds)
}

/// Sets the bounded number of `[grind, job, *]` observations the package's
/// own `sinal/forwarder.Forwarder` holds in flight at once. Exceeding it
/// drops the observation and is reported once per drain via
/// `sinal/forwarder.dropped_event` (`[sinal, forwarder, dropped]`); it never
/// affects job outcomes. Validated positive by `validate`, before any
/// process starts.
pub fn observation_capacity(settings: Settings, capacity: Int) -> Settings {
  Settings(..settings, observation_capacity: capacity)
}

pub type ConfigError {
  InvalidDatabaseUrl
  InvalidPoolSize
  InvalidUniqueLockWait
  InvalidObservationCapacity
}

pub opaque type ValidatedSettings {
  ValidatedSettings(
    pog.Config,
    storage_owner: String,
    unique_lock_wait_ms: Int,
    observation_capacity: Int,
  )
}

/// Checks the URL and pool bound before any PostgreSQL process is started.
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
    settings.unique_lock_wait_ms > 0,
    settings.observation_capacity > 0
  {
    False, _, _ -> Error(InvalidPoolSize)
    True, False, _ -> Error(InvalidUniqueLockWait)
    True, True, False -> Error(InvalidObservationCapacity)
    True, True, True ->
      case pog.url_config(settings.pool_name, settings.database_url) {
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
              ),
            storage_owner:,
            unique_lock_wait_ms: settings.unique_lock_wait_ms,
            observation_capacity: settings.observation_capacity,
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
    forwarder: Forwarder,
  )
}

// Pog 4.1 lets a pgo_pool checkout exit escape from execute/transaction when
// the named pool disappears. Keep that failure at the storage boundary so a
// queue actor can retain ownership and report an unavailable store instead of
// crashing. These wrappers catch only the known checkout-exit shape.
@external(erlang, "grind_postgres_ffi", "execute_safely")
fn execute_safely(
  query: pog.Query(a),
  on connection: pog.Connection,
) -> Result(pog.Returned(a), pog.QueryError)

/// Generic form of `execute_safely`, for calling a Squirrel-generated query
/// function (`grind/internal/sql`) that invokes `pog.execute` itself rather
/// than going through `execute_safely`. Wrapping the call in a zero-arity
/// closure keeps the same no-crash-on-checkout-failure guarantee without a
/// hand-written wrapper per generated function.
@external(erlang, "grind_postgres_ffi", "call_safely")
fn call_safely(
  run: fn() -> Result(pog.Returned(a), pog.QueryError),
) -> Result(pog.Returned(a), pog.QueryError)

@external(erlang, "grind_postgres_ffi", "transaction_safely")
fn transaction_safely(
  connection: pog.Connection,
  callback: fn(pog.Connection) -> Result(a, b),
) -> Result(a, pog.TransactionError(b))

pub type StartError {
  PoolStartFailed(actor.StartError)
}

/// Starts a package-owned PostgreSQL pool after settings have been
/// validated. Also starts this `Database`'s own `sinal/forwarder.Forwarder`
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
  case static_supervisor.start(supervisor) {
    Ok(started) -> {
      process.unlink(started.pid)
      Ok(Database(
        pog.named_connection(pool_name),
        started.pid,
        storage_owner,
        unique_lock_wait_ms,
        fwd,
      ))
    }
    Error(error) -> Error(PoolStartFailed(error))
  }
}

/// Stops the pool process owned by this Database value.
@external(erlang, "grind_postgres_ffi", "stop_supervisor")
fn stop_supervisor(pid: process.Pid) -> Nil

pub fn close(database: Database) -> Nil {
  let Database(supervisor_pid:, ..) = database
  stop_supervisor(supervisor_pid)
}

pub type StorageError {
  MigrationQueryFailed(pog.QueryError)
  IncompatibleSchema
  UnsupportedSchemaVersion(Int)
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
  ResolutionCommitUnknown(String)
}

/// Applies an audited operator decision to an uncertain job.
pub fn resolve_uncertain(
  database: Database,
  handle: JobHandle(input, output, error),
  resolution_id: String,
  resolved_by: String,
  details: String,
  resolution: Resolution(output, error),
) -> Result(ResolutionResult, ResolutionError) {
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
      <- result.try(case resolution {
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
        transaction_safely(connection, fn(transaction) {
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
  case execute_safely(query, on: connection) {
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
      use stored <- result.try(case execute_safely(select, on: connection) {
        Error(error) -> Error(ReconciliationQueryFailed(error))
        Ok(returned) ->
          case returned.rows {
            [stored] -> Ok(stored)
            _ -> Error(ReconciliationNotRequired)
          }
      })
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
  use _ <- result.try(case execute_safely(insert, on: connection) {
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
  use state <- result.try(case execute_safely(update, on: connection) {
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

/// Installs the current PostgreSQL schema atomically on an empty schema.
/// Repeated calls are safe; older experimental schema versions fail closed.
pub fn migrate(database: Database) -> Result(Nil, StorageError) {
  let Database(connection:, ..) = database
  case
    transaction_safely(connection, fn(transaction) {
      migrate_transaction(transaction)
    })
  {
    Ok(Nil) -> Ok(Nil)
    Error(pog.TransactionQueryError(error)) ->
      Error(MigrationQueryFailed(error))
    Error(pog.TransactionRolledBack(error)) -> Error(error)
  }
}

type SchemaGeneration {
  FreshSchema
  ExistingSchema
}

fn read_schema_generation(
  connection: pog.Connection,
) -> Result(SchemaGeneration, StorageError) {
  let query =
    pog.query(
      "SELECT count(*)::bigint, count(*) FILTER (WHERE relname IN ('grind_schema_migrations', 'grind_jobs', 'grind_job_resolutions', 'grind_job_acknowledgements', 'grind_unique_submissions') AND relkind = 'r')::bigint, count(*) FILTER (WHERE relname = 'grind_attempts_id_seq' AND relkind = 'S')::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = current_schema() AND relname IN ('grind_schema_migrations', 'grind_jobs', 'grind_job_resolutions', 'grind_job_acknowledgements', 'grind_unique_submissions', 'grind_attempts_id_seq')",
    )
    |> pog.returning({
      use owned_objects <- decode.field(0, decode.int)
      use owned_tables <- decode.field(1, decode.int)
      use attempt_sequences <- decode.field(2, decode.int)
      decode.success(#(owned_objects, owned_tables, attempt_sequences))
    })
  use #(owned_objects, owned_tables, attempt_sequences) <- result.try(
    case execute_safely(query, on: connection) {
      Error(error) -> Error(MigrationQueryFailed(error))
      Ok(returned) ->
        case returned.rows {
          [counts] -> Ok(counts)
          _ -> Error(IncompatibleSchema)
        }
    },
  )
  case owned_objects, owned_tables, attempt_sequences {
    0, 0, 0 -> Ok(FreshSchema)
    6, 5, 1 -> read_installed_schema_version(connection)
    // The exact object shape of a never-migrated schema v10 install (four
    // tables, no `grind_unique_submissions`, the same attempt sequence). Read
    // the marker to confirm it is genuinely the legacy v10 install rather
    // than some other tampered or partial state.
    5, 4, 1 -> read_legacy_schema_marker(connection)
    _, _, _ -> Error(IncompatibleSchema)
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
  case execute_safely(query, on: connection) {
    Error(_) -> Error(IncompatibleSchema)
    Ok(returned) ->
      case returned.rows {
        [version] -> Ok(version)
        _ -> Error(IncompatibleSchema)
      }
  }
}

fn read_installed_schema_version(
  connection: pog.Connection,
) -> Result(SchemaGeneration, StorageError) {
  use #(count, minimum, maximum) <- result.try(read_schema_marker(connection))
  case count, minimum, maximum {
    1, 11, 11 -> read_unique_key_columns(connection)
    1, version, _ -> Error(UnsupportedSchemaVersion(version))
    _, _, _ -> Error(IncompatibleSchema)
  }
}

/// Cheap fail-closed check: the full v11 object shape (tables, sequence,
/// marker) could in principle exist without `grind_jobs` ever having gained
/// its two uniqueness key columns (a partial or tampered install). Checked
/// here, alongside the marker, rather than trusted from object counts alone.
fn read_unique_key_columns(
  connection: pog.Connection,
) -> Result(SchemaGeneration, StorageError) {
  let query =
    pog.query(
      "SELECT count(*) = 2 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'grind_jobs' AND column_name IN ('unique_key_contract', 'unique_key_sha256')",
    )
    |> pog.returning({
      use present <- decode.field(0, decode.bool)
      decode.success(present)
    })
  case execute_safely(query, on: connection) {
    Error(_) -> Error(IncompatibleSchema)
    Ok(returned) ->
      case returned.rows {
        [True] -> Ok(ExistingSchema)
        _ -> Error(IncompatibleSchema)
      }
  }
}

/// The v10 object shape has no `grind_unique_submissions` table and never
/// gained the new `grind_jobs` key columns; it is only ever a genuine
/// pre-v11 install, marked 10, never a partially-installed or tampered v11
/// schema (that shape is caught separately as `IncompatibleSchema` by
/// `read_schema_generation`'s object-count check, since dropping just
/// `grind_unique_submissions` from a real v11 install still leaves the
/// marker at 11). Fresh-install-only: there is no migration to v11.
fn read_legacy_schema_marker(
  connection: pog.Connection,
) -> Result(SchemaGeneration, StorageError) {
  use #(count, minimum, maximum) <- result.try(read_schema_marker(connection))
  case count, minimum, maximum {
    1, 10, 10 -> Error(UnsupportedSchemaVersion(10))
    _, _, _ -> Error(IncompatibleSchema)
  }
}

fn migrate_transaction(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
  use schema_generation <- result.try(read_schema_generation(connection))
  case schema_generation {
    FreshSchema -> create_fresh_schema(connection)
    ExistingSchema -> Ok(Nil)
  }
}

/// The DDL `create_fresh_schema` executes, one statement per list element, in
/// order. Kept byte-identical (statement text, minus the trailing `;` psql
/// needs) to `src/grind/internal/schema.sql` — a plain-SQL copy of the same
/// DDL that `scripts/generate-sql.sh` applies with `psql` so a disposable
/// database has Grind's schema before `gleam run -m squirrel` inspects real
/// query types against it. `postgres_schema_ddl_matches_sql_file_test`
/// (test/grind_test.gleam) proves the two stay equal.
///
/// `@internal`: exposed only for that test, not part of the public API.
@internal
pub fn schema_ddl_statements() -> List(String) {
  [
    "CREATE TABLE grind_schema_migrations (version integer PRIMARY KEY, installed_at timestamptz NOT NULL DEFAULT clock_timestamp())",
    "CREATE TABLE grind_jobs ("
      <> "id bigserial PRIMARY KEY, storage_owner text NOT NULL, queue text NOT NULL, "
      <> "worker_id text NOT NULL, worker_version text NOT NULL, input_version text NOT NULL, input jsonb NOT NULL, "
      <> "output_version text NOT NULL, output jsonb, error_version text, error jsonb, "
      <> "state text NOT NULL CONSTRAINT grind_jobs_state_check CHECK (state IN ('queued', 'scheduled', 'retryable', 'executing', 'succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'uncertain', 'discarded', 'cancelled')), "
      <> "available_at timestamptz NOT NULL, inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(), "
      <> "attempt_id bigint, attempt_epoch bigint NOT NULL DEFAULT 0, attempt_owner text, "
      <> "lease_expires_at timestamptz, attempt_count bigint NOT NULL DEFAULT 0, max_attempts bigint NOT NULL DEFAULT 20, delivery_count bigint NOT NULL DEFAULT 0, snooze_count bigint NOT NULL DEFAULT 0, failure_description text, failure_cause text, uncertain_at timestamptz, cancel_requested_at timestamptz, "
      <> "unique_key_contract text, unique_key_sha256 bytea, "
      <> "CONSTRAINT grind_jobs_max_attempts_check CHECK (max_attempts > 0), "
      <> "CONSTRAINT grind_jobs_unique_key_check CHECK ((unique_key_contract IS NULL) = (unique_key_sha256 IS NULL) AND (unique_key_sha256 IS NULL OR octet_length(unique_key_sha256) = 32)))",
    "CREATE INDEX grind_jobs_unique_candidate_idx ON grind_jobs (storage_owner, worker_id, worker_version, unique_key_contract, unique_key_sha256) WHERE unique_key_sha256 IS NOT NULL",
    "CREATE TABLE grind_job_resolutions ("
      <> "storage_owner text NOT NULL, queue text NOT NULL, job_id bigint NOT NULL, worker_id text, worker_version text, "
      <> "resolution_id text NOT NULL, attempt_id bigint NOT NULL, attempt_epoch bigint NOT NULL, "
      <> "attempt_owner text NOT NULL, lease_expires_at timestamptz NOT NULL, "
      <> "decision text NOT NULL CONSTRAINT grind_job_resolutions_decision_check CHECK (decision IN ('confirm_success', 'confirm_business_failure', 'authorize_replay')), "
      <> "target_state text NOT NULL CONSTRAINT grind_job_resolutions_target_state_check CHECK (target_state IN ('queued', 'succeeded', 'business_failed')), "
      <> "payload_version text, payload jsonb, "
      <> "resolved_by text NOT NULL, details text NOT NULL, resolved_at timestamptz NOT NULL DEFAULT clock_timestamp(), "
      <> "CONSTRAINT grind_job_resolutions_pkey PRIMARY KEY (storage_owner, resolution_id))",
    "CREATE TABLE grind_job_acknowledgements ("
      <> "storage_owner text NOT NULL, command_id text NOT NULL, queue text NOT NULL, job_id bigint NOT NULL, "
      <> "worker_id text NOT NULL, worker_version text NOT NULL, attempt_id bigint NOT NULL, attempt_epoch bigint NOT NULL, "
      <> "attempt_owner text NOT NULL, committed_state text NOT NULL CONSTRAINT grind_job_acknowledgements_committed_state_check CHECK (committed_state IN ('succeeded', 'business_failed', 'retryable', 'runtime_failed', 'scheduled', 'discarded', 'cancelled', 'uncertain')), "
      <> "failure_cause text CONSTRAINT grind_job_acknowledgements_failure_cause_check CHECK (failure_cause IS NULL OR failure_cause IN ('budget_exhausted', 'retry_declined')), "
      <> "proposal_sha256 bytea NOT NULL CONSTRAINT grind_job_acknowledgements_proposal_sha256_check CHECK (octet_length(proposal_sha256) = 32), "
      <> "committed_at timestamptz NOT NULL DEFAULT clock_timestamp(), "
      <> "CONSTRAINT grind_job_acknowledgements_pkey PRIMARY KEY (storage_owner, command_id), "
      <> "CONSTRAINT grind_job_acknowledgements_attempt_key UNIQUE (storage_owner, job_id, attempt_id, attempt_epoch))",
    "CREATE SEQUENCE grind_attempts_id_seq AS bigint START WITH 1 INCREMENT BY 1 MINVALUE 1 CACHE 1 NO CYCLE",
    "CREATE TABLE grind_unique_submissions ("
      <> "storage_owner text NOT NULL, submission_id text NOT NULL, queue text NOT NULL, "
      <> "worker_id text NOT NULL, worker_version text NOT NULL, "
      <> "request_sha256 bytea NOT NULL CONSTRAINT grind_unique_submissions_request_sha256_check CHECK (octet_length(request_sha256) = 32), "
      <> "decision text NOT NULL CONSTRAINT grind_unique_submissions_decision_check CHECK (decision IN ('inserted', 'existing', 'rescheduled')), "
      <> "job_id bigint NOT NULL, job_queue text NOT NULL, "
      <> "observed_state text NOT NULL CONSTRAINT grind_unique_submissions_observed_state_check CHECK (observed_state IN ('queued', 'scheduled', 'retryable', 'executing', 'succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'uncertain', 'discarded', 'cancelled')), "
      <> "decided_at timestamptz NOT NULL DEFAULT clock_timestamp(), "
      <> "rescheduled_from timestamptz, rescheduled_to timestamptz, "
      <> "CONSTRAINT grind_unique_submissions_pkey PRIMARY KEY (storage_owner, submission_id))",
    "INSERT INTO grind_schema_migrations (version) VALUES (11)",
  ]
}

fn create_fresh_schema(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
  list.try_each(schema_ddl_statements(), fn(statement) {
    run_statement(connection, statement)
  })
}

fn run_statement(
  connection: pog.Connection,
  statement: String,
) -> Result(Nil, StorageError) {
  case execute_safely(pog.query(statement), on: connection) {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(_) -> Ok(Nil)
  }
}

pub type SubmitError {
  EmptyQueueName
  NegativeAvailability
  UnexpectedInsertRows
  /// The insert query itself failed or its reply was lost; either way, this
  /// is not knowably "did not commit" — a `pog.QueryError` here can also mean
  /// the connection was lost after PostgreSQL already committed the row.
  /// Retrying a plain `submit`/`submit_at` call after this error can
  /// therefore create a duplicate job, since neither has any request
  /// identity to deduplicate against. A caller that must retry safely should
  /// use `submit_unique` with a caller-chosen `SubmissionId` instead: its
  /// admission transaction records that identity durably, so a retried
  /// request converges on the original outcome rather than inserting again.
  SubmitQueryFailed(pog.QueryError)
}

/// Persists an immediate job without invoking its worker.
pub fn submit(
  database: Database,
  queue: String,
  worker: Worker(input, output, error),
  input: input,
) -> Result(JobHandle(input, output, error), SubmitError) {
  submit_with_availability(database, queue, worker, input, None)
}

/// Persists a job at an absolute Unix-millisecond availability time.
pub fn submit_at(
  database: Database,
  queue: String,
  worker: Worker(input, output, error),
  input: input,
  available_at: job.AvailableAt,
) -> Result(JobHandle(input, output, error), SubmitError) {
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
) -> Result(JobHandle(input, output, error), SubmitError) {
  case queue, available_at_unix_ms {
    "", _ -> Error(EmptyQueueName)
    _, Some(availability) if availability < 0 -> Error(NegativeAvailability)
    _, _ -> {
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
      case execute_safely(query, on: connection) {
        Error(error) -> Error(SubmitQueryFailed(error))
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
            _ -> Error(UnexpectedInsertRows)
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

pub type ArgumentError {
  ArgumentQueryFailed(pog.QueryError)
  JobNotFound
  StorageOwnerMismatch
  QueueRouteMismatch(expected: String, actual: String)
  WorkerContractMismatch(
    expected_id: String,
    expected_version: String,
    actual_id: String,
    actual_version: String,
  )
  ArgumentCodecFailed(worker.StoredCodecError)
}

pub type HandleBindError {
  HandleBindQueryFailed(pog.QueryError)
  HandleBindNotFound
  HandleBindWorkerContractMismatch
  HandleBindCodecContractMismatch
}

/// Reconstructs a typed handle from durable identity after an application restart.
/// `id` is scoped to this database's storage owner, and the current worker
/// definition must exactly match the stored worker and codec contract.
pub fn bind_handle(
  database: Database,
  worker: Worker(input, output, error),
  id: Int,
) -> Result(JobHandle(input, output, error), HandleBindError) {
  let Database(connection:, storage_owner:, ..) = database
  let worker.Metadata(
    id: expected_worker_id,
    worker_version: expected_worker_version,
    input_version: expected_input_version,
    output_version: expected_output_version,
    error_version: expected_error_version,
    ..,
  ) = worker.metadata(worker)
  case call_safely(fn() { sql.bind_handle(connection, id, storage_owner) }) {
    Error(error) -> Error(HandleBindQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Error(HandleBindNotFound)
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
            False -> Error(HandleBindWorkerContractMismatch)
            True ->
              case
                stored_input_version == expected_input_version
                && stored_output_version == expected_output_version
                && stored_error_version == expected_error_version
              {
                False -> Error(HandleBindCodecContractMismatch)
                True -> Ok(job.new_handle(id, storage_owner, queue, worker))
              }
          }
        _ -> Error(HandleBindNotFound)
      }
  }
}

/// Reloads the typed, version-checked input from PostgreSQL.
pub fn arguments(
  database: Database,
  handle: JobHandle(input, output, error),
) -> Result(input, ArgumentError) {
  let Database(connection:, storage_owner:, ..) = database
  let #(id, handle_owner, handle_queue, worker_id, worker_version, input_codec) =
    job.storage_fields(handle)
  case call_safely(fn() { sql.arguments(connection, id) }) {
    Error(error) -> Error(ArgumentQueryFailed(error))
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
                      |> result.map_error(ArgumentCodecFailed)
                  }
              }
          }
        _ -> Error(JobNotFound)
      }
  }
}

pub type StateError {
  StateQueryFailed(pog.QueryError)
  StateJobNotFound
  StateStorageOwnerMismatch
  StateQueueMismatch
  StateWorkerContractMismatch
  InvalidStoredState(String)
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
        transaction_safely(connection, fn(transaction) {
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

/// Builds and forwards `[grind, job, cancellation]` from a proven-committed
/// `CancellationResult`. Called only from `cancel`, strictly after
/// `transaction_safely` has already returned — never from inside a
/// transaction callback (the same discipline `acknowledge` and
/// `resolve_uncertain` follow). Only `CancelledBeforeRun` and
/// `CancellationRequested` are genuine writes; every read-only outcome emits
/// nothing.
/// Which `[grind, job, cancellation]` outcome, if any, a `CancellationResult`
/// reports. Only `CancelledBeforeRun`/`CancellationRequested` are genuine
/// writes; every read-only outcome (`AlreadyCancelled`, `AlreadyUncertain`,
/// `AlreadyFinished`) maps to `None` and must never emit.
fn cancellation_outcome_of(
  result: CancellationResult,
) -> Option(observation.CancellationOutcome) {
  case result {
    CancelledBeforeRun -> Some(observation.CancelledBeforeRunOutcome)
    CancellationRequested -> Some(observation.CancellationRequestedOutcome)
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
      observation.cancellation(),
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
    case call_safely(fn() { sql.cancel_lock(connection, id) }) {
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
        case call_safely(fn() { sql.cancel_before_run(connection, id) }) {
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
        case call_safely(fn() { sql.cancel_executing(connection, id) }) {
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
  /// reasoning `unique.AdmissionFailed`'s doc comment gives for the
  /// uniqueness-admission path. Safe to retry the same acknowledgement.
  QueueAckFailed(pog.QueryError)
  /// The ACK transaction may have committed although its reply was lost.
  /// Reconcile this stable command ID before considering any replay.
  QueueAckUnknown(command_id: String, proposed: worker.Execution)
  /// The exact worker proposal was not committed because the claim no longer
  /// owned a live, executing row. The attached reason is a database snapshot.
  QueueAckStale(proposed: worker.Execution, reason: AckRejection)
  QueueAckRejected
  QueueAckCommandConflict
  QueueAckProposalCodecMismatch(kind: String, expected: String, actual: String)
  QueueCodecMismatch(kind: String, expected: String, actual: String)
}

pub type AckRejection {
  AckRecordMissing
  AckLeaseExpired(attempt_id: Int, epoch: Int, owner: String)
  AckOwnershipChanged(
    state: String,
    attempt_id: Option(Int),
    epoch: Option(Int),
    owner: Option(String),
  )
  AckStateChanged(state: String)
  AckRecordChanged(
    state: String,
    attempt_id: Option(Int),
    epoch: Option(Int),
    owner: Option(String),
  )
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
    business_failure_cause: Option(job.BusinessFailureCause),
    committed_at: String,
  )
}

pub type AckReconciliationError {
  AckReceiptQueryFailed(pog.QueryError)
  AckReceiptNotFound
  AckReceiptStorageOwnerMismatch
  AckReceiptRouteMismatch
  AckReceiptWorkerContractMismatch
  AckReceiptInvalidState(String)
}

type Claim {
  Claim(
    id: Int,
    attempt_id: Int,
    epoch: Int,
    input_version: String,
    encoded_input: String,
    worker_id: String,
    worker_version: String,
    output_version: String,
    error_version: Option(String),
    current_attempt: Int,
    max_attempts: Int,
    snooze_count: Int,
    delivery_count: Int,
    previous_state: String,
  )
}

type AckProposal {
  AckProposal(
    proposed_state: String,
    failure_cause: Option(String),
    requested_delay_ms: Option(Int),
    output_version: Option(String),
    output: Option(String),
    error_version: Option(String),
    error: Option(String),
    failure_description: Option(String),
  )
}

/// The proven-committed disposition of one acknowledgement command, carried
/// from wherever it was proven (a fresh write, or a durable receipt read
/// back matching this exact command) up to `acknowledge`, which is the only
/// place that emits `[grind, job, acknowledged]` — never from inside a
/// transaction callback. `via_receipt_match: True` means this call's own
/// transaction did not write anything new; the exact same command was
/// already durably applied, so the observation's `confirmation` is
/// `Reconciled` rather than `Replied`. `available_at_unix_ms` is only ever
/// known for a fresh write (read from that write's own `RETURNING`); a
/// receipt match cannot recover it, since `grind_job_acknowledgements` does
/// not retain `available_at`.
type AckCommit {
  AckCommit(
    committed_state: String,
    failure_cause: Option(String),
    available_at_unix_ms: Option(Int),
    via_receipt_match: Bool,
  )
}

/// A claimed row bound to its registered typed execution closure.
@internal
pub opaque type ClaimedJob {
  ClaimedJob(claim: Claim, run: fn() -> worker.Execution)
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

/// Atomically claims one due row without running its handler in the caller.
/// The queue actor owns renewal and acknowledgement after this boundary.
@internal
pub fn claim_one(
  database: Database,
  queue: String,
  workers: Registry,
  attempt_owner: String,
  lease_duration_ms: Int,
) -> Result(Option(ClaimedJob), QueueRunError) {
  let identities = registry.identities(workers)
  case quarantine_expired(database, queue, identities) {
    Error(error) -> Error(error)
    Ok(Nil) ->
      claim_registered_job(
        database,
        queue,
        workers,
        attempt_owner,
        identities,
        lease_duration_ms,
      )
  }
}

/// Executes only the typed closure captured when a matching job was claimed.
@internal
pub fn execute_claim(claimed: ClaimedJob) -> worker.Execution {
  let ClaimedJob(run:, ..) = claimed
  run()
}

/// Returns stable fencing fields for the queue actor's private active entry.
@internal
pub fn claim_identity(claimed: ClaimedJob) -> #(Int, Int, Int) {
  let ClaimedJob(claim: claim, ..) = claimed
  let Claim(id:, attempt_id:, epoch:, ..) = claim
  #(id, attempt_id, epoch)
}

fn claim_previous_state(claim: Claim) -> String {
  let Claim(previous_state:, ..) = claim
  previous_state
}

/// Renders the fenced "the lease is still live" predicate, comparing
/// `lease_expires_at` against the given SQL time expression (in production,
/// always `clock_timestamp()`). Renewal, contract-mismatch release, every
/// acknowledgement disposition, and the ack-rejection reader all gate on
/// this exact fragment; a test can render the identical production text
/// against a fixed instant instead of duplicating the comparison by hand.
/// `now_expression` is spliced into the query text verbatim, not bound as a
/// parameter — it must be trusted SQL supplied by this module or a test
/// (a literal function call or column reference such as `clock_timestamp()`
/// or `instant`), never untrusted or caller-supplied input.
@internal
pub fn live_lease_predicate(now_expression: String) -> String {
  "lease_expires_at > " <> now_expression
}

/// The complement of `live_lease_predicate`: true once the lease has
/// already expired at the given time expression. Used by the quarantine
/// scan that looks for abandoned attempts. Kept as its own mirrored
/// fragment (rather than `NOT (` <> live_lease_predicate <> `)`) so the
/// production SQL text is unchanged by this extraction. Same trust
/// requirement as `live_lease_predicate`: `now_expression` is spliced in
/// verbatim and must be trusted SQL, never user input.
@internal
pub fn expired_lease_predicate(now_expression: String) -> String {
  "lease_expires_at <= " <> now_expression
}

/// Extends the current lease using PostgreSQL's clock and current row values.
/// An expired or fenced claim cannot be renewed.
@internal
pub fn renew_claim(
  database: Database,
  queue: String,
  attempt_owner: String,
  claimed: ClaimedJob,
  lease_duration_ms: Int,
) -> Result(Bool, QueueRunError) {
  let Database(connection:, storage_owner:, ..) = database
  let ClaimedJob(claim: claim, ..) = claimed
  let Claim(id:, attempt_id:, epoch:, ..) = claim
  let query =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() + ($7::double precision * interval '1 millisecond') WHERE id = $1 AND storage_owner = $2 AND queue = $3 AND state = 'executing' AND attempt_id = $4 AND attempt_epoch = $5 AND attempt_owner = $6 AND "
      <> live_lease_predicate("clock_timestamp()")
      <> " RETURNING id",
    )
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.parameter(pog.int(lease_duration_ms))
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(QueueClaimFailed(error))
    Ok(returned) ->
      case returned.rows {
        [_] -> Ok(True)
        [] -> Ok(False)
        _ -> Error(QueueAckRejected)
      }
  }
}

/// Releases a claim only when its exact attempt still owns the executing row.
/// This is used when the temporary worker child failed to start before
/// receiving `StartAttempt`, so it refunds the uninvoked attempt.
@internal
pub fn release_unstarted_claim(
  database: Database,
  queue: String,
  attempt_owner: String,
  claimed: ClaimedJob,
) -> Result(Bool, QueueRunError) {
  let Database(connection:, storage_owner:, forwarder:, ..) = database
  let ClaimedJob(claim: claim, ..) = claimed
  let Claim(id:, attempt_id:, epoch:, worker_id:, worker_version:, ..) = claim
  case
    call_safely(fn() {
      sql.release_unstarted_claim(
        connection,
        id,
        storage_owner,
        queue,
        attempt_id,
        epoch,
        attempt_owner,
        claim_previous_state(claim),
      )
    })
  {
    Error(error) -> Error(QueueClaimReleaseFailed(error))
    Ok(returned) ->
      case returned.rows {
        [_] -> {
          case job.state_of_stored(claim_previous_state(claim)) {
            Error(Nil) -> Nil
            Ok(restored_state) ->
              emit_released(
                forwarder,
                queue,
                id,
                worker_id,
                worker_version,
                attempt_id,
                epoch,
                claim.current_attempt,
                restored_state,
              )
          }
          Ok(True)
        }
        [] -> Ok(False)
        _ -> Error(QueueClaimReleaseRejected)
      }
  }
}

/// Builds and forwards `[grind, job, released]` for a claim refunded before
/// its worker ever ran. Called only after the release's own fenced
/// `UPDATE ... RETURNING` already returned that row. `epoch` is the
/// *released* attempt's own epoch (the value this claim was made under) —
/// the row's own `attempt_epoch` column is incremented again by the next
/// claim, so it no longer matches this event's `epoch` by the time a handler
/// might read it back.
fn emit_released(
  fwd: Forwarder,
  queue: String,
  job_id: Int,
  worker_id: String,
  worker_version: String,
  attempt_id: Int,
  epoch: Int,
  attempt: Int,
  restored_state: State,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      observation.released(),
      observation.ReleasedMeasurements(count: 1),
      observation.ReleasedMetadata(
        ref: observation.JobRef(job_id:, queue:, worker_id:, worker_version:),
        attempt: observation.AttemptRef(attempt_id:, epoch:, attempt:),
        restored_state:,
      ),
    )
  Nil
}

/// Commits the proposed worker outcome through the same attempt fence.
@internal
pub fn acknowledge_claim(
  database: Database,
  queue: String,
  attempt_owner: String,
  claimed: ClaimedJob,
  execution: worker.Execution,
) -> Result(Bool, QueueRunError) {
  let ClaimedJob(claim:, ..) = claimed
  let Claim(output_version:, error_version:, ..) = claim
  case execution {
    worker.ExecutedSuccess(actual_version, _) ->
      case actual_version == output_version {
        True ->
          acknowledge(
            database,
            queue,
            attempt_owner,
            claim,
            execution,
            output_version,
          )
        False ->
          Error(QueueAckProposalCodecMismatch(
            "output",
            output_version,
            actual_version,
          ))
      }
    worker.ExecutedBusinessFailure(actual_version, _, _, _) ->
      case actual_version == error_version {
        True ->
          acknowledge(
            database,
            queue,
            attempt_owner,
            claim,
            execution,
            output_version,
          )
        False ->
          Error(QueueAckProposalCodecMismatch(
            "error",
            option_version(error_version),
            option_version(actual_version),
          ))
      }
    worker.ExecutedRetryable(actual_version, _, _, _) ->
      case actual_version == error_version {
        True ->
          acknowledge(
            database,
            queue,
            attempt_owner,
            claim,
            execution,
            output_version,
          )
        False ->
          Error(QueueAckProposalCodecMismatch(
            "error",
            option_version(error_version),
            option_version(actual_version),
          ))
      }
    worker.ExecutedInvalidInput(_) ->
      acknowledge(
        database,
        queue,
        attempt_owner,
        claim,
        execution,
        output_version,
      )
    worker.ExecutedSnoozed(_, _)
    | worker.ExecutedDiscarded(_)
    | worker.ExecutedCancelled(_)
    | worker.ExecutedUncertain(_) ->
      acknowledge(
        database,
        queue,
        attempt_owner,
        claim,
        execution,
        output_version,
      )
  }
}

fn claim_registered_job(
  database: Database,
  queue: String,
  workers: Registry,
  attempt_owner: String,
  identities: List(#(String, String)),
  lease_duration_ms: Int,
) -> Result(Option(ClaimedJob), QueueRunError) {
  let Database(connection:, storage_owner:, forwarder:, ..) = database
  let eligibility =
    list.index_map(identities, fn(_, index) {
      let id_parameter = 5 + index * 2
      let version_parameter = id_parameter + 1
      "(worker_id = $"
      <> int.to_string(id_parameter)
      <> " AND worker_version = $"
      <> int.to_string(version_parameter)
      <> ")"
    })
    |> string.join(" OR ")
  let eligible_state =
    "state IN ('queued', 'scheduled', 'retryable') AND available_at <= clock_timestamp()"
  let sql =
    "WITH candidate AS (SELECT id, state AS previous_state FROM grind_jobs WHERE storage_owner = $1 AND queue = $2 AND "
    <> eligible_state
    <> " AND cancel_requested_at IS NULL AND ("
    <> eligibility
    <> ") ORDER BY available_at, id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE grind_jobs AS job SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = job.attempt_epoch + 1, attempt_owner = $3, lease_expires_at = clock_timestamp() + ($4::double precision * interval '1 millisecond'), attempt_count = job.attempt_count + 1, delivery_count = job.delivery_count + 1 FROM candidate WHERE job.id = candidate.id RETURNING job.id, job.attempt_id, job.attempt_epoch, job.input_version, job.input::text, job.worker_id, job.worker_version, job.output_version, job.error_version, job.attempt_count, job.max_attempts, job.snooze_count, job.delivery_count, candidate.previous_state"
  let parameters =
    list.append(
      [
        pog.text(storage_owner),
        pog.text(queue),
        pog.text(attempt_owner),
        pog.int(lease_duration_ms),
      ],
      list.flat_map(identities, fn(identity) {
        let #(worker_id, worker_version) = identity
        [pog.text(worker_id), pog.text(worker_version)]
      }),
    )
  let query =
    list.fold(parameters, pog.query(sql), fn(query, parameter) {
      pog.parameter(query, parameter)
    })
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      use attempt_id <- decode.field(1, decode.int)
      use epoch <- decode.field(2, decode.int)
      use input_version <- decode.field(3, decode.string)
      use encoded_input <- decode.field(4, decode.string)
      use worker_id <- decode.field(5, decode.string)
      use worker_version <- decode.field(6, decode.string)
      use output_version <- decode.field(7, decode.string)
      use error_version <- decode.field(8, decode.optional(decode.string))
      use current_attempt <- decode.field(9, decode.int)
      use max_attempts <- decode.field(10, decode.int)
      use snooze_count <- decode.field(11, decode.int)
      use delivery_count <- decode.field(12, decode.int)
      use previous_state <- decode.field(13, decode.string)
      decode.success(Claim(
        id:,
        attempt_id:,
        epoch:,
        input_version:,
        encoded_input:,
        worker_id:,
        worker_version:,
        output_version:,
        error_version:,
        current_attempt:,
        max_attempts:,
        snooze_count:,
        delivery_count:,
        previous_state:,
      ))
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(QueueClaimFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Ok(None)
        [claim] -> {
          let Claim(
            id: claimed_id,
            attempt_id:,
            epoch:,
            input_version:,
            encoded_input:,
            worker_id:,
            worker_version:,
            output_version:,
            error_version:,
            current_attempt:,
            max_attempts:,
            snooze_count:,
            previous_state:,
            ..,
          ) = claim
          case job.state_of_stored(previous_state) {
            Error(Nil) -> Nil
            Ok(previous_state) ->
              emit_claimed(
                forwarder,
                queue,
                claimed_id,
                worker_id,
                worker_version,
                attempt_id,
                epoch,
                current_attempt,
                previous_state,
              )
          }
          case registry.select(workers, queue, worker_id, worker_version) {
            Error(_) -> Error(QueueAckRejected)
            Ok(#(registered_input, registered_output, registered_error, run)) ->
              case
                codec_contract_mismatch(
                  input_version,
                  registered_input,
                  output_version,
                  registered_output,
                  error_version,
                  registered_error,
                )
              {
                Some(#(kind, expected, actual)) -> {
                  case
                    mark_contract_mismatch(
                      database,
                      queue,
                      attempt_owner,
                      claim,
                      kind,
                      expected,
                      actual,
                    )
                  {
                    Error(error) -> Error(error)
                    Ok(True) ->
                      Error(QueueCodecMismatch(kind:, expected:, actual:))
                    Ok(False) -> Error(QueueAckRejected)
                  }
                }
                None ->
                  Ok(
                    Some(
                      ClaimedJob(claim:, run: fn() {
                        run(
                          input_version,
                          encoded_input,
                          worker.RetryContext(
                            current_attempt:,
                            max_attempts:,
                            snooze_count:,
                          ),
                        )
                      }),
                    ),
                  )
              }
          }
        }
        _ -> Error(QueueAckRejected)
      }
  }
}

/// Builds and forwards `[grind, job, claimed]` for one freshly claimed row.
/// Called only after the claim's own fenced `UPDATE ... RETURNING` already
/// returned that row — an autocommitted statement, so a returned row is
/// already the proof of commit. Never blocks the caller: see
/// `grind/observation`'s module documentation for the forwarder's delivery
/// semantics.
fn emit_claimed(
  fwd: Forwarder,
  queue: String,
  job_id: Int,
  worker_id: String,
  worker_version: String,
  attempt_id: Int,
  epoch: Int,
  attempt: Int,
  previous_state: State,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      observation.claimed(),
      observation.ClaimedMeasurements(count: 1),
      observation.ClaimedMetadata(
        ref: observation.JobRef(job_id:, queue:, worker_id:, worker_version:),
        attempt: observation.AttemptRef(attempt_id:, epoch:, attempt:),
        previous_state:,
      ),
    )
  Nil
}

fn quarantine_expired(
  database: Database,
  queue: String,
  identities: List(#(String, String)),
) -> Result(Nil, QueueRunError) {
  case identities {
    [] -> Ok(Nil)
    _ -> {
      let Database(connection:, storage_owner:, forwarder:, ..) = database
      let eligibility =
        list.index_map(identities, fn(_, index) {
          let id_parameter = 3 + index * 2
          let version_parameter = id_parameter + 1
          "(worker_id = $"
          <> int.to_string(id_parameter)
          <> " AND worker_version = $"
          <> int.to_string(version_parameter)
          <> ")"
        })
        |> string.join(" OR ")
      let sql =
        "WITH candidate AS (SELECT id FROM grind_jobs WHERE storage_owner = $1 AND queue = $2 AND state = 'executing' AND "
        <> expired_lease_predicate("clock_timestamp()")
        <> " AND ("
        <> eligibility
        <> ") ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE grind_jobs AS job SET state = 'uncertain', failure_description = CASE WHEN job.cancel_requested_at IS NOT NULL THEN 'expired after cancellation request; prior effect unknown' WHEN job.failure_description IS NULL THEN 'expired attempt requires outcome reconciliation' ELSE job.failure_description || '; expired attempt requires outcome reconciliation' END, uncertain_at = clock_timestamp() FROM candidate WHERE job.id = candidate.id RETURNING job.id, job.worker_id, job.worker_version, job.attempt_id, job.attempt_epoch, job.attempt_count, (job.cancel_requested_at IS NOT NULL)"
      let parameters =
        list.append(
          [pog.text(storage_owner), pog.text(queue)],
          list.flat_map(identities, fn(identity) {
            let #(worker_id, worker_version) = identity
            [pog.text(worker_id), pog.text(worker_version)]
          }),
        )
      let query =
        list.fold(parameters, pog.query(sql), fn(query, parameter) {
          pog.parameter(query, parameter)
        })
        |> pog.returning({
          use id <- decode.field(0, decode.int)
          use worker_id <- decode.field(1, decode.string)
          use worker_version <- decode.field(2, decode.string)
          use attempt_id <- decode.field(3, decode.optional(decode.int))
          use attempt_epoch <- decode.field(4, decode.int)
          use attempt_count <- decode.field(5, decode.int)
          use cancellation_was_requested <- decode.field(6, decode.bool)
          decode.success(#(
            id,
            worker_id,
            worker_version,
            attempt_id,
            attempt_epoch,
            attempt_count,
            cancellation_was_requested,
          ))
        })
      case execute_safely(query, on: connection) {
        Error(error) -> Error(QueueClaimFailed(error))
        Ok(returned) -> {
          list.each(returned.rows, fn(row) {
            emit_quarantined(forwarder, queue, row)
          })
          Ok(Nil)
        }
      }
    }
  }
}

/// Builds and forwards `[grind, job, quarantined]` for one row the
/// quarantine scan's own `RETURNING` reports as moved to `uncertain`. Called
/// only after that autocommitted `UPDATE` already returned the row.
/// `attempt_id` is only absent for a row with no attempt on record at all —
/// unreachable for a row this scan can find (it only ever matches
/// `state = 'executing'`, which always has one), but decoded as optional
/// defensively rather than asserted, and skipped (fail-closed, like every
/// other stored-state mapping in this module) rather than guessed at.
fn emit_quarantined(
  fwd: Forwarder,
  queue: String,
  row: #(Int, String, String, Option(Int), Int, Int, Bool),
) -> Nil {
  let #(
    job_id,
    worker_id,
    worker_version,
    attempt_id,
    epoch,
    attempt,
    cancellation_was_requested,
  ) = row
  case attempt_id {
    None -> Nil
    Some(attempt_id) -> {
      let _ =
        forwarder.emit(
          fwd,
          observation.quarantined(),
          observation.QuarantinedMeasurements(count: 1),
          observation.QuarantinedMetadata(
            ref: observation.JobRef(
              job_id:,
              queue:,
              worker_id:,
              worker_version:,
            ),
            attempt: observation.AttemptRef(attempt_id:, epoch:, attempt:),
            cancellation_was_requested:,
          ),
        )
      Nil
    }
  }
}

fn codec_contract_mismatch(
  stored_input: String,
  registered_input: String,
  stored_output: String,
  registered_output: String,
  stored_error: Option(String),
  registered_error: Option(String),
) -> Option(#(String, String, String)) {
  case stored_input == registered_input {
    False -> Some(#("input", stored_input, registered_input))
    True ->
      case stored_output == registered_output {
        False -> Some(#("output", stored_output, registered_output))
        True ->
          case stored_error == registered_error {
            False ->
              Some(#(
                "error",
                option_version(stored_error),
                option_version(registered_error),
              ))
            True -> None
          }
      }
  }
}

fn option_version(version: Option(String)) -> String {
  case version {
    Some(version) -> version
    None -> "none"
  }
}

fn mark_contract_mismatch(
  database: Database,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  kind: String,
  expected: String,
  actual: String,
) -> Result(Bool, QueueRunError) {
  let Database(connection:, storage_owner:, forwarder:, ..) = database
  let Claim(
    id:,
    attempt_id:,
    epoch:,
    worker_id:,
    worker_version:,
    current_attempt:,
    ..,
  ) = claim
  let query =
    pog.query(
      "UPDATE grind_jobs SET state = 'contract_mismatch', failure_description = $7, attempt_id = NULL, attempt_owner = NULL, lease_expires_at = NULL, attempt_count = GREATEST(attempt_count - 1, 0) WHERE id = $1 AND storage_owner = $2 AND queue = $3 AND state = 'executing' AND attempt_id = $4 AND attempt_epoch = $5 AND attempt_owner = $6 AND "
      <> live_lease_predicate("clock_timestamp()")
      <> " RETURNING id",
    )
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.parameter(pog.text(
      "stored "
      <> kind
      <> " codec version "
      <> expected
      <> " does not match registered version "
      <> actual,
    ))
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(QueueAckFailed(error))
    Ok(returned) ->
      case returned.rows {
        [_] -> {
          case codec_kind_of_stored(kind) {
            Error(Nil) -> Nil
            Ok(kind) ->
              emit_contract_mismatch(
                forwarder,
                queue,
                id,
                worker_id,
                worker_version,
                attempt_id,
                epoch,
                current_attempt,
                kind,
                expected,
                actual,
              )
          }
          Ok(True)
        }
        _ -> Error(QueueAckRejected)
      }
  }
}

/// Builds and forwards `[grind, job, contract_mismatch]` for a claim released
/// because a registered worker's codec contract no longer matches what was
/// persisted at admission. Called only after this release's own fenced
/// `UPDATE ... RETURNING` already returned that row.
fn emit_contract_mismatch(
  fwd: Forwarder,
  queue: String,
  job_id: Int,
  worker_id: String,
  worker_version: String,
  attempt_id: Int,
  epoch: Int,
  attempt: Int,
  kind: observation.CodecKind,
  expected_version: String,
  actual_version: String,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      observation.contract_mismatch(),
      observation.ContractMismatchMeasurements(count: 1),
      observation.ContractMismatchMetadata(
        ref: observation.JobRef(job_id:, queue:, worker_id:, worker_version:),
        attempt: observation.AttemptRef(attempt_id:, epoch:, attempt:),
        kind:,
        expected_version:,
        actual_version:,
      ),
    )
  Nil
}

fn codec_kind_of_stored(kind: String) -> Result(observation.CodecKind, Nil) {
  case kind {
    "input" -> Ok(observation.InputCodec)
    "output" -> Ok(observation.OutputCodec)
    "error" -> Ok(observation.ErrorCodec)
    _ -> Error(Nil)
  }
}

fn acknowledge(
  database: Database,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  execution: worker.Execution,
  expected_output_version: String,
) -> Result(Bool, QueueRunError) {
  let Database(connection:, storage_owner:, forwarder:, ..) = database
  let Claim(
    id:,
    attempt_id:,
    epoch:,
    worker_id:,
    worker_version:,
    current_attempt:,
    ..,
  ) = claim
  let proposal = case execution {
    worker.ExecutedSuccess(version, encoded) ->
      AckProposal(
        proposed_state: "succeeded",
        failure_cause: None,
        requested_delay_ms: None,
        output_version: Some(version),
        output: Some(encoded),
        error_version: None,
        error: None,
        failure_description: None,
      )
    worker.ExecutedBusinessFailure(version, encoded, description, cause) ->
      AckProposal(
        proposed_state: "business_failed",
        failure_cause: Some(cause),
        requested_delay_ms: None,
        output_version: None,
        output: None,
        error_version: version,
        error: encoded,
        failure_description: Some(description),
      )
    worker.ExecutedRetryable(version, encoded, description, delay_ms) ->
      AckProposal(
        proposed_state: "retryable",
        failure_cause: None,
        requested_delay_ms: Some(delay_ms),
        output_version: None,
        output: None,
        error_version: version,
        error: encoded,
        failure_description: Some(description),
      )
    worker.ExecutedInvalidInput(description) ->
      AckProposal(
        proposed_state: "runtime_failed",
        failure_cause: None,
        requested_delay_ms: None,
        output_version: None,
        output: None,
        error_version: None,
        error: None,
        failure_description: Some(description),
      )
    worker.ExecutedSnoozed(delay_ms, reason) ->
      AckProposal(
        proposed_state: "snoozed",
        failure_cause: None,
        requested_delay_ms: Some(delay_ms),
        output_version: None,
        output: None,
        error_version: None,
        error: None,
        failure_description: Some(reason),
      )
    worker.ExecutedDiscarded(reason) ->
      AckProposal(
        proposed_state: "discarded",
        failure_cause: None,
        requested_delay_ms: None,
        output_version: None,
        output: None,
        error_version: None,
        error: None,
        failure_description: Some(reason),
      )
    worker.ExecutedCancelled(reason) ->
      AckProposal(
        proposed_state: "cancelled",
        failure_cause: None,
        requested_delay_ms: None,
        output_version: None,
        output: None,
        error_version: None,
        error: None,
        failure_description: Some(reason),
      )
    worker.ExecutedUncertain(reason) ->
      AckProposal(
        proposed_state: "uncertain",
        failure_cause: None,
        requested_delay_ms: None,
        output_version: None,
        output: None,
        error_version: None,
        error: None,
        failure_description: Some(reason),
      )
  }
  let command_id = acknowledgement_command_id(id, attempt_id, epoch)
  let transaction_result =
    transaction_safely(connection, fn(transaction) {
      acknowledge_transaction(
        transaction,
        storage_owner,
        queue,
        attempt_owner,
        claim,
        command_id,
        proposal,
        execution,
        expected_output_version,
      )
    })
  case
    resolve_ack_transaction_result(
      connection,
      storage_owner,
      queue,
      attempt_owner,
      claim,
      command_id,
      proposal,
      execution,
      transaction_result,
    )
  {
    Error(error) -> Error(error)
    Ok(commit) -> {
      case job.state_of_stored(commit.committed_state) {
        Error(Nil) -> Nil
        Ok(committed_state) -> {
          let confirmation = case commit.via_receipt_match {
            True -> observation.Reconciled
            False -> observation.Replied
          }
          emit_acknowledged(
            forwarder,
            queue,
            id,
            worker_id,
            worker_version,
            attempt_id,
            epoch,
            current_attempt,
            proposed_of_execution(execution),
            committed_state,
            observation_failure_cause(commit.failure_cause),
            commit.available_at_unix_ms,
            confirmation,
            command_id,
          )
        }
      }
      Ok(True)
    }
  }
}

/// Builds and forwards `[grind, job, acknowledged]` from a proven-committed
/// `AckCommit`. Called only from `acknowledge`, strictly after
/// `transaction_safely` (and, on a lost reply, `reconcile_unknown_ack`) has
/// already returned — never from inside a transaction callback. The
/// forwarder hand-off result is ignored: an observation is a diagnostic
/// side channel, never a policy decision, and its own failure or drop must
/// never affect a job's committed outcome.
fn emit_acknowledged(
  fwd: Forwarder,
  queue: String,
  job_id: Int,
  worker_id: String,
  worker_version: String,
  attempt_id: Int,
  epoch: Int,
  attempt: Int,
  proposed: observation.Proposed,
  committed_state: State,
  failure_cause: Option(job.BusinessFailureCause),
  available_at_unix_ms: Option(Int),
  confirmation: observation.Confirmation,
  command_id: String,
) -> Nil {
  let _ =
    forwarder.emit(
      fwd,
      observation.acknowledged(),
      observation.AcknowledgedMeasurements(count: 1),
      observation.AcknowledgedMetadata(
        ref: observation.JobRef(job_id:, queue:, worker_id:, worker_version:),
        attempt: observation.AttemptRef(attempt_id:, epoch:, attempt:),
        proposed:,
        committed_state:,
        failure_cause:,
        available_at_unix_ms:,
        confirmation:,
        command_id:,
      ),
    )
  Nil
}

fn proposed_of_execution(execution: worker.Execution) -> observation.Proposed {
  case execution {
    worker.ExecutedSuccess(_, _) -> observation.ProposedSuccess
    worker.ExecutedBusinessFailure(_, _, _, _) ->
      observation.ProposedBusinessFailure
    worker.ExecutedRetryable(_, _, _, _) -> observation.ProposedRetryable
    worker.ExecutedInvalidInput(_) -> observation.ProposedRuntimeFailed
    worker.ExecutedSnoozed(_, _) -> observation.ProposedSnoozed
    worker.ExecutedDiscarded(_) -> observation.ProposedDiscarded
    worker.ExecutedCancelled(_) -> observation.ProposedCancelled
    worker.ExecutedUncertain(_) -> observation.ProposedUncertain
  }
}

fn observation_failure_cause(
  raw: Option(String),
) -> Option(job.BusinessFailureCause) {
  case raw {
    Some("budget_exhausted") -> Some(job.BudgetExhausted)
    Some("retry_declined") -> Some(job.RetryDeclined)
    _ -> None
  }
}

fn resolve_ack_transaction_result(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
  execution: worker.Execution,
  transaction_result: Result(AckCommit, pog.TransactionError(QueueRunError)),
) -> Result(AckCommit, QueueRunError) {
  case transaction_result {
    Ok(commit) -> Ok(commit)
    Error(pog.TransactionRolledBack(error)) -> Error(error)
    Error(pog.TransactionQueryError(_)) ->
      reconcile_unknown_ack(
        connection,
        storage_owner,
        queue,
        attempt_owner,
        claim,
        command_id,
        proposal,
        execution,
      )
  }
}

fn reconcile_unknown_ack(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
  execution: worker.Execution,
) -> Result(AckCommit, QueueRunError) {
  case
    matching_acknowledgement(
      connection,
      storage_owner,
      queue,
      attempt_owner,
      claim,
      command_id,
      proposal,
    )
  {
    Ok(Some(#(True, committed_state, failure_cause))) ->
      Ok(AckCommit(
        committed_state:,
        failure_cause:,
        available_at_unix_ms: None,
        via_receipt_match: True,
      ))
    Ok(Some(#(False, _, _))) -> Error(QueueAckCommandConflict)
    Ok(None) | Error(_) -> Error(QueueAckUnknown(command_id, execution))
  }
}

fn acknowledge_transaction(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
  execution: worker.Execution,
  expected_output_version: String,
) -> Result(AckCommit, QueueRunError) {
  case
    matching_acknowledgement(
      connection,
      storage_owner,
      queue,
      attempt_owner,
      claim,
      command_id,
      proposal,
    )
  {
    Error(error) -> Error(error)
    Ok(Some(#(True, committed_state, failure_cause))) ->
      Ok(AckCommit(
        committed_state:,
        failure_cause:,
        available_at_unix_ms: None,
        via_receipt_match: True,
      ))
    Ok(Some(#(False, _, _))) -> Error(QueueAckCommandConflict)
    Ok(None) -> {
      let Claim(id:, attempt_id:, epoch:, error_version:, ..) = claim
      let AckProposal(
        proposed_state:,
        failure_cause:,
        output_version: _,
        requested_delay_ms:,
        output:,
        error_version: proposed_error_version,
        error:,
        failure_description:,
      ) = proposal
      let #(sql, parameters) = case proposed_state {
        "succeeded" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'succeeded' END, output = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $1::jsonb END, error = NULL, error_version = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE NULL END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND output_version = $8 AND "
            <> live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, output),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
            pog.text(expected_output_version),
          ],
        )
        "business_failed" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'business_failed' END, output = NULL, error = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $1::jsonb END, error_version = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $2 END, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $3 END, failure_cause = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $4 END, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL WHERE id = $5 AND storage_owner = $6 AND queue = $7 AND state = 'executing' AND attempt_id = $8 AND attempt_epoch = $9 AND attempt_owner = $10 AND error_version IS NOT DISTINCT FROM $11 AND "
            <> live_lease_predicate("clock_timestamp()")
            <> " AND (cancel_requested_at IS NOT NULL OR (($4 = 'budget_exhausted' AND attempt_count >= max_attempts) OR ($4 = 'retry_declined' AND attempt_count < max_attempts))) RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, error),
            pog.nullable(pog.text, proposed_error_version),
            pog.nullable(pog.text, failure_description),
            pog.nullable(pog.text, failure_cause),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
            pog.nullable(pog.text, error_version),
          ],
        )
        "retryable" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'retryable' END, available_at = CASE WHEN cancel_requested_at IS NOT NULL THEN available_at ELSE clock_timestamp() + ($1::double precision * interval '1 millisecond') END, output = NULL, error = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $2::jsonb END, error_version = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE $3 END, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $4 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL WHERE id = $5 AND storage_owner = $6 AND queue = $7 AND state = 'executing' AND attempt_id = $8 AND attempt_epoch = $9 AND attempt_owner = $10 AND error_version IS NOT DISTINCT FROM $11 AND (attempt_count < max_attempts OR cancel_requested_at IS NOT NULL) AND "
            <> live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.int, requested_delay_ms),
            pog.nullable(pog.text, error),
            pog.nullable(pog.text, proposed_error_version),
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
            pog.nullable(pog.text, error_version),
          ],
        )
        "snoozed" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'scheduled' END, available_at = CASE WHEN cancel_requested_at IS NOT NULL THEN available_at ELSE clock_timestamp() + ($1::double precision * interval '1 millisecond') END, snooze_count = CASE WHEN cancel_requested_at IS NOT NULL THEN snooze_count ELSE snooze_count + 1 END, attempt_count = CASE WHEN cancel_requested_at IS NOT NULL THEN attempt_count ELSE GREATEST(attempt_count - 1, 0) END, output = NULL, error = NULL, error_version = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $2 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL WHERE id = $3 AND storage_owner = $4 AND queue = $5 AND state = 'executing' AND attempt_id = $6 AND attempt_epoch = $7 AND attempt_owner = $8 AND "
            <> live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.int, requested_delay_ms),
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        "discarded" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'discarded' END, output = NULL, error = NULL, error_version = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $1 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND "
            <> live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        "cancelled" -> #(
          "UPDATE grind_jobs SET state = 'cancelled', output = NULL, error = NULL, error_version = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $1 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND "
            <> live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        "uncertain" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'uncertain' END, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $1 END, uncertain_at = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE clock_timestamp() END, attempt_owner = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE attempt_owner END, lease_expires_at = CASE WHEN cancel_requested_at IS NOT NULL THEN NULL ELSE lease_expires_at END, cancel_requested_at = NULL WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND "
            <> live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        "runtime_failed" -> #(
          "UPDATE grind_jobs SET state = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled' ELSE 'runtime_failed' END, output = NULL, error = NULL, error_version = NULL, failure_description = CASE WHEN cancel_requested_at IS NOT NULL THEN 'cancelled by caller' ELSE $1 END, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL, cancel_requested_at = NULL WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND "
            <> live_lease_predicate("clock_timestamp()")
            <> " RETURNING id, state, failure_description, (extract(epoch FROM available_at) * 1000)::bigint",
          [
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
          ],
        )
        _ -> #("SELECT 1 WHERE FALSE", [])
      }
      let query =
        list.fold(parameters, pog.query(sql), fn(query, parameter) {
          pog.parameter(query, parameter)
        })
        |> pog.returning({
          use updated_id <- decode.field(0, decode.int)
          use committed_state <- decode.field(1, decode.string)
          use committed_description <- decode.field(
            2,
            decode.optional(decode.string),
          )
          use committed_available_at_ms <- decode.field(3, decode.int)
          decode.success(#(
            updated_id,
            committed_state,
            committed_description,
            committed_available_at_ms,
          ))
        })
      case execute_safely(query, on: connection) {
        Error(error) -> Error(QueueAckFailed(error))
        Ok(returned) ->
          case returned.rows {
            [] ->
              // A duplicate ACK can wait behind the first writer's row lock.
              // Re-read its receipt after the UPDATE observes the committed
              // state instead of misreporting that exact retry as stale.
              case
                matching_acknowledgement(
                  connection,
                  storage_owner,
                  queue,
                  attempt_owner,
                  claim,
                  command_id,
                  proposal,
                )
              {
                Ok(Some(#(True, committed_state, failure_cause))) ->
                  Ok(AckCommit(
                    committed_state:,
                    failure_cause:,
                    available_at_unix_ms: None,
                    via_receipt_match: True,
                  ))
                Ok(Some(#(False, _, _))) -> Error(QueueAckCommandConflict)
                Ok(None) ->
                  current_ack_rejection(
                    connection,
                    storage_owner,
                    queue,
                    attempt_owner,
                    claim,
                    execution,
                  )
                Error(error) -> Error(error)
              }
            [#(_, committed_state, _committed_description, available_at_ms)] ->
              insert_acknowledgement(
                connection,
                storage_owner,
                queue,
                attempt_owner,
                claim,
                command_id,
                proposal,
                committed_state,
                available_at_for_observation(committed_state, available_at_ms),
              )
            _ -> Error(QueueAckRejected)
          }
      }
    }
  }
}

/// `available_at` is only a meaningful "next eligibility" signal for a
/// *committed* `retryable`/`scheduled` outcome — read from the acknowledge
/// UPDATE's own `RETURNING`, never from the proposal. A proposed
/// retry/snooze whose `available_at` write was itself overridden (a
/// concurrent cancellation commits `cancelled` instead, leaving
/// `available_at` at its unrelated pre-ack value) must not report that
/// stale value as if it were a real next-eligibility time; gating on the
/// committed state, not the proposed one, is what keeps this correct. For
/// every other outcome the column still holds a value (it is `NOT NULL`),
/// but that value is not what an observer means by "when does this job
/// become eligible again", so it is not surfaced there.
fn available_at_for_observation(
  committed_state: String,
  available_at_ms: Int,
) -> Option(Int) {
  case committed_state {
    "retryable" | "scheduled" -> Some(available_at_ms)
    _ -> None
  }
}

fn current_ack_rejection(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  execution: worker.Execution,
) -> Result(AckCommit, QueueRunError) {
  let Claim(id:, attempt_id:, epoch:, ..) = claim
  let query =
    pog.query(
      "SELECT state, attempt_id, attempt_epoch, attempt_owner, "
      <> live_lease_predicate("clock_timestamp()")
      <> " FROM grind_jobs WHERE id = $1 AND storage_owner = $2 AND queue = $3",
    )
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(queue))
    |> pog.returning({
      use state <- decode.field(0, decode.string)
      use current_attempt <- decode.field(1, decode.optional(decode.int))
      use current_epoch <- decode.field(2, decode.optional(decode.int))
      use current_owner <- decode.field(3, decode.optional(decode.string))
      use lease_live <- decode.field(4, decode.bool)
      decode.success(#(
        state,
        current_attempt,
        current_epoch,
        current_owner,
        lease_live,
      ))
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(QueueAckFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Error(QueueAckStale(execution, AckRecordMissing))
        [#(state, current_attempt, current_epoch, current_owner, lease_live)] ->
          case state {
            "executing" ->
              case
                current_attempt == Some(attempt_id)
                && current_epoch == Some(epoch)
                && current_owner == Some(attempt_owner)
              {
                False ->
                  Error(QueueAckStale(
                    execution,
                    AckOwnershipChanged(
                      state:,
                      attempt_id: current_attempt,
                      epoch: current_epoch,
                      owner: current_owner,
                    ),
                  ))
                True ->
                  case lease_live {
                    False ->
                      Error(QueueAckStale(
                        execution,
                        AckLeaseExpired(attempt_id, epoch, attempt_owner),
                      ))
                    True ->
                      Error(QueueAckStale(
                        execution,
                        AckRecordChanged(
                          state:,
                          attempt_id: current_attempt,
                          epoch: current_epoch,
                          owner: current_owner,
                        ),
                      ))
                  }
              }
            _ -> Error(QueueAckStale(execution, AckStateChanged(state)))
          }
        _ -> Error(QueueAckRejected)
      }
  }
}

/// The stable acknowledgement command ID for one attempt fence. Exposed only
/// so tests can construct the same ID an internal reconciliation path would,
/// without re-encoding this format themselves.
@internal
pub fn acknowledgement_command_id(
  job_id: Int,
  attempt_id: Int,
  epoch: Int,
) -> String {
  "grind-ack:"
  <> int.to_string(job_id)
  <> ":"
  <> int.to_string(attempt_id)
  <> ":"
  <> int.to_string(epoch)
}

/// The fixed-order envelope deliberately hashes PostgreSQL's JSONB rendering,
/// not a cross-runtime canonical JSON representation. Presence flags keep SQL
/// NULL distinct from JSON null. Large numeric textual variants may conflict.
fn acknowledgement_fingerprint_sql(first_parameter: Int) -> String {
  let proposed_state = sql_parameter(first_parameter, "text")
  let output_version = sql_parameter(first_parameter + 1, "text")
  let output = sql_parameter(first_parameter + 2, "text")
  let error_version = sql_parameter(first_parameter + 3, "text")
  let error = sql_parameter(first_parameter + 4, "text")
  let reason = sql_parameter(first_parameter + 5, "text")
  let delay = sql_parameter(first_parameter + 6, "bigint")
  let cause = sql_parameter(first_parameter + 7, "text")
  "sha256(convert_to(jsonb_build_array('grind-ack-proposal-v1', "
  <> proposed_state
  <> ", "
  <> output_version
  <> " IS NOT NULL, "
  <> output_version
  <> ", "
  <> output
  <> " IS NOT NULL, CASE WHEN "
  <> output
  <> " IS NULL THEN NULL::jsonb ELSE "
  <> output
  <> "::jsonb END, "
  <> error_version
  <> " IS NOT NULL, "
  <> error_version
  <> ", "
  <> error
  <> " IS NOT NULL, CASE WHEN "
  <> error
  <> " IS NULL THEN NULL::jsonb ELSE "
  <> error
  <> "::jsonb END, "
  <> reason
  <> " IS NOT NULL, "
  <> reason
  <> ", "
  <> delay
  <> " IS NOT NULL, "
  <> delay
  <> ", "
  <> cause
  <> " IS NOT NULL, "
  <> cause
  <> ")::text, 'UTF8'))"
}

fn sql_parameter(index: Int, cast: String) -> String {
  "$" <> int.to_string(index) <> "::" <> cast
}

/// Looks up the exact acknowledgement row for `command_id`, if one exists,
/// and reports both whether it matches this exact proposal and the
/// disposition it actually committed — so a caller that finds a match can
/// build the `Reconciled` observation from a real receipt read rather than
/// re-deriving it from this call's own (possibly stale) proposal.
fn matching_acknowledgement(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
) -> Result(Option(#(Bool, String, Option(String))), QueueRunError) {
  let Claim(id:, attempt_id:, epoch:, worker_id:, worker_version:, ..) = claim
  let AckProposal(
    proposed_state:,
    failure_cause:,
    requested_delay_ms:,
    output_version:,
    output:,
    error_version:,
    error:,
    failure_description:,
  ) = proposal
  let query =
    pog.query(
      "SELECT storage_owner = $1 AND command_id = $2 AND queue = $3 AND job_id = $4 AND worker_id = $5 AND worker_version = $6 AND attempt_id = $7 AND attempt_epoch = $8 AND attempt_owner = $9 AND proposal_sha256 = "
      <> acknowledgement_fingerprint_sql(10)
      <> ", committed_state, failure_cause FROM grind_job_acknowledgements WHERE storage_owner = $1 AND command_id = $2",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(command_id))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.parameter(pog.text(proposed_state))
    |> pog.parameter(pog.nullable(pog.text, output_version))
    |> pog.parameter(pog.nullable(pog.text, output))
    |> pog.parameter(pog.nullable(pog.text, error_version))
    |> pog.parameter(pog.nullable(pog.text, error))
    |> pog.parameter(pog.nullable(pog.text, failure_description))
    |> pog.parameter(pog.nullable(pog.int, requested_delay_ms))
    |> pog.parameter(pog.nullable(pog.text, failure_cause))
    |> pog.returning({
      use matches <- decode.field(0, decode.bool)
      use committed_state <- decode.field(1, decode.string)
      use committed_failure_cause <- decode.field(
        2,
        decode.optional(decode.string),
      )
      decode.success(#(matches, committed_state, committed_failure_cause))
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(QueueAckFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Ok(None)
        [row] -> Ok(Some(row))
        _ -> Error(QueueAckRejected)
      }
  }
}

fn insert_acknowledgement(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
  actual_committed_state: String,
  available_at_unix_ms: Option(Int),
) -> Result(AckCommit, QueueRunError) {
  let Claim(id:, attempt_id:, epoch:, worker_id:, worker_version:, ..) = claim
  let AckProposal(
    proposed_state:,
    failure_cause:,
    requested_delay_ms:,
    output_version:,
    output:,
    error_version:,
    error:,
    failure_description:,
  ) = proposal
  let committed_failure_cause = case actual_committed_state {
    "cancelled" -> None
    _ -> failure_cause
  }
  let query =
    pog.query(
      "INSERT INTO grind_job_acknowledgements (storage_owner, command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, committed_state, failure_cause, proposal_sha256) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $18, $19, "
      <> acknowledgement_fingerprint_sql(10)
      <> ") RETURNING command_id",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(command_id))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.parameter(pog.text(proposed_state))
    |> pog.parameter(pog.nullable(pog.text, output_version))
    |> pog.parameter(pog.nullable(pog.text, output))
    |> pog.parameter(pog.nullable(pog.text, error_version))
    |> pog.parameter(pog.nullable(pog.text, error))
    |> pog.parameter(pog.nullable(pog.text, failure_description))
    |> pog.parameter(pog.nullable(pog.int, requested_delay_ms))
    |> pog.parameter(pog.nullable(pog.text, failure_cause))
    |> pog.parameter(pog.text(actual_committed_state))
    |> pog.parameter(pog.nullable(pog.text, committed_failure_cause))
    |> pog.returning({
      use inserted_command_id <- decode.field(0, decode.string)
      decode.success(inserted_command_id)
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(QueueAckFailed(error))
    Ok(returned) ->
      case returned.rows {
        [_] ->
          Ok(AckCommit(
            committed_state: actual_committed_state,
            failure_cause: committed_failure_cause,
            available_at_unix_ms:,
            via_receipt_match: False,
          ))
        _ -> Error(QueueAckRejected)
      }
  }
}

pub fn state(
  database: Database,
  handle: JobHandle(input, output, error),
) -> Result(State, StateError) {
  let Database(connection:, storage_owner:, ..) = database
  let #(id, handle_owner, handle_queue, worker_id, worker_version, _) =
    job.storage_fields(handle)
  case call_safely(fn() { sql.state(connection, id) }) {
    Error(error) -> Error(StateQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Error(StateJobNotFound)
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
            False -> Error(StateStorageOwnerMismatch)
            True ->
              case stored_queue == handle_queue {
                False -> Error(StateQueueMismatch)
                True ->
                  case
                    stored_worker == worker_id
                    && stored_worker_version == worker_version
                  {
                    False -> Error(StateWorkerContractMismatch)
                    True ->
                      job.state_of_stored(state)
                      |> result.replace_error(InvalidStoredState(state))
                  }
              }
          }
        _ -> Error(StateJobNotFound)
      }
  }
}

pub type OutcomeError {
  OutcomeQueryFailed(pog.QueryError)
  OutcomeJobNotFound
  OutcomeStorageOwnerMismatch
  OutcomeQueueMismatch
  OutcomeWorkerContractMismatch
  OutcomeCodecFailed(worker.StoredCodecError)
  InvalidOutcomeState(String)
}

/// Reads the last committed result without confusing it with a handler return.
pub fn outcome(
  database: Database,
  handle: JobHandle(input, output, error),
) -> Result(job.Outcome(output, error), OutcomeError) {
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
  case call_safely(fn() { sql.outcome(connection, id) }) {
    Error(error) -> Error(OutcomeQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Error(OutcomeJobNotFound)
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
        _ -> Error(OutcomeJobNotFound)
      }
  }
}

/// Reads a compact acknowledgement receipt by its stable command ID.
/// Historical typed values are not retained here; `outcome` reads the job's
/// current typed result independently.
pub fn reconcile_acknowledgement(
  database: Database,
  handle: JobHandle(input, output, error),
  command_id: String,
) -> Result(AcknowledgementReceipt, AckReconciliationError) {
  let Database(connection:, storage_owner: database_owner, ..) = database
  let #(id, handle_owner, queue, worker_id, worker_version, _) =
    job.storage_fields(handle)
  case handle_owner == database_owner {
    False -> Error(AckReceiptStorageOwnerMismatch)
    True -> {
      case
        call_safely(fn() {
          sql.reconcile_acknowledgement(connection, database_owner, command_id)
        })
      {
        Error(error) -> Error(AckReceiptQueryFailed(error))
        Ok(returned) ->
          case returned.rows {
            [] -> Error(AckReceiptNotFound)
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
                to_char: committed_at,
              ) = receipt
              case stored_owner == database_owner && stored_id == id {
                False -> Error(AckReceiptStorageOwnerMismatch)
                True ->
                  case stored_queue == queue {
                    False -> Error(AckReceiptRouteMismatch)
                    True ->
                      case
                        stored_worker == worker_id
                        && stored_worker_version == worker_version
                      {
                        False -> Error(AckReceiptWorkerContractMismatch)
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
                            committed_at:,
                          ))
                        }
                      }
                  }
              }
            }
            _ -> Error(AckReceiptNotFound)
          }
      }
    }
  }
}

fn acknowledgement_state(
  state: String,
) -> Result(job.State, AckReconciliationError) {
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
    other -> Error(AckReceiptInvalidState(other))
  }
}

fn acknowledgement_failure_cause(
  cause: Option(String),
) -> Result(Option(job.BusinessFailureCause), AckReconciliationError) {
  case cause {
    Some("budget_exhausted") -> Ok(Some(job.BudgetExhausted))
    Some("retry_declined") -> Ok(Some(job.RetryDeclined))
    None -> Ok(None)
    Some(other) -> Error(AckReceiptInvalidState(other))
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
) -> Result(job.Outcome(output, error), OutcomeError) {
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
    False -> Error(OutcomeStorageOwnerMismatch)
    True ->
      case stored_queue == handle_queue {
        False -> Error(OutcomeQueueMismatch)
        True ->
          case
            stored_worker == handle_worker
            && stored_worker_version == handle_worker_version
          {
            False -> Error(OutcomeWorkerContractMismatch)
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
) -> Result(job.Outcome(output, error), OutcomeError) {
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
          |> result.map_error(OutcomeCodecFailed)
        None -> Error(InvalidOutcomeState("successful row has no output"))
      }
    "business_failed" -> {
      let cause = case failure_cause {
        Some("budget_exhausted") -> Some(job.BudgetExhausted)
        Some("retry_declined") -> Some(job.RetryDeclined)
        _ -> None
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
          |> result.map_error(OutcomeCodecFailed)
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
    other -> Error(InvalidOutcomeState(other))
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
/// full admission transaction.
pub fn submit_unique(
  database: Database,
  queue: String,
  submission_id: unique.SubmissionId,
  worker: Worker(input, output, error),
  input: input,
  availability: unique.Availability,
  policy: unique.Policy(input),
  on_conflict: unique.ConflictAction,
) -> Result(
  unique.Admission(input, output, error),
  unique.SubmitError(input, output, error),
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
        Some(unique.submission_id_value(submission_id)),
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
fn admission_job_id(admission: unique.Admission(input, output, error)) -> Int {
  case admission {
    unique.Inserted(handle) -> job.id_value(handle)
    unique.Existing(conflict) -> unique.conflict_job_id(conflict)
    unique.Rescheduled(conflict) -> unique.conflict_job_id(conflict)
  }
}

/// Re-reads the receipt a `CommitUnknown` command would have written,
/// without repeating the admission transaction. A receipt matching the
/// retained request returns its recorded decision; a receipt that does not
/// match, or no receipt yet, both surface the same way a fresh `submit_unique`
/// call would (`SubmissionConflict` / another `CommitUnknown`).
pub fn reconcile_unique(
  database: Database,
  pending: unique.PendingSubmission(input, output, error),
) -> Result(
  unique.Admission(input, output, error),
  unique.SubmitError(input, output, error),
) {
  let Database(connection:, ..) = database
  unique_admission.reconcile(connection, pending)
}
