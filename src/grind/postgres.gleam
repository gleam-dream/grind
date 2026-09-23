import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option.{type Option, None, Some, unwrap}
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/result
import gleam/string
import grind/job.{type JobHandle, type State, Queued, Scheduled}
import grind/registry.{type Registry}
import grind/worker.{type Worker}
import pog

/// Pure PostgreSQL pool settings. Validation does not acquire a resource.
pub type Settings {
  Settings(
    database_url: String,
    pool_name: process.Name(pog.Message),
    pool_size: Int,
  )
}

pub fn settings(
  database_url: String,
  pool_name: process.Name(pog.Message),
) -> Settings {
  Settings(database_url:, pool_name:, pool_size: 10)
}

pub fn pool_size(settings: Settings, pool_size: Int) -> Settings {
  Settings(..settings, pool_size:)
}

pub type ConfigError {
  InvalidDatabaseUrl
  InvalidPoolSize
}

pub opaque type ValidatedSettings {
  ValidatedSettings(pog.Config, storage_owner: String)
}

/// Checks the URL and pool bound before any PostgreSQL process is started.
pub fn validate(settings: Settings) -> Result(ValidatedSettings, ConfigError) {
  case settings.pool_size > 0 {
    False -> Error(InvalidPoolSize)
    True ->
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
            pog.pool_size(config, settings.pool_size),
            storage_owner:,
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

@external(erlang, "grind_postgres_ffi", "transaction_safely")
fn transaction_safely(
  connection: pog.Connection,
  callback: fn(pog.Connection) -> Result(a, b),
) -> Result(a, pog.TransactionError(b))

pub type StartError {
  PoolStartFailed(actor.StartError)
}

/// Starts a package-owned PostgreSQL pool after settings have been validated.
pub fn start(settings: ValidatedSettings) -> Result(Database, StartError) {
  let ValidatedSettings(config, storage_owner:) = settings
  let pog.Config(pool_name:, ..) = config
  let supervisor =
    static_supervisor.new(static_supervisor.OneForOne)
    |> static_supervisor.add(pog.supervised(config))
  case static_supervisor.start(supervisor) {
    Ok(started) -> {
      process.unlink(started.pid)
      Ok(Database(pog.named_connection(pool_name), started.pid, storage_owner))
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

pub type QueueConfigurationError {
  QueuePolicyQueryFailed(pog.QueryError)
  ExpiredAttemptPolicyConflict
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
      let Database(connection:, storage_owner: database_owner, ..) = database
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
        Ok(result) -> Ok(result)
        Error(pog.TransactionQueryError(_)) ->
          Error(ResolutionCommitUnknown(resolution_id))
        Error(pog.TransactionRolledBack(error)) -> Error(error)
      }
    }
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
          |> result.map(ResolutionAlreadyApplied)
      }
    Ok(None) -> apply_uncertain_resolution(connection, command)
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
          "SELECT storage_owner, queue, worker_id, worker_version, state, attempt_id, attempt_epoch, attempt_owner, lease_expires_at::text, output_version, error_version FROM grind_jobs WHERE id = $1 FOR UPDATE",
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
                False -> Error(ReconciliationNotRequired)
                True ->
                  case codec_matches {
                    False -> Error(ResolutionCodecMismatch)
                    True ->
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

/// Persists the safety policy for a queue so consumers sharing its storage
/// route cannot silently disagree about replay after lease expiry.
@internal
pub fn configure_queue(
  database: Database,
  queue: String,
  replay_expired: Bool,
) -> Result(Nil, QueueConfigurationError) {
  let Database(connection:, storage_owner:, ..) = database
  let expected = case replay_expired {
    True -> "replay_at_least_once"
    False -> "require_reconciliation"
  }
  let insert =
    pog.query(
      "INSERT INTO grind_queue_policies (storage_owner, queue, expired_attempt_policy) VALUES ($1, $2, $3) ON CONFLICT (storage_owner, queue) DO NOTHING",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.text(expected))
  use _ <- result.try(case execute_safely(insert, on: connection) {
    Error(error) -> Error(QueuePolicyQueryFailed(error))
    Ok(_) -> Ok(Nil)
  })
  let read =
    pog.query(
      "SELECT expired_attempt_policy FROM grind_queue_policies WHERE storage_owner = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(queue))
    |> pog.returning({
      use policy <- decode.field(0, decode.string)
      decode.success(policy)
    })
  case execute_safely(read, on: connection) {
    Error(error) -> Error(QueuePolicyQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [policy] ->
          case policy == expected {
            True -> Ok(Nil)
            False -> Error(ExpiredAttemptPolicyConflict)
          }
        _ -> Error(ExpiredAttemptPolicyConflict)
      }
  }
}

/// Creates and validates schema version 1 atomically. Safe to run more than once.
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
  ExistingSchema(
    version: Int,
    has_jobs: Bool,
    has_queue_policies: Bool,
    has_resolutions: Bool,
    has_acknowledgements: Bool,
    has_attempt_sequence: Bool,
  )
}

fn read_schema_generation(
  connection: pog.Connection,
) -> Result(SchemaGeneration, StorageError) {
  let query =
    pog.query(
      "SELECT to_regclass(current_schema() || '.grind_schema_migrations') IS NOT NULL, to_regclass(current_schema() || '.grind_jobs') IS NOT NULL, to_regclass(current_schema() || '.grind_queue_policies') IS NOT NULL, to_regclass(current_schema() || '.grind_job_resolutions') IS NOT NULL, to_regclass(current_schema() || '.grind_job_acknowledgements') IS NOT NULL, to_regclass(current_schema() || '.grind_attempts_id_seq') IS NOT NULL",
    )
    |> pog.returning({
      use has_migrations <- decode.field(0, decode.bool)
      use has_jobs <- decode.field(1, decode.bool)
      use has_queue_policies <- decode.field(2, decode.bool)
      use has_resolutions <- decode.field(3, decode.bool)
      use has_acknowledgements <- decode.field(4, decode.bool)
      use has_attempt_sequence <- decode.field(5, decode.bool)
      decode.success(#(
        has_migrations,
        has_jobs,
        has_queue_policies,
        has_resolutions,
        has_acknowledgements,
        has_attempt_sequence,
      ))
    })
  use
    #(
      has_migrations,
      has_jobs,
      has_queue_policies,
      has_resolutions,
      has_acknowledgements,
      has_attempt_sequence,
    )
  <- result.try(case execute_safely(query, on: connection) {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [schema] -> Ok(schema)
        _ -> Error(IncompatibleSchema)
      }
  })
  case has_migrations {
    False ->
      case
        has_jobs
        || has_queue_policies
        || has_resolutions
        || has_acknowledgements
        || has_attempt_sequence
      {
        True -> Error(IncompatibleSchema)
        False -> Ok(FreshSchema)
      }
    True ->
      read_installed_schema_version(
        connection,
        has_jobs,
        has_queue_policies,
        has_resolutions,
        has_acknowledgements,
        has_attempt_sequence,
      )
  }
}

fn read_installed_schema_version(
  connection: pog.Connection,
  has_jobs: Bool,
  has_queue_policies: Bool,
  has_resolutions: Bool,
  has_acknowledgements: Bool,
  has_attempt_sequence: Bool,
) -> Result(SchemaGeneration, StorageError) {
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
  use #(count, minimum, maximum) <- result.try(
    case execute_safely(query, on: connection) {
      Error(_) -> Error(IncompatibleSchema)
      Ok(returned) ->
        case returned.rows {
          [version] -> Ok(version)
          _ -> Error(IncompatibleSchema)
        }
    },
  )
  let v3_evidence_valid = case maximum >= 3 {
    False -> True
    True -> has_queue_policies && has_resolutions
  }
  let v4_evidence_valid = case maximum >= 4 {
    False -> True
    True -> has_acknowledgements && has_attempt_sequence
  }
  // Every installed generation uses attempt IDs as fencing identity. Recreating
  // a missing sequence would risk reusing an ID that a live or historical
  // attempt already owns.
  let attempt_sequence_valid = has_attempt_sequence
  case
    has_jobs,
    count == maximum && minimum == 1 && maximum > 0 && maximum <= 6,
    v3_evidence_valid,
    v4_evidence_valid,
    attempt_sequence_valid
  {
    False, _, _, _, _ -> Error(IncompatibleSchema)
    _, False, _, _, _ ->
      case maximum > 6 {
        True -> Error(UnsupportedSchemaVersion(maximum))
        False -> Error(IncompatibleSchema)
      }
    _, _, False, _, _ | _, _, _, False, _ | _, _, _, _, False ->
      Error(IncompatibleSchema)
    _, _, True, True, True ->
      Ok(ExistingSchema(
        version: maximum,
        has_jobs:,
        has_queue_policies:,
        has_resolutions:,
        has_acknowledgements:,
        has_attempt_sequence:,
      ))
  }
}

fn migrate_transaction(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
  use _schema_generation <- result.try(read_schema_generation(connection))
  let migration_sql =
    "CREATE TABLE IF NOT EXISTS grind_schema_migrations (version integer PRIMARY KEY, installed_at timestamptz NOT NULL DEFAULT clock_timestamp())"
  let jobs_sql =
    "CREATE TABLE IF NOT EXISTS grind_jobs ("
    <> "id bigserial PRIMARY KEY, storage_owner text NOT NULL, queue text NOT NULL, "
    <> "worker_id text NOT NULL, worker_version text NOT NULL, input_version text NOT NULL, input jsonb NOT NULL, "
    <> "output_version text NOT NULL, output jsonb, error_version text, error jsonb, "
    <> "state text NOT NULL CONSTRAINT grind_jobs_state_check CHECK (state IN ('queued', 'scheduled', 'retryable', 'executing', 'succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'uncertain', 'discarded', 'cancelled')), "
    <> "available_at timestamptz NOT NULL, inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(), "
    <> "attempt_id bigint, attempt_epoch bigint NOT NULL DEFAULT 0, attempt_owner text, "
    <> "lease_expires_at timestamptz, attempt_count bigint NOT NULL DEFAULT 0, max_attempts bigint NOT NULL DEFAULT 20, delivery_count bigint NOT NULL DEFAULT 0, snooze_count bigint NOT NULL DEFAULT 0, failure_description text, failure_cause text, uncertain_at timestamptz, CONSTRAINT grind_jobs_max_attempts_check CHECK (max_attempts > 0))"
  let queue_policies_sql =
    "CREATE TABLE IF NOT EXISTS grind_queue_policies ("
    <> "storage_owner text NOT NULL, queue text NOT NULL, "
    <> "expired_attempt_policy text NOT NULL CONSTRAINT grind_queue_policies_expiry_policy_check CHECK (expired_attempt_policy IN ('require_reconciliation', 'replay_at_least_once')), "
    <> "CONSTRAINT grind_queue_policies_pkey PRIMARY KEY (storage_owner, queue))"
  let resolutions_sql =
    "CREATE TABLE IF NOT EXISTS grind_job_resolutions ("
    <> "storage_owner text NOT NULL, queue text NOT NULL, job_id bigint NOT NULL, worker_id text, worker_version text, "
    <> "resolution_id text NOT NULL, attempt_id bigint NOT NULL, attempt_epoch bigint NOT NULL, "
    <> "attempt_owner text NOT NULL, lease_expires_at timestamptz NOT NULL, "
    <> "decision text NOT NULL CONSTRAINT grind_job_resolutions_decision_check CHECK (decision IN ('confirm_success', 'confirm_business_failure', 'authorize_replay')), "
    <> "target_state text NOT NULL CONSTRAINT grind_job_resolutions_target_state_check CHECK (target_state IN ('queued', 'succeeded', 'business_failed')), "
    <> "payload_version text, payload jsonb, "
    <> "resolved_by text NOT NULL, details text NOT NULL, resolved_at timestamptz NOT NULL DEFAULT clock_timestamp(), "
    <> "CONSTRAINT grind_job_resolutions_pkey PRIMARY KEY (storage_owner, resolution_id))"
  let acknowledgements_sql =
    "CREATE TABLE IF NOT EXISTS grind_job_acknowledgements ("
    <> "storage_owner text NOT NULL, command_id text NOT NULL, queue text NOT NULL, job_id bigint NOT NULL, "
    <> "worker_id text NOT NULL, worker_version text NOT NULL, attempt_id bigint NOT NULL, attempt_epoch bigint NOT NULL, "
    <> "attempt_owner text NOT NULL, proposed_state text NOT NULL CONSTRAINT grind_job_acknowledgements_proposed_state_check CHECK (proposed_state IN ('succeeded', 'business_failed', 'retryable', 'runtime_failed', 'snoozed', 'discarded', 'cancelled', 'uncertain')), "
    <> "committed_state text NOT NULL CONSTRAINT grind_job_acknowledgements_committed_state_check CHECK (committed_state IN ('succeeded', 'business_failed', 'retryable', 'runtime_failed', 'scheduled', 'discarded', 'cancelled', 'uncertain')), "
    <> "output_version text, output jsonb, error_version text, error jsonb, failure_description text, requested_delay_ms bigint, failure_cause text, "
    <> "committed_at timestamptz NOT NULL DEFAULT clock_timestamp(), "
    <> "CONSTRAINT grind_job_acknowledgements_pkey PRIMARY KEY (storage_owner, command_id), "
    <> "CONSTRAINT grind_job_acknowledgements_attempt_key UNIQUE (storage_owner, job_id, attempt_id, attempt_epoch))"
  let attempts_sql = "CREATE SEQUENCE IF NOT EXISTS grind_attempts_id_seq"
  use _ <- result.try(run_statement(connection, migration_sql))
  use _ <- result.try(run_statement(connection, jobs_sql))
  use _ <- result.try(run_statement(connection, queue_policies_sql))
  use _ <- result.try(run_statement(connection, resolutions_sql))
  use _ <- result.try(run_statement(connection, acknowledgements_sql))
  use _ <- result.try(run_statement(connection, attempts_sql))
  use _ <- result.try(upgrade_schema_versions(connection))
  use #(migration_columns, migration_columns_valid) <- result.try(
    inspect_columns(
      connection,
      "grind_schema_migrations",
      "(column_name = 'version' AND data_type = 'integer' AND is_nullable = 'NO') OR (column_name = 'installed_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'NO')",
    ),
  )
  case migration_columns == 2 && migration_columns_valid == 2 {
    False -> Error(IncompatibleSchema)
    True -> {
      use #(job_columns, job_columns_valid) <- result.try(inspect_columns(
        connection,
        "grind_jobs",
        "(column_name = 'id' AND data_type = 'bigint' AND is_nullable = 'NO') OR (column_name = 'storage_owner' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'queue' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'worker_id' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'worker_version' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'input_version' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'input' AND data_type = 'jsonb' AND is_nullable = 'NO') OR (column_name = 'output_version' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'output' AND data_type = 'jsonb' AND is_nullable = 'YES') OR (column_name = 'error_version' AND data_type = 'text' AND is_nullable = 'YES') OR (column_name = 'error' AND data_type = 'jsonb' AND is_nullable = 'YES') OR (column_name = 'state' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'available_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'NO') OR (column_name = 'inserted_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'NO') OR (column_name = 'attempt_id' AND data_type = 'bigint' AND is_nullable = 'YES') OR (column_name = 'attempt_epoch' AND data_type = 'bigint' AND is_nullable = 'NO') OR (column_name = 'attempt_owner' AND data_type = 'text' AND is_nullable = 'YES') OR (column_name = 'lease_expires_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'YES') OR (column_name = 'attempt_count' AND data_type = 'bigint' AND is_nullable = 'NO') OR (column_name = 'snooze_count' AND data_type = 'bigint' AND is_nullable = 'NO') OR (column_name = 'failure_description' AND data_type = 'text' AND is_nullable = 'YES') OR (column_name = 'uncertain_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'YES')",
      ))
      case job_columns == 25 && job_columns_valid == 22 {
        False -> Error(IncompatibleSchema)
        True -> {
          use _ <- result.try(validate_v6_job_columns(connection))
          use #(queue_columns, queue_columns_valid) <- result.try(
            inspect_columns(
              connection,
              "grind_queue_policies",
              "(column_name = 'storage_owner' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'queue' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'expired_attempt_policy' AND data_type = 'text' AND is_nullable = 'NO')",
            ),
          )
          case queue_columns == 3 && queue_columns_valid == 3 {
            True -> {
              use #(resolution_columns, resolution_columns_valid) <- result.try(
                inspect_columns(
                  connection,
                  "grind_job_resolutions",
                  "(column_name = 'storage_owner' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'queue' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'job_id' AND data_type = 'bigint' AND is_nullable = 'NO') OR (column_name = 'worker_id' AND data_type = 'text' AND is_nullable = 'YES') OR (column_name = 'worker_version' AND data_type = 'text' AND is_nullable = 'YES') OR (column_name = 'resolution_id' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'attempt_id' AND data_type = 'bigint' AND is_nullable = 'NO') OR (column_name = 'attempt_epoch' AND data_type = 'bigint' AND is_nullable = 'NO') OR (column_name = 'attempt_owner' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'lease_expires_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'NO') OR (column_name = 'decision' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'target_state' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'payload_version' AND data_type = 'text' AND is_nullable = 'YES') OR (column_name = 'payload' AND data_type = 'jsonb' AND is_nullable = 'YES') OR (column_name = 'resolved_by' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'details' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'resolved_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'NO')",
                ),
              )
              case resolution_columns == 17 && resolution_columns_valid == 17 {
                True -> {
                  use #(acknowledgement_columns, acknowledgement_columns_valid) <- result.try(
                    inspect_columns(
                      connection,
                      "grind_job_acknowledgements",
                      "(column_name = 'storage_owner' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'command_id' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'queue' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'job_id' AND data_type = 'bigint' AND is_nullable = 'NO') OR (column_name = 'worker_id' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'worker_version' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'attempt_id' AND data_type = 'bigint' AND is_nullable = 'NO') OR (column_name = 'attempt_epoch' AND data_type = 'bigint' AND is_nullable = 'NO') OR (column_name = 'attempt_owner' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'proposed_state' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'committed_state' AND data_type = 'text' AND is_nullable = 'NO') OR (column_name = 'output_version' AND data_type = 'text' AND is_nullable = 'YES') OR (column_name = 'output' AND data_type = 'jsonb' AND is_nullable = 'YES') OR (column_name = 'error_version' AND data_type = 'text' AND is_nullable = 'YES') OR (column_name = 'error' AND data_type = 'jsonb' AND is_nullable = 'YES') OR (column_name = 'failure_description' AND data_type = 'text' AND is_nullable = 'YES') OR (column_name = 'requested_delay_ms' AND data_type = 'bigint' AND is_nullable = 'YES') OR (column_name = 'committed_at' AND data_type = 'timestamp with time zone' AND is_nullable = 'NO')",
                    ),
                  )
                  case
                    acknowledgement_columns == 19
                    && acknowledgement_columns_valid == 18
                  {
                    True -> {
                      use _ <- result.try(validate_v6_ack_columns(connection))
                      validate_schema_contract(connection)
                    }
                    False -> Error(IncompatibleSchema)
                  }
                }
                False -> Error(IncompatibleSchema)
              }
            }
            False -> Error(IncompatibleSchema)
          }
        }
      }
    }
  }
}

fn upgrade_schema_versions(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
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
  use #(count, minimum, maximum) <- result.try(
    case execute_safely(query, on: connection) {
      Error(error) -> Error(MigrationQueryFailed(error))
      Ok(returned) ->
        case returned.rows {
          [version] -> Ok(version)
          _ -> Error(IncompatibleSchema)
        }
    },
  )
  case count, minimum, maximum {
    0, 0, 0 ->
      run_statement(
        connection,
        "INSERT INTO grind_schema_migrations (version) VALUES (1), (2), (3), (4), (5), (6)",
      )
    1, 1, 1 -> {
      use _ <- result.try(upgrade_schema_v1_to_v2(connection))
      use _ <- result.try(upgrade_schema_v2_to_v3(connection))
      use _ <- result.try(upgrade_schema_v3_to_v4(connection))
      use _ <- result.try(upgrade_schema_v4_to_v5(connection))
      upgrade_schema_v5_to_v6(connection)
    }
    2, 1, 2 -> {
      use _ <- result.try(upgrade_schema_v2_to_v3(connection))
      use _ <- result.try(upgrade_schema_v3_to_v4(connection))
      use _ <- result.try(upgrade_schema_v4_to_v5(connection))
      upgrade_schema_v5_to_v6(connection)
    }
    3, 1, 3 -> {
      use _ <- result.try(upgrade_schema_v3_to_v4(connection))
      use _ <- result.try(upgrade_schema_v4_to_v5(connection))
      upgrade_schema_v5_to_v6(connection)
    }
    4, 1, 4 -> {
      use _ <- result.try(upgrade_schema_v4_to_v5(connection))
      upgrade_schema_v5_to_v6(connection)
    }
    5, 1, 5 -> upgrade_schema_v5_to_v6(connection)
    6, 1, 6 -> Ok(Nil)
    _, _, unsupported -> Error(UnsupportedSchemaVersion(unsupported))
  }
}

fn upgrade_schema_v4_to_v5(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_jobs ADD COLUMN IF NOT EXISTS snooze_count bigint NOT NULL DEFAULT 0",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_acknowledgements ADD COLUMN IF NOT EXISTS requested_delay_ms bigint",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_job_acknowledgements_proposed_state_check",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT grind_job_acknowledgements_committed_state_check",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_proposed_state_check CHECK (proposed_state IN ('succeeded', 'business_failed', 'runtime_failed', 'snoozed', 'discarded', 'cancelled', 'uncertain'))",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_committed_state_check CHECK (committed_state IN ('succeeded', 'business_failed', 'runtime_failed', 'scheduled', 'discarded', 'cancelled', 'uncertain'))",
  ))
  run_statement(
    connection,
    "INSERT INTO grind_schema_migrations (version) VALUES (5)",
  )
}

fn validate_v6_job_columns(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
  use #(columns, valid) <- result.try(inspect_columns(
    connection,
    "grind_jobs",
    "(column_name = 'max_attempts' AND data_type = 'bigint' AND is_nullable = 'NO') OR (column_name = 'delivery_count' AND data_type = 'bigint' AND is_nullable = 'NO') OR (column_name = 'failure_cause' AND data_type = 'text' AND is_nullable = 'YES')",
  ))
  use limit_check <- result.try(exact_constraint_count(
    connection,
    "grind_jobs",
    "grind_jobs_max_attempts_check",
    "CHECK ((max_attempts > 0))",
  ))
  use max_attempts_default <- result.try(exact_column_default_count(
    connection,
    "grind_jobs",
    "max_attempts",
    "20",
  ))
  use delivery_count_default <- result.try(exact_column_default_count(
    connection,
    "grind_jobs",
    "delivery_count",
    "0",
  ))
  case
    columns == 25
    && valid == 3
    && limit_check == 1
    && max_attempts_default == 1
    && delivery_count_default == 1
  {
    True -> Ok(Nil)
    False -> Error(IncompatibleSchema)
  }
}

fn exact_column_default_count(
  connection: pog.Connection,
  table: String,
  column: String,
  expression: String,
) -> Result(Int, StorageError) {
  let query =
    pog.query(
      "SELECT count(*)::bigint FROM pg_attribute a JOIN pg_class t ON t.oid = a.attrelid JOIN pg_namespace n ON n.oid = t.relnamespace JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum WHERE n.nspname = current_schema() AND t.relname = $1 AND a.attname = $2 AND NOT a.attisdropped AND pg_get_expr(d.adbin, d.adrelid) = $3",
    )
    |> pog.parameter(pog.text(table))
    |> pog.parameter(pog.text(column))
    |> pog.parameter(pog.text(expression))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [count] -> Ok(count)
        _ -> Error(IncompatibleSchema)
      }
  }
}

fn validate_v6_ack_columns(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
  use #(columns, valid) <- result.try(inspect_columns(
    connection,
    "grind_job_acknowledgements",
    "column_name = 'failure_cause' AND data_type = 'text' AND is_nullable = 'YES'",
  ))
  case columns == 19 && valid == 1 {
    True -> Ok(Nil)
    False -> Error(IncompatibleSchema)
  }
}

fn upgrade_schema_v5_to_v6(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_jobs ADD COLUMN IF NOT EXISTS max_attempts bigint NOT NULL DEFAULT 20",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_jobs ADD COLUMN IF NOT EXISTS delivery_count bigint NOT NULL DEFAULT 0",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_jobs ADD COLUMN IF NOT EXISTS failure_cause text",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_jobs DROP CONSTRAINT IF EXISTS grind_jobs_max_attempts_check",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_jobs ADD CONSTRAINT grind_jobs_max_attempts_check CHECK (max_attempts > 0)",
  ))
  use _ <- result.try(run_statement(
    connection,
    "UPDATE grind_jobs SET delivery_count = attempt_count WHERE delivery_count = 0 AND attempt_count > 0",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_jobs DROP CONSTRAINT IF EXISTS grind_jobs_state_check",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_jobs ADD CONSTRAINT grind_jobs_state_check CHECK (state IN ('queued', 'scheduled', 'retryable', 'executing', 'succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'uncertain', 'discarded', 'cancelled'))",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_acknowledgements ADD COLUMN IF NOT EXISTS failure_cause text",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT IF EXISTS grind_job_acknowledgements_proposed_state_check",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_acknowledgements DROP CONSTRAINT IF EXISTS grind_job_acknowledgements_committed_state_check",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_proposed_state_check CHECK (proposed_state IN ('succeeded', 'business_failed', 'retryable', 'runtime_failed', 'snoozed', 'discarded', 'cancelled', 'uncertain'))",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_acknowledgements ADD CONSTRAINT grind_job_acknowledgements_committed_state_check CHECK (committed_state IN ('succeeded', 'business_failed', 'retryable', 'runtime_failed', 'scheduled', 'discarded', 'cancelled', 'uncertain'))",
  ))
  run_statement(
    connection,
    "INSERT INTO grind_schema_migrations (version) VALUES (6)",
  )
}

fn upgrade_schema_v2_to_v3(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_resolutions ADD COLUMN IF NOT EXISTS target_state text",
  ))
  use _ <- result.try(run_statement(
    connection,
    "UPDATE grind_job_resolutions SET target_state = CASE decision WHEN 'authorize_replay' THEN 'queued' WHEN 'confirm_success' THEN 'succeeded' ELSE 'business_failed' END WHERE target_state IS NULL",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_resolutions ALTER COLUMN target_state SET NOT NULL",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_resolutions ADD COLUMN IF NOT EXISTS payload_version text",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_resolutions ADD COLUMN IF NOT EXISTS payload jsonb",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_resolutions ADD COLUMN IF NOT EXISTS worker_id text",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_job_resolutions ADD COLUMN IF NOT EXISTS worker_version text",
  ))
  use _ <- result.try(run_statement(
    connection,
    "UPDATE grind_job_resolutions AS resolution SET worker_id = job.worker_id, worker_version = job.worker_version FROM grind_jobs AS job WHERE resolution.storage_owner = job.storage_owner AND resolution.queue = job.queue AND resolution.job_id = job.id AND (resolution.worker_id IS NULL OR resolution.worker_version IS NULL)",
  ))
  use _ <- result.try(run_statement(
    connection,
    "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'grind_job_resolutions_target_state_check') THEN ALTER TABLE grind_job_resolutions ADD CONSTRAINT grind_job_resolutions_target_state_check CHECK (target_state IN ('queued', 'succeeded', 'business_failed')); END IF; END $$",
  ))
  run_statement(
    connection,
    "INSERT INTO grind_schema_migrations (version) VALUES (3)",
  )
}

fn upgrade_schema_v3_to_v4(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
  run_statement(
    connection,
    "INSERT INTO grind_schema_migrations (version) VALUES (4)",
  )
}

fn upgrade_schema_v1_to_v2(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
  let expected_v1_state_check =
    "CHECK ((state = ANY (ARRAY['queued'::text, 'scheduled'::text, 'executing'::text, 'succeeded'::text, 'business_failed'::text, 'runtime_failed'::text, 'contract_mismatch'::text, 'discarded'::text, 'cancelled'::text])))"
  let query =
    pog.query(
      "SELECT (SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'grind_jobs')::bigint, (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_jobs' AND c.conname = 'grind_jobs_state_check' AND c.contype = 'c' AND c.convalidated AND pg_get_constraintdef(c.oid) = $1)::bigint",
    )
    |> pog.parameter(pog.text(expected_v1_state_check))
    |> pog.returning({
      use columns <- decode.field(0, decode.int)
      use state_check <- decode.field(1, decode.int)
      decode.success(#(columns, state_check))
    })
  use _ <- result.try(case execute_safely(query, on: connection) {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [#(20, 1)] -> Ok(Nil)
        _ -> Error(IncompatibleSchema)
      }
  })
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_jobs ADD COLUMN IF NOT EXISTS uncertain_at timestamptz",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_jobs DROP CONSTRAINT grind_jobs_state_check",
  ))
  use _ <- result.try(run_statement(
    connection,
    "ALTER TABLE grind_jobs ADD CONSTRAINT grind_jobs_state_check CHECK (state IN ('queued', 'scheduled', 'executing', 'succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'uncertain', 'discarded', 'cancelled'))",
  ))
  run_statement(
    connection,
    "INSERT INTO grind_schema_migrations (version) VALUES (2)",
  )
}

fn run_statement(
  connection: pog.Connection,
  sql: String,
) -> Result(Nil, StorageError) {
  case execute_safely(pog.query(sql), on: connection) {
    Ok(_) -> Ok(Nil)
    Error(error) -> Error(MigrationQueryFailed(error))
  }
}

fn inspect_columns(
  connection: pog.Connection,
  table: String,
  valid_column_condition: String,
) -> Result(#(Int, Int), StorageError) {
  let sql =
    "SELECT count(*)::bigint, count(*) FILTER (WHERE "
    <> valid_column_condition
    <> ")::bigint FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1"
  let query =
    pog.query(sql)
    |> pog.parameter(pog.text(table))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      use valid <- decode.field(1, decode.int)
      decode.success(#(count, valid))
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [counts] -> Ok(counts)
        _ -> Error(IncompatibleSchema)
      }
  }
}

fn ensure_schema_version(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
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
  use #(count, minimum, maximum) <- result.try(
    case execute_safely(query, on: connection) {
      Error(error) -> Error(MigrationQueryFailed(error))
      Ok(returned) ->
        case returned.rows {
          [version] -> Ok(version)
          _ -> Error(IncompatibleSchema)
        }
    },
  )
  case count, minimum, maximum {
    0, 0, 0 ->
      run_statement(
        connection,
        "INSERT INTO grind_schema_migrations (version) VALUES (1), (2), (3), (4), (5), (6)",
      )
    6, 1, 6 -> Ok(Nil)
    _, _, unsupported -> Error(UnsupportedSchemaVersion(unsupported))
  }
}

fn validate_schema_contract(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
  let expected_state_check =
    "CHECK ((state = ANY (ARRAY['queued'::text, 'scheduled'::text, 'retryable'::text, 'executing'::text, 'succeeded'::text, 'business_failed'::text, 'runtime_failed'::text, 'contract_mismatch'::text, 'uncertain'::text, 'discarded'::text, 'cancelled'::text])))"
  let expected_queue_policy_check =
    "CHECK ((expired_attempt_policy = ANY (ARRAY['require_reconciliation'::text, 'replay_at_least_once'::text])))"
  let expected_resolution_check =
    "CHECK ((decision = ANY (ARRAY['confirm_success'::text, 'confirm_business_failure'::text, 'authorize_replay'::text])))"
  let expected_resolution_target_check =
    "CHECK ((target_state = ANY (ARRAY['queued'::text, 'succeeded'::text, 'business_failed'::text])))"
  let expected_ack_state_check =
    "CHECK ((proposed_state = ANY (ARRAY['succeeded'::text, 'business_failed'::text, 'retryable'::text, 'runtime_failed'::text, 'snoozed'::text, 'discarded'::text, 'cancelled'::text, 'uncertain'::text])))"
  let expected_ack_committed_check =
    "CHECK ((committed_state = ANY (ARRAY['succeeded'::text, 'business_failed'::text, 'retryable'::text, 'runtime_failed'::text, 'scheduled'::text, 'discarded'::text, 'cancelled'::text, 'uncertain'::text])))"
  use resolution_target_check <- result.try(exact_constraint_count(
    connection,
    "grind_job_resolutions",
    "grind_job_resolutions_target_state_check",
    expected_resolution_target_check,
  ))
  use acknowledgement_proposed_check <- result.try(exact_constraint_count(
    connection,
    "grind_job_acknowledgements",
    "grind_job_acknowledgements_proposed_state_check",
    expected_ack_state_check,
  ))
  use acknowledgement_committed_check <- result.try(exact_constraint_count(
    connection,
    "grind_job_acknowledgements",
    "grind_job_acknowledgements_committed_state_check",
    expected_ack_committed_check,
  ))
  let query =
    pog.query(
      "SELECT (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_jobs' AND c.contype = 'p' AND pg_get_constraintdef(c.oid) = 'PRIMARY KEY (id)')::bigint, (SELECT count(*) FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'grind_jobs' AND column_name = 'id' AND column_default = 'nextval(''grind_jobs_id_seq''::regclass)')::bigint, (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_jobs' AND c.conname = 'grind_jobs_state_check' AND c.contype = 'c' AND c.convalidated AND pg_get_constraintdef(c.oid) = $1)::bigint, (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_sequence s ON s.seqrelid = c.oid WHERE n.nspname = current_schema() AND c.relname = 'grind_attempts_id_seq' AND c.relkind = 'S' AND s.seqtypid = 'bigint'::regtype AND s.seqstart = 1 AND s.seqincrement = 1 AND s.seqmin = 1 AND s.seqcache = 1 AND NOT s.seqcycle)::bigint, (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_schema_migrations' AND c.contype = 'p' AND pg_get_constraintdef(c.oid) = 'PRIMARY KEY (version)')::bigint, (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_jobs' AND c.contype IN ('p', 'c'))::bigint, (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_schema_migrations' AND c.contype IN ('p', 'c'))::bigint, (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_queue_policies' AND c.contype = 'p' AND pg_get_constraintdef(c.oid) = 'PRIMARY KEY (storage_owner, queue)')::bigint, (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_queue_policies' AND c.conname = 'grind_queue_policies_expiry_policy_check' AND c.contype = 'c' AND c.convalidated AND pg_get_constraintdef(c.oid) = $2)::bigint, (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_queue_policies' AND c.contype IN ('p', 'c'))::bigint, (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_job_resolutions' AND c.contype = 'p' AND pg_get_constraintdef(c.oid) = 'PRIMARY KEY (storage_owner, resolution_id)')::bigint, (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_job_resolutions' AND c.conname = 'grind_job_resolutions_decision_check' AND c.contype = 'c' AND c.convalidated AND pg_get_constraintdef(c.oid) = $3)::bigint, (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_job_resolutions' AND c.contype IN ('p', 'c'))::bigint",
    )
    |> pog.parameter(pog.text(expected_state_check))
    |> pog.parameter(pog.text(expected_queue_policy_check))
    |> pog.parameter(pog.text(expected_resolution_check))
    |> pog.returning({
      use primary_key <- decode.field(0, decode.int)
      use id_default <- decode.field(1, decode.int)
      use state_check <- decode.field(2, decode.int)
      use attempt_sequence <- decode.field(3, decode.int)
      use migration_key <- decode.field(4, decode.int)
      use jobs_constraints <- decode.field(5, decode.int)
      use migration_constraints <- decode.field(6, decode.int)
      use queue_policy_key <- decode.field(7, decode.int)
      use queue_policy_check <- decode.field(8, decode.int)
      use queue_policy_constraints <- decode.field(9, decode.int)
      use resolution_key <- decode.field(10, decode.int)
      use resolution_check <- decode.field(11, decode.int)
      use resolution_constraints <- decode.field(12, decode.int)
      decode.success(#(
        primary_key,
        id_default,
        state_check,
        attempt_sequence,
        migration_key,
        jobs_constraints,
        migration_constraints,
        queue_policy_key,
        queue_policy_check,
        queue_policy_constraints,
        resolution_key,
        resolution_check,
        resolution_constraints,
      ))
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [#(1, 1, 1, 1, 1, 3, 1, 1, 1, 2, 1, 1, 3)] ->
          case resolution_target_check == 1 {
            True ->
              case
                acknowledgement_proposed_check == 1
                && acknowledgement_committed_check == 1
              {
                True -> validate_acknowledgement_constraints(connection)
                False -> Error(IncompatibleSchema)
              }
            False -> Error(IncompatibleSchema)
          }
        _ -> Error(IncompatibleSchema)
      }
  }
}

fn validate_acknowledgement_constraints(
  connection: pog.Connection,
) -> Result(Nil, StorageError) {
  let query =
    pog.query(
      "SELECT (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_job_acknowledgements' AND c.contype = 'p' AND pg_get_constraintdef(c.oid) = 'PRIMARY KEY (storage_owner, command_id)')::bigint, (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_job_acknowledgements' AND c.contype = 'u' AND pg_get_constraintdef(c.oid) = 'UNIQUE (storage_owner, job_id, attempt_id, attempt_epoch)')::bigint, (SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = 'grind_job_acknowledgements' AND c.contype IN ('p', 'u', 'c'))::bigint",
    )
    |> pog.returning({
      use primary_key <- decode.field(0, decode.int)
      use attempt_key <- decode.field(1, decode.int)
      use constraints <- decode.field(2, decode.int)
      decode.success(#(primary_key, attempt_key, constraints))
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [#(1, 1, 4)] -> ensure_schema_version(connection)
        _ -> Error(IncompatibleSchema)
      }
  }
}

fn exact_constraint_count(
  connection: pog.Connection,
  table: String,
  name: String,
  definition: String,
) -> Result(Int, StorageError) {
  let query =
    pog.query(
      "SELECT count(*)::bigint FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = current_schema() AND t.relname = $1 AND c.conname = $2 AND c.contype = 'c' AND c.convalidated AND pg_get_constraintdef(c.oid) = $3",
    )
    |> pog.parameter(pog.text(table))
    |> pog.parameter(pog.text(name))
    |> pog.parameter(pog.text(definition))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(MigrationQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [count] -> Ok(count)
        _ -> Error(IncompatibleSchema)
      }
  }
}

pub type SubmitError {
  EmptyQueueName
  NegativeAvailability
  UnexpectedInsertRows
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
      let Database(connection:, storage_owner:, ..) = database
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
        <> "CASE WHEN $10::bigint IS NULL THEN clock_timestamp() ELSE to_timestamp($10::double precision / 1000.0) END) RETURNING id"
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
          decode.success(id)
        })
      case execute_safely(query, on: connection) {
        Error(error) -> Error(SubmitQueryFailed(error))
        Ok(returned) ->
          case returned.rows {
            [id] -> Ok(job.new_handle(id, storage_owner, queue, worker))
            _ -> Error(UnexpectedInsertRows)
          }
      }
    }
  }
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
  let query =
    pog.query(
      "SELECT queue, worker_id, worker_version, input_version, output_version, error_version FROM grind_jobs WHERE id = $1 AND storage_owner = $2",
    )
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(storage_owner))
    |> pog.returning({
      use queue <- decode.field(0, decode.string)
      use stored_worker_id <- decode.field(1, decode.string)
      use stored_worker_version <- decode.field(2, decode.string)
      use stored_input_version <- decode.field(3, decode.string)
      use stored_output_version <- decode.field(4, decode.string)
      use stored_error_version <- decode.field(
        5,
        decode.optional(decode.string),
      )
      decode.success(#(
        queue,
        stored_worker_id,
        stored_worker_version,
        stored_input_version,
        stored_output_version,
        stored_error_version,
      ))
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(HandleBindQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Error(HandleBindNotFound)
        [
          #(
            queue,
            stored_worker_id,
            stored_worker_version,
            stored_input_version,
            stored_output_version,
            stored_error_version,
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
  let query =
    pog.query(
      "SELECT input::text, input_version, storage_owner, queue, worker_id, worker_version FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use encoded <- decode.field(0, decode.string)
      use codec_version <- decode.field(1, decode.string)
      use stored_owner <- decode.field(2, decode.string)
      use stored_queue <- decode.field(3, decode.string)
      use stored_worker <- decode.field(4, decode.string)
      use stored_worker_version <- decode.field(5, decode.string)
      decode.success(#(
        encoded,
        codec_version,
        stored_owner,
        stored_queue,
        stored_worker,
        stored_worker_version,
      ))
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(ArgumentQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Error(JobNotFound)
        [
          #(
            encoded,
            codec_version,
            stored_owner,
            stored_queue,
            stored_worker,
            stored_worker_version,
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

/// Failures returned while running one claimed job. `QueueCodecMismatch` is
/// returned after the row is durably marked `ContractMismatch`; it is an error
/// to the batch caller but a committed job disposition.
pub type QueueRunError {
  QueueClaimFailed(pog.QueryError)
  QueueClaimReleaseFailed(pog.QueryError)
  QueueClaimReleaseRejected
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

pub type AcknowledgementReceipt(output, error) {
  AcknowledgementReceipt(
    command_id: String,
    attempt_id: Int,
    attempt_epoch: Int,
    outcome: job.Outcome(output, error),
  )
}

pub type AckReconciliationError {
  AckReceiptQueryFailed(pog.QueryError)
  AckReceiptNotFound
  AckReceiptStorageOwnerMismatch
  AckReceiptRouteMismatch
  AckReceiptWorkerContractMismatch
  AckReceiptPayloadMismatch
  AckReceiptCodecFailed(worker.StoredCodecError)
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
    business_attempt_charged: Bool,
    previous_state: String,
  )
}

type AckProposal {
  AckProposal(
    proposed_state: String,
    committed_state: String,
    failure_cause: Option(String),
    requested_delay_ms: Option(Int),
    output_version: Option(String),
    output: Option(String),
    error_version: Option(String),
    error: Option(String),
    failure_description: Option(String),
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
  replay_expired: Bool,
  lease_duration_ms: Int,
) -> Result(Option(ClaimedJob), QueueRunError) {
  let identities = registry.identities(workers)
  case replay_expired {
    True ->
      claim_registered_job(
        database,
        queue,
        workers,
        attempt_owner,
        identities,
        True,
        lease_duration_ms,
      )
    False ->
      case quarantine_expired(database, queue, identities) {
        Error(error) -> Error(error)
        Ok(Nil) ->
          claim_registered_job(
            database,
            queue,
            workers,
            attempt_owner,
            identities,
            False,
            lease_duration_ms,
          )
      }
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

fn claim_business_attempt_charged(claim: Claim) -> Bool {
  let Claim(business_attempt_charged:, ..) = claim
  business_attempt_charged
}

fn claim_previous_state(claim: Claim) -> String {
  let Claim(previous_state:, ..) = claim
  previous_state
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
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() + ($7::double precision * interval '1 millisecond') WHERE id = $1 AND storage_owner = $2 AND queue = $3 AND state = 'executing' AND attempt_id = $4 AND attempt_epoch = $5 AND attempt_owner = $6 AND lease_expires_at > clock_timestamp() RETURNING id",
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
/// This is used only when the temporary worker child failed to start before
/// receiving `StartAttempt`, so it is safe to refund the uninvoked attempt.
/// An unstarted replay of an expired executing row keeps its new fenced owner
/// and records that the earlier external effect remains unconfirmed. The
/// expired row can then be quarantined and audited instead of becoming an
/// ownerless reconciliation sink.
@internal
pub fn release_unstarted_claim(
  database: Database,
  queue: String,
  attempt_owner: String,
  claimed: ClaimedJob,
) -> Result(Bool, QueueRunError) {
  let Database(connection:, storage_owner:, ..) = database
  let ClaimedJob(claim: claim, ..) = claimed
  let Claim(id:, attempt_id:, epoch:, ..) = claim
  let query =
    pog.query(
      "UPDATE grind_jobs SET state = CASE WHEN $7 = 'executing' THEN 'executing' ELSE $7 END, attempt_epoch = attempt_epoch + 1, attempt_owner = CASE WHEN $7 = 'executing' THEN $6 ELSE NULL END, lease_expires_at = CASE WHEN $7 = 'executing' THEN clock_timestamp() ELSE NULL END, failure_description = CASE WHEN $7 = 'executing' THEN 'Unstarted replay claim; prior effect remains unconfirmed' ELSE failure_description END, attempt_count = CASE WHEN $8 THEN GREATEST(attempt_count - 1, 0) ELSE attempt_count END WHERE id = $1 AND storage_owner = $2 AND queue = $3 AND state = 'executing' AND attempt_id = $4 AND attempt_epoch = $5 AND attempt_owner = $6 RETURNING id",
    )
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text(storage_owner))
    |> pog.parameter(pog.text(queue))
    |> pog.parameter(pog.int(attempt_id))
    |> pog.parameter(pog.int(epoch))
    |> pog.parameter(pog.text(attempt_owner))
    |> pog.parameter(pog.text(claim_previous_state(claim)))
    |> pog.parameter(pog.bool(claim_business_attempt_charged(claim)))
    |> pog.returning({
      use returned_id <- decode.field(0, decode.int)
      decode.success(returned_id)
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(QueueClaimReleaseFailed(error))
    Ok(returned) ->
      case returned.rows {
        [_] -> Ok(True)
        [] -> Ok(False)
        _ -> Error(QueueClaimReleaseRejected)
      }
  }
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
  acknowledge_claim_with_reply_mode(
    database,
    queue,
    attempt_owner,
    claimed,
    execution,
    False,
  )
}

fn acknowledge_claim_with_reply_mode(
  database: Database,
  queue: String,
  attempt_owner: String,
  claimed: ClaimedJob,
  execution: worker.Execution,
  simulate_lost_reply: Bool,
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
            simulate_lost_reply,
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
            simulate_lost_reply,
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
            simulate_lost_reply,
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
        simulate_lost_reply,
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
        simulate_lost_reply,
      )
  }
}

/// Test seam that injects a lost transaction reply and unavailable immediate
/// reconciliation at the production result boundary after PostgreSQL commits.
@internal
pub fn acknowledge_claim_with_lost_reply_after_commit(
  database: Database,
  queue: String,
  attempt_owner: String,
  claimed: ClaimedJob,
  execution: worker.Execution,
) -> Result(Bool, QueueRunError) {
  acknowledge_claim_with_reply_mode(
    database,
    queue,
    attempt_owner,
    claimed,
    execution,
    True,
  )
}

/// Compatibility stepping API retained for internal tests while callers move
/// to the queue-owned claim/run/renew/ack protocol.
@internal
pub fn process_one(
  database: Database,
  queue: String,
  workers: Registry,
  attempt_owner: String,
  replay_expired: Bool,
) -> Result(Bool, QueueRunError) {
  case
    claim_one(database, queue, workers, attempt_owner, replay_expired, 30_000)
  {
    Error(error) -> Error(error)
    Ok(None) -> Ok(False)
    Ok(Some(claimed)) -> {
      let execution = execute_claim(claimed)
      acknowledge_claim(database, queue, attempt_owner, claimed, execution)
    }
  }
}

fn claim_registered_job(
  database: Database,
  queue: String,
  workers: Registry,
  attempt_owner: String,
  identities: List(#(String, String)),
  replay_expired: Bool,
  lease_duration_ms: Int,
) -> Result(Option(ClaimedJob), QueueRunError) {
  let Database(connection:, storage_owner:, ..) = database
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
  let eligible_state = case replay_expired {
    True ->
      "((state IN ('queued', 'scheduled', 'retryable') AND available_at <= clock_timestamp()) OR (state = 'executing' AND lease_expires_at <= clock_timestamp()))"
    False ->
      "(state IN ('queued', 'scheduled', 'retryable') AND available_at <= clock_timestamp())"
  }
  let sql =
    "WITH candidate AS (SELECT id, state AS previous_state FROM grind_jobs WHERE storage_owner = $1 AND queue = $2 AND "
    <> eligible_state
    <> " AND ("
    <> eligibility
    <> ") ORDER BY available_at, id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE grind_jobs AS job SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = job.attempt_epoch + 1, attempt_owner = $3, lease_expires_at = clock_timestamp() + ($4::double precision * interval '1 millisecond'), attempt_count = job.attempt_count + CASE WHEN candidate.previous_state IN ('queued', 'scheduled', 'retryable') THEN 1 ELSE 0 END, delivery_count = job.delivery_count + 1 FROM candidate WHERE job.id = candidate.id RETURNING job.id, job.attempt_id, job.attempt_epoch, job.input_version, job.input::text, job.worker_id, job.worker_version, job.output_version, job.error_version, job.attempt_count, job.max_attempts, job.snooze_count, job.delivery_count, candidate.previous_state IN ('queued', 'scheduled', 'retryable'), candidate.previous_state"
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
      use business_attempt_charged <- decode.field(13, decode.bool)
      use previous_state <- decode.field(14, decode.string)
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
        business_attempt_charged:,
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
            input_version:,
            encoded_input:,
            worker_id:,
            worker_version:,
            output_version:,
            error_version:,
            current_attempt:,
            max_attempts:,
            snooze_count:,
            ..,
          ) = claim
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

fn quarantine_expired(
  database: Database,
  queue: String,
  identities: List(#(String, String)),
) -> Result(Nil, QueueRunError) {
  case identities {
    [] -> Ok(Nil)
    _ -> {
      let Database(connection:, storage_owner:, ..) = database
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
        "WITH candidate AS (SELECT id FROM grind_jobs WHERE storage_owner = $1 AND queue = $2 AND state = 'executing' AND lease_expires_at <= clock_timestamp() AND ("
        <> eligibility
        <> ") ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE grind_jobs AS job SET state = 'uncertain', failure_description = CASE WHEN job.failure_description IS NULL THEN 'expired attempt requires outcome reconciliation' ELSE job.failure_description || '; expired attempt requires outcome reconciliation' END, uncertain_at = clock_timestamp() FROM candidate WHERE job.id = candidate.id"
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
      case execute_safely(query, on: connection) {
        Error(error) -> Error(QueueClaimFailed(error))
        Ok(_) -> Ok(Nil)
      }
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
  let Database(connection:, storage_owner:, ..) = database
  let Claim(id:, attempt_id:, epoch:, ..) = claim
  let query =
    pog.query(
      "UPDATE grind_jobs SET state = 'contract_mismatch', failure_description = $7, attempt_id = NULL, attempt_owner = NULL, lease_expires_at = NULL, attempt_count = CASE WHEN $8 THEN GREATEST(attempt_count - 1, 0) ELSE attempt_count END WHERE id = $1 AND storage_owner = $2 AND queue = $3 AND state = 'executing' AND attempt_id = $4 AND attempt_epoch = $5 AND attempt_owner = $6 AND lease_expires_at > clock_timestamp() RETURNING id",
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
    |> pog.parameter(pog.bool(claim_business_attempt_charged(claim)))
    |> pog.returning({
      use id <- decode.field(0, decode.int)
      decode.success(id)
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(QueueAckFailed(error))
    Ok(returned) ->
      case returned.rows {
        [_] -> Ok(True)
        _ -> Error(QueueAckRejected)
      }
  }
}

fn acknowledge(
  database: Database,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  execution: worker.Execution,
  expected_output_version: String,
  simulate_lost_reply: Bool,
) -> Result(Bool, QueueRunError) {
  let Database(connection:, storage_owner:, ..) = database
  let Claim(id:, attempt_id:, epoch:, ..) = claim
  let proposal = case execution {
    worker.ExecutedSuccess(version, encoded) ->
      AckProposal(
        proposed_state: "succeeded",
        committed_state: "succeeded",
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
        committed_state: "business_failed",
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
        committed_state: "retryable",
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
        committed_state: "runtime_failed",
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
        committed_state: "scheduled",
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
        committed_state: "discarded",
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
        committed_state: "cancelled",
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
        committed_state: "uncertain",
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
    simulate_lost_reply,
  )
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
  transaction_result: Result(Bool, pog.TransactionError(QueueRunError)),
  simulate_lost_reply: Bool,
) -> Result(Bool, QueueRunError) {
  case transaction_result {
    Ok(result) ->
      case result, simulate_lost_reply {
        True, True ->
          reconcile_unknown_ack(
            connection,
            storage_owner,
            queue,
            attempt_owner,
            claim,
            command_id,
            proposal,
            execution,
            False,
          )
        _, _ -> Ok(result)
      }
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
        True,
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
  receipt_lookup_available: Bool,
) -> Result(Bool, QueueRunError) {
  case receipt_lookup_available {
    False -> Error(QueueAckUnknown(command_id, execution))
    True ->
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
        Ok(Some(True)) -> Ok(True)
        Ok(Some(False)) -> Error(QueueAckCommandConflict)
        Ok(None) | Error(_) -> Error(QueueAckUnknown(command_id, execution))
      }
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
) -> Result(Bool, QueueRunError) {
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
    Ok(Some(True)) -> Ok(True)
    Ok(Some(False)) -> Error(QueueAckCommandConflict)
    Ok(None) -> {
      let Claim(
        id:,
        attempt_id:,
        epoch:,
        error_version:,
        business_attempt_charged:,
        ..,
      ) = claim
      let AckProposal(
        proposed_state:,
        committed_state: _,
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
          "UPDATE grind_jobs SET state = 'succeeded', output = $1::jsonb, error = NULL, error_version = NULL, failure_description = NULL, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND output_version = $8 AND lease_expires_at > clock_timestamp() RETURNING id",
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
          "UPDATE grind_jobs SET state = 'business_failed', output = NULL, error = $1::jsonb, error_version = $2, failure_description = $3, failure_cause = $4, attempt_owner = NULL, lease_expires_at = NULL WHERE id = $5 AND storage_owner = $6 AND queue = $7 AND state = 'executing' AND attempt_id = $8 AND attempt_epoch = $9 AND attempt_owner = $10 AND error_version IS NOT DISTINCT FROM $11 AND lease_expires_at > clock_timestamp() AND (($4 = 'budget_exhausted' AND attempt_count >= max_attempts) OR ($4 = 'retry_declined' AND attempt_count < max_attempts)) RETURNING id",
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
          "UPDATE grind_jobs SET state = 'retryable', available_at = clock_timestamp() + ($1::double precision * interval '1 millisecond'), output = NULL, error = $2::jsonb, error_version = $3, failure_description = $4, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL WHERE id = $5 AND storage_owner = $6 AND queue = $7 AND state = 'executing' AND attempt_id = $8 AND attempt_epoch = $9 AND attempt_owner = $10 AND error_version IS NOT DISTINCT FROM $11 AND attempt_count < max_attempts AND lease_expires_at > clock_timestamp() RETURNING id",
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
          "UPDATE grind_jobs SET state = 'scheduled', available_at = clock_timestamp() + ($1::double precision * interval '1 millisecond'), snooze_count = snooze_count + 1, attempt_count = CASE WHEN $9 THEN GREATEST(attempt_count - 1, 0) ELSE attempt_count END, output = NULL, error = NULL, error_version = NULL, failure_description = $2, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL WHERE id = $3 AND storage_owner = $4 AND queue = $5 AND state = 'executing' AND attempt_id = $6 AND attempt_epoch = $7 AND attempt_owner = $8 AND lease_expires_at > clock_timestamp() RETURNING id",
          [
            pog.nullable(pog.int, requested_delay_ms),
            pog.nullable(pog.text, failure_description),
            pog.int(id),
            pog.text(storage_owner),
            pog.text(queue),
            pog.int(attempt_id),
            pog.int(epoch),
            pog.text(attempt_owner),
            pog.bool(business_attempt_charged),
          ],
        )
        "discarded" -> #(
          "UPDATE grind_jobs SET state = 'discarded', output = NULL, error = NULL, error_version = NULL, failure_description = $1, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND lease_expires_at > clock_timestamp() RETURNING id",
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
          "UPDATE grind_jobs SET state = 'cancelled', output = NULL, error = NULL, error_version = NULL, failure_description = $1, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND lease_expires_at > clock_timestamp() RETURNING id",
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
          "UPDATE grind_jobs SET state = 'uncertain', failure_description = $1, uncertain_at = clock_timestamp() WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND lease_expires_at > clock_timestamp() RETURNING id",
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
          "UPDATE grind_jobs SET state = 'runtime_failed', output = NULL, error = NULL, error_version = NULL, failure_description = $1, failure_cause = NULL, attempt_owner = NULL, lease_expires_at = NULL WHERE id = $2 AND storage_owner = $3 AND queue = $4 AND state = 'executing' AND attempt_id = $5 AND attempt_epoch = $6 AND attempt_owner = $7 AND lease_expires_at > clock_timestamp() RETURNING id",
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
          decode.success(updated_id)
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
                Ok(Some(True)) -> Ok(True)
                Ok(Some(False)) -> Error(QueueAckCommandConflict)
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
            [_] ->
              insert_acknowledgement(
                connection,
                storage_owner,
                queue,
                attempt_owner,
                claim,
                command_id,
                proposal,
              )
            _ -> Error(QueueAckRejected)
          }
      }
    }
  }
}

fn current_ack_rejection(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  execution: worker.Execution,
) -> Result(Bool, QueueRunError) {
  let Claim(id:, attempt_id:, epoch:, ..) = claim
  let query =
    pog.query(
      "SELECT state, attempt_id, attempt_epoch, attempt_owner, lease_expires_at > clock_timestamp() FROM grind_jobs WHERE id = $1 AND storage_owner = $2 AND queue = $3",
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

fn acknowledgement_command_id(
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

fn matching_acknowledgement(
  connection: pog.Connection,
  storage_owner: String,
  queue: String,
  attempt_owner: String,
  claim: Claim,
  command_id: String,
  proposal: AckProposal,
) -> Result(Option(Bool), QueueRunError) {
  let Claim(id:, attempt_id:, epoch:, worker_id:, worker_version:, ..) = claim
  let AckProposal(
    proposed_state:,
    committed_state:,
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
      "SELECT storage_owner = $1 AND command_id = $2 AND queue = $3 AND job_id = $4 AND worker_id = $5 AND worker_version = $6 AND attempt_id = $7 AND attempt_epoch = $8 AND attempt_owner = $9 AND proposed_state = $10 AND committed_state = $11 AND requested_delay_ms IS NOT DISTINCT FROM $12 AND output_version IS NOT DISTINCT FROM $13 AND output IS NOT DISTINCT FROM $14::jsonb AND error_version IS NOT DISTINCT FROM $15 AND error IS NOT DISTINCT FROM $16::jsonb AND failure_description IS NOT DISTINCT FROM $17 AND failure_cause IS NOT DISTINCT FROM $18 FROM grind_job_acknowledgements WHERE storage_owner = $1 AND command_id = $2",
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
    |> pog.parameter(pog.text(committed_state))
    |> pog.parameter(pog.nullable(pog.int, requested_delay_ms))
    |> pog.parameter(pog.nullable(pog.text, output_version))
    |> pog.parameter(pog.nullable(pog.text, output))
    |> pog.parameter(pog.nullable(pog.text, error_version))
    |> pog.parameter(pog.nullable(pog.text, error))
    |> pog.parameter(pog.nullable(pog.text, failure_description))
    |> pog.parameter(pog.nullable(pog.text, failure_cause))
    |> pog.returning({
      use matches <- decode.field(0, decode.bool)
      decode.success(matches)
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(QueueAckFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Ok(None)
        [matches] -> Ok(Some(matches))
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
) -> Result(Bool, QueueRunError) {
  let Claim(id:, attempt_id:, epoch:, worker_id:, worker_version:, ..) = claim
  let AckProposal(
    proposed_state:,
    committed_state:,
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
      "INSERT INTO grind_job_acknowledgements (storage_owner, command_id, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, attempt_owner, proposed_state, committed_state, output_version, output, error_version, error, failure_description, requested_delay_ms, failure_cause) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14, $15::jsonb, $16, $17, $18) RETURNING command_id",
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
    |> pog.parameter(pog.text(committed_state))
    |> pog.parameter(pog.nullable(pog.text, output_version))
    |> pog.parameter(pog.nullable(pog.text, output))
    |> pog.parameter(pog.nullable(pog.text, error_version))
    |> pog.parameter(pog.nullable(pog.text, error))
    |> pog.parameter(pog.nullable(pog.text, failure_description))
    |> pog.parameter(pog.nullable(pog.int, requested_delay_ms))
    |> pog.parameter(pog.nullable(pog.text, failure_cause))
    |> pog.returning({
      use inserted_command_id <- decode.field(0, decode.string)
      decode.success(inserted_command_id)
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(QueueAckFailed(error))
    Ok(returned) ->
      case returned.rows {
        [_] -> Ok(True)
        _ -> Error(QueueAckRejected)
      }
  }
}

/// Reads the committed admission state.
pub fn state(
  database: Database,
  handle: JobHandle(input, output, error),
) -> Result(State, StateError) {
  let Database(connection:, storage_owner:, ..) = database
  let #(id, handle_owner, handle_queue, worker_id, worker_version, _) =
    job.storage_fields(handle)
  let query =
    pog.query(
      "SELECT storage_owner, queue, worker_id, worker_version, state FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use stored_owner <- decode.field(0, decode.string)
      use stored_queue <- decode.field(1, decode.string)
      use stored_worker <- decode.field(2, decode.string)
      use stored_worker_version <- decode.field(3, decode.string)
      use state <- decode.field(4, decode.string)
      decode.success(#(
        stored_owner,
        stored_queue,
        stored_worker,
        stored_worker_version,
        state,
      ))
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(StateQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Error(StateJobNotFound)
        [
          #(
            stored_owner,
            stored_queue,
            stored_worker,
            stored_worker_version,
            state,
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
                      case state {
                        "queued" -> Ok(Queued)
                        "scheduled" -> Ok(Scheduled)
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
  let query =
    pog.query(
      "SELECT storage_owner, queue, worker_id, worker_version, state, output::text, output_version, error::text, error_version, failure_description, failure_cause FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use stored_owner <- decode.field(0, decode.string)
      use stored_queue <- decode.field(1, decode.string)
      use stored_worker <- decode.field(2, decode.string)
      use stored_worker_version <- decode.field(3, decode.string)
      use state <- decode.field(4, decode.string)
      use encoded_output <- decode.field(5, decode.optional(decode.string))
      use output_version <- decode.field(6, decode.string)
      use encoded_error <- decode.field(7, decode.optional(decode.string))
      use error_version <- decode.field(8, decode.optional(decode.string))
      use failure_description <- decode.field(9, decode.optional(decode.string))
      use failure_cause <- decode.field(10, decode.optional(decode.string))
      decode.success(#(
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
      ))
    })
  case execute_safely(query, on: connection) {
    Error(error) -> Error(OutcomeQueryFailed(error))
    Ok(returned) ->
      case returned.rows {
        [] -> Error(OutcomeJobNotFound)
        [stored] ->
          outcome_from_row(
            storage_owner,
            handle_owner,
            handle_queue,
            worker_id,
            worker_version,
            output_codec,
            error_codec,
            stored,
          )
        _ -> Error(OutcomeJobNotFound)
      }
  }
}

/// Reads an acknowledgement receipt by its stable command ID and decodes the
/// committed result with the codecs retained by this typed handle.
pub fn reconcile_acknowledgement(
  database: Database,
  handle: JobHandle(input, output, error),
  command_id: String,
) -> Result(AcknowledgementReceipt(output, error), AckReconciliationError) {
  let Database(connection:, storage_owner: database_owner, ..) = database
  let #(id, handle_owner, queue, worker_id, worker_version, _) =
    job.storage_fields(handle)
  let #(_, _, _, _, _, output_codec, error_codec) = job.result_fields(handle)
  case handle_owner == database_owner {
    False -> Error(AckReceiptStorageOwnerMismatch)
    True -> {
      let query =
        pog.query(
          "SELECT storage_owner, queue, job_id, worker_id, worker_version, attempt_id, attempt_epoch, proposed_state, committed_state, output::text, output_version, error::text, error_version, failure_description, failure_cause FROM grind_job_acknowledgements WHERE storage_owner = $1 AND command_id = $2",
        )
        |> pog.parameter(pog.text(database_owner))
        |> pog.parameter(pog.text(command_id))
        |> pog.returning({
          use stored_owner <- decode.field(0, decode.string)
          use stored_queue <- decode.field(1, decode.string)
          use stored_id <- decode.field(2, decode.int)
          use stored_worker <- decode.field(3, decode.string)
          use stored_worker_version <- decode.field(4, decode.string)
          use attempt_id <- decode.field(5, decode.int)
          use attempt_epoch <- decode.field(6, decode.int)
          use proposed_state <- decode.field(7, decode.string)
          use committed_state <- decode.field(8, decode.string)
          use encoded_output <- decode.field(9, decode.optional(decode.string))
          use output_version <- decode.field(10, decode.optional(decode.string))
          use encoded_error <- decode.field(11, decode.optional(decode.string))
          use error_version <- decode.field(12, decode.optional(decode.string))
          use failure_description <- decode.field(
            13,
            decode.optional(decode.string),
          )
          use failure_cause <- decode.field(14, decode.optional(decode.string))
          decode.success(#(
            stored_owner,
            stored_queue,
            stored_id,
            stored_worker,
            stored_worker_version,
            attempt_id,
            attempt_epoch,
            proposed_state,
            committed_state,
            encoded_output,
            output_version,
            encoded_error,
            error_version,
            failure_description,
            failure_cause,
          ))
        })
      case execute_safely(query, on: connection) {
        Error(error) -> Error(AckReceiptQueryFailed(error))
        Ok(returned) ->
          case returned.rows {
            [] -> Error(AckReceiptNotFound)
            [receipt] -> {
              let #(
                stored_owner,
                stored_queue,
                stored_id,
                stored_worker,
                stored_worker_version,
                attempt_id,
                attempt_epoch,
                _proposed_state,
                committed_state,
                encoded_output,
                output_version,
                encoded_error,
                error_version,
                failure_description,
                failure_cause,
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
                          let result =
                            outcome_value(
                              committed_state,
                              output_codec,
                              error_codec,
                              encoded_output,
                              output_version |> option.unwrap(""),
                              encoded_error,
                              error_version,
                              failure_description,
                              failure_cause,
                            )
                          case result {
                            Ok(outcome) ->
                              Ok(AcknowledgementReceipt(
                                command_id:,
                                attempt_id:,
                                attempt_epoch:,
                                outcome:,
                              ))
                            Error(OutcomeCodecFailed(error)) ->
                              Error(AckReceiptCodecFailed(error))
                            Error(InvalidOutcomeState(state)) ->
                              Error(AckReceiptInvalidState(state))
                            Error(_) ->
                              Error(AckReceiptInvalidState(committed_state))
                          }
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
      Ok(job.FailedOperationally(failure_description |> unwrap("job discarded")))
    "cancelled" ->
      Ok(job.FailedOperationally(failure_description |> unwrap("job cancelled")))
    other -> Error(InvalidOutcomeState(other))
  }
}
