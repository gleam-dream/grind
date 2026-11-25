//// Declared settlement of abandoned jobs: one take and one try at a time
//// (docs/design/design.typ, "Declared settlement of abandoned jobs";
//// ADR-0021).
////
//// `settle_next` takes the next due job of a worker version the registry
//// declares a settlement for, in a committed statement that moves the job's
//// due time and counts the try, then runs the try in a process linked to
//// the caller. The try locks the job without waiting, checks that it is
//// still due, hands it to the declared function under the application's
//// `search_path`, and applies the decision through the operator resolution
//// protocol in the same Grind-owned transaction. Every failure commits
//// nothing of the try; the take already made the job due later.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/list
import gleam/option.{type Option, None, Some}
import gleam/result
import gleam/string
import grind/internal/diagnostics
import grind/internal/events
import grind/internal/job.{type State}
import grind/internal/postgres.{type Database}
import grind/internal/postgres/resolution
import grind/internal/postgres/resolution_queries.{type ResolutionCommand}
import grind/internal/postgres/resolution_transaction
import grind/internal/registry.{type Registry}
import grind/internal/settlement/declared
import grind/internal/store
import grind/internal/worker
import grind/telemetry
import pog
import sinal/forwarder

/// What one call of `settle_next` did.
pub type Outcome {
  /// No job of a declared worker version is due in the queue.
  NothingDue
  /// The job's decision committed with the function's writes; the job is
  /// now in this state.
  Settled(job_id: Int, state: State)
  /// The function kept the job uncertain for an operator; its writes
  /// committed and the mark ended.
  Declined(job_id: Int)
  /// The try committed nothing; the job is due again at the time its take
  /// set.
  TryFailed(job_id: Int, failure: TryFailure)
  /// The job was no longer due when the try locked it: another settler or an
  /// operator decided it first, or holds its row. Nothing was written.
  Superseded(job_id: Int)
  /// The try's COMMIT was sent and its reply was lost or cut; the next take
  /// finds the job decided or still due.
  CommitUnknown(job_id: Int)
}

/// The job an outcome is about, or `None` for `NothingDue`.
pub fn outcome_job_id(outcome: Outcome) -> Option(Int) {
  case outcome {
    NothingDue -> None
    Settled(job_id:, ..)
    | Declined(job_id:)
    | TryFailed(job_id:, ..)
    | Superseded(job_id:)
    | CommitUnknown(job_id:) -> Some(job_id)
  }
}

/// Why a try committed nothing.
pub type TryFailure {
  FunctionFailed(reason: String)
  FunctionCrashed(reason: String)
  /// The try outlived the storage deadline and was ended.
  FunctionTimedOut
  /// The function ended or replaced the transaction it was given.
  TransactionEnded
  /// The resolution protocol refused the decision, or the reason of a
  /// `LeaveUncertain` cannot be stored.
  Refused(reason: String)
  /// The stored input does not decode under the deployed codec.
  Undecodable(reason: String)
  StorageFailed(reason: String)
}

/// The take: one due job, its due time moved by one lease doubling per try
/// up to the larger of one hour and one lease, its try counted.
type Taken {
  Taken(
    job_id: Int,
    queue: String,
    worker_id: String,
    worker_version: String,
    attempt_id: Int,
    settlement_try: Int,
    correlation: Option(String),
  )
}

const retry_cap_ms = 3_600_000

/// Takes and tries the next due job of `workers`' queue. A take that fails
/// returns the storage error and changes nothing.
pub fn settle_next(
  database: Database,
  workers: Registry,
  lease_ms: Int,
) -> Result(Outcome, pog.QueryError) {
  settle_next_except(database, workers, lease_ms, [])
}

/// `settle_next`, never taking a job in `already`: a round that runs one try
/// per job passes the jobs it already tried, which a short lease may have
/// made due again.
pub fn settle_next_except(
  database: Database,
  workers: Registry,
  lease_ms: Int,
  already: List(Int),
) -> Result(Outcome, pog.QueryError) {
  settle_next_of(
    database,
    registry.queue(workers),
    registry.settlements(workers),
    lease_ms,
    already,
  )
}

/// `settle_next_except` given only the queue and its workers' settlements
/// (`registry.settlements`), so a caller that holds no registry can run it.
pub fn settle_next_of(
  database: Database,
  queue: String,
  settling: List(#(String, String, declared.Settle)),
  lease_ms: Int,
  already: List(Int),
) -> Result(Outcome, pog.QueryError) {
  case settling {
    [] -> Ok(NothingDue)
    _ -> {
      use taken <- result.map(take(database, queue, settling, lease_ms, already))
      case taken {
        None -> NothingDue
        Some(taken) -> {
          let assert Ok(#(_, _, settle)) =
            list.find(settling, fn(entry) {
              entry.0 == taken.worker_id && entry.1 == taken.worker_version
            })
          run_try(database, taken, settle)
        }
      }
    }
  }
}

/// Marks the jobs of `workers`' queue that a node of the previous release
/// held uncertain by lease expiry without a mark: uncertain, no
/// acknowledgement receipt for the retained attempt, and no mark for it.
/// It also marks the jobs held that way before schema 14, which adds the
/// mark without a backfill. Returns how many it marked.
pub fn recognise_unmarked(
  database: Database,
  workers: Registry,
) -> Result(Int, pog.QueryError) {
  recognise_unmarked_of(
    database,
    registry.queue(workers),
    registry.settlements(workers),
  )
}

/// `recognise_unmarked` given only the queue and its workers' settlements.
pub fn recognise_unmarked_of(
  database: Database,
  queue: String,
  settling: List(#(String, String, declared.Settle)),
) -> Result(Int, pog.QueryError) {
  case settling {
    [] -> Ok(0)
    _ ->
      pog.query(
        "UPDATE grind_jobs AS job SET settle_attempt_id = job.attempt_id, settle_at = clock_timestamp(), settle_tries = 0 WHERE job.queue = $1 AND job.state = 'uncertain' AND job.attempt_id IS NOT NULL AND job.settle_attempt_id IS DISTINCT FROM job.attempt_id AND (job.worker_id, job.worker_version) IN (SELECT * FROM unnest($2::text[], $3::text[])) AND NOT EXISTS (SELECT 1 FROM grind_job_acknowledgements AS receipt WHERE receipt.job_id = job.id AND receipt.attempt_id = job.attempt_id AND receipt.attempt_epoch = job.attempt_epoch) RETURNING job.id",
      )
      |> pog.parameter(pog.text(queue))
      |> identities(settling)
      |> pog.returning(decode.field(0, decode.int, decode.success))
      |> store.execute_safely(on: postgres.connection(database))
      |> result.map(fn(returned) { list.length(returned.rows) })
  }
}

fn identities(
  query: pog.Query(a),
  settling: List(#(String, String, declared.Settle)),
) -> pog.Query(a) {
  query
  |> pog.parameter(pog.array(
    pog.text,
    list.map(settling, fn(entry) { entry.0 }),
  ))
  |> pog.parameter(pog.array(
    pog.text,
    list.map(settling, fn(entry) { entry.1 }),
  ))
}

fn take(
  database: Database,
  queue: String,
  settling: List(#(String, String, declared.Settle)),
  lease_ms: Int,
  already: List(Int),
) -> Result(Option(Taken), pog.QueryError) {
  pog.query(
    "UPDATE grind_jobs AS job SET settle_at = clock_timestamp() + make_interval(secs => GREATEST(LEAST($4::float8 * power(2::float8, LEAST(job.settle_tries, 40)::float8), GREATEST($5::float8, $4::float8)), $4::float8) / 1000.0), settle_tries = job.settle_tries + 1 FROM (SELECT id FROM grind_jobs WHERE queue = $1 AND state = 'uncertain' AND settle_at IS NOT NULL AND settle_attempt_id = attempt_id AND settle_at <= clock_timestamp() AND (worker_id, worker_version) IN (SELECT * FROM unnest($2::text[], $3::text[])) AND NOT (id = ANY($6::bigint[])) ORDER BY settle_at, id FOR NO KEY UPDATE SKIP LOCKED LIMIT 1) AS candidate WHERE job.id = candidate.id RETURNING job.id, job.queue, job.worker_id, job.worker_version, job.settle_attempt_id, job.settle_tries, job.correlation",
  )
  |> pog.parameter(pog.text(queue))
  |> identities(settling)
  |> pog.parameter(pog.int(lease_ms))
  |> pog.parameter(pog.int(retry_cap_ms))
  |> pog.parameter(pog.array(pog.int, already))
  |> pog.returning({
    use job_id <- decode.field(0, decode.int)
    use queue <- decode.field(1, decode.string)
    use worker_id <- decode.field(2, decode.string)
    use worker_version <- decode.field(3, decode.string)
    use attempt_id <- decode.field(4, decode.int)
    use settlement_try <- decode.field(5, decode.int)
    use correlation <- decode.field(6, decode.optional(decode.string))
    decode.success(Taken(
      job_id:,
      queue:,
      worker_id:,
      worker_version:,
      attempt_id:,
      settlement_try:,
      correlation:,
    ))
  })
  |> store.execute_safely(on: postgres.connection(database))
  |> result.map(fn(returned) {
    case returned.rows {
      [taken] -> Some(taken)
      _ -> None
    }
  })
}

/// Runs the try in a process linked to the caller, so the try ends with
/// it, and ends a try that outlives the storage deadline.
fn run_try(
  database: Database,
  taken: Taken,
  settle: declared.Settle,
) -> Outcome {
  let started_us = diagnostics.monotonic_us()
  let outcome = run_linked_try(database, taken, settle)
  observe(database, taken, outcome, diagnostics.monotonic_us() - started_us)
  outcome
}

/// `[grind, diagnostic, settlement]` for a try that applied no decision; an
/// applied decision was reported as `[grind, job, resolved]`.
fn observe(
  database: Database,
  taken: Taken,
  outcome: Outcome,
  duration_us: Int,
) -> Nil {
  let reported = case outcome {
    NothingDue | Settled(..) -> None
    Declined(_) -> Some(telemetry.SettlementDeclined)
    Superseded(_) -> Some(telemetry.SettlementSuperseded)
    CommitUnknown(_) -> Some(telemetry.SettlementCommitUnknown)
    TryFailed(_, failure) ->
      Some(telemetry.SettlementFailed(failure_kind(failure)))
  }
  case reported {
    None -> Nil
    Some(outcome) -> {
      let _ =
        forwarder.emit(
          postgres.forwarder(database),
          telemetry.settlement(),
          telemetry.SettlementMeasurements(
            count: 1,
            settlement_try: taken.settlement_try,
            duration_us:,
          ),
          telemetry.SettlementMetadata(
            ref: telemetry.JobRef(
              job_id: taken.job_id,
              queue: taken.queue,
              worker_id: taken.worker_id,
              worker_version: taken.worker_version,
              correlation: events.correlation(taken.job_id, taken.correlation),
            ),
            attempt_id: taken.attempt_id,
            outcome:,
          ),
        )
      Nil
    }
  }
}

fn failure_kind(failure: TryFailure) -> telemetry.SettlementFailure {
  case failure {
    FunctionFailed(_) -> telemetry.SettlementFunctionFailed
    FunctionCrashed(_) -> telemetry.SettlementFunctionCrashed
    FunctionTimedOut -> telemetry.SettlementFunctionTimedOut
    TransactionEnded -> telemetry.SettlementTransactionEnded
    Refused(_) -> telemetry.SettlementDecisionRefused
    Undecodable(_) -> telemetry.SettlementInputUndecodable
    StorageFailed(_) -> telemetry.SettlementStorageFailed
  }
}

fn run_linked_try(
  database: Database,
  taken: Taken,
  settle: declared.Settle,
) -> Outcome {
  let reply = process.new_subject()
  let committing = process.new_subject()
  let pid =
    process.spawn(fn() {
      process.send(reply, try_once(database, taken, settle, committing))
    })
  let monitor = process.monitor(pid)
  let deadline_ms = postgres.statement_deadline_ms(database) + 1000
  let outcome = case process.receive(reply, within: deadline_ms) {
    Ok(outcome) -> outcome
    Error(Nil) -> {
      process.unlink(pid)
      process.kill(pid)
      // The kill is asynchronous: wait for the try to be gone, so a commit
      // intent it sent before dying is in the mailbox when we look.
      let _ =
        process.new_selector()
        |> process.select_specific_monitor(monitor, fn(_) { Nil })
        |> process.selector_receive(within: deadline_ms)
      case process.receive(committing, within: 0) {
        Ok(Nil) -> CommitUnknown(taken.job_id)
        Error(Nil) -> TryFailed(taken.job_id, FunctionTimedOut)
      }
    }
  }
  // Leave nothing of the try in the caller's mailbox: a long-lived settler
  // runs one try after another.
  process.demonitor_process(monitor)
  let _ = process.receive(committing, within: 0)
  outcome
}

type Locked {
  Locked(
    transaction_id: String,
    queue: String,
    input_version: String,
    encoded_input: String,
    attempt: Int,
    cause: worker.AbandonmentCause,
    replays: Int,
    authorized_replays: Int,
    try_number: Int,
    cancellation_requested: Bool,
    held_at_us: Int,
    description: String,
    correlation: Option(String),
  )
}

type Commit {
  CommitResolved(State, ResolutionCommand)
  CommitDeclined
}

/// Why a try's transaction rolled back.
type Abort {
  /// The try failed; the job is due again later.
  Failed(TryFailure)
  /// The job was no longer due under the taken mark, or another holder had
  /// its row.
  NotDue
  /// The decision can never commit; the mark ends as declined, with this
  /// reason, in a separate write.
  Declines(reason: String)
}

fn try_once(
  database: Database,
  taken: Taken,
  settle: declared.Settle,
  committing: process.Subject(Nil),
) -> Outcome {
  let Taken(job_id:, attempt_id:, ..) = taken
  let installation = postgres.installation(database)
  let deadline_ms = postgres.statement_deadline_ms(database)
  let outcome =
    store.transaction_safely(postgres.connection(database), fn(transaction) {
      use locked <- result.try(lock(transaction, job_id, attempt_id))
      use Nil <- result.try(
        enter_application(transaction) |> result.map_error(Failed),
      )
      let request =
        declared.Request(
          installation:,
          job_id:,
          queue: locked.queue,
          input_version: locked.input_version,
          encoded_input: locked.encoded_input,
          attempt_id:,
          attempt: locked.attempt,
          cause: locked.cause,
          replays: locked.replays,
          authorized_replays: locked.authorized_replays,
          try_number: locked.try_number,
          cancellation_requested: locked.cancellation_requested,
          held_at_us: locked.held_at_us,
          description: locked.description,
          correlation: locked.correlation,
        )
      // A decision that cannot commit ends the try with its own reason,
      // even when a failed statement of the function aborted the
      // transaction.
      use decided <- result.try(case settle(transaction, request) {
        Error(declared.FunctionFailed(reason)) ->
          Error(Failed(FunctionFailed(reason)))
        Error(declared.FunctionCrashed(reason)) ->
          Error(Failed(FunctionCrashed(reason)))
        Error(declared.Undecodable(reason)) ->
          Error(Failed(Undecodable(reason)))
        Error(declared.Refused(reason)) -> Error(Failed(Refused(reason)))
        Error(declared.RefusedForGood(reason)) -> Error(Declines(reason))
        Ok(decided) -> Ok(decided)
      })
      use Nil <- result.try(
        enter_grind(
          transaction,
          installation,
          deadline_ms,
          locked.transaction_id,
        )
        |> result.map_error(Failed),
      )
      let commit = case decided {
        declared.Resolve(command) -> {
          use state <- result.try(
            resolution.apply_fresh_in(transaction, command)
            |> result.map_error(fn(error) {
              Failed(Refused(string.inspect(error)))
            }),
          )
          use Nil <- result.map(end_mark(transaction, job_id, attempt_id, None))
          CommitResolved(state, command)
        }
        declared.Leave(reason) ->
          end_mark(transaction, job_id, attempt_id, Some(reason))
          |> result.replace(CommitDeclined)
      }
      // From here COMMIT may be sent: a try ended after this point has an
      // unknown outcome, not a failed one.
      case commit {
        Ok(_) -> process.send(committing, Nil)
        Error(_) -> Nil
      }
      commit
    })
  case outcome {
    Ok(CommitResolved(state, command)) -> {
      resolution.emit_settled(
        postgres.forwarder(database),
        events.correlation(job_id, taken.correlation),
        command,
        state,
      )
      Settled(job_id, state)
    }
    Ok(CommitDeclined) -> Declined(job_id)
    Error(pog.TransactionRolledBack(Failed(failure))) ->
      TryFailed(job_id, failure)
    Error(pog.TransactionRolledBack(NotDue)) -> Superseded(job_id)
    Error(pog.TransactionRolledBack(Declines(reason))) ->
      decline(database, job_id, attempt_id, reason, committing)
    Error(pog.TransactionQueryError(_)) -> CommitUnknown(job_id)
  }
}

/// Ends the mark of a decision that can never commit, as declined, without
/// the function's writes, which rolled back with the try.
fn decline(
  database: Database,
  job_id: Int,
  attempt_id: Int,
  reason: String,
  committing: process.Subject(Nil),
) -> Outcome {
  case
    store.transaction_safely(postgres.connection(database), fn(transaction) {
      use Nil <- result.map(end_mark(
        transaction,
        job_id,
        attempt_id,
        Some(reason),
      ))
      // From here COMMIT may be sent, as for a try's own commit.
      process.send(committing, Nil)
    })
  {
    Ok(Nil) -> Declined(job_id)
    Error(pog.TransactionRolledBack(NotDue)) -> Superseded(job_id)
    Error(pog.TransactionRolledBack(Failed(failure))) ->
      TryFailed(job_id, failure)
    Error(pog.TransactionRolledBack(Declines(_))) -> Superseded(job_id)
    Error(pog.TransactionQueryError(_)) -> CommitUnknown(job_id)
  }
}

/// Locks the job without waiting and reads it, only while it is still due
/// under the mark the take counted. An attempt Grind acknowledged as
/// abandoned, at its timeout or after its handler crashed, has an
/// acknowledgement receipt and the job's `failure_cause` names which; one
/// whose lease expired has no receipt. A timeout held before the cause was
/// stored has a receipt and no cause.
fn lock(
  transaction: pog.Connection,
  job_id: Int,
  attempt_id: Int,
) -> Result(Locked, Abort) {
  pog.query(
    "SELECT pg_current_xact_id()::text, job.queue, job.input_version, job.input::text, job.attempt_count, CASE WHEN NOT EXISTS (SELECT 1 FROM grind_job_acknowledgements AS receipt WHERE receipt.job_id = job.id AND receipt.attempt_id = job.attempt_id AND receipt.attempt_epoch = job.attempt_epoch) THEN 'lease_expired' WHEN job.failure_cause = 'handler_crashed' THEN 'handler_crashed' ELSE 'timed_out' END, job.replay_count, (SELECT count(*) FROM grind_job_resolutions AS resolution WHERE resolution.job_id = job.id AND resolution.decision = 'authorize_replay')::bigint, job.settle_tries, job.cancel_requested_at IS NOT NULL, (extract(epoch FROM COALESCE(job.uncertain_at, clock_timestamp())) * 1000000)::bigint, COALESCE(job.failure_description, ''), job.correlation FROM grind_jobs AS job WHERE job.id = $1 AND job.state = 'uncertain' AND job.settle_at IS NOT NULL AND job.settle_attempt_id = job.attempt_id AND job.attempt_id = $2 FOR NO KEY UPDATE OF job SKIP LOCKED",
  )
  |> pog.parameter(pog.int(job_id))
  |> pog.parameter(pog.int(attempt_id))
  |> pog.returning({
    use transaction_id <- decode.field(0, decode.string)
    use queue <- decode.field(1, decode.string)
    use input_version <- decode.field(2, decode.string)
    use encoded_input <- decode.field(3, decode.string)
    use attempt <- decode.field(4, decode.int)
    use cause <- decode.field(5, decode.string)
    use replays <- decode.field(6, decode.int)
    use authorized_replays <- decode.field(7, decode.int)
    use try_number <- decode.field(8, decode.int)
    use cancellation_requested <- decode.field(9, decode.bool)
    use held_at_us <- decode.field(10, decode.int)
    use description <- decode.field(11, decode.string)
    use correlation <- decode.field(12, decode.optional(decode.string))
    decode.success(Locked(
      transaction_id:,
      queue:,
      input_version:,
      encoded_input:,
      attempt:,
      cause: case cause {
        "lease_expired" -> worker.LeaseExpired
        "handler_crashed" -> worker.HandlerCrashed
        _ -> worker.TimedOut
      },
      replays:,
      authorized_replays:,
      try_number:,
      cancellation_requested:,
      held_at_us:,
      description:,
      correlation:,
    ))
  })
  |> store.execute_safely(on: transaction)
  |> result.map_error(fn(error) { Failed(storage_failed(error)) })
  |> result.try(fn(returned) {
    case returned.rows {
      [locked] -> Ok(locked)
      _ -> Error(NotDue)
    }
  })
}

/// Sets the application's `search_path`, saved by the managed checkout,
/// for the rest of the transaction. A pool Grind owns alone has none, and
/// the function sees Grind's schema.
fn enter_application(transaction: pog.Connection) -> Result(Nil, TryFailure) {
  case application_search_path() {
    None -> Ok(Nil)
    Some(search_path) ->
      pog.query("SELECT set_config('search_path', $1, true)")
      |> pog.parameter(pog.text(search_path))
      |> store.execute_safely(on: transaction)
      |> result.replace(Nil)
      |> result.map_error(storage_failed)
  }
}

/// Scopes the transaction to Grind again after the function returned, and
/// refuses a transaction the function ended or replaced.
fn enter_grind(
  transaction: pog.Connection,
  installation: job.Installation,
  deadline_ms: Int,
  transaction_id: String,
) -> Result(Nil, TryFailure) {
  use current <- result.try(
    pog.query("SELECT pg_current_xact_id()::text")
    |> pog.returning(decode.field(0, decode.string, decode.success))
    |> store.execute_safely(on: transaction)
    |> result.map_error(fn(error) {
      case error {
        // A failed statement of the function aborted the transaction and
        // the function went on as if it had not.
        pog.PostgresqlError(code: "25P02", ..) ->
          FunctionFailed(
            "a failed statement aborted the settlement transaction",
          )
        _ -> TransactionEnded
      }
    }),
  )
  use Nil <- result.try(case current.rows {
    [id] if id == transaction_id -> Ok(Nil)
    _ -> Error(TransactionEnded)
  })
  resolution_transaction.enter(transaction, installation, deadline_ms)
  |> result.replace(Nil)
  |> result.map_error(fn(error) {
    case error {
      resolution_transaction.QueryFailed(error) -> storage_failed(error)
      _ -> TransactionEnded
    }
  })
}

/// Ends the mark of the settled attempt: it keeps the attempt and loses
/// its due time. A declined settlement appends its reason to the job's
/// description and requires the job to be uncertain under the same mark.
fn end_mark(
  transaction: pog.Connection,
  job_id: Int,
  attempt_id: Int,
  declined: Option(String),
) -> Result(Nil, Abort) {
  let query = case declined {
    None ->
      pog.query(
        "UPDATE grind_jobs SET settle_at = NULL WHERE id = $1 AND settle_attempt_id = $2 RETURNING id",
      )
      |> pog.parameter(pog.int(job_id))
      |> pog.parameter(pog.int(attempt_id))
    Some(reason) ->
      pog.query(
        "UPDATE grind_jobs SET settle_at = NULL, failure_description = CASE WHEN failure_description IS NULL THEN $3 ELSE failure_description || '; ' || $3 END WHERE id = $1 AND state = 'uncertain' AND attempt_id = $2 AND settle_attempt_id = $2 AND settle_at IS NOT NULL RETURNING id",
      )
      |> pog.parameter(pog.int(job_id))
      |> pog.parameter(pog.int(attempt_id))
      |> pog.parameter(pog.text(reason))
  }
  query
  |> pog.returning(decode.field(0, decode.int, decode.success))
  |> store.execute_safely(on: transaction)
  |> result.map_error(fn(error) { Failed(storage_failed(error)) })
  |> result.try(fn(returned) {
    case returned.rows {
      [_] -> Ok(Nil)
      _ -> Error(NotDue)
    }
  })
}

fn storage_failed(error: pog.QueryError) -> TryFailure {
  StorageFailed(string.inspect(error))
}

@external(erlang, "grind_postgres_ffi", "application_search_path")
fn application_search_path() -> Option(String)
