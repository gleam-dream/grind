import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleeunit/should
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/support/concurrency.{
  ClaimGateAcquired, ClaimGateReleased, ReleaseAttempt, spawn_lock_holder,
  spawn_submit, unique_test_lock_key,
}
import grind/support/consumer.{manual_policy}
import grind/support/env.{
  database_url, mark_database_test_executed, queue_database_url,
}
import grind/support/lock_wait.{await_lock_wait_counts}
import grind/support/queue_signals.{WorkerInvoked}
import grind/support/unique_fixture.{unique_test_suffix, with_unique_databases}
import grind/worker
import pog

pub fn postgres_uncertain_replay_requires_audited_resolution_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_uncertain_resolution_test(database_url)
  }
}

fn run_uncertain_resolution_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("resolve-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "resolve-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let invocations = process.new_subject()
  let assert Ok(worker) =
    worker.define("resolve.echo", "v1", input_codec, output_codec, fn(value) {
      process.send(invocations, WorkerInvoked)
      Ok("resolved-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("uncertain-resolution")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(handle) =
    postgres.submit(database, "uncertain-resolution", worker, 12)
  let #(id, _, _, _, _, _) = job.storage_fields(handle)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 121, attempt_epoch = 6, attempt_owner = 'lost-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))

  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "resolution-121",
      "on-call",
      "confirm external idempotency record before replay",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "resolution-121",
      "on-call",
      "confirm external idempotency record before replay",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionAlreadyApplied(job.Queued)))
  let assert Ok(audit) =
    pog.query(
      "SELECT resolution_id, job_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at IS NOT NULL, decision, resolved_by, details FROM grind_job_resolutions WHERE job_id = $1 AND resolution_id = $2",
    )
    |> pog.parameter(pog.int(id))
    |> pog.parameter(pog.text("resolution-121"))
    |> pog.returning({
      use resolution_id <- decode.field(0, decode.string)
      use job_id <- decode.field(1, decode.int)
      use attempt_id <- decode.field(2, decode.int)
      use attempt_epoch <- decode.field(3, decode.int)
      use attempt_owner <- decode.field(4, decode.string)
      use expiry_retained <- decode.field(5, decode.bool)
      use decision <- decode.field(6, decode.string)
      use resolved_by <- decode.field(7, decode.string)
      use details <- decode.field(8, decode.string)
      decode.success(#(
        resolution_id,
        job_id,
        attempt_id,
        attempt_epoch,
        attempt_owner,
        expiry_retained,
        decision,
        resolved_by,
        details,
      ))
    })
    |> pog.execute(on: connection)
  let assert [
    #(
      resolution_id,
      job_id,
      attempt_id,
      attempt_epoch,
      attempt_owner,
      expiry_retained,
      decision,
      resolved_by,
      details,
    ),
  ] = audit.rows
  resolution_id |> should.equal("resolution-121")
  job_id |> should.equal(id)
  attempt_id |> should.equal(121)
  attempt_epoch |> should.equal(6)
  attempt_owner |> should.equal("lost-consumer")
  expiry_retained |> should.equal(True)
  decision |> should.equal("authorize_replay")
  resolved_by |> should.equal("on-call")
  details |> should.equal("confirm external idempotency record before replay")
  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(invocations, within: 0) |> should.equal(Ok(WorkerInvoked))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("resolved-12")))
  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "resolution-121",
      "on-call",
      "confirm external idempotency record before replay",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionAlreadyApplied(job.Queued)))
  mark_database_test_executed("audited-uncertain-resolution-passed")
}

pub fn postgres_resolution_command_binds_typed_payload_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_resolution_payload_test(database_url)
  }
}

fn run_resolution_payload_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "resolution-payload-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "resolution-payload-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(worker) =
    worker.define(
      "resolution.payload",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) =
    postgres.submit(database, "resolution-payload", worker, 2)
  let #(id, _, _, _, _, _) = job.storage_fields(handle)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 222, attempt_epoch = 3, attempt_owner = 'lost-payload-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: connection)
  let assert Ok(workers) = registry.new("resolution-payload")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "resolution-payload-222",
      "on-call",
      "operator observed committed application key",
      postgres.ConfirmSuccess("approved"),
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Succeeded)))
  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "resolution-payload-222",
      "on-call",
      "operator observed committed application key",
      postgres.ConfirmSuccess("different"),
    ),
  )
  |> should.equal(Error(postgres.ResolutionCommandConflict))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("approved")))
  mark_database_test_executed("resolution-payload-bound")
}

/// R4: `reconcile_matching_owner`'s idempotency check
/// (`resolution_receipt_outcome`, shared with `apply_uncertain_resolution`'s
/// own re-check after its `FOR UPDATE`) runs once before that lock and is
/// never re-checked when the locked row is no longer `uncertain` — a
/// concurrent retry of the *same* `resolution_id` and payload that waits
/// behind the first resolution's row lock would otherwise misreport
/// `ReconciliationNotRequired` instead of the recorded
/// `ResolutionAlreadyApplied` outcome. Forced to genuinely overlap: A's
/// `write_resolution` update is blocked behind a test-only `BEFORE UPDATE`
/// barrier trigger scoped to this job (`OLD.state = 'uncertain'`); B — the
/// identical resolution command, from a separate pool — starts while A is
/// blocked, and B's own `SELECT ... FOR UPDATE` then genuinely waits on the
/// row lock A already holds (confirmed via `pg_stat_activity`'s
/// `transactionid` wait event). Once A completes and commits, B must return
/// `Ok(ResolutionAlreadyApplied(target_state))` — not
/// `Error(ReconciliationNotRequired)` — and only one resolution row and one
/// state transition (one redelivery) must exist.
pub fn postgres_resolution_concurrent_same_outcome_applied_once_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_resolution_concurrent_test(database_url)
  }
}

fn run_resolution_concurrent_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  use entries <- with_unique_databases(database_url, [
    "grind_resolution_concurrent_a_" <> suffix,
    "grind_resolution_concurrent_b_" <> suffix,
  ])
  let assert [#(database_a, connection_a), #(database_b, _)] = entries

  let assert Ok(input_codec) =
    worker.codec(
      "resolution-concurrent-input-" <> suffix <> "-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "resolution-concurrent-output-" <> suffix <> "-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "resolution.concurrent-" <> suffix,
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let test_queue = "resolution-concurrent-" <> suffix
  let assert Ok(handle) = postgres.submit(database_a, test_queue, definition, 3)
  let #(job_id, _, _, _, _, _) = job.storage_fields(handle)

  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 1, attempt_epoch = 1, attempt_owner = 'resolution-concurrent-owner', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.execute(on: connection_a)

  let lock_key = unique_test_lock_key(5)
  let trigger_name = "grind_test_resolution_overlap_" <> suffix
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> trigger_name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = "
      <> int.to_string(job_id)
      <> " AND OLD.state = 'uncertain' THEN PERFORM pg_advisory_xact_lock("
      <> int.to_string(lock_key)
      <> "); END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection_a)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER "
      <> trigger_name
      <> " BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION "
      <> trigger_name
      <> "()",
    )
    |> pog.execute(on: connection_a)
  use <- exception.defer(fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS " <> trigger_name <> " ON grind_jobs")
      |> pog.execute(on: connection_a)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> trigger_name <> "()")
      |> pog.execute(on: connection_a)
    Nil
  })

  let acquire_query =
    pog.query(
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_resolution_overlap_barrier",
    )
    |> pog.parameter(pog.int(lock_key))
  let #(lock_ready, lock_finished) =
    spawn_lock_holder(connection_a, acquire_query)
  let assert Ok(ClaimGateAcquired(release_lock)) =
    process.receive(lock_ready, within: 5000)
  use <- exception.defer(fn() {
    process.send(release_lock, ReleaseAttempt)
    Nil
  })

  let resolution_id = "resolution-concurrent-" <> suffix
  let resolved_by = "on-call-" <> suffix
  let details = "confirm external idempotency record before replay"
  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    postgres.resolve_uncertain(
      database_a,
      handle,
      postgres.ResolutionRequest(
        resolution_id,
        resolved_by,
        details,
        postgres.AuthorizeReplay,
      ),
    )
  })

  // A must actually be blocked inside `write_resolution`'s `UPDATE`,
  // behind the barrier (an advisory wait), before B starts.
  await_lock_wait_counts(connection_a, 1, 0, 500) |> should.equal(True)

  spawn_submit(result_b, fn() {
    postgres.resolve_uncertain(
      database_b,
      handle,
      postgres.ResolutionRequest(
        resolution_id,
        resolved_by,
        details,
        postgres.AuthorizeReplay,
      ),
    )
  })

  // B must be genuinely waiting on the row lock A's own `SELECT ... FOR
  // UPDATE` (still held through A's blocked `UPDATE`) holds
  // (`transactionid`, a real tuple-lock wait — not the advisory wait A is
  // parked on) before we release the barrier.
  await_lock_wait_counts(connection_a, 1, 1, 500) |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 5000)
  let assert Ok(outcome_b) = process.receive(result_b, within: 5000)
  outcome_a |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  outcome_b |> should.equal(Ok(postgres.ResolutionAlreadyApplied(job.Queued)))

  let assert Ok(resolution_count_returned) =
    pog.query(
      "SELECT count(*)::bigint FROM grind_job_resolutions WHERE job_id = $1 AND resolution_id = $2",
    )
    |> pog.parameter(pog.int(job_id))
    |> pog.parameter(pog.text(resolution_id))
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
    |> pog.execute(on: connection_a)
  let assert [1] = resolution_count_returned.rows
  postgres.state(database_a, handle) |> should.equal(Ok(job.Queued))

  mark_database_test_executed("resolution-concurrent-same-outcome-applied-once")
}
