import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{Some}
import gleeunit
import gleeunit/should
import grind
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/worker
import pog

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn version_test() {
  grind.version()
  |> should.equal("0.1.0")
}

pub type LookupFailure {
  AccountMissing(account_id: Int)
}

type WorkerProbe {
  WorkerInvoked
  LaterWorkerInvoked
}

type LeaseCommand {
  ReleaseAttempt
}

type LeaseSignal {
  FirstAttemptStarted(process.Subject(LeaseCommand))
  TakeoverAttemptStarted(process.Subject(LeaseCommand))
}

type PolicyStartSignal {
  PolicyStarterReady
  StartPolicyRace
  PolicyStartResult(Result(Bool, queue.StartError))
}

pub fn invocation_preserves_the_application_error_test() {
  let assert Ok(input_codec) =
    worker.codec("account-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("account-output-v1", json.string, decode.string)
  let assert Ok(account_lookup) =
    worker.define(
      "accounts.lookup",
      "v1",
      input_codec,
      output_codec,
      fn(account_id) { Error(AccountMissing(account_id)) },
    )

  worker.invoke(account_lookup, 42)
  |> should.equal(Error(AccountMissing(42)))
}

@external(erlang, "grind_test_env", "database_url")
fn database_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "queue_database_url")
fn queue_database_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "owner_a_url")
fn owner_a_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "owner_b_url")
fn owner_b_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_bad_url")
fn schema_bad_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_v1_url")
fn schema_v1_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "schema_v2_url")
fn schema_v2_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "resolution_route_a_url")
fn resolution_route_a_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "resolution_route_b_url")
fn resolution_route_b_url() -> Result(String, Nil)

@external(erlang, "grind_test_env", "mark_database_test_executed")
fn mark_database_test_executed(contract: String) -> Nil

pub fn postgres_admission_round_trips_typed_arguments_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_postgres_admission_test(database_url)
  }
}

fn run_postgres_admission_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_test_pool")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("integer-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("text-output-v1", json.string, decode.string)
  let assert Ok(counter) =
    worker.define(
      "counter.increment",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value + 1)) },
    )
  let assert Ok(handle) = postgres.submit(database, "default", counter, 41)

  postgres.arguments(database, handle)
  |> should.equal(Ok(41))

  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET worker_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("v2"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)
  postgres.arguments(database, handle)
  |> should.equal(
    Error(postgres.WorkerContractMismatch(
      expected_id: "counter.increment",
      expected_version: "v1",
      actual_id: "counter.increment",
      actual_version: "v2",
    )),
  )
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET worker_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("v1"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)

  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET input_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("integer-input-v2"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)
  postgres.arguments(database, handle)
  |> should.equal(
    Error(
      postgres.ArgumentCodecFailed(worker.CodecVersionMismatch(
        expected: "integer-input-v1",
        got: "integer-input-v2",
      )),
    ),
  )
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET input_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("integer-input-v1"))
    |> pog.parameter(pog.text("counter.increment"))
    |> pog.execute(on: connection)

  postgres.state(database, handle)
  |> should.equal(Ok(job.Queued))
  postgres.close(database)
  let assert Ok(reopened) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(reopened) })
  postgres.state(reopened, handle)
  |> should.equal(Ok(job.Queued))
  mark_database_test_executed("admission-read-passed")
}

pub fn postgres_handles_are_bound_to_storage_owner_test() {
  case owner_a_url(), owner_b_url() {
    Ok(database_url_a), Ok(database_url_b) ->
      run_storage_owner_test(database_url_a, database_url_b)
    _, _ -> Nil
  }
}

fn run_storage_owner_test(
  database_url_a: String,
  database_url_b: String,
) -> Nil {
  let assert Ok(validated_a) =
    postgres.settings(database_url_a, process.new_name("grind_owner_a"))
    |> postgres.validate
  let assert Ok(database_a) = postgres.start(validated_a)
  use <- exception.defer(fn() { postgres.close(database_a) })
  let assert Ok(validated_b) =
    postgres.settings(database_url_b, process.new_name("grind_owner_b"))
    |> postgres.validate
  let assert Ok(database_b) = postgres.start(validated_b)
  use <- exception.defer(fn() { postgres.close(database_b) })
  let assert Ok(Nil) = postgres.migrate(database_a)
  let assert Ok(Nil) = postgres.migrate(database_b)
  let assert Ok(input_codec) =
    worker.codec("integer-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("text-output-v1", json.string, decode.string)
  let assert Ok(counter) =
    worker.define(
      "counter.increment",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value + 1)) },
    )
  let assert Ok(handle_a) = postgres.submit(database_a, "default", counter, 41)
  let assert Ok(_handle_b) = postgres.submit(database_b, "default", counter, 42)

  postgres.arguments(database_b, handle_a)
  |> should.equal(Error(postgres.StorageOwnerMismatch))
  postgres.state(database_b, handle_a)
  |> should.equal(Error(postgres.StateStorageOwnerMismatch))
  mark_database_test_executed("storage-owner-passed")
}

pub fn postgres_migration_rejects_incompatible_existing_schema_test() {
  case schema_bad_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_incompatible_schema_test(database_url)
  }
}

pub fn postgres_migration_upgrades_v1_without_losing_live_attempt_test() {
  case schema_v1_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_v1_migration_test(database_url)
  }
}

pub fn postgres_migration_preserves_legacy_resolution_receipt_test() {
  case schema_v2_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_v2_migration_test(database_url)
  }
}

fn run_v1_migration_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_schema_v1")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "CREATE TABLE grind_schema_migrations (version integer PRIMARY KEY, installed_at timestamptz NOT NULL DEFAULT clock_timestamp())",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TABLE grind_jobs (id bigserial PRIMARY KEY, storage_owner text NOT NULL, queue text NOT NULL, worker_id text NOT NULL, worker_version text NOT NULL, input_version text NOT NULL, input jsonb NOT NULL, output_version text NOT NULL, output jsonb, error_version text, error jsonb, state text NOT NULL CONSTRAINT grind_jobs_state_check CHECK (state IN ('queued', 'scheduled', 'executing', 'succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'discarded', 'cancelled')), available_at timestamptz NOT NULL, inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(), attempt_id bigint, attempt_epoch bigint NOT NULL DEFAULT 0, attempt_owner text, lease_expires_at timestamptz, attempt_count bigint NOT NULL DEFAULT 0, failure_description text)",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("CREATE SEQUENCE grind_attempts_id_seq")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("INSERT INTO grind_schema_migrations (version) VALUES (1)")
    |> pog.execute(on: connection)
  let assert Ok(input_codec) =
    worker.codec("legacy-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("legacy-output-v1", json.string, decode.string)
  let assert Ok(legacy_worker) =
    worker.define("legacy.echo", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(handle) = postgres.submit(database, "legacy", legacy_worker, 73)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 77, attempt_epoch = 4, attempt_owner = 'legacy-consumer', lease_expires_at = clock_timestamp() + interval '30 seconds', attempt_count = 2 WHERE id = $1",
    )
    |> pog.parameter(pog.int(1))
    |> pog.execute(on: connection)

  postgres.migrate(database) |> should.equal(Ok(Nil))
  postgres.migrate(database) |> should.equal(Ok(Nil))
  postgres.arguments(database, handle) |> should.equal(Ok(73))
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))
  let assert Ok(migrated_attempt) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner, lease_expires_at > clock_timestamp(), uncertain_at IS NULL, (SELECT count(*) = 3 FROM grind_schema_migrations) FROM grind_jobs WHERE id = $1",
    )
    |> pog.parameter(pog.int(1))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      use lease_live <- decode.field(3, decode.bool)
      use no_uncertain_at <- decode.field(4, decode.bool)
      use versions_present <- decode.field(5, decode.bool)
      decode.success(#(
        attempt_id,
        attempt_epoch,
        attempt_owner,
        lease_live,
        no_uncertain_at,
        versions_present,
      ))
    })
    |> pog.execute(on: connection)
  let assert [
    #(
      attempt_id,
      attempt_epoch,
      attempt_owner,
      lease_live,
      no_uncertain_at,
      versions_present,
    ),
  ] = migrated_attempt.rows
  attempt_id |> should.equal(77)
  attempt_epoch |> should.equal(4)
  attempt_owner |> should.equal("legacy-consumer")
  lease_live |> should.equal(True)
  no_uncertain_at |> should.equal(True)
  versions_present |> should.equal(True)
  mark_database_test_executed("v1-migration-preserved-data")
}

fn run_v2_migration_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_schema_v2")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = pog.named_connection(pool_name)
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("legacy-resolution-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("legacy-resolution-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "legacy.resolution",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) = postgres.submit(database, "legacy", definition, 7)
  let durable_id = job.id_value(handle)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'succeeded', output = '\"legacy-value\"'::jsonb WHERE id = $1",
    )
    |> pog.parameter(pog.int(durable_id))
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_job_resolutions DROP CONSTRAINT grind_job_resolutions_target_state_check",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE grind_job_resolutions DROP COLUMN worker_id, DROP COLUMN worker_version, DROP COLUMN target_state, DROP COLUMN payload_version, DROP COLUMN payload",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query("DELETE FROM grind_schema_migrations WHERE version = 3")
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "INSERT INTO grind_job_resolutions (storage_owner, queue, job_id, resolution_id, attempt_id, attempt_epoch, attempt_owner, lease_expires_at, decision, resolved_by, details) VALUES ($1, 'legacy', $2, 'legacy-resolution', 91, 6, 'legacy-owner', clock_timestamp() - interval '1 minute', 'confirm_success', 'operator', 'approved by prior operator')",
    )
    |> pog.parameter(pog.text(postgres.storage_owner(database)))
    |> pog.parameter(pog.int(durable_id))
    |> pog.execute(on: connection)

  postgres.migrate(database) |> should.equal(Ok(Nil))
  postgres.migrate(database) |> should.equal(Ok(Nil))
  let assert Ok(legacy_receipt) =
    pog.query(
      "SELECT target_state, payload_version IS NULL, payload IS NULL, (SELECT count(*) = 3 FROM grind_schema_migrations), decision, details, worker_id, worker_version FROM grind_job_resolutions WHERE resolution_id = 'legacy-resolution'",
    )
    |> pog.returning({
      use target_state <- decode.field(0, decode.string)
      use no_payload_version <- decode.field(1, decode.bool)
      use no_payload <- decode.field(2, decode.bool)
      use all_versions <- decode.field(3, decode.bool)
      use decision <- decode.field(4, decode.string)
      use details <- decode.field(5, decode.string)
      use stored_worker_id <- decode.field(6, decode.optional(decode.string))
      use stored_worker_version <- decode.field(
        7,
        decode.optional(decode.string),
      )
      decode.success(#(
        target_state,
        no_payload_version,
        no_payload,
        all_versions,
        decision,
        details,
        stored_worker_id,
        stored_worker_version,
      ))
    })
    |> pog.execute(on: connection)
  let assert [
    #(
      target_state,
      no_payload_version,
      no_payload,
      all_versions,
      decision,
      details,
      stored_worker_id,
      stored_worker_version,
    ),
  ] = legacy_receipt.rows
  target_state |> should.equal("succeeded")
  no_payload_version |> should.equal(True)
  no_payload |> should.equal(True)
  all_versions |> should.equal(True)
  decision |> should.equal("confirm_success")
  details |> should.equal("approved by prior operator")
  stored_worker_id |> should.equal(Some("legacy.resolution"))
  stored_worker_version |> should.equal(Some("v1"))
  postgres.resolve_uncertain(
    database,
    handle,
    "legacy-resolution",
    "operator",
    "approved by prior operator",
    postgres.ConfirmSuccess("legacy-value"),
  )
  |> should.equal(Error(postgres.ResolutionCommandConflict))
  mark_database_test_executed("v2-legacy-resolution-preserved")
}

pub fn postgres_queue_commits_typed_worker_success_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_postgres_queue_success_test(database_url)
  }
}

pub fn postgres_queue_rejects_output_codec_drift_before_invocation_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_output_codec_mismatch_test(database_url)
  }
}

pub fn postgres_queue_persists_typed_business_failure_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_business_failure_test(database_url)
  }
}

pub fn postgres_automatic_queue_skips_incompatible_job_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_automatic_queue_fairness_test(database_url)
  }
}

pub fn postgres_queue_policy_limits_jobs_per_tick_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_queue_batch_policy_test(database_url)
  }
}

pub fn postgres_scheduled_jobs_observe_database_due_time_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_scheduled_due_time_test(database_url)
  }
}

fn run_scheduled_due_time_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_scheduled_boundary")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("scheduled-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("scheduled-output-v1", json.int, decode.int)
  let probe = process.new_subject()
  let assert Ok(scheduled_worker) =
    worker.define("scheduled.echo", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, WorkerInvoked)
      Ok(value)
    })
  let assert Ok(workers) = registry.new("scheduled-boundary")
  let assert Ok(workers) = registry.register(workers, scheduled_worker)
  let connection = pog.named_connection(pool_name)
  let assert Ok(returned) =
    pog.query(
      "SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint + 60000",
    )
    |> pog.returning({
      use value <- decode.field(0, decode.int)
      decode.success(value)
    })
    |> pog.execute(on: connection)
  let assert [future_unix_ms] = returned.rows
  let assert Ok(available_at) = job.available_at(future_unix_ms)
  let assert Ok(handle) =
    postgres.submit_at(
      database,
      "scheduled-boundary",
      scheduled_worker,
      17,
      available_at,
    )
  postgres.state(database, handle) |> should.equal(Ok(job.Scheduled))
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET available_at = clock_timestamp() WHERE worker_id = $1 AND queue = $2 AND state = 'scheduled'",
    )
    |> pog.parameter(pog.text("scheduled.echo"))
    |> pog.parameter(pog.text("scheduled-boundary"))
    |> pog.execute(on: connection)

  queue.process_one(consumer) |> should.equal(Ok(True))
  process.receive(probe, within: 0) |> should.equal(Ok(WorkerInvoked))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("scheduled-due-time-passed")
}

pub fn postgres_manual_batch_reports_acknowledged_prefix_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_batch_partial_error_test(database_url)
  }
}

fn run_batch_partial_error_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_batch_partial")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("partial-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("partial-output-v1", json.int, decode.int)
  let assert Ok(first_worker) =
    worker.define(
      "batch.partial.first",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value + 1) },
    )
  let assert Ok(incompatible_worker) =
    worker.define(
      "batch.partial.incompatible",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value + 1) },
    )
  let assert Ok(last_worker) =
    worker.define(
      "batch.partial.last",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value + 1) },
    )
  let assert Ok(workers) = registry.new("batch-partial")
  let assert Ok(workers) = registry.register(workers, first_worker)
  let assert Ok(workers) = registry.register(workers, incompatible_worker)
  let assert Ok(workers) = registry.register(workers, last_worker)
  let assert Ok(first) =
    postgres.submit(database, "batch-partial", first_worker, 1)
  let assert Ok(incompatible) =
    postgres.submit(database, "batch-partial", incompatible_worker, 2)
  let assert Ok(last) =
    postgres.submit(database, "batch-partial", last_worker, 3)
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET output_version = $1 WHERE worker_id = $2 AND queue = $3",
    )
    |> pog.parameter(pog.text("partial-output-v2"))
    |> pog.parameter(pog.text("batch.partial.incompatible"))
    |> pog.parameter(pog.text("batch-partial"))
    |> pog.execute(on: connection)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_jobs_per_poll(3)
    |> queue.validate_policy
  let assert Ok(consumer) =
    queue.start_manual_with_policy(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_available(consumer)
  |> should.equal(queue.BatchStopped(
    acknowledged_before_error: 1,
    error: queue.QueueProcessFailed(postgres.QueueCodecMismatch(
      kind: "output",
      expected: "partial-output-v2",
      actual: "partial-output-v1",
    )),
  ))
  postgres.state(database, first) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, incompatible)
  |> should.equal(Ok(job.ContractMismatch))
  postgres.state(database, last) |> should.equal(Ok(job.Queued))
  mark_database_test_executed("batch-partial-commit-count-passed")
}

pub fn postgres_expired_attempt_is_taken_over_and_stale_ack_is_fenced_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_takeover_fencing_test(database_url)
  }
}

fn run_takeover_fencing_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_takeover_fence")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("takeover-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("takeover-output-v1", json.string, decode.string)
  let signals = process.new_subject()
  let assert Ok(first_worker) =
    worker.define("takeover.echo", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(signals, FirstAttemptStarted(release))
      case process.receive(release, within: 15_000) {
        Ok(ReleaseAttempt) -> Ok("obsolete-" <> int.to_string(value))
        Error(Nil) -> Error(Nil)
      }
    })
  let assert Ok(takeover_worker) =
    worker.define("takeover.echo", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(signals, TakeoverAttemptStarted(release))
      case process.receive(release, within: 15_000) {
        Ok(ReleaseAttempt) -> Ok("current-" <> int.to_string(value))
        Error(Nil) -> Error(Nil)
      }
    })
  let assert Ok(first_registry) = registry.new("takeover-fence")
  let assert Ok(first_registry) =
    registry.register(first_registry, first_worker)
  let assert Ok(takeover_registry) = registry.new("takeover-fence")
  let assert Ok(takeover_registry) =
    registry.register(takeover_registry, takeover_worker)
  let assert Ok(replay_policy) =
    queue.default_policy()
    |> queue.with_expired_attempt_policy(queue.ReplayAtLeastOnce)
    |> queue.validate_policy
  let assert Ok(first_consumer) =
    queue.start_manual_with_policy(database, first_registry, replay_policy)
  use <- exception.defer(fn() { queue.stop(first_consumer) })
  let assert Ok(takeover_consumer) =
    queue.start_manual_with_policy(database, takeover_registry, replay_policy)
  use <- exception.defer(fn() { queue.stop(takeover_consumer) })
  let assert Ok(competing_consumer) =
    queue.start_manual_with_policy(database, takeover_registry, replay_policy)
  use <- exception.defer(fn() { queue.stop(competing_consumer) })
  let assert Ok(handle) =
    postgres.submit(database, "takeover-fence", first_worker, 7)
  let first_reply = process.new_subject()
  let first_finished = process.new_subject()
  let _ =
    process.spawn(fn() {
      process.send(first_reply, queue.process_one(first_consumer))
    })
  let assert Ok(FirstAttemptStarted(first_release)) =
    process.receive(signals, within: 5000)
  use <- exception.defer(fn() {
    settle_attempt(first_finished, first_release, first_reply)
  })

  queue.process_one(competing_consumer) |> should.equal(Ok(False))
  let connection = pog.named_connection(pool_name)
  let assert Ok(first_claim) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner FROM grind_jobs WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("takeover.echo"))
    |> pog.parameter(pog.text("takeover-fence"))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      decode.success(#(attempt_id, attempt_epoch, attempt_owner))
    })
    |> pog.execute(on: connection)
  let assert [#(first_attempt_id, first_epoch, first_owner)] = first_claim.rows
  first_epoch |> should.equal(1)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE worker_id = $1 AND queue = $2 AND state = 'executing'",
    )
    |> pog.parameter(pog.text("takeover.echo"))
    |> pog.parameter(pog.text("takeover-fence"))
    |> pog.execute(on: connection)

  let takeover_reply = process.new_subject()
  let takeover_finished = process.new_subject()
  let _ =
    process.spawn(fn() {
      process.send(takeover_reply, queue.process_one(takeover_consumer))
    })
  let assert Ok(TakeoverAttemptStarted(takeover_release)) =
    process.receive(signals, within: 5000)
  use <- exception.defer(fn() {
    settle_attempt(takeover_finished, takeover_release, takeover_reply)
  })
  let assert Ok(takeover_claim) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner FROM grind_jobs WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("takeover.echo"))
    |> pog.parameter(pog.text("takeover-fence"))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      decode.success(#(attempt_id, attempt_epoch, attempt_owner))
    })
    |> pog.execute(on: connection)
  let assert [#(takeover_attempt_id, takeover_epoch, takeover_owner)] =
    takeover_claim.rows
  takeover_attempt_id |> should.not_equal(first_attempt_id)
  takeover_epoch |> should.equal(first_epoch + 1)
  takeover_owner |> should.not_equal(first_owner)
  queue.process_one(competing_consumer) |> should.equal(Ok(False))

  process.send(first_release, ReleaseAttempt)
  let first_ack = process.receive(first_reply, within: 5000)
  process.send(first_finished, Nil)
  first_ack
  |> should.equal(
    Ok(Error(queue.QueueProcessFailed(postgres.QueueAckRejected))),
  )
  postgres.state(database, handle) |> should.equal(Ok(job.Executing))

  process.send(takeover_release, ReleaseAttempt)
  let takeover_ack = process.receive(takeover_reply, within: 5000)
  process.send(takeover_finished, Nil)
  takeover_ack |> should.equal(Ok(Ok(True)))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("current-7")))
  mark_database_test_executed("expired-attempt-takeover-passed")
}

pub fn postgres_expired_attempt_requires_reconciliation_by_default_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_expired_attempt_quarantine_test(database_url)
  }
}

pub fn postgres_mixed_consumers_reject_replay_policy_conflict_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_mixed_consumer_policy_test(database_url)
  }
}

pub fn postgres_concurrent_queue_policy_start_has_one_winner_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_concurrent_policy_start_test(database_url)
  }
}

fn run_concurrent_policy_start_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_policy_start_race")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("policy-race-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("policy-race-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("policy.race", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("durable-policy-race")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(default_policy) =
    queue.default_policy() |> queue.validate_policy
  let assert Ok(replay_policy) =
    queue.default_policy()
    |> queue.with_expired_attempt_policy(queue.ReplayAtLeastOnce)
    |> queue.validate_policy

  let ready = process.new_subject()
  let results = process.new_subject()
  let default_gate = process.new_name("grind_policy_start_default_gate")
  let replay_gate = process.new_name("grind_policy_start_replay_gate")
  let _ =
    process.spawn(fn() {
      let assert Ok(Nil) = process.register(process.self(), default_gate)
      process.send(ready, PolicyStarterReady)
      let _ = process.receive(process.named_subject(default_gate), within: 5000)
      let _ = process.unregister(default_gate)
      let result =
        queue.start_manual_with_policy(database, workers, default_policy)
      let outcome = case result {
        Ok(consumer) -> {
          queue.stop(consumer)
          Ok(True)
        }
        Error(error) -> Error(error)
      }
      process.send(results, PolicyStartResult(outcome))
    })
  let _ =
    process.spawn(fn() {
      let assert Ok(Nil) = process.register(process.self(), replay_gate)
      process.send(ready, PolicyStarterReady)
      let _ = process.receive(process.named_subject(replay_gate), within: 5000)
      let _ = process.unregister(replay_gate)
      let result =
        queue.start_manual_with_policy(database, workers, replay_policy)
      let outcome = case result {
        Ok(consumer) -> {
          queue.stop(consumer)
          Ok(True)
        }
        Error(error) -> Error(error)
      }
      process.send(results, PolicyStartResult(outcome))
    })
  process.receive(ready, within: 5000) |> should.equal(Ok(PolicyStarterReady))
  process.receive(ready, within: 5000) |> should.equal(Ok(PolicyStarterReady))
  process.send(process.named_subject(default_gate), StartPolicyRace)
  process.send(process.named_subject(replay_gate), StartPolicyRace)
  let assert Ok(PolicyStartResult(first)) =
    process.receive(results, within: 10_000)
  let assert Ok(PolicyStartResult(second)) =
    process.receive(results, within: 10_000)
  let first_started = case first {
    Ok(True) -> True
    _ -> False
  }
  let second_started = case second {
    Ok(True) -> True
    _ -> False
  }
  let conflict_count =
    case first {
      Error(queue.QueueConfigurationFailed(
        postgres.ExpiredAttemptPolicyConflict,
      )) -> 1
      _ -> 0
    }
    + case second {
      Error(queue.QueueConfigurationFailed(
        postgres.ExpiredAttemptPolicyConflict,
      )) -> 1
      _ -> 0
    }
  case first_started, second_started {
    True, False -> Nil
    False, True -> Nil
    _, _ -> should.fail()
  }
  conflict_count |> should.equal(1)
  mark_database_test_executed("concurrent-policy-start-single-winner")
}

fn run_mixed_consumer_policy_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_mixed_queue_policy")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("policy-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("policy-output-v1", json.string, decode.string)
  let assert Ok(worker) =
    worker.define("policy.echo", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("durable-policy")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(default_consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(default_consumer) })
  let assert Ok(replay_policy) =
    queue.default_policy()
    |> queue.with_expired_attempt_policy(queue.ReplayAtLeastOnce)
    |> queue.validate_policy

  queue.start_manual_with_policy(database, workers, replay_policy)
  |> should.equal(
    Error(queue.QueueConfigurationFailed(postgres.ExpiredAttemptPolicyConflict)),
  )
  mark_database_test_executed("mixed-consumer-policy-rejected")
}

pub fn postgres_expiry_quarantine_is_bounded_per_attempt_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_bounded_quarantine_test(database_url)
  }
}

pub fn postgres_acknowledgement_rejects_exact_database_expiry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_exact_expiry_test(database_url)
  }
}

pub fn postgres_uncertain_replay_requires_audited_resolution_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_uncertain_resolution_test(database_url)
  }
}

fn run_uncertain_resolution_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_uncertain_resolution")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("resolve-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("resolve-output-v1", json.string, decode.string)
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
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 121, attempt_epoch = 6, attempt_owner = 'lost-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))

  postgres.resolve_uncertain(
    database,
    handle,
    "resolution-121",
    "on-call",
    "confirm external idempotency record before replay",
    postgres.AuthorizeReplay,
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  postgres.resolve_uncertain(
    database,
    handle,
    "resolution-121",
    "on-call",
    "confirm external idempotency record before replay",
    postgres.AuthorizeReplay,
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
    "resolution-121",
    "on-call",
    "confirm external idempotency record before replay",
    postgres.AuthorizeReplay,
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

pub fn postgres_resolution_rebind_checks_storage_owner_test() {
  case resolution_route_a_url(), resolution_route_b_url() {
    Ok(database_a_url), Ok(database_b_url) ->
      run_resolution_rebind_route_test(database_a_url, database_b_url)
    _, _ -> Nil
  }
}

fn run_resolution_rebind_route_test(
  database_a_url: String,
  database_b_url: String,
) -> Nil {
  let pool_a = process.new_name("grind_resolution_route_a")
  let pool_b = process.new_name("grind_resolution_route_b")
  let pool_a_after_restart =
    process.new_name("grind_resolution_route_a_rebound")
  let assert Ok(settings_a) =
    postgres.settings(database_a_url, pool_a) |> postgres.validate
  let assert Ok(settings_b) =
    postgres.settings(database_b_url, pool_b) |> postgres.validate
  let assert Ok(settings_a_after_restart) =
    postgres.settings(database_a_url, pool_a_after_restart) |> postgres.validate
  let assert Ok(database_a) = postgres.start(settings_a)
  use <- exception.defer(fn() { postgres.close(database_a) })
  let assert Ok(database_b) = postgres.start(settings_b)
  use <- exception.defer(fn() { postgres.close(database_b) })
  let assert Ok(database_a_after_restart) =
    postgres.start(settings_a_after_restart)
  use <- exception.defer(fn() { postgres.close(database_a_after_restart) })
  let assert Ok(Nil) = postgres.migrate(database_a)
  let assert Ok(Nil) = postgres.migrate(database_b)
  let assert Ok(input_codec) =
    worker.codec("route-recovery-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("route-recovery-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("route.recovery", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(other_worker) =
    worker.define("route.other", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(wrong_input_codec) =
    worker.codec("route-recovery-input-v2", json.int, decode.int)
  let assert Ok(wrong_codec_worker) =
    worker.define(
      "route.recovery",
      "v1",
      wrong_input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle_a) =
    postgres.submit(database_a, "route-recovery", definition, 3)
  let assert Ok(handle_b) =
    postgres.submit(database_b, "route-recovery", definition, 3)
  let durable_id = job.id_value(handle_a)
  job.id_value(handle_b) |> should.equal(durable_id)
  let connection_a = pog.named_connection(pool_a)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 302, attempt_epoch = 5, attempt_owner = 'lost-owner', lease_expires_at = clock_timestamp(), uncertain_at = clock_timestamp() WHERE storage_owner = $1 AND id = $2",
    )
    |> pog.parameter(pog.text(postgres.storage_owner(database_a)))
    |> pog.parameter(pog.int(durable_id))
    |> pog.execute(on: connection_a)
  postgres.bind_handle(database_a_after_restart, other_worker, durable_id)
  |> should.equal(Error(postgres.HandleBindWorkerContractMismatch))
  postgres.bind_handle(database_a_after_restart, wrong_codec_worker, durable_id)
  |> should.equal(Error(postgres.HandleBindCodecContractMismatch))
  let rebound = process.new_subject()
  let _ =
    process.spawn(fn() {
      process.send(
        rebound,
        postgres.bind_handle(database_a_after_restart, definition, durable_id),
      )
    })
  let assert Ok(Ok(recovered_handle)) = process.receive(rebound, within: 5000)
  postgres.state(database_a_after_restart, recovered_handle)
  |> should.equal(Ok(job.Uncertain))
  postgres.resolve_uncertain(
    database_a_after_restart,
    recovered_handle,
    "same-id-different-store",
    "operator",
    "rebind after storage owner restart",
    postgres.ConfirmSuccess("approved"),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Succeeded)))
  postgres.resolve_uncertain(
    database_a_after_restart,
    handle_b,
    "same-id-different-store",
    "operator",
    "rebind after storage owner restart",
    postgres.ConfirmSuccess("approved"),
  )
  |> should.equal(Error(postgres.ResolutionRouteMismatch))
  mark_database_test_executed("resolution-rebind-owner-checked")
}

fn run_resolution_payload_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_resolution_payload")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("resolution-payload-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("resolution-payload-output-v1", json.string, decode.string)
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
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'uncertain', attempt_id = 222, attempt_epoch = 3, attempt_owner = 'lost-payload-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(id))
    |> pog.execute(on: connection)
  let assert Ok(workers) = registry.new("resolution-payload")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  postgres.resolve_uncertain(
    database,
    handle,
    "resolution-payload-222",
    "on-call",
    "operator observed committed application key",
    postgres.ConfirmSuccess("approved"),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Succeeded)))
  postgres.resolve_uncertain(
    database,
    handle,
    "resolution-payload-222",
    "on-call",
    "operator observed committed application key",
    postgres.ConfirmSuccess("different"),
  )
  |> should.equal(Error(postgres.ResolutionCommandConflict))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("approved")))
  mark_database_test_executed("resolution-payload-bound")
}

fn run_exact_expiry_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_exact_expiry")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("exact-expiry-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("exact-expiry-output-v1", json.string, decode.string)
  let assert Ok(worker) =
    worker.define(
      "exact-expiry.echo",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) = postgres.submit(database, "exact-expiry", worker, 5)
  let #(id, _, _, _, _, _) = job.storage_fields(handle)
  let connection = pog.named_connection(pool_name)
  let assert Ok(boundary) =
    pog.query(
      "WITH database_time AS MATERIALIZED (SELECT clock_timestamp() AS instant), boundary AS MATERIALIZED (UPDATE grind_jobs AS job SET lease_expires_at = database_time.instant FROM database_time WHERE job.id = $1 RETURNING job.lease_expires_at, database_time.instant) SELECT lease_expires_at = instant, lease_expires_at > instant FROM boundary",
    )
    |> pog.parameter(pog.int(id))
    |> pog.returning({
      use exact <- decode.field(0, decode.bool)
      use acknowledgement_allowed <- decode.field(1, decode.bool)
      decode.success(#(exact, acknowledgement_allowed))
    })
    |> pog.execute(on: connection)
  let assert [#(exact, acknowledgement_allowed)] = boundary.rows
  exact |> should.equal(True)
  acknowledgement_allowed |> should.equal(False)
  mark_database_test_executed("exact-expiry-rejected")
}

fn run_bounded_quarantine_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_bounded_quarantine")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("bounded-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("bounded-output-v1", json.string, decode.string)
  let assert Ok(worker) =
    worker.define("bounded.echo", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("bounded-quarantine")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(first) =
    postgres.submit(database, "bounded-quarantine", worker, 1)
  let assert Ok(second) =
    postgres.submit(database, "bounded-quarantine", worker, 2)
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_owner = 'expired-owner', lease_expires_at = clock_timestamp() WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("bounded.echo"))
    |> pog.parameter(pog.text("bounded-quarantine"))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  let states = [
    postgres.state(database, first),
    postgres.state(database, second),
  ]
  let uncertain_count =
    list.count(states, fn(state) { state == Ok(job.Uncertain) })
  uncertain_count |> should.equal(1)
  let executing_count =
    list.count(states, fn(state) { state == Ok(job.Executing) })
  executing_count |> should.equal(1)
  mark_database_test_executed("quarantine-bounded-passed")
}

fn run_expired_attempt_quarantine_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_expired_quarantine")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("quarantine-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("quarantine-output-v1", json.string, decode.string)
  let probe = process.new_subject()
  let assert Ok(worker) =
    worker.define("quarantine.echo", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, WorkerInvoked)
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("expired-quarantine")
  let assert Ok(workers) = registry.register(workers, worker)
  let assert Ok(handle) =
    postgres.submit(database, "expired-quarantine", worker, 9)
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_owner = 'dead-consumer', lease_expires_at = clock_timestamp() WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("quarantine.echo"))
    |> pog.parameter(pog.text("expired-quarantine"))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  process.receive(probe, within: 0) |> should.equal(Error(Nil))
  postgres.arguments(database, handle) |> should.equal(Ok(9))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired attempt requires outcome reconciliation",
    )),
  )
  let assert Ok(expired_attempt) =
    pog.query(
      "SELECT attempt_id, attempt_epoch, attempt_owner, lease_expires_at <= clock_timestamp(), failure_description, uncertain_at IS NOT NULL FROM grind_jobs WHERE worker_id = $1 AND queue = $2",
    )
    |> pog.parameter(pog.text("quarantine.echo"))
    |> pog.parameter(pog.text("expired-quarantine"))
    |> pog.returning({
      use attempt_id <- decode.field(0, decode.int)
      use attempt_epoch <- decode.field(1, decode.int)
      use attempt_owner <- decode.field(2, decode.string)
      use lease_expired <- decode.field(3, decode.bool)
      use reason <- decode.field(4, decode.string)
      use uncertainty_time_recorded <- decode.field(5, decode.bool)
      decode.success(#(
        attempt_id,
        attempt_epoch,
        attempt_owner,
        lease_expired,
        reason,
        uncertainty_time_recorded,
      ))
    })
    |> pog.execute(on: connection)
  let assert [
    #(
      attempt_id,
      attempt_epoch,
      attempt_owner,
      lease_expired,
      reason,
      uncertainty_time_recorded,
    ),
  ] = expired_attempt.rows
  attempt_id |> should.not_equal(0)
  attempt_epoch |> should.equal(1)
  attempt_owner |> should.equal("dead-consumer")
  lease_expired |> should.equal(True)
  reason |> should.equal("expired attempt requires outcome reconciliation")
  uncertainty_time_recorded |> should.equal(True)
  mark_database_test_executed("expired-attempt-quarantine-passed")
}

fn settle_attempt(
  finished: process.Subject(Nil),
  release: process.Subject(LeaseCommand),
  reply: process.Subject(Result(Bool, queue.ProcessError)),
) -> Nil {
  case process.receive(finished, within: 0) {
    Ok(Nil) -> Nil
    Error(Nil) -> {
      process.send(release, ReleaseAttempt)
      let _ = process.receive(reply, within: 5000)
      Nil
    }
  }
}

fn run_queue_batch_policy_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_queue_batch")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("batch-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("batch-output-v1", json.int, decode.int)
  let assert Ok(increment) =
    worker.define("batch.increment", "v1", input_codec, output_codec, fn(value) {
      Ok(value + 1)
    })
  let assert Ok(workers) = registry.new("batch-policy")
  let assert Ok(workers) = registry.register(workers, increment)
  let assert Ok(first) = postgres.submit(database, "batch-policy", increment, 1)
  let assert Ok(second) =
    postgres.submit(database, "batch-policy", increment, 2)
  let assert Ok(third) = postgres.submit(database, "batch-policy", increment, 3)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_jobs_per_poll(2)
    |> queue.validate_policy
  let assert Ok(consumer) =
    queue.start_manual_with_policy(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_available(consumer)
  |> should.equal(queue.BatchCompleted(2))
  postgres.state(database, first) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, second) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, third) |> should.equal(Ok(job.Queued))
  queue.process_available(consumer)
  |> should.equal(queue.BatchCompleted(1))
  postgres.state(database, third) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("queue-batch-policy-passed")
}

fn run_automatic_queue_fairness_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_queue_fairness")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("fairness-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("fairness-output-v1", json.string, decode.string)
  let probe = process.new_subject()
  let assert Ok(incompatible) =
    worker.define("queue.drift", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, WorkerInvoked)
      Ok(int.to_string(value))
    })
  let assert Ok(later) =
    worker.define("queue.later", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, LaterWorkerInvoked)
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("automatic-fairness")
  let assert Ok(workers) = registry.register(workers, incompatible)
  let assert Ok(workers) = registry.register(workers, later)
  let assert Ok(incompatible_handle) =
    postgres.submit(database, "automatic-fairness", incompatible, 1)
  let assert Ok(later_handle) =
    postgres.submit(database, "automatic-fairness", later, 2)
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET output_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("fairness-output-v2"))
    |> pog.parameter(pog.text("queue.drift"))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  process.receive(probe, within: 5000)
  |> should.equal(Ok(LaterWorkerInvoked))
  // A request queued to the same actor returns only after the handler and its
  // acknowledgement have completed, so the following state reads are committed.
  queue.process_one(consumer)
  |> should.equal(Ok(False))
  postgres.state(database, incompatible_handle)
  |> should.equal(Ok(job.ContractMismatch))
  postgres.state(database, later_handle)
  |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("automatic-contract-skip-passed")
}

fn run_business_failure_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_business_failure")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("lookup-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("lookup-output-v1", json.string, decode.string)
  let assert Ok(error_codec) =
    worker.codec(
      "lookup-error-v1",
      encode_lookup_failure,
      decode_lookup_failure(),
    )
  let assert Ok(lookup) =
    worker.define_with_error_codec(
      "accounts.lookup.failure",
      "v1",
      input_codec,
      output_codec,
      error_codec,
      fn(account_id) { Error(AccountMissing(account_id)) },
    )
  let assert Ok(workers) = registry.new("business-failures")
  let assert Ok(workers) = registry.register(workers, lookup)
  let assert Ok(handle) =
    postgres.submit(database, "business-failures", lookup, 42)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(Ok(True))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.BusinessFailedWith(AccountMissing(42))))
  postgres.state(database, handle)
  |> should.equal(Ok(job.BusinessFailed))
  mark_database_test_executed("typed-business-failure-passed")
}

fn encode_lookup_failure(error: LookupFailure) -> json.Json {
  case error {
    AccountMissing(account_id) ->
      json.object([
        #("kind", json.string("account_missing")),
        #("account_id", json.int(account_id)),
      ])
  }
}

fn decode_lookup_failure() -> decode.Decoder(LookupFailure) {
  use kind <- decode.field("kind", decode.string)
  use account_id <- decode.field("account_id", decode.int)
  case kind {
    "account_missing" -> decode.success(AccountMissing(account_id))
    _ -> decode.failure(AccountMissing(account_id), "known lookup failure kind")
  }
}

fn run_output_codec_mismatch_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_codec_mismatch")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("mismatch-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("mismatch-output-v1", json.string, decode.string)
  let probe = process.new_subject()
  let assert Ok(effect) =
    worker.define("codec.drift", "v1", input_codec, output_codec, fn(value) {
      process.send(probe, WorkerInvoked)
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("codec-drift")
  let assert Ok(workers) = registry.register(workers, effect)
  let assert Ok(handle) = postgres.submit(database, "codec-drift", effect, 9)
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query("UPDATE grind_jobs SET output_version = $1 WHERE worker_id = $2")
    |> pog.parameter(pog.text("mismatch-output-v2"))
    |> pog.parameter(pog.text("codec.drift"))
    |> pog.execute(on: connection)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(
    Error(
      queue.QueueProcessFailed(postgres.QueueCodecMismatch(
        kind: "output",
        expected: "mismatch-output-v2",
        actual: "mismatch-output-v1",
      )),
    ),
  )
  postgres.state(database, handle)
  |> should.equal(Ok(job.ContractMismatch))
  process.receive(probe, within: 0)
  |> should.equal(Error(Nil))
  mark_database_test_executed("codec-contract-rejected")
}

fn run_postgres_queue_success_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_queue_success")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("queue-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("queue-output-v1", json.string, decode.string)
  let assert Ok(increment) =
    worker.define("queue.increment", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value + 1))
    })
  let assert Ok(workers) = registry.new("default")
  let assert Ok(workers) = registry.register(workers, increment)
  let assert Ok(handle) = postgres.submit(database, "default", increment, 41)
  let assert Ok(consumer) = queue.start_manual(database, workers)
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer)
  |> should.equal(Ok(True))
  postgres.state(database, handle)
  |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("42")))
  mark_database_test_executed("committed-success-passed")
}

fn run_incompatible_schema_test(database_url: String) -> Nil {
  let pool_name = process.new_name("grind_bad_schema")
  let assert Ok(validated) =
    postgres.settings(database_url, pool_name) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let connection = pog.named_connection(pool_name)
  let assert Ok(_) =
    pog.query(
      "CREATE TABLE grind_jobs (id bigserial PRIMARY KEY, storage_owner text NOT NULL, queue text NOT NULL, worker_id text NOT NULL, worker_version text NOT NULL, input_version text NOT NULL, input jsonb NOT NULL, output_version text NOT NULL, output jsonb, error_version text, error jsonb, state text NOT NULL CONSTRAINT grind_jobs_state_check CHECK (state <> 'executing' AND state <> 'succeeded'), available_at timestamptz NOT NULL, inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(), attempt_id bigint, attempt_epoch bigint NOT NULL DEFAULT 0, attempt_owner text, lease_expires_at timestamptz, attempt_count bigint NOT NULL DEFAULT 0, failure_description text)",
    )
    |> pog.execute(on: connection)

  postgres.migrate(database)
  |> should.equal(Error(postgres.IncompatibleSchema))
  mark_database_test_executed("incompatible-schema-rejected")
}
