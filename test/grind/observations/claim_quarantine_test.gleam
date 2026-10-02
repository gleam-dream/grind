import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{None, Some}
import gleeunit/should
import grind/internal/attempt
import grind/job
import grind/observation
import grind/postgres
import grind/queue
import grind/registry
import grind/support/consumer.{manual_policy}
import grind/support/env.{
  database_url, mark_database_test_executed, quarantine_url, queue_database_url,
}
import grind/support/observers.{detach}
import grind/support/submissions.{
  unique_test_worker, unique_test_worker_versioned,
}
import grind/support/unique_fixture.{unique_test_suffix}
import grind/worker
import pog
import sinal

pub fn postgres_claimed_observation_emission_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_claimed_observation_emission_test(database_url)
  }
}

/// The claim itself autocommits as a single fenced `UPDATE ... RETURNING`,
/// so a returned row is already the proof of commit: `attempt_id`/`epoch`
/// come from that same row, `attempt` is the row's own `attempt_count`, and
/// `previous_state` is what the row held immediately before this claim.
fn run_claimed_observation_emission_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("claimed-emission-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("claimed-emission-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "claimed.emission",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("claimed-emission")
  let assert Ok(workers) = registry.register(workers, definition)

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.claimed(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(handle) =
    postgres.submit(database, "claimed-emission", definition, 5)
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "claimed-emission",
      workers,
      "claimed-emission-owner",
      30_000,
    )
  let #(claimed_id, attempt_id, epoch) = attempt.claim_identity(claimed)
  claimed_id |> should.equal(job.id_value(handle))

  let assert Ok(#(measurements, metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.ClaimedMeasurements(count: 1))
  metadata.ref.job_id |> should.equal(claimed_id)
  metadata.ref.queue |> should.equal("claimed-emission")
  metadata.ref.worker_id |> should.equal("claimed.emission")
  metadata.ref.worker_version |> should.equal("v1")
  metadata.attempt.attempt_id |> should.equal(attempt_id)
  metadata.attempt.epoch |> should.equal(epoch)
  metadata.attempt.attempt |> should.equal(1)
  metadata.previous_state |> should.equal(job.Queued)
  mark_database_test_executed("claimed-observation-emission-passed")
}

pub fn postgres_claimed_observation_absent_when_nothing_due_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_claimed_observation_absent_when_nothing_due_test(database_url)
  }
}

/// `claim_one` returning `Ok(None)` (nothing due) never calls the emit path
/// at all — proven here by a real claim through the exact same producer
/// arriving as the very next `claimed` observation.
fn run_claimed_observation_absent_when_nothing_due_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("claimed-absent-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("claimed-absent-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("claimed.absent", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(workers) = registry.new("claimed-absent")
  let assert Ok(workers) = registry.register(workers, definition)

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.claimed(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  attempt.claim_one(
    database,
    "claimed-absent",
    workers,
    "claimed-absent-owner",
    30_000,
  )
  |> should.equal(Ok(None))
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  let assert Ok(sentinel_handle) =
    postgres.submit(database, "claimed-absent", definition, 6)
  let assert Ok(Some(sentinel_claimed)) =
    attempt.claim_one(
      database,
      "claimed-absent",
      workers,
      "claimed-absent-owner",
      30_000,
    )
  let #(sentinel_id, _, _) = attempt.claim_identity(sentinel_claimed)
  sentinel_id |> should.equal(job.id_value(sentinel_handle))
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(sentinel_id)
  mark_database_test_executed(
    "claimed-observation-absent-when-nothing-due-passed",
  )
}

pub fn postgres_quarantined_observation_emission_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_quarantined_observation_emission_test(database_url)
  }
}

/// The claim-time quarantine scan finds at most one abandoned attempt per
/// call (`LIMIT 1`); this drives it twice to observe one event per row, with
/// `cancellation_was_requested` distinguishing an ordinary abandoned attempt
/// from one that also had a pending cancellation request.
fn run_quarantined_observation_emission_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("quarantined-emission-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("quarantined-emission-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "quarantined.emission",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("quarantined-emission")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle_a) =
    postgres.submit(database, "quarantined-emission", definition, 1)
  let assert Ok(handle_b) =
    postgres.submit(database, "quarantined-emission", definition, 2)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_count = 1, attempt_owner = 'dead-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle_a)))
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_count = 1, attempt_owner = 'dead-consumer', lease_expires_at = clock_timestamp(), cancel_requested_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle_b)))
    |> pog.execute(on: connection)

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.quarantined(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  attempt.claim_one(
    database,
    "quarantined-emission",
    workers,
    "quarantined-emission-owner",
    30_000,
  )
  |> should.equal(Ok(None))
  let assert Ok(#(measurements_a, metadata_a)) =
    process.receive(signal, within: 5000)
  measurements_a |> should.equal(observation.QuarantinedMeasurements(count: 1))
  metadata_a.ref.job_id |> should.equal(job.id_value(handle_a))
  metadata_a.ref.queue |> should.equal("quarantined-emission")
  metadata_a.ref.worker_id |> should.equal("quarantined.emission")
  metadata_a.attempt.epoch |> should.equal(1)
  metadata_a.attempt.attempt |> should.equal(1)
  metadata_a.cancellation_was_requested |> should.equal(False)

  attempt.claim_one(
    database,
    "quarantined-emission",
    workers,
    "quarantined-emission-owner",
    30_000,
  )
  |> should.equal(Ok(None))
  let assert Ok(#(_, metadata_b)) = process.receive(signal, within: 5000)
  metadata_b.ref.job_id |> should.equal(job.id_value(handle_b))
  metadata_b.cancellation_was_requested |> should.equal(True)

  postgres.state(database, handle_a) |> should.equal(Ok(job.Uncertain))
  postgres.state(database, handle_b) |> should.equal(Ok(job.Uncertain))
  mark_database_test_executed("quarantined-observation-emission-passed")
}

pub fn postgres_quarantined_observation_absent_when_nothing_expired_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_quarantined_observation_absent_test(database_url)
  }
}

/// The quarantine scan runs on every `claim_one` call, whether or not
/// anything is actually expired; an ordinary claim with nothing to quarantine
/// must never emit — proven by a genuinely quarantined row through the exact
/// same producer arriving as the very next `quarantined` observation.
fn run_quarantined_observation_absent_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("quarantined-absent-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("quarantined-absent-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "quarantined.absent",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("quarantined-absent")
  let assert Ok(workers) = registry.register(workers, definition)

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.quarantined(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let assert Ok(handle) =
    postgres.submit(database, "quarantined-absent", definition, 3)
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "quarantined-absent",
      workers,
      "quarantined-absent-owner",
      30_000,
    )
  let #(claimed_id, _, _) = attempt.claim_identity(claimed)
  claimed_id |> should.equal(job.id_value(handle))
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(claimed_id))
    |> pog.execute(on: connection)
  attempt.claim_one(
    database,
    "quarantined-absent",
    workers,
    "quarantined-absent-owner",
    30_000,
  )
  |> should.equal(Ok(None))
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(claimed_id)
  mark_database_test_executed("quarantined-observation-absent-passed")
}

type OrderingEvent {
  ClaimedOrderingEvent(attempt_id: Int)
  AcknowledgedOrderingEvent(attempt_id: Int)
}

pub fn postgres_claimed_observation_precedes_acknowledged_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_claimed_precedes_acknowledged_test(database_url)
  }
}

/// This scenario observes `[grind, job, claimed]` before
/// `[grind, job, acknowledged]`, matched by their shared `attempt_id`.
/// The coordinator emits the claim and the attempt process emits the ACK.
/// Sinal's per-producer FIFO contract does not guarantee order across those
/// producers; this test records the sequence observed in this scenario.
fn run_claimed_precedes_acknowledged_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("ordering-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("ordering-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "ordering.claimed.acknowledged",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("ordering-claimed-acknowledged")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(_) =
    postgres.submit(database, "ordering-claimed-acknowledged", definition, 7)

  let signal = process.new_subject()
  let claimed_attachment =
    sinal.observe(observation.claimed(), fn(_measurements, metadata) {
      process.send(signal, ClaimedOrderingEvent(metadata.attempt.attempt_id))
    })
  use <- exception.defer(fn() { detach(claimed_attachment) })
  let acknowledged_attachment =
    sinal.observe(observation.acknowledged(), fn(_measurements, metadata) {
      process.send(
        signal,
        AcknowledgedOrderingEvent(metadata.attempt.attempt_id),
      )
    })
  use <- exception.defer(fn() { detach(acknowledged_attachment) })

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(True))

  let assert Ok(ClaimedOrderingEvent(claimed_attempt_id)) =
    process.receive(signal, within: 5000)
  let assert Ok(AcknowledgedOrderingEvent(acknowledged_attempt_id)) =
    process.receive(signal, within: 5000)
  claimed_attempt_id |> should.equal(acknowledged_attempt_id)
  mark_database_test_executed("claimed-precedes-acknowledged-ordering-passed")
}

// -- Decision A (2026-09-25): cross-version quarantine coverage -------------
//
// `docs/RELEASE-READINESS.md`, "Old-version executing rows". Before this
// decision, a consumer's per-poll quarantine scan (`claim_one`, via
// `quarantine_expired_in_queue`) only ever considered rows whose
// `(worker_id, worker_version)` the polling consumer itself registered, so
// after a worker-version bump an old version's still-`executing` row (its
// consumer long gone) was never quarantined by anything: the new consumer's
// scan skipped it (unregistered identity), and nothing else ever looked at
// it. The fix drops that identity filter from the per-queue scan entirely —
// quarantining never decodes or runs code, so there is no reason to gate it
// on registration — and adds a public, storage-owner-wide
// `postgres.quarantine_expired` for a queue no consumer polls at all.

/// (i) A job claimed under worker version `v1`, whose consumer is long gone,
/// still gets quarantined by a *different* consumer that now only registers
/// `v2` of the same worker id, once its lease has expired — proving the
/// per-queue scan no longer restricts itself to registered identities.
pub fn postgres_quarantine_covers_unregistered_worker_version_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_quarantine_covers_unregistered_worker_version_test(database_url)
  }
}

fn run_quarantine_covers_unregistered_worker_version_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)

  let worker_id = "old-version.echo-" <> suffix
  let worker_v1 = unique_test_worker_versioned(worker_id, "v1")
  let worker_v2 = unique_test_worker_versioned(worker_id, "v2")
  let test_queue = "old-version-quarantine-" <> suffix

  let assert Ok(handle) = postgres.submit(database, test_queue, worker_v1, 1)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_owner = 'retired-v1-consumer', lease_expires_at = clock_timestamp() WHERE worker_id = $1 AND worker_version = 'v1' AND queue = $2",
    )
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(test_queue))
    |> pog.execute(on: connection)

  // Only `v2` is ever registered from here on — the `v1` consumer that
  // claimed this row is gone for good, exactly as after a worker-version
  // bump. Nothing in this registry can ever claim the `v1` row, but this
  // consumer's own quarantine scan must still see it.
  let assert Ok(workers_v2) = registry.new(test_queue)
  let assert Ok(workers_v2) = registry.register(workers_v2, worker_v2)
  let assert Ok(consumer) = queue.start(database, workers_v2, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.outcome(database, handle)
  |> should.equal(
    Ok(job.ReconciliationRequired(
      "expired attempt requires outcome reconciliation",
    )),
  )
  mark_database_test_executed(
    "quarantine-covers-unregistered-worker-version-passed",
  )
}

/// (ii) The public, storage-owner-wide `quarantine_expired` sweeps an
/// expired `executing` row in a queue no consumer ever polls at all — the
/// per-queue scan above only ever runs as part of some consumer's own
/// `claim_one`, so a queue nothing polls needs this separate operation. Also
/// proves `limit` validation and bounding (a non-positive limit is rejected
/// before touching storage, and a limit smaller than the number of expired
/// rows quarantines only that many, idempotent to call again for the
/// remainder), that the sweep genuinely crosses queues (two different
/// unpolled queues, one row each, both eventually quarantined), and that
/// the `[grind, job, quarantined]` observation's own `queue` field names
/// each row's real queue — never a hardcoded or swapped one — since the
/// global sweep, unlike the per-queue scan, spans more than one queue in a
/// single call.
///
/// Runs against a database dedicated to this test alone
/// (`GRIND_TEST_QUARANTINE_URL`), not the shared `GRIND_TEST_DATABASE_URL`:
/// `quarantine_expired` sweeps every expired `executing` row across the
/// whole schema, so sharing a database with dozens of other tests would
/// make this test's own row/observation counts depend on whatever unrelated
/// expired rows those other tests happen to leave behind at the moment this
/// one runs — a dedicated database is a dedicated schema, immune to that
/// ordering.
pub fn postgres_quarantine_expired_global_operation_test() {
  case quarantine_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_quarantine_expired_global_test(database_url)
  }
}

fn run_quarantine_expired_global_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let connection = postgres.connection(database)

  let worker_def = unique_test_worker("unpolled-queue.echo-" <> suffix)
  let queue_a = "unpolled-quarantine-a-" <> suffix
  let queue_b = "unpolled-quarantine-b-" <> suffix

  let assert Ok(first) = postgres.submit(database, queue_a, worker_def, 1)
  let assert Ok(second) = postgres.submit(database, queue_b, worker_def, 2)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = nextval('grind_attempts_id_seq'), attempt_epoch = 1, attempt_owner = 'no-consumer-ever-polled', lease_expires_at = clock_timestamp() WHERE id = ANY($1::bigint[])",
    )
    |> pog.parameter(
      pog.array(pog.int, [job.id_value(first), job.id_value(second)]),
    )
    |> pog.execute(on: connection)

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.quarantined(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  postgres.quarantine_expired(database, limit: 0)
  |> should.equal(Error(postgres.NonPositiveLimit))
  postgres.quarantine_expired(database, limit: -1)
  |> should.equal(Error(postgres.NonPositiveLimit))
  postgres.state(database, first) |> should.equal(Ok(job.Executing))
  postgres.state(database, second) |> should.equal(Ok(job.Executing))
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  postgres.quarantine_expired(database, limit: 1) |> should.equal(Ok(1))
  let assert Ok(#(measurements_one, metadata_one)) =
    process.receive(signal, within: 5000)
  measurements_one
  |> should.equal(observation.QuarantinedMeasurements(count: 1))
  let states_after_one = [
    postgres.state(database, first),
    postgres.state(database, second),
  ]
  list.count(states_after_one, fn(state) { state == Ok(job.Uncertain) })
  |> should.equal(1)
  list.count(states_after_one, fn(state) { state == Ok(job.Executing) })
  |> should.equal(1)
  // Whichever row the sweep picked first (by `id`, not scoped to either
  // queue), the observation's own `queue` field must name that exact row's
  // real queue.
  case metadata_one.ref.job_id == job.id_value(first) {
    True -> metadata_one.ref.queue |> should.equal(queue_a)
    False -> {
      metadata_one.ref.job_id |> should.equal(job.id_value(second))
      metadata_one.ref.queue |> should.equal(queue_b)
    }
  }

  postgres.quarantine_expired(database, limit: 10) |> should.equal(Ok(1))
  let assert Ok(#(_, metadata_two)) = process.receive(signal, within: 5000)
  postgres.state(database, first) |> should.equal(Ok(job.Uncertain))
  postgres.state(database, second) |> should.equal(Ok(job.Uncertain))
  case metadata_two.ref.job_id == job.id_value(first) {
    True -> metadata_two.ref.queue |> should.equal(queue_a)
    False -> {
      metadata_two.ref.job_id |> should.equal(job.id_value(second))
      metadata_two.ref.queue |> should.equal(queue_b)
    }
  }
  // The sweep genuinely crossed queues -- both distinct queues were the
  // subject of one event each, not both events reporting the same queue.
  list.contains([metadata_one.ref.queue, metadata_two.ref.queue], queue_a)
  |> should.equal(True)
  list.contains([metadata_one.ref.queue, metadata_two.ref.queue], queue_b)
  |> should.equal(True)

  // Nothing left to quarantine now.
  postgres.quarantine_expired(database, limit: 10) |> should.equal(Ok(0))
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  mark_database_test_executed("quarantine-expired-global-operation-passed")
}
