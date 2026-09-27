import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/list
import gleam/option.{Some}
import gleeunit/should
import grind/internal/attempt
import grind/job
import grind/observation
import grind/postgres
import grind/queue
import grind/registry
import grind/support/ack_queries.{count_acknowledgements_for_job}
import grind/support/concurrency.{
  ClaimGateAcquired, ClaimGateReleased, ReleaseAttempt, spawn_lock_holder,
  spawn_submit, unique_test_lock_key,
}
import grind/support/consumer.{manual_policy}
import grind/support/env.{
  mark_database_test_executed, queue_database_url, repeatable_read_url,
}
import grind/support/lock_wait.{await_lock_wait_counts}
import grind/support/observation_fixtures.{
  AcknowledgedSignal, DroppedSignal, OverflowGateEntered,
  assert_next_observation_is_sentinel, attach_acknowledged_observer,
  attach_dropped_observer, drain_subject_count, register_sentinel_worker,
}
import grind/support/observers.{detach}
import grind/support/unique_fixture.{unique_test_suffix, with_unique_databases}
import grind/worker
import pog
import sinal

pub fn postgres_acknowledged_observation_overflow_reports_dropped_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_overflow_reports_dropped_test(database_url)
  }
}

/// Overflow: a `Database` started with `observation_capacity(1)` and a
/// gate-blocked acknowledged handler holds the forwarder's single in-flight
/// slot; job B's own `[grind, job, claimed]` and `[grind, job, acknowledged]`
/// observations are both forwarded while that slot is still held (one
/// `Forwarder` per `Database` carries every `[grind, job, *]` event, not one
/// per event kind), so both exceed capacity and are dropped — coalesced into
/// one `[sinal, forwarder, dropped]` report with `rejected: 2` — never
/// affecting either job's committed state. Job A's own `claimed` observation
/// is not among them: it is emitted and drained before A's `acknowledged`
/// handler ever blocks the forwarder.
fn run_acknowledged_observation_overflow_reports_dropped_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url)
    |> postgres.with_observation_capacity(1)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("observation-overflow-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("observation-overflow-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "observation.overflow",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok("overflow-" <> int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("observation-overflow")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle_a) =
    postgres.submit(database, "observation-overflow", definition, 1)
  let assert Ok(handle_b) =
    postgres.submit(database, "observation-overflow", definition, 2)

  // See the isolation test above: the release gate is created *inside* the
  // handler so it is owned by the forwarder process that receives it.
  let gate_entered = process.new_subject()
  let acknowledged_attachment =
    attach_acknowledged_observer("overflow", fn(_measurements, _metadata) {
      let gate = process.new_subject()
      process.send(gate_entered, OverflowGateEntered(gate))
      let assert Ok(Nil) = process.receive(gate, within: 10_000)
      Nil
    })
  use <- exception.defer(fn() { detach(acknowledged_attachment) })
  let dropped_signal = process.new_subject()
  let dropped_attachment =
    attach_dropped_observer("overflow", fn(measurements, metadata) {
      process.send(dropped_signal, DroppedSignal(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(dropped_attachment) })

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  // Job A occupies the forwarder's only in-flight slot and blocks there.
  queue.process_one(consumer) |> should.equal(Ok(True))
  let assert Ok(OverflowGateEntered(gate)) =
    process.receive(gate_entered, within: 5000)

  // Job B's own acknowledgement still commits normally; only its forwarded
  // observation is dropped for exceeding capacity while A's slot is held.
  queue.process_one(consumer) |> should.equal(Ok(True))

  process.send(gate, Nil)
  let assert Ok(DroppedSignal(dropped_measurements, dropped_metadata)) =
    process.receive(dropped_signal, within: 10_000)
  dropped_measurements.rejected |> should.equal(2)
  dropped_metadata.forwarder |> should.not_equal("")

  postgres.state(database, handle_a) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, handle_b) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed(
    "acknowledged-observation-overflow-reports-dropped-passed",
  )
}

pub fn postgres_acknowledged_observation_raising_handler_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_raising_handler_test(database_url)
  }
}

/// A handler that raises never affects the job's own committed outcome:
/// native `:telemetry` isolates the raise (detaching the faulty handler),
/// and by the time any handler runs at all, `forwarder.emit`'s own hand-off
/// to the forwarder has already returned, decoupled from the coordinator.
fn run_acknowledged_observation_raising_handler_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("observation-raising-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("observation-raising-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define(
      "observation.raising",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok("raising-" <> int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("observation-raising")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(handle) =
    postgres.submit(database, "observation-raising", definition, 7)
  let attachment =
    attach_acknowledged_observer("raising", fn(_measurements, _metadata) {
      panic as "deliberately raising acknowledged observer"
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, handle) |> should.equal(Ok(job.Succeeded))
  postgres.outcome(database, handle)
  |> should.equal(Ok(job.SucceededWith("raising-7")))
  mark_database_test_executed(
    "acknowledged-observation-raising-handler-outcome-unchanged-passed",
  )
}

pub fn postgres_forwarder_crash_loop_does_not_stop_the_pool_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_forwarder_crash_loop_test(database_url)
  }
}

/// Isolation hole (coordinator review, round 1 follow-up): a handler that
/// itself exits or is killed is not isolated by native `:telemetry` the way
/// a raise is (see `sinal/forwarder`'s own module documentation) and can
/// take the forwarder process down. Before the fix, the forwarder was a
/// permanent sibling of the PostgreSQL pool under one `OneForOne` supervisor
/// with the default restart intensity (2 restarts / 5 seconds); repeatedly
/// killing the forwarder exhausted that shared supervisor's own restart
/// budget, which then terminated *all* of its children, including the pool
/// — acks and submits after that point failed with the pool gone. The fix
/// (`grind/postgres.start`) nests the forwarder under its own supervisor,
/// added to the root as a `Temporary` child: a `Temporary` child's
/// termination is never restarted and never counts toward the parent
/// supervisor's own restart intensity, so the forwarder subtree exhausting
/// itself can never affect the pool. This test drives enough acknowledged
/// events, each killing whichever forwarder incarnation handles it, to
/// exceed the default restart intensity well within its period, then proves
/// the pool still serves a fresh submit and ack afterward.
fn run_forwarder_crash_loop_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("forwarder-crash-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec("forwarder-crash-output-v1", json.string, decode.string)
  let assert Ok(definition) =
    worker.define("forwarder.crash", "v1", input_codec, output_codec, fn(value) {
      Ok("crash-" <> int.to_string(value))
    })
  let assert Ok(workers) = registry.new("forwarder-crash")
  let assert Ok(workers) = registry.register(workers, definition)

  let assert Ok(id) = sinal.handler_id("grind-test-forwarder-crash-loop")
  let assert Ok(attachment) =
    sinal.observe(id, observation.acknowledged(), fn(_measurements, _metadata) {
      process.kill(process.self())
    })
  use <- exception.defer(fn() { detach(attachment) })

  // A second, non-killing handler on the same descriptor: since both
  // handlers run in whichever forwarder incarnation is currently live,
  // this one is delivered exactly when the killing handler above is —
  // giving a direct runtime count of how many `acknowledged` events the
  // forwarder actually managed to deliver, instead of only inferring
  // "the restart budget must be exhausted by now" from elapsed sleep time.
  let observed = process.new_subject()
  let assert Ok(counter_id) =
    sinal.handler_id("grind-test-forwarder-crash-loop-counter")
  let assert Ok(counter_attachment) =
    sinal.observe(counter_id, observation.acknowledged(), fn(_, _) {
      process.send(observed, Nil)
    })
  use <- exception.defer(fn() { detach(counter_attachment) })

  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })

  // Six acknowledged events, each killing the forwarder incarnation that
  // handles it, well exceeds the default restart intensity (2 restarts / 5s)
  // while staying comfortably inside its 5-second period. A short sleep
  // between each keeps each emit landing on a live incarnation rather than
  // racing a mid-flight restart.
  list.repeat(Nil, 6)
  |> list.each(fn(_) {
    let assert Ok(_) =
      postgres.submit(database, "forwarder-crash", definition, 1)
    queue.process_one(consumer) |> should.equal(Ok(True))
    process.sleep(80)
  })

  // Degraded state actually reached, not assumed: strictly fewer than six
  // of the loop's own acknowledgements were ever delivered to either
  // handler, proving the nested supervisor's restart budget was genuinely
  // exhausted partway through — every acknowledgement after that point got
  // `ForwarderUnavailable` and never reached `:telemetry` dispatch at all.
  let received_during_loop = drain_subject_count(observed, 0)
  { received_during_loop < 6 } |> should.equal(True)

  // The pool must still be alive and serving submit/ack after the
  // forwarder's own restart budget is long exhausted.
  let assert Ok(final_handle) =
    postgres.submit(database, "forwarder-crash", definition, 99)
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, final_handle) |> should.equal(Ok(job.Succeeded))

  // And the degraded state is permanent, not transient: this job committed
  // and the pool is plainly healthy, yet its own acknowledgement produced
  // zero further deliveries — a `Temporary` child's exhausted subtree is
  // never restarted, so observations do not quietly come back on their own.
  drain_subject_count(observed, 0) |> should.equal(0)
  mark_database_test_executed("forwarder-crash-loop-pool-survives-passed")
}

pub fn postgres_acknowledged_observation_reconciled_on_sequential_duplicate_ack_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_reconciled_sequential_duplicate_test(
        database_url,
      )
  }
}

/// Coordinator review, round 1 follow-up: proves the *early* receipt-match
/// site — the `matching_acknowledgement` check at the very top of
/// `acknowledge_transaction`, before any `UPDATE` is attempted — is also
/// `Reconciled`, not just `reconcile_unknown_ack`'s post-lost-reply site
/// already proven in round 1. Reached deterministically, with no forced
/// concurrency needed: a second, purely sequential call to
/// `acknowledge_claim` with the exact same `ClaimedJob`/`Execution` finds
/// the first call's own receipt already durably recorded. Named mutation:
/// hardcoding `via_receipt_match: False` at this site makes this test fail
/// (the second event would report `Replied`, not `Reconciled`).
fn run_acknowledged_observation_reconciled_sequential_duplicate_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("observation-dup-sequential-input-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "observation-dup-sequential-output-v1",
      json.string,
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "observation.dup.sequential",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("observation-dup-sequential")
  let assert Ok(workers) = registry.register(workers, definition)
  let #(workers, sentinel_worker) =
    register_sentinel_worker(workers, "dup-sequential")
  let assert Ok(_handle) =
    postgres.submit(database, "observation-dup-sequential", definition, 6)
  let attempt_owner = "observation-dup-sequential-owner"
  let assert Ok(Some(claimed)) =
    attempt.claim_one(
      database,
      "observation-dup-sequential",
      workers,
      attempt_owner,
      30_000,
    )
  let execution = attempt.execute_claim(claimed)

  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer("dup-sequential", fn(measurements, metadata) {
      process.send(signal, AcknowledgedSignal(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  attempt.acknowledge(
    database,
    "observation-dup-sequential",
    attempt_owner,
    claimed,
    execution,
  )
  |> should.equal(Ok(True))
  let assert Ok(AcknowledgedSignal(_, first_metadata)) =
    process.receive(signal, within: 5000)
  first_metadata.confirmation |> should.equal(observation.Replied)
  first_metadata.committed_state |> should.equal(job.Succeeded)

  // The exact same claim/execution, acknowledged a second time: this is the
  // early receipt-match site, reached with no concurrency at all.
  attempt.acknowledge(
    database,
    "observation-dup-sequential",
    attempt_owner,
    claimed,
    execution,
  )
  |> should.equal(Ok(True))
  let assert Ok(AcknowledgedSignal(_, second_metadata)) =
    process.receive(signal, within: 5000)
  second_metadata.confirmation |> should.equal(observation.Reconciled)
  second_metadata.committed_state |> should.equal(job.Succeeded)
  second_metadata.command_id |> should.equal(first_metadata.command_id)

  // Exactly two events, deterministically: a sentinel claim/ack through the
  // same database's forwarder must be the very next event.
  let assert Ok(_sentinel_handle) =
    postgres.submit(database, "observation-dup-sequential", sentinel_worker, 7)
  let assert Ok(Some(sentinel_claimed)) =
    attempt.claim_one(
      database,
      "observation-dup-sequential",
      workers,
      attempt_owner,
      30_000,
    )
  let sentinel_execution = attempt.execute_claim(sentinel_claimed)
  let #(sentinel_job_id, _, _) = attempt.claim_identity(sentinel_claimed)
  attempt.acknowledge(
    database,
    "observation-dup-sequential",
    attempt_owner,
    sentinel_claimed,
    sentinel_execution,
  )
  |> should.equal(Ok(True))
  assert_next_observation_is_sentinel(signal, sentinel_job_id)

  mark_database_test_executed(
    "acknowledged-observation-reconciled-on-sequential-duplicate-passed",
  )
}

pub fn postgres_acknowledged_observation_reconciled_on_concurrent_duplicate_ack_test() {
  case repeatable_read_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_acknowledged_observation_reconciled_concurrent_duplicate_test(
        database_url,
      )
  }
}

/// Coordinator review, round 1 follow-up: proves the *other* untested
/// receipt-match site — the re-check after a 0-row fenced `UPDATE` inside
/// `acknowledge_transaction` — is `Reconciled`. Reuses
/// `run_ack_duplicate_repeatable_read_test`'s exact forced-overlap
/// mechanism (a `BEFORE UPDATE` trigger parking the first acknowledgement
/// behind a held advisory lock while a second, concurrent acknowledgement
/// for the *same* claim genuinely waits on the row lock the first holds —
/// confirmed via `pg_stat_activity` wait events, not inferred): A's
/// `UPDATE` commits first (a fresh write, `Replied`); B's own `UPDATE` then
/// affects zero rows against the now-committed row and re-checks the
/// receipt, finding A's — this is the site under test, and only reachable
/// this way, not sequentially. Named mutation: hardcoding
/// `via_receipt_match: False` at this site makes this test fail (B's event
/// would report `Replied`, not `Reconciled`, and/or a second `Replied`
/// event would appear instead of one `Replied` and one `Reconciled`).
fn run_acknowledged_observation_reconciled_concurrent_duplicate_test(
  database_url: String,
) -> Nil {
  let suffix = unique_test_suffix()
  use entries <- with_unique_databases(database_url, [
    "grind_ack_obs_rr_a_" <> suffix,
    "grind_ack_obs_rr_b_" <> suffix,
  ])
  let assert [#(database_a, connection_a), #(database_b, _)] = entries

  let assert Ok(input_codec) =
    worker.codec("ack-obs-rr-input-" <> suffix <> "-v1", json.int, decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "ack-obs-rr-output-" <> suffix <> "-v1",
      json.string,
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "ack.obs.rr-" <> suffix,
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(workers) = registry.new("ack-obs-rr-" <> suffix)
  let assert Ok(workers) = registry.register(workers, definition)
  let #(workers, sentinel_worker) =
    register_sentinel_worker(workers, "dup-concurrent-" <> suffix)
  let test_queue = "ack-obs-rr-" <> suffix
  let attempt_owner = "ack-obs-rr-owner-" <> suffix

  let assert Ok(_handle) =
    postgres.submit(database_a, test_queue, definition, 8)
  let assert Ok(Some(claimed)) =
    attempt.claim_one(database_a, test_queue, workers, attempt_owner, 30_000)
  let execution = attempt.execute_claim(claimed)
  let #(job_id, _, _) = attempt.claim_identity(claimed)

  let signal = process.new_subject()
  let attachment =
    attach_acknowledged_observer(
      "dup-concurrent-" <> suffix,
      fn(measurements, metadata) {
        process.send(signal, AcknowledgedSignal(measurements, metadata))
      },
    )
  use <- exception.defer(fn() { detach(attachment) })

  let lock_key = unique_test_lock_key(5)
  let trigger_name = "grind_test_ack_obs_overlap_" <> suffix
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> trigger_name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.id = "
      <> int.to_string(job_id)
      <> " AND OLD.state = 'executing' AND NEW.state <> 'executing' THEN PERFORM pg_advisory_xact_lock("
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
      "SELECT true FROM (SELECT pg_advisory_xact_lock($1)) AS grind_test_ack_obs_overlap_barrier",
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

  let result_a = process.new_subject()
  let result_b = process.new_subject()
  spawn_submit(result_a, fn() {
    attempt.acknowledge(
      database_a,
      test_queue,
      attempt_owner,
      claimed,
      execution,
    )
  })
  await_lock_wait_counts(connection_a, 1, 0, 500) |> should.equal(True)

  spawn_submit(result_b, fn() {
    attempt.acknowledge(
      database_b,
      test_queue,
      attempt_owner,
      claimed,
      execution,
    )
  })
  await_lock_wait_counts(connection_a, 1, 1, 500) |> should.equal(True)

  process.send(release_lock, ReleaseAttempt)
  process.receive(lock_finished, within: 5000)
  |> should.equal(Ok(ClaimGateReleased(True)))

  let assert Ok(outcome_a) = process.receive(result_a, within: 5000)
  let assert Ok(outcome_b) = process.receive(result_b, within: 5000)
  outcome_a |> should.equal(Ok(True))
  outcome_b |> should.equal(Ok(True))
  count_acknowledgements_for_job(connection_a, job_id) |> should.equal(1)

  let assert Ok(AcknowledgedSignal(_, event_1)) =
    process.receive(signal, within: 5000)
  let assert Ok(AcknowledgedSignal(_, event_2)) =
    process.receive(signal, within: 5000)

  // Exactly two events, deterministically for `database_a`'s own producer
  // stream (the only one anything further runs through here): a sentinel
  // claim/ack through the same database's forwarder must be the very next
  // event. `database_b` is not exercised again after its one duplicate-ack
  // call above, so nothing further could arrive from it either.
  let assert Ok(_sentinel_handle) =
    postgres.submit(database_a, test_queue, sentinel_worker, 11)
  let assert Ok(Some(sentinel_claimed)) =
    attempt.claim_one(database_a, test_queue, workers, attempt_owner, 30_000)
  let sentinel_execution = attempt.execute_claim(sentinel_claimed)
  let #(sentinel_job_id, _, _) = attempt.claim_identity(sentinel_claimed)
  attempt.acknowledge(
    database_a,
    test_queue,
    attempt_owner,
    sentinel_claimed,
    sentinel_execution,
  )
  |> should.equal(Ok(True))
  assert_next_observation_is_sentinel(signal, sentinel_job_id)

  let confirmations = [event_1.confirmation, event_2.confirmation]
  list.contains(confirmations, observation.Replied) |> should.equal(True)
  list.contains(confirmations, observation.Reconciled) |> should.equal(True)
  event_1.command_id |> should.equal(event_2.command_id)
  event_1.committed_state |> should.equal(job.Succeeded)
  event_2.committed_state |> should.equal(job.Succeeded)
  mark_database_test_executed(
    "acknowledged-observation-reconciled-on-concurrent-duplicate-passed",
  )
}
