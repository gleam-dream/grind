import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleeunit/should
import grind/job
import grind/observation
import grind/postgres
import grind/queue
import grind/registry
import grind/support/ack_queries.{wait_for_commit_trigger_backend}
import grind/support/consumer.{manual_policy}
import grind/support/env.{
  database_url, mark_database_test_executed, queue_database_url,
}
import grind/support/job_state.{retry_transient_query}
import grind/support/observers.{detach}
import grind/support/syncrep.{terminate_backend}
import grind/support/unique_fixture.{unique_test_suffix}
import grind/worker
import pog
import sinal

pub fn postgres_resolved_observation_replied_and_reconciled_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_resolved_observation_replied_reconciled_test(database_url)
  }
}

/// The first audited resolution of an `uncertain` job is this call's own
/// fresh commit (`Replied`); replaying the exact same `resolution_id` is
/// proven by `resolution_receipt_outcome`'s own receipt read
/// (`ResolutionAlreadyApplied`), so the second observation is `Reconciled`.
fn run_resolved_observation_replied_reconciled_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "resolved-emission-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "resolved-emission-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "resolved.emission",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) =
    postgres.submit(database, "resolved-emission", definition, 12)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 501, attempt_epoch = 3, attempt_owner = 'lost-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: connection)
  let assert Ok(workers) = registry.new("resolved-emission")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.resolved(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "resolution-emission-1",
      "on-call",
      "confirm before replay",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  let assert Ok(#(measurements, first_metadata)) =
    process.receive(signal, within: 5000)
  measurements |> should.equal(observation.ResolvedMeasurements(count: 1))
  first_metadata.ref.job_id |> should.equal(job.id_value(handle))
  first_metadata.ref.queue |> should.equal("resolved-emission")
  first_metadata.decision |> should.equal(observation.DecisionAuthorizeReplay)
  first_metadata.committed_state |> should.equal(job.Queued)
  first_metadata.resolution_id |> should.equal("resolution-emission-1")
  first_metadata.resolved_by |> should.equal("on-call")
  first_metadata.confirmation |> should.equal(observation.Replied)

  postgres.resolve_uncertain(
    database,
    handle,
    postgres.ResolutionRequest(
      "resolution-emission-1",
      "on-call",
      "confirm before replay",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionAlreadyApplied(job.Queued)))
  let assert Ok(#(_, second_metadata)) = process.receive(signal, within: 5000)
  second_metadata.ref.job_id |> should.equal(job.id_value(handle))
  second_metadata.confirmation |> should.equal(observation.Reconciled)
  mark_database_test_executed("resolved-observation-replied-reconciled-passed")
}

pub fn postgres_resolved_observation_absent_on_reconciliation_not_required_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_resolved_observation_absent_test(database_url)
  }
}

/// `resolve_uncertain` against a job that is not (or no longer) `uncertain`
/// commits nothing (`ReconciliationNotRequired`) and must never emit —
/// proven by a genuine audited resolution through the exact same producer
/// arriving as the very next `resolved` observation.
fn run_resolved_observation_absent_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "resolved-absent-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "resolved-absent-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define("resolved.absent", "v1", input_codec, output_codec, fn(value) {
      Ok(int.to_string(value))
    })
  let assert Ok(queued_handle) =
    postgres.submit(database, "resolved-absent", definition, 4)
  let assert Ok(uncertain_handle) =
    postgres.submit(database, "resolved-absent", definition, 5)
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 777, attempt_epoch = 2, attempt_owner = 'lost-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(uncertain_handle)))
    |> pog.execute(on: connection)
  let assert Ok(workers) = registry.new("resolved-absent")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  // The quarantine scan moves `uncertain_handle` to `uncertain`; the
  // still-genuinely-due `queued_handle` is what this same call then claims
  // and runs to completion.
  queue.process_one(consumer) |> should.equal(Ok(True))
  postgres.state(database, queued_handle) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, uncertain_handle) |> should.equal(Ok(job.Uncertain))

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.resolved(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  postgres.resolve_uncertain(
    database,
    queued_handle,
    postgres.ResolutionRequest(
      "resolution-absent-1",
      "on-call",
      "not actually uncertain",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Error(postgres.ReconciliationNotRequired))
  process.receive(signal, within: 0) |> should.equal(Error(Nil))

  postgres.resolve_uncertain(
    database,
    uncertain_handle,
    postgres.ResolutionRequest(
      "resolution-absent-2",
      "on-call",
      "genuinely uncertain",
      postgres.AuthorizeReplay,
    ),
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(uncertain_handle))
  mark_database_test_executed("resolved-observation-absent-passed")
}

pub fn postgres_resolved_observation_absent_on_commit_unknown_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_resolved_observation_commit_unknown_test(database_url)
  }
}

/// A genuinely aborted commit (a deferred trigger's `pg_sleep` fires during
/// `resolve_uncertain`'s own transaction `COMMIT`; killing that backend
/// aborts the whole transaction — nothing committed) reports
/// `ResolutionCommitUnknown` and must never emit — the same "commit
/// genuinely unknown" case `postgres_submit_unique_aborted_commit_is_commit_unknown_test`
/// proves for admission. Once the trigger is dropped, retrying the exact
/// same `resolution_id` against the still-`uncertain` job (the aborted
/// transaction rolled back its own `grind_jobs` update too) is the sentinel
/// through the same producer.
fn run_resolved_observation_commit_unknown_test(database_url: String) -> Nil {
  let suffix = unique_test_suffix()
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "resolved-commit-unknown-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "resolved-commit-unknown-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let assert Ok(definition) =
    worker.define(
      "resolved.commit.unknown-" <> suffix,
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(int.to_string(value)) },
    )
  let assert Ok(handle) =
    postgres.submit(
      database,
      "resolved-commit-unknown-" <> suffix,
      definition,
      3,
    )
  let assert Ok(sentinel_handle) =
    postgres.submit(
      database,
      "resolved-commit-unknown-" <> suffix,
      definition,
      4,
    )
  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 909, attempt_epoch = 4, attempt_owner = 'lost-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(handle)))
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "UPDATE grind_jobs SET state = 'executing', attempt_id = 910, attempt_epoch = 4, attempt_owner = 'lost-consumer', lease_expires_at = clock_timestamp() WHERE id = $1",
    )
    |> pog.parameter(pog.int(job.id_value(sentinel_handle)))
    |> pog.execute(on: connection)
  let assert Ok(workers) = registry.new("resolved-commit-unknown-" <> suffix)
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(consumer) = queue.start(database, workers, manual_policy())
  use <- exception.defer(fn() { queue.stop(consumer) })
  queue.process_one(consumer) |> should.equal(Ok(False))
  queue.process_one(consumer) |> should.equal(Ok(False))
  postgres.state(database, handle) |> should.equal(Ok(job.Uncertain))
  postgres.state(database, sentinel_handle) |> should.equal(Ok(job.Uncertain))

  let resolution_id = "resolution-commit-unknown-" <> suffix
  let trigger_name = "grind_test_resolved_commit_unknown_" <> suffix
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION "
      <> trigger_name
      <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NOT (NEW.resolution_id = '"
      <> resolution_id
      <> "') THEN RETURN NEW; END IF; PERFORM pg_sleep(30); RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE CONSTRAINT TRIGGER "
      <> trigger_name
      <> " AFTER INSERT ON grind_job_resolutions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION "
      <> trigger_name
      <> "()",
    )
    |> pog.execute(on: connection)
  let drop_trigger = fn() {
    let _ =
      pog.query(
        "DROP TRIGGER IF EXISTS " <> trigger_name <> " ON grind_job_resolutions",
      )
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS " <> trigger_name <> "()")
      |> pog.execute(on: connection)
    Nil
  }
  use <- exception.defer(drop_trigger)

  let signal = process.new_subject()
  let attachment =
    sinal.observe(observation.resolved(), fn(measurements, metadata) {
      process.send(signal, #(measurements, metadata))
    })
  use <- exception.defer(fn() { detach(attachment) })

  let reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(
        reply,
        postgres.resolve_uncertain(
          database,
          handle,
          postgres.ResolutionRequest(
            resolution_id,
            "on-call",
            "aborted commit proof",
            postgres.AuthorizeReplay,
          ),
        ),
      )
    })
  let assert Ok(backend_pid) = wait_for_commit_trigger_backend(connection, 300)
  terminate_backend(connection, backend_pid) |> should.equal(True)
  let assert Ok(Error(postgres.ResolutionCommitUnknown(returned_resolution_id))) =
    process.receive(reply, within: 10_000)
  returned_resolution_id |> should.equal(resolution_id)
  process.receive(signal, within: 0) |> should.equal(Error(Nil))
  drop_trigger()

  // The pool just had a connection deliberately terminated
  // (`terminate_backend`, above): `pgo_connection`'s own supervised restart
  // of that connection is a real, transient recovery window, not a
  // steady-state failure — see `retry_transient_query`'s other call sites
  // (for example `run_unique_aborted_commit_test`) for the same pattern.
  retry_transient_query(fn() { postgres.state(database, handle) }, 20)
  |> should.equal(Ok(job.Uncertain))

  // Sentinel: a distinct job/resolution through the exact same producer, so
  // a stray event wrongly emitted for the commit-unknown job above (which
  // would carry *that* job's id) is caught as a mismatch here rather than
  // coincidentally matching.
  retry_transient_query(
    fn() {
      postgres.resolve_uncertain(
        database,
        sentinel_handle,
        postgres.ResolutionRequest(
          "resolution-commit-unknown-sentinel-" <> suffix,
          "on-call",
          "aborted commit proof",
          postgres.AuthorizeReplay,
        ),
      )
    },
    20,
  )
  |> should.equal(Ok(postgres.ResolutionApplied(job.Queued)))
  let assert Ok(#(_, sentinel_metadata)) = process.receive(signal, within: 5000)
  sentinel_metadata.ref.job_id |> should.equal(job.id_value(sentinel_handle))
  sentinel_metadata.confirmation |> should.equal(observation.Replied)
  mark_database_test_executed(
    "resolved-observation-absent-on-commit-unknown-passed",
  )
}
