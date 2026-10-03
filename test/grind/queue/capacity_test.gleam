import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/result
import gleeunit/should
import grind/internal/consumer as queue
import grind/internal/diagnostic
import grind/internal/job
import grind/internal/postgres
import grind/internal/registry
import grind/internal/worker
import grind/support/concurrency.{ReleaseAttempt}
import grind/support/diagnostics
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/job_state.{job_finished_at_ms, wait_for_job_state}
import grind/support/observers.{detach}
import grind/support/queue_signals.{CapacityWorkerStarted}
import grind/support/queue_timing.{database_time_ms}
import grind/support/worker_failure.{AccountMissing}
import pog

pub fn postgres_consumer_enforces_configured_capacity_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_consumer_capacity_test(database_url)
  }
}

pub fn postgres_automatic_consumer_fills_only_available_slots_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_automatic_consumer_capacity_test(database_url)
  }
}

fn run_consumer_capacity_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec("capacity-input-v1", worker.infallible(json.int), decode.int)
  let assert Ok(output_codec) =
    worker.codec(
      "capacity-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("capacity.echo", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, CapacityWorkerStarted(value, release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("capacity-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("consumer-capacity")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(first_handle) =
    postgres.submit(database, "consumer-capacity", definition, 1)
  let assert Ok(second_handle) =
    postgres.submit(database, "consumer-capacity", definition, 2)
  let assert Ok(third_handle) =
    postgres.submit(database, "consumer-capacity", definition, 3)
  let #(capacity, attachment) =
    diagnostics.capture(diagnostic.capacity(), fn(meta) {
      meta.queue.queue == "consumer-capacity"
    })
  use <- exception.defer(fn() { detach(attachment) })
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(2)
    |> queue.with_manual_polling
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  let first_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(first_reply, queue.process_one(consumer))
    })
  let assert Ok(CapacityWorkerStarted(_, first_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(first_release, ReleaseAttempt)
    Nil
  })
  let second_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(second_reply, queue.process_one(consumer))
    })
  let assert Ok(CapacityWorkerStarted(_, second_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(second_release, ReleaseAttempt)
    Nil
  })
  queue.process_one(consumer) |> should.equal(Error(queue.QueueBusy))
  process.receive(started, within: 0) |> should.equal(Error(Nil))
  postgres.state(database, third_handle) |> should.equal(Ok(job.Queued))
  let assert Ok(#(initial, identity)) = process.receive(capacity, 5000)
  initial |> should.equal(diagnostic.CapacityMeasurements(2, 0, 0, 0, 2))
  identity.draining |> should.equal(False)
  let assert Ok(#(one, _)) = process.receive(capacity, 5000)
  one |> should.equal(diagnostic.CapacityMeasurements(2, 1, 1, 0, 1))
  let assert Ok(#(full, _)) = process.receive(capacity, 5000)
  full |> should.equal(diagnostic.CapacityMeasurements(2, 2, 2, 0, 0))

  process.send(first_release, ReleaseAttempt)
  process.send(second_release, ReleaseAttempt)
  process.receive(first_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  process.receive(second_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, first_handle) |> should.equal(Ok(job.Succeeded))
  postgres.state(database, second_handle) |> should.equal(Ok(job.Succeeded))
  let assert Ok(#(empty, same_identity)) =
    diagnostics.await(capacity, fn(sample) { sample.0.active == 0 }, 5000)
  empty |> should.equal(initial)
  same_identity |> should.equal(identity)
  let third_reply = process.new_subject()
  let _ =
    process.spawn_unlinked(fn() {
      process.send(third_reply, queue.process_one(consumer))
    })
  let assert Ok(CapacityWorkerStarted(_, third_release)) =
    process.receive(started, within: 5000)
  process.send(third_release, ReleaseAttempt)
  process.receive(third_reply, within: 5000) |> should.equal(Ok(Ok(True)))
  postgres.state(database, third_handle) |> should.equal(Ok(job.Succeeded))
  mark_database_test_executed("consumer-capacity-two-enforced")
  mark_database_test_executed("diagnostic-local-capacity-transitions")
}

fn run_automatic_consumer_capacity_test(database_url: String) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "auto-capacity-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "auto-capacity-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define("auto.capacity", "v1", input_codec, output_codec, fn(value) {
      let release = process.new_subject()
      process.send(started, CapacityWorkerStarted(value, release))
      case process.receive(release, within: 10_000) {
        Ok(ReleaseAttempt) -> Ok("automatic-capacity-" <> int.to_string(value))
        Error(Nil) -> Error(AccountMissing(value))
      }
    })
  let assert Ok(workers) = registry.new("consumer-capacity-auto")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(first_handle) =
    postgres.submit(database, "consumer-capacity-auto", definition, 11)
  let assert Ok(second_handle) =
    postgres.submit(database, "consumer-capacity-auto", definition, 12)
  let assert Ok(third_handle) =
    postgres.submit(database, "consumer-capacity-auto", definition, 13)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(60_000)
    |> queue.with_maximum_concurrency(2)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  let assert Ok(CapacityWorkerStarted(11, first_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(first_release, ReleaseAttempt)
    Nil
  })
  let assert Ok(CapacityWorkerStarted(12, second_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(second_release, ReleaseAttempt)
    Nil
  })
  process.receive(started, within: 0) |> should.equal(Error(Nil))
  postgres.state(database, third_handle) |> should.equal(Ok(job.Queued))

  // Completing one child releases one local slot, which the same automatic
  // poll batch fills while the other child remains blocked.
  process.send(first_release, ReleaseAttempt)
  let assert Ok(CapacityWorkerStarted(13, third_release)) =
    process.receive(started, within: 5000)
  process.send(second_release, ReleaseAttempt)
  process.send(third_release, ReleaseAttempt)
  wait_for_job_state(database, first_handle, job.Succeeded, 250)
  |> should.equal(True)
  wait_for_job_state(database, second_handle, job.Succeeded, 250)
  |> should.equal(True)
  wait_for_job_state(database, third_handle, job.Succeeded, 250)
  |> should.equal(True)
  mark_database_test_executed("automatic-consumer-capacity-two-enforced")
}

/// `maximum_concurrency: 2` with one long-running job must not leave the
/// other slot idle: a job submitted only after the first has already
/// claimed and started must still be picked up promptly, by a freshly
/// scheduled poll, rather than waiting for the first job to finish. Before
/// the fix, `continue_if_idle` only ever armed the next poll timer once
/// `active` was fully empty, so free capacity went unused for as long as any
/// one attempt kept running.
pub fn postgres_automatic_consumer_polls_while_capacity_free_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_automatic_consumer_polls_while_capacity_free_test(database_url)
  }
}

fn run_automatic_consumer_polls_while_capacity_free_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "auto-free-capacity-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "auto-free-capacity-output-v1",
      worker.infallible(json.string),
      decode.string,
    )
  let started = process.new_subject()
  let assert Ok(definition) =
    worker.define(
      "auto.free-capacity",
      "v1",
      input_codec,
      output_codec,
      fn(value) {
        let release = process.new_subject()
        process.send(started, CapacityWorkerStarted(value, release))
        case process.receive(release, within: 10_000) {
          Ok(ReleaseAttempt) -> Ok("free-capacity-" <> int.to_string(value))
          Error(Nil) -> Error(AccountMissing(value))
        }
      },
    )
  let assert Ok(workers) = registry.new("consumer-free-capacity-auto")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(first_handle) =
    postgres.submit(database, "consumer-free-capacity-auto", definition, 21)
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(50)
    |> queue.with_maximum_concurrency(2)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() { queue.stop(consumer) })

  // Job 1 claims the first slot and blocks on its own gate. The claim right
  // after it finds nothing else due (job 2 does not exist yet) and goes
  // idle, which is exactly the state the fix must keep polling from.
  let assert Ok(CapacityWorkerStarted(21, first_release)) =
    process.receive(started, within: 5000)
  use <- exception.defer(fn() {
    process.send(first_release, ReleaseAttempt)
    Nil
  })

  // Submitted only now: this job did not exist at the poll that claimed job
  // 1, so only a later, freshly scheduled poll can ever find it due.
  let assert Ok(second_handle) =
    postgres.submit(database, "consumer-free-capacity-auto", definition, 22)

  // The second slot is free and job 1 is still blocked; job 2 must start
  // within a handful of poll intervals, not only once job 1 finishes.
  let assert Ok(CapacityWorkerStarted(22, second_release)) =
    process.receive(started, within: 2000)
  postgres.state(database, first_handle) |> should.equal(Ok(job.Executing))

  process.send(first_release, ReleaseAttempt)
  process.send(second_release, ReleaseAttempt)
  wait_for_job_state(database, first_handle, job.Succeeded, 250)
  |> should.equal(True)
  wait_for_job_state(database, second_handle, job.Succeeded, 250)
  |> should.equal(True)
  mark_database_test_executed(
    "automatic-consumer-polls-while-capacity-free-passed",
  )
}

pub fn postgres_automatic_consumer_drains_backlog_without_per_interval_ceiling_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_automatic_consumer_drains_backlog_without_per_interval_ceiling_test(
        database_url,
      )
  }
}

/// Oban-like automatic polling: with a backlog well beyond
/// `maximum_concurrency` and a deliberately long `poll_interval`, every free
/// slot keeps refilling as soon as a claim succeeds instead of waiting for
/// the next `Poll` timer, so the whole backlog drains in a small, bounded
/// number of intervals rather than one claim per interval regardless of
/// concurrency (the ceiling `docs/RISKS.md` risk 6 used to document: with
/// the old per-poll claim budget, 50 jobs at a 1000ms interval took roughly
/// 50 intervals — about 50 seconds — to drain no matter how high
/// `maximum_concurrency` was set). Bounded by the database's own clock, not
/// the test process's local wall clock or a blind sleep: `finished_at` on
/// every one of the 50 rows is read back and compared against a `t0`
/// timestamp read from the same database connection right after submission.
fn run_automatic_consumer_drains_backlog_without_per_interval_ceiling_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "auto-backlog-drain-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "auto-backlog-drain-output-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(definition) =
    worker.define(
      "auto.backlog-drain",
      "v1",
      input_codec,
      output_codec,
      fn(value) { Ok(value) },
    )
  let assert Ok(workers) = registry.new("consumer-backlog-drain-auto")
  let assert Ok(workers) = registry.register(workers, definition)
  let job_count = 50
  submit_backlog(database, definition, job_count)
  let connection = postgres.connection(database)
  let assert Ok(t0) = database_time_ms(connection)
  let poll_interval_ms = 1000
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(poll_interval_ms)
    |> queue.with_maximum_concurrency(10)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  // Today's per-poll ceiling would need ~job_count intervals (~50s) to
  // finish; the fix must drain the whole backlog in a small, fixed number
  // of intervals instead. Bounded local wait purely to give the system a
  // chance to finish — the actual pass/fail evidence below reads database
  // state and database timestamps, never this loop's own timing.
  let max_wait_ms = 4 * poll_interval_ms
  wait_for_succeeded_count(
    connection,
    "consumer-backlog-drain-auto",
    job_count,
    max_wait_ms / 50,
  )
  |> should.equal(job_count)

  let assert Ok(finished_count) =
    finished_count_for_queue(connection, "consumer-backlog-drain-auto")
  finished_count |> should.equal(job_count)

  let assert Ok(last_finished_ms) =
    max_finished_at_ms_for_queue(connection, "consumer-backlog-drain-auto")
  // "A few intervals", not one claim per interval: comfortably under half
  // of what the old per-poll ceiling would have needed for this backlog
  // (job_count intervals), measured entirely from the database's own clock.
  should.be_true(last_finished_ms - t0 < job_count / 2 * poll_interval_ms)
  mark_database_test_executed("automatic-consumer-drains-backlog-passed")
}

fn submit_backlog(
  database: postgres.Database,
  definition: worker.Worker(Int, Int, error),
  remaining: Int,
) -> Nil {
  case remaining > 0 {
    False -> Nil
    True -> {
      let assert Ok(_) =
        postgres.submit(
          database,
          "consumer-backlog-drain-auto",
          definition,
          remaining,
        )
      submit_backlog(database, definition, remaining - 1)
    }
  }
}

fn wait_for_succeeded_count(
  connection: pog.Connection,
  queue_name: String,
  target: Int,
  remaining_checks: Int,
) -> Int {
  let assert Ok(count) = finished_count_for_queue(connection, queue_name)
  case count >= target, remaining_checks > 0 {
    True, _ -> count
    False, True -> {
      process.sleep(50)
      wait_for_succeeded_count(
        connection,
        queue_name,
        target,
        remaining_checks - 1,
      )
    }
    False, False -> count
  }
}

fn finished_count_for_queue(
  connection: pog.Connection,
  queue_name: String,
) -> Result(Int, Nil) {
  pog.query(
    "SELECT count(*) FROM grind_jobs WHERE queue = $1 AND state = 'succeeded'",
  )
  |> pog.parameter(pog.text(queue_name))
  |> pog.returning({
    use count <- decode.field(0, decode.int)
    decode.success(count)
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [count] -> Ok(count)
      _ -> Error(Nil)
    }
  })
}

fn max_finished_at_ms_for_queue(
  connection: pog.Connection,
  queue_name: String,
) -> Result(Int, Nil) {
  pog.query(
    "SELECT (extract(epoch FROM max(finished_at)) * 1000)::bigint FROM grind_jobs WHERE queue = $1 AND state = 'succeeded'",
  )
  |> pog.parameter(pog.text(queue_name))
  |> pog.returning({
    use finished_at <- decode.field(0, decode.int)
    decode.success(finished_at)
  })
  |> pog.execute(on: connection)
  |> result.map_error(fn(_) { Nil })
  |> result.try(fn(returned) {
    case returned.rows {
      [finished_at] -> Ok(finished_at)
      _ -> Error(Nil)
    }
  })
}

pub fn postgres_automatic_consumer_waits_full_interval_when_idle_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_automatic_consumer_waits_full_interval_when_idle_test(database_url)
  }
}

/// Companion to the backlog-drain fix above: an idle automatic consumer
/// (no due job at all) must still back off to the full `poll_interval`
/// between claim attempts rather than busy-looping now that a successful
/// claim refills its slot immediately. A job submitted well after the
/// consumer has gone idle is not claimed until close to the next scheduled
/// `Poll` tick — not immediately (which a busy loop would do) and not many
/// intervals later either — measured from the database's own clock on both
/// ends (submission and `finished_at`).
fn run_automatic_consumer_waits_full_interval_when_idle_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "auto-idle-wait-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "auto-idle-wait-output-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(definition) =
    worker.define("auto.idle-wait", "v1", input_codec, output_codec, fn(value) {
      Ok(value)
    })
  let assert Ok(workers) = registry.new("consumer-idle-wait-auto")
  let assert Ok(workers) = registry.register(workers, definition)
  let poll_interval_ms = 700
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(poll_interval_ms)
    |> queue.with_maximum_concurrency(1)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })
  let connection = postgres.connection(database)
  // Lets the immediate startup poll run and go idle (well under one
  // interval), so the job below is submitted into a genuinely idle
  // consumer rather than racing its very first poll.
  process.sleep(150)

  let assert Ok(t_submit) = database_time_ms(connection)
  let assert Ok(handle) =
    postgres.submit(database, "consumer-idle-wait-auto", definition, 1)

  // Not claimed well before the next scheduled poll: a busy loop would
  // claim this almost immediately after submission instead.
  process.sleep(poll_interval_ms / 2)
  postgres.state(database, handle) |> should.equal(Ok(job.Queued))

  wait_for_job_state(database, handle, job.Succeeded, 100)
  |> should.equal(True)
  let assert Ok(finished_ms) =
    job_finished_at_ms(connection, job.id_value(handle))
  let elapsed = finished_ms - t_submit
  // Claimed close to one interval after submission, not immediately (no
  // busy loop) and not several intervals later either.
  should.be_true(elapsed >= poll_interval_ms / 2)
  should.be_true(elapsed < 2 * poll_interval_ms)
  mark_database_test_executed("automatic-consumer-waits-full-interval-passed")
}

pub fn postgres_automatic_fill_does_not_hot_loop_on_claim_error_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(database_url) ->
      run_automatic_fill_does_not_hot_loop_on_claim_error_test(database_url)
  }
}

/// Regression coverage for the `FillSlots`-message refactor
/// (`fill_automatic_slots`/`request_fill`, `docs/RISKS.md` risks 4 and 5): a
/// claim that fails outright (not merely "found nothing") must still yield
/// to the next `Poll` timer rather than being retried immediately from
/// inside the same message handler — `start_attempt`'s `Error` branch always
/// goes through `finish_without_claim`/`continue_if_idle`, never through
/// `request_fill`, so a persistently broken storage cannot turn into a hot
/// retry loop of claim attempts. One job is seeded so a candidate always
/// exists to claim; every claim's own `UPDATE ... SET state = 'executing'`
/// is broken by a `BEFORE UPDATE` trigger that unconditionally raises after
/// bumping a plain sequence (`nextval`, not rolled back by the trigger's own
/// forced abort, unlike an ordinary table write would be) — so the sequence
/// counts exactly how many times a claim attempt actually reached that
/// statement. Left running across `poll_count` intervals, the count must
/// stay at `poll_count + 1` (the immediate startup poll, plus one more per
/// `Poll` timer tick) — never noticeably higher, which is what a hot loop
/// would produce.
fn run_automatic_fill_does_not_hot_loop_on_claim_error_test(
  database_url: String,
) -> Nil {
  let assert Ok(validated) =
    postgres.settings(database_url) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  let assert Ok(Nil) = postgres.migrate(database)
  let assert Ok(input_codec) =
    worker.codec(
      "fill-hot-loop-input-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(output_codec) =
    worker.codec(
      "fill-hot-loop-output-v1",
      worker.infallible(json.int),
      decode.int,
    )
  let assert Ok(definition) =
    worker.define("fill.hot-loop", "v1", input_codec, output_codec, fn(value) {
      Ok(value)
    })
  let assert Ok(workers) = registry.new("fill-hot-loop")
  let assert Ok(workers) = registry.register(workers, definition)
  let assert Ok(_) = postgres.submit(database, "fill-hot-loop", definition, 1)

  let connection = postgres.connection(database)
  let assert Ok(_) =
    pog.query(
      "CREATE SEQUENCE IF NOT EXISTS grind_test_fill_hot_loop_claim_attempts",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE FUNCTION grind_test_fill_hot_loop_trigger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.queue = 'fill-hot-loop' AND NEW.state = 'executing' THEN PERFORM nextval('grind_test_fill_hot_loop_claim_attempts'); RAISE EXCEPTION 'grind test forced claim failure'; END IF; RETURN NEW; END $$",
    )
    |> pog.execute(on: connection)
  let assert Ok(_) =
    pog.query(
      "CREATE TRIGGER grind_test_fill_hot_loop BEFORE UPDATE ON grind_jobs FOR EACH ROW EXECUTE FUNCTION grind_test_fill_hot_loop_trigger()",
    )
    |> pog.execute(on: connection)
  use <- exception.defer(fn() {
    let _ =
      pog.query("DROP TRIGGER IF EXISTS grind_test_fill_hot_loop ON grind_jobs")
      |> pog.execute(on: connection)
    let _ =
      pog.query("DROP FUNCTION IF EXISTS grind_test_fill_hot_loop_trigger()")
      |> pog.execute(on: connection)
    let _ =
      pog.query(
        "DROP SEQUENCE IF EXISTS grind_test_fill_hot_loop_claim_attempts",
      )
      |> pog.execute(on: connection)
    Nil
  })

  let poll_interval_ms = 200
  let poll_count = 4
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(poll_interval_ms)
    |> queue.with_maximum_concurrency(3)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, workers, policy)
  use <- exception.defer(fn() {
    let _ = queue.stop(consumer)
    Nil
  })

  // The immediate startup poll plus `poll_count` timer ticks, with a little
  // slack for scheduling jitter.
  process.sleep(poll_interval_ms * poll_count + poll_interval_ms / 2)

  let assert Ok(attempts) =
    pog.query("SELECT last_value FROM grind_test_fill_hot_loop_claim_attempts")
    |> pog.returning({
      use last_value <- decode.field(0, decode.int)
      decode.success(last_value)
    })
    |> pog.execute(on: connection)
    |> result.map_error(fn(_) { Nil })
    |> result.try(fn(returned) {
      case returned.rows {
        [last_value] -> Ok(last_value)
        _ -> Error(Nil)
      }
    })
  // At most one claim attempt per interval (the startup poll counts as one),
  // never a hot loop retrying within the same interval.
  should.be_true(attempts <= poll_count + 1)
  should.be_true(attempts >= 1)
  mark_database_test_executed("automatic-fill-no-hot-loop-on-claim-error")
}
