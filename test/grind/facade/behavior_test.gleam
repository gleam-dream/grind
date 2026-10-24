//// Bounded handlers, snoozes and abandoned attempts; cancellation that
//// reaches the handler; transactional submit; draining for tests.

import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/option.{Some}
import gleam/result
import gleam/string
import gleam/time/duration
import gleeunit/should
import grind
import grind/admin
import grind/facade/support.{fast, int_codec, unique, with_runtime}
import grind/job
import grind/queue
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/support/observers.{detach}
import grind/telemetry
import grind/testing
import grind/worker
import one_shot
import pog
import sinal

pub fn facade_snooze_limit_ends_the_job_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let snoozing =
        worker.responding(
          unique("behavior.snooze"),
          input: int_codec(),
          output: int_codec(),
          handle: fn(_context, _n) {
            worker.Snoozed(after: duration.milliseconds(0), reason: "429")
          },
        )
        |> worker.with_queue(unique("behavior-snooze"))
        |> worker.with_max_snoozes(2)
      use jobs <- with_runtime(url, fn(config) {
        config |> grind.with_worker(snoozing) |> grind.without_consumers
      })
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(snoozing, 1))
      // Two snoozes are allowed; the third ends the job.
      testing.drain(
        jobs,
        queue: snoozing.queue,
        limit: 10,
        within: duration.seconds(20),
      )
      |> should.equal(Ok(3))
      grind.outcome(jobs, handle)
      |> should.equal(
        Ok(grind.Failed(
          grind.BusinessUnrecorded,
          Some(job.SnoozeLimitReached),
          "snooze limit of 2 reached: 429",
        )),
      )
      mark_database_test_executed("facade-snooze-limit-passed")
    }
  }
}

pub fn facade_handler_timeout_holds_the_job_uncertain_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let stuck =
        worker.new(
          unique("behavior.stuck"),
          input: int_codec(),
          output: int_codec(),
          perform: fn(n) {
            process.sleep(30_000)
            Ok(n)
          },
        )
        |> worker.with_queue(unique("behavior-stuck"))
        |> worker.with_timeout(worker.After(duration.milliseconds(200)))
      use jobs <- with_runtime(url, grind.with_worker(_, stuck))
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(stuck, 1))
      let assert Ok(grind.Uncertain(evidence)) =
        grind.await(jobs, handle, within: duration.seconds(10))
      string.contains(evidence, "timeout of 200 ms") |> should.be_true
      // An operator finds it, and authorizes a replay.
      let assert Ok(listed) =
        admin.list(
          jobs,
          admin.query(limit: 10)
            |> admin.in_queue(stuck.queue)
            |> admin.in_state(job.Uncertain),
        )
      listed
      |> list_ids
      |> should.equal([job.id(handle)])
      let assert Ok(admin.Applied(job.Queued)) =
        admin.resolve_uncertain(
          jobs,
          handle,
          admin.resolution(
            admin.AuthorizeReplay,
            id: unique("replay"),
            by: "operator",
            details: "handler was stuck",
          ),
        )
      mark_database_test_executed("facade-timeout-uncertain-passed")
    }
  }
}

fn list_ids(jobs: List(admin.JobSummary)) -> List(Int) {
  case jobs {
    [] -> []
    [first, ..rest] -> [first.id, ..list_ids(rest)]
  }
}

pub fn facade_abandoned_attempt_replays_after_lease_expiry_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      // The first delivery hangs past its timeout; the redelivery answers
      // with the business attempt it was given.
      let first_delivery = one_shot.armed()
      let replaying =
        worker.responding(
          unique("behavior.replay"),
          input: int_codec(),
          output: int_codec(),
          handle: fn(context, n) {
            case one_shot.take(first_delivery) {
              True -> {
                process.sleep(30_000)
                worker.Succeeded(n)
              }
              False -> worker.Succeeded(n * 10 + worker.attempt(context))
            }
          },
        )
        |> worker.with_queue(unique("behavior-replay"))
        |> worker.with_timeout(worker.After(duration.milliseconds(200)))
        |> worker.with_abandonment(worker.ReplayAfterLeaseExpiry(max_replays: 1))
      let quarantined = process.new_subject()
      let attachment =
        sinal.observe(telemetry.quarantined(), fn(_measurements, metadata) {
          case metadata.ref.worker_id == replaying.id {
            True -> process.send(quarantined, metadata)
            False -> Nil
          }
        })
      use <- exception.defer(fn() { detach(attachment) })
      let claimed = process.new_subject()
      let claims =
        sinal.observe(telemetry.claimed(), fn(_measurements, metadata) {
          case metadata.ref.worker_id == replaying.id {
            True -> process.send(claimed, metadata.attempt)
            False -> Nil
          }
        })
      use <- exception.defer(fn() { detach(claims) })
      use jobs <- with_runtime(url, fn(config) {
        config
        |> fast
        |> grind.with_worker(replaying)
        |> grind.with_queue(
          queue.new(replaying.queue)
          |> queue.with_lease(duration.seconds(6))
          |> queue.with_poll_interval(duration.milliseconds(100)),
        )
      })
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(replaying, 21))
      // The first attempt is stopped by its timeout and not acknowledged;
      // once its lease expires the claim-time scan replays the job.
      grind.await(jobs, handle, within: duration.seconds(30))
      |> should.equal(Ok(grind.Succeeded(211)))
      let assert Ok(first_claim) = process.receive(claimed, within: 1000)
      let assert Ok(second_claim) = process.receive(claimed, within: 1000)

      // The replay is observable, and names the attempt that expired.
      let assert Ok(replay) = process.receive(quarantined, within: 1000)
      replay.ref.job_id |> should.equal(job.id(handle))
      replay.replayed |> should.be_true
      replay.cancellation_was_requested |> should.be_false
      replay.attempt |> should.equal(first_claim)

      // The replay did not use a business attempt: the redelivery is
      // attempt 1 again, under a new attempt id and epoch.
      first_claim.attempt |> should.equal(1)
      second_claim.attempt |> should.equal(1)
      { second_claim.attempt_id != first_claim.attempt_id } |> should.be_true
      second_claim.epoch |> should.equal(first_claim.epoch + 1)
      let assert Ok([summary]) =
        admin.list(
          jobs,
          admin.query(limit: 1)
            |> admin.in_queue(replaying.queue)
            |> admin.after(job.id(handle) - 1),
        )
      summary.replay_count |> should.equal(1)
      summary.attempt |> should.equal(1)
      mark_database_test_executed("facade-abandonment-replay-passed")
    }
  }
}

pub fn facade_cancellation_reaches_the_running_handler_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let running = process.new_subject()
      let cancellable =
        worker.responding(
          unique("behavior.cancel"),
          input: int_codec(),
          output: int_codec(),
          handle: fn(context, n) {
            process.send(running, Nil)
            case
              process.selector_receive(
                worker.cancellation(context),
                within: 20_000,
              )
            {
              Ok(Nil) -> worker.Cancelled("stopped on request")
              Error(Nil) -> worker.Succeeded(n)
            }
          },
        )
        |> worker.with_queue(unique("behavior-cancel"))
      use jobs <- with_runtime(url, fn(config) {
        config
        |> fast
        |> grind.with_worker(cancellable)
        |> grind.with_queue(
          queue.new(cancellable.queue) |> queue.with_lease(duration.seconds(6)),
        )
      })
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(cancellable, 1))
      let assert Ok(Nil) = process.receive(running, within: 10_000)
      grind.cancel(jobs, handle)
      |> should.equal(Ok(grind.CancellationRequested))
      // The renewal two seconds later carries the cancellation to the
      // handler.
      let assert Ok(grind.Cancelled(_)) =
        grind.await(jobs, handle, within: duration.seconds(10))
      mark_database_test_executed("facade-cancellation-reaches-handler-passed")
    }
  }
}

pub fn facade_submit_in_commits_with_the_caller_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let echo_worker =
        worker.new(
          unique("behavior.tx"),
          input: int_codec(),
          output: int_codec(),
          perform: fn(n) { Ok(n) },
        )
        |> worker.with_queue(unique("behavior-tx"))
      use jobs <- with_runtime(url, grind.with_worker(_, echo_worker))
      let db = grind.connection(jobs)
      let assert Ok(Nil) =
        pog.query(
          "CREATE TABLE IF NOT EXISTS facade_orders (id text PRIMARY KEY)",
        )
        |> pog.execute(db)
        |> result.replace(Nil)
      let order = unique("order")

      // Committed: the order row and the job become visible together.
      let assert Ok(grind.Inserted(handle)) =
        pog.transaction(db, fn(tx) {
          let assert Ok(_) =
            pog.query("INSERT INTO facade_orders (id) VALUES ($1)")
            |> pog.parameter(pog.text(order))
            |> pog.execute(tx)
          grind.submit_in(
            jobs,
            tx,
            job.new(echo_worker, 5) |> job.with_id("receipt:" <> order),
          )
        })
      grind.await(jobs, handle, within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded(5)))

      // Rolled back: no job exists for the id, so resubmitting inserts.
      let rolled_back: Result(Nil, pog.TransactionError(String)) =
        pog.transaction(db, fn(tx) {
          let assert Ok(grind.Inserted(_)) =
            grind.submit_in(
              jobs,
              tx,
              job.new(echo_worker, 6) |> job.with_id("rollback:" <> order),
            )
          Error("abandon")
        })
      let assert Error(pog.TransactionRolledBack("abandon")) = rolled_back
      let assert Ok(grind.Inserted(_)) =
        grind.submit(
          jobs,
          job.new(echo_worker, 7) |> job.with_id("rollback:" <> order),
        )

      // The pool is not a transaction.
      grind.submit_in(jobs, db, job.new(echo_worker, 8))
      |> should.equal(Error(grind.NotInTransaction))

      // A REPEATABLE READ transaction is refused before writing.
      let refused =
        pog.transaction(db, fn(tx) {
          let assert Ok(_) =
            pog.query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            |> pog.execute(tx)
          Ok(grind.submit_in(jobs, tx, job.new(echo_worker, 9)))
        })
      refused
      |> should.equal(
        Ok(Error(grind.TransactionIsolationUnsupported("repeatable read"))),
      )

      // The caller's search_path is restored after the admission.
      let assert Ok(path) =
        pog.transaction(db, fn(tx) {
          let assert Ok(_) =
            pog.query("SELECT set_config('search_path', 'pg_catalog', true)")
            |> pog.execute(tx)
          let assert Ok(grind.Inserted(_)) =
            grind.submit_in(jobs, tx, job.new(echo_worker, 10))
          let assert Ok(returned) =
            pog.query("SELECT current_setting('search_path')")
            |> pog.returning(text_column())
            |> pog.execute(tx)
          let assert [path] = returned.rows
          Ok(path)
        })
      path |> should.equal("pg_catalog")
      mark_database_test_executed("facade-submit-in-passed")
    }
  }
}

fn text_column() -> decode.Decoder(String) {
  use value <- decode.field(0, decode.string)
  decode.success(value)
}

pub fn facade_drain_is_bounded_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let slow =
        worker.new(
          unique("behavior.drain"),
          input: int_codec(),
          output: int_codec(),
          perform: fn(n) {
            process.sleep(n)
            Ok(n)
          },
        )
        |> worker.with_queue(unique("behavior-drain"))
      use jobs <- with_runtime(url, fn(config) {
        config |> grind.with_worker(slow) |> grind.without_consumers
      })
      let assert Ok(grind.Inserted(_)) = grind.submit(jobs, job.new(slow, 1))
      let assert Ok(grind.Inserted(_)) = grind.submit(jobs, job.new(slow, 1))
      testing.drain(
        jobs,
        queue: slow.queue,
        limit: 1,
        within: duration.seconds(10),
      )
      |> should.equal(Ok(1))
      testing.drain(
        jobs,
        queue: slow.queue,
        limit: 5,
        within: duration.seconds(10),
      )
      |> should.equal(Ok(1))
      testing.drain(
        jobs,
        queue: "nowhere",
        limit: 5,
        within: duration.seconds(1),
      )
      |> should.equal(Error(testing.UnknownQueue("nowhere")))
      let assert Ok(grind.Inserted(_)) = grind.submit(jobs, job.new(slow, 5000))
      testing.drain(
        jobs,
        queue: slow.queue,
        limit: 5,
        within: duration.milliseconds(300),
      )
      |> should.equal(Error(testing.DrainTimedOut(0)))
      mark_database_test_executed("facade-drain-passed")
    }
  }
}
