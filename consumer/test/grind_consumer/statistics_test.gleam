//// Public queue statistics: committed counts, scheduling ages and refusals.
//// Contract: docs/design/design.typ, Reads, administration and retention.

import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import gleam/time/duration
import gleeunit/should
import grind
import grind/admin
import grind/job
import grind/testing
import grind/worker
import grind_consumer/support/env
import pog

fn codec() -> worker.Codec(String) {
  worker.codec(worker.infallible(json.string), decode.string)
}

fn definition(
  queue: String,
  input_version: String,
) -> worker.Worker(String, String, String) {
  worker.responding(
    "probe." <> queue,
    codec() |> worker.with_codec_version(input_version),
    worker.codec(
      fn(value) {
        case value {
          "invalid" -> Error("private codec rejection")
          _ -> Ok(json.string(value))
        }
      },
      decode.string,
    ),
    fn(_, input) {
      case input {
        "uncertain" -> worker.Uncertain("private external evidence")
        "failure" | "retry" -> worker.Failed(input)
        "discard" -> worker.Discarded("private reason")
        "cancel" -> worker.Cancelled("private reason")
        _ -> worker.Succeeded(input)
      }
    },
  )
  |> worker.with_error_codec(codec())
  |> worker.with_queue(queue)
  |> worker.with_max_attempts(2)
  |> worker.with_retry_policy(fn(error, _) {
    case error {
      "retry" -> worker.RetryAfter(duration.seconds(60))
      _ -> worker.DoNotRetry
    }
  })
}

fn with_jobs(
  schema: String,
  work: worker.Worker(String, String, String),
  run: fn(grind.Grind) -> Nil,
) -> Nil {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      use jobs <- env.with_grind(url, fn(config) {
        config
        |> grind.with_schema(schema)
        |> grind.with_statement_deadline(duration.milliseconds(1500))
        |> grind.with_unique_lock_wait(duration.milliseconds(100))
        |> grind.with_worker(work)
        |> grind.with_worker(definition("secondary", "1"))
        |> grind.without_consumers
        |> grind.without_pruner
      })
      run(jobs)
      env.mark("consumer-" <> schema <> "-passed")
    }
  }
}

fn snapshot(jobs: grind.Grind, queue: String) -> admin.Statistics {
  let assert Ok(value) = admin.statistics(jobs, queue:)
  list.length(value.states) |> should.equal(11)
  should.be_true(value.sampled_at_ms > 0)
  value
}

fn group(
  snapshot: admin.Statistics,
  state: job.State,
) -> admin.StateStatistics {
  let assert Ok(value) =
    list.find(snapshot.states, fn(item) { item.state == state })
  value
}

fn total(snapshot: admin.Statistics) -> Int {
  list.fold(snapshot.states, 0, fn(n, state) { n + state.count })
}

pub fn empty_queue_and_installation_isolation_test() -> Nil {
  use a <- with_jobs("statistics_a", definition("primary", "1"))
  use b <- with_jobs("statistics_b", definition("primary", "1"))
  let empty = snapshot(a, "primary")
  list.each(empty.states, fn(state) {
    state.count |> should.equal(0)
    state.oldest_job_age_ms |> should.equal(None)
    state.due_count |> should.equal(0)
    state.oldest_due_age_ms |> should.equal(None)
  })
  let assert Ok(_) =
    grind.submit(a, job.new(definition("primary", "1"), "private-payload"))
  let assert Ok(_) =
    grind.submit(a, job.new(definition("secondary", "1"), "private-secondary"))
  total(snapshot(a, "primary")) |> should.equal(1)
  total(snapshot(a, "secondary")) |> should.equal(1)
  total(snapshot(b, "primary")) |> should.equal(0)
  total(snapshot(a, "primary' OR true --")) |> should.equal(0)
  let before = admin.list(a, admin.query(limit: 100))
  let _ = snapshot(a, "primary")
  admin.list(a, admin.query(limit: 100)) |> should.equal(before)
}

// Negative control: a transition between public list pages overcounts Queued.
pub fn paginated_counts_are_not_a_current_snapshot_test() -> Nil {
  let work = definition("primary", "1")
  use jobs <- with_jobs("statistics_pages", work)
  let assert Ok(first) = grind.submit(jobs, job.new(work, "private-input-a"))
  let assert Ok(_) = grind.submit(jobs, job.new(work, "private-input-b"))
  let query =
    admin.query(limit: 1)
    |> admin.in_queue("primary")
    |> admin.in_state(job.Queued)
  let assert Ok([page_one]) = admin.list(jobs, query)
  grind.cancel(jobs, grind.handle(first))
  |> should.equal(Ok(grind.CancelledBeforeRun))
  let assert Ok(page_two) = admin.list(jobs, query |> admin.after(page_one.id))
  1 + list.length(page_two) |> should.equal(2)
  let current = snapshot(jobs, "primary")
  group(current, job.Queued).count |> should.equal(1)
  group(current, job.Cancelled).count |> should.equal(1)
  total(current) |> should.equal(2)
  string.contains(string.inspect(current), "private-input") |> should.be_false
}

pub fn delayed_work_becomes_due_without_changing_stored_state_test() -> Nil {
  let work = definition("primary", "1")
  use jobs <- with_jobs("statistics_due", work)
  let assert Ok(_) = grind.submit(jobs, job.new(work, "immediate"))
  let assert Ok(_) =
    grind.submit(
      jobs,
      job.new(work, "delayed") |> job.after(duration.seconds(2)),
    )
  let before = snapshot(jobs, "primary")
  group(before, job.Queued).due_count |> should.equal(1)
  group(before, job.Scheduled).count |> should.equal(1)
  group(before, job.Scheduled).due_count |> should.equal(0)
  group(before, job.Scheduled).oldest_due_age_ms |> should.equal(None)
  process.sleep(2100)
  let after = snapshot(jobs, "primary")
  should.be_true(after.sampled_at_ms >= before.sampled_at_ms + 2000)
  let scheduled = group(after, job.Scheduled)
  scheduled.count |> should.equal(1)
  scheduled.due_count |> should.equal(1)
  let assert Some(overdue) = scheduled.oldest_due_age_ms
  should.be_true(overdue >= 0)
  let assert Some(age) = scheduled.oldest_job_age_ms
  should.be_true(age >= overdue)
}

pub fn retained_outcomes_and_contract_refusal_remain_distinct_test() -> Nil {
  let work = definition("primary", "1")
  use jobs <- with_jobs("statistics_outcomes", work)
  let inputs = [
    "success",
    "uncertain",
    "failure",
    "retry",
    "discard",
    "cancel",
    "invalid",
  ]
  list.each(inputs, fn(input) {
    let assert Ok(_) = grind.submit(jobs, job.new(work, input))
    Nil
  })
  // The runtime knows worker version 1/input codec 1; admission retains an older
  // input codec and the actual consumer parks it before invoking the handler.
  let assert Ok(_) =
    grind.submit(
      jobs,
      job.new(definition("primary", "old"), "private-incompatible"),
    )
  let assert Error(testing.DrainFailed(7, _)) =
    testing.drain(
      jobs,
      queue: "primary",
      limit: 20,
      within: duration.seconds(5),
    )
  let current = snapshot(jobs, "primary")
  list.each(
    [
      job.Succeeded,
      job.Uncertain,
      job.BusinessFailed,
      job.Retryable,
      job.Discarded,
      job.Cancelled,
      job.RuntimeFailed,
      job.ContractMismatch,
    ],
    fn(state) {
      let value = group(current, state)
      value.count |> should.equal(1)
      value.due_count |> should.equal(0)
      value.oldest_job_age_ms |> should.not_equal(None)
    },
  )
  total(current) |> should.equal(8)
  string.contains(string.inspect(current), "private") |> should.be_false
  group(snapshot(jobs, "primary"), job.Uncertain).count |> should.equal(1)
}

pub fn executing_is_durable_but_not_due_test() -> Nil {
  let entered = process.new_subject()
  let finished = process.new_subject()
  let work =
    worker.responding("probe.held", codec(), codec(), fn(_, input) {
      let release = process.new_subject()
      process.send(entered, release)
      let assert Ok(Nil) = process.receive(release, within: 5000)
      worker.Succeeded(input)
    })
    |> worker.with_queue("primary")
  use jobs <- with_jobs("statistics_executing", work)
  let assert Ok(_) = grind.submit(jobs, job.new(work, "private-held-payload"))
  let _ =
    process.spawn(fn() {
      process.send(
        finished,
        testing.drain(
          jobs,
          queue: "primary",
          limit: 1,
          within: duration.seconds(8),
        ),
      )
    })
  let assert Ok(release) = process.receive(entered, within: 2000)
  use <- exception.defer(fn() { process.send(release, Nil) })
  let executing = group(snapshot(jobs, "primary"), job.Executing)
  executing.count |> should.equal(1)
  executing.due_count |> should.equal(0)
  executing.oldest_due_age_ms |> should.equal(None)
  process.send(release, Nil)
  process.receive(finished, within: 2000) |> should.equal(Ok(Ok(1)))
  group(snapshot(jobs, "primary"), job.Succeeded).count |> should.equal(1)
}

pub fn staged_admission_is_invisible_and_rollback_stays_empty_test() -> Nil {
  let work = definition("primary", "1")
  use jobs <- with_jobs("statistics_transaction", work)
  pog.transaction(grind.connection(jobs), fn(tx) {
    let assert Ok(_) =
      grind.submit_in(jobs, tx, job.new(work, "private-uncommitted"))
    total(snapshot(jobs, "primary")) |> should.equal(0)
    Error(Nil)
  })
  |> should.equal(Error(pog.TransactionRolledBack(Nil)))
  total(snapshot(jobs, "primary")) |> should.equal(0)
  let assert Ok(_) = grind.submit(jobs, job.new(work, "committed"))
  total(snapshot(jobs, "primary")) |> should.equal(1)
}

pub fn database_refusal_and_stopped_runtime_never_report_zero_test() -> Nil {
  use jobs <- with_jobs("statistics_locked", definition("primary", "1"))
  let entered = process.new_subject()
  let released = process.new_subject()
  let _ =
    process.spawn(fn() {
      let result =
        pog.transaction(grind.connection(jobs), fn(tx) {
          let assert Ok(_) =
            pog.query(
              "LOCK TABLE statistics_locked.grind_jobs IN ACCESS EXCLUSIVE MODE",
            )
            |> pog.execute(tx)
          let release = process.new_subject()
          process.send(entered, release)
          let assert Ok(Nil) = process.receive(release, within: 6000)
          Ok(Nil)
        })
      process.send(released, result)
    })
  let assert Ok(release) = process.receive(entered, within: 1000)
  use <- exception.defer(fn() { process.send(release, Nil) })
  let assert Error(admin.Unavailable(_)) =
    admin.statistics(jobs, queue: "primary")
  process.send(release, Nil)
  process.receive(released, within: 1000) |> should.equal(Ok(Ok(Nil)))
  total(snapshot(jobs, "primary")) |> should.equal(0)
  grind.stop(jobs) |> should.equal(Ok(grind.StoppedCleanly))
  admin.statistics(jobs, queue: "primary")
  |> should.equal(Error(admin.NotRunning))
}

// Simulate a clock moving behind an already persisted insertion timestamp.
pub fn a_nonempty_group_can_have_an_age_of_zero_test() -> Nil {
  let work = definition("primary", "1")
  use jobs <- with_jobs("statistics_clock", work)
  let assert Ok(_) = grind.submit(jobs, job.new(work, "private-clock-payload"))
  let assert Ok(_) =
    pog.query(
      "UPDATE statistics_clock.grind_jobs SET inserted_at = clock_timestamp() + interval '1 day'",
    )
    |> pog.execute(grind.connection(jobs))
  let current = group(snapshot(jobs, "primary"), job.Queued)
  current.count |> should.equal(1)
  current.oldest_job_age_ms |> should.equal(Some(0))
}

// Fault injection only: an unsupported persisted state must not disappear from
// a snapshot that otherwise looks complete. The database is disposable.
pub fn an_unknown_stored_state_refuses_the_whole_snapshot_test() -> Nil {
  let work = definition("primary", "1")
  use jobs <- with_jobs("statistics_unknown", work)
  let assert Ok(_) =
    grind.submit(jobs, job.new(work, "private-corrupt-payload"))
  let db = grind.connection(jobs)
  let assert Ok(_) =
    pog.query(
      "ALTER TABLE statistics_unknown.grind_jobs DROP CONSTRAINT grind_jobs_state_check",
    )
    |> pog.execute(db)
  let assert Ok(_) =
    pog.query("UPDATE statistics_unknown.grind_jobs SET state = 'future_state'")
    |> pog.execute(db)
  admin.statistics(jobs, queue: "primary")
  |> should.equal(Error(admin.RecordMismatch))
  total(snapshot(jobs, "secondary")) |> should.equal(0)
}
