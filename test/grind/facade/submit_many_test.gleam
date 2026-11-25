//// Bulk admission: many jobs in one transaction, as `submit` admits one.

import gleam/int
import gleam/list
import gleam/string
import gleam/time/duration
import gleeunit/should
import grind
import grind/admin
import grind/facade/support.{int_codec, string_codec, unique, with_runtime}
import grind/job
import grind/support/env.{mark_database_test_executed, queue_database_url}
import grind/unique
import grind/worker

pub fn facade_submit_many_admits_every_job_in_order_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let queue = unique("facade-many")
      let echo_worker =
        worker.new(
          unique("facade.many"),
          input: int_codec(),
          output: int_codec(),
          perform: fn(n) { Ok(n) },
        )
        |> worker.with_queue(queue)
      use jobs <- with_runtime(url, fn(config) {
        config |> grind.with_worker(echo_worker) |> grind.without_consumers
      })
      let inputs = list.index_map(list.repeat(Nil, 50), fn(_, i) { i + 1 })
      let assert Ok(admissions) =
        grind.submit_many(jobs, list.map(inputs, job.new(echo_worker, _)))
      list.length(admissions) |> should.equal(50)
      // Each admission is the job of the input at its position.
      list.zip(admissions, inputs)
      |> list.each(fn(pair) {
        let assert #(grind.Inserted(handle), input) = pair
        grind.arguments(jobs, handle) |> should.equal(Ok(input))
        grind.state(jobs, handle) |> should.equal(Ok(job.Queued))
      })
      count(jobs, queue) |> should.equal(50)
      grind.submit_many(jobs, []) |> should.equal(Ok([]))
      mark_database_test_executed("facade-submit-many-in-order")
    }
  }
}

/// One job the input codec or payload limit refuses refuses the batch:
/// nothing is written.
pub fn facade_submit_many_writes_nothing_when_one_job_is_refused_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let queue = unique("facade-many-refused")
      let echo_worker =
        worker.new(
          unique("facade.many.refused"),
          input: string_codec(),
          output: string_codec(),
          perform: fn(text) { Ok(text) },
        )
        |> worker.with_queue(queue)
      use jobs <- with_runtime(url, fn(config) {
        config
        |> grind.with_worker(echo_worker)
        |> grind.with_max_payload_bytes(16)
        |> grind.without_consumers
      })
      grind.submit_many(jobs, [
        job.new(echo_worker, "short"),
        job.new(echo_worker, string.repeat("x", 64)),
      ])
      |> should.equal(Error(grind.PayloadTooLarge(bytes: 66, limit: 16)))
      count(jobs, queue) |> should.equal(0)
      mark_database_test_executed("facade-submit-many-refused")
    }
  }
}

/// A job id is an idempotency key in a batch as it is for `submit`: a
/// repeated batch returns the jobs the first one admitted, and a reused id
/// with another input is a conflict that writes nothing.
pub fn facade_submit_many_job_ids_are_idempotency_keys_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let queue = unique("facade-many-ids")
      let echo_worker =
        worker.new(
          unique("facade.many.ids"),
          input: int_codec(),
          output: int_codec(),
          perform: fn(n) { Ok(n) },
        )
        |> worker.with_queue(queue)
      use jobs <- with_runtime(url, fn(config) {
        config |> grind.with_worker(echo_worker) |> grind.without_consumers
      })
      let id = unique("order")
      let batch =
        list.map([1, 2, 3], fn(n) {
          job.new(echo_worker, n) |> job.with_id(id <> "-" <> int.to_string(n))
        })
      let assert Ok(first) = grind.submit_many(jobs, batch)
      let assert Ok(again) = grind.submit_many(jobs, batch)
      list.map(again, admitted_id) |> should.equal(list.map(first, admitted_id))
      count(jobs, queue) |> should.equal(3)

      grind.submit_many(jobs, [
        job.new(echo_worker, 4) |> job.with_id(id <> "-4"),
        job.new(echo_worker, 9) |> job.with_id(id <> "-1"),
      ])
      |> should.equal(Error(grind.IdConflict))
      count(jobs, queue) |> should.equal(3)
      mark_database_test_executed("facade-submit-many-ids")
    }
  }
}

/// A job with a uniqueness policy keeps its meaning in a batch: the second
/// job with the same key finds the first, which the same transaction wrote.
pub fn facade_submit_many_applies_uniqueness_in_order_test() {
  case queue_database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let queue = unique("facade-many-unique")
      let echo_worker =
        worker.new(
          unique("facade.many.unique"),
          input: int_codec(),
          output: int_codec(),
          perform: fn(n) { Ok(n) },
        )
        |> worker.with_queue(queue)
      use jobs <- with_runtime(url, fn(config) {
        config |> grind.with_worker(echo_worker) |> grind.without_consumers
      })
      let policy =
        unique.policy(
          unique.full_input(),
          unique.within(duration.hours(1), from: unique.FromInsertion),
        )
      let assert Ok([
        grind.Inserted(plain),
        grind.Inserted(first),
        grind.Existing(conflict),
      ]) =
        grind.submit_many(jobs, [
          job.new(echo_worker, 1),
          job.new(echo_worker, 2) |> job.unique(policy),
          job.new(echo_worker, 2) |> job.unique(policy),
        ])
      conflict.job_id |> should.equal(job.id(first))
      grind.arguments(jobs, plain) |> should.equal(Ok(1))
      count(jobs, queue) |> should.equal(2)
      mark_database_test_executed("facade-submit-many-unique")
    }
  }
}

fn admitted_id(admission: grind.Admission(Int, Int, e)) -> Int {
  let assert grind.Inserted(handle) = admission
  job.id(handle)
}

fn count(jobs: grind.Grind, queue: String) -> Int {
  let assert Ok(rows) =
    admin.list(jobs, admin.query(limit: 100) |> admin.in_queue(queue))
  list.length(rows)
}
