//// A validating codec from outside the package: a rejected input writes
//// nothing; a rejected output ends the job as a runtime failure.

import gleam/option.{None}
import gleam/time/duration
import gleeunit/should
import grind
import grind/job
import grind/worker
import grind_consumer
import grind_consumer/support/env

pub fn a_validating_codec_rejects_before_and_after_the_handler_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let negate =
        worker.new(
          env.unique("codec.negate"),
          input: grind_consumer.amount_codec(),
          output: grind_consumer.amount_codec(),
          perform: fn(amount) { Ok(0 - amount) },
        )
        |> worker.with_queue(env.unique("codec"))
      use jobs <- env.with_grind(url, grind.with_worker(_, negate))
      grind.submit(jobs, job.new(negate, -1))
      |> should.equal(Error(grind.InvalidInput("amount must not be negative")))
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(negate, 1))
      grind.await(jobs, handle, within: duration.seconds(10))
      |> should.equal(
        Ok(grind.Failed(
          grind.RuntimeFailure,
          None,
          "output codec rejected the handler's output: amount must not be negative",
        )),
      )
      env.mark("consumer-validating-codec-passed")
    }
  }
}
