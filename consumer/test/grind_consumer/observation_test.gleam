//// Telemetry from outside the package: handlers attached to the public
//// descriptors see each job's correlation and committed state.

import gleam/erlang/process
import gleam/time/duration
import gleeunit/should
import grind
import grind/job
import grind/telemetry
import grind/worker
import grind_consumer
import grind_consumer/support/env
import sinal
import sinal/correlation

fn double() -> worker.Worker(Int, Int, Nil) {
  worker.new(
    env.unique("observed.double"),
    input: grind_consumer.amount_codec(),
    output: grind_consumer.amount_codec(),
    perform: fn(amount) { Ok(amount * 2) },
  )
  |> worker.with_queue(env.unique("observed"))
}

pub fn public_consumer_observes_claimed_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let double = double()
      let claimed = process.new_subject()
      let capacity = process.new_subject()
      let claimed_handler =
        sinal.observe(telemetry.claimed(), fn(_measurements, metadata) {
          case metadata.ref.queue == double.queue {
            True -> process.send(claimed, metadata)
            False -> Nil
          }
        })
      let capacity_handler =
        sinal.observe(telemetry.capacity(), fn(measurements, metadata) {
          case metadata.queue.queue == double.queue {
            True -> process.send(capacity, measurements)
            False -> Nil
          }
        })
      use jobs <- env.with_grind(url, grind.with_worker(_, double))
      let assert Ok(order) = correlation.from_string(env.unique("order"))
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(double, 4) |> job.with_correlation(order))
      let assert Ok(claim) = process.receive(claimed, within: 10_000)
      claim.ref.job_id |> should.equal(job.id(handle))
      claim.ref.correlation |> should.equal(order)
      claim.previous_state |> should.equal(job.Queued)
      env.mark("consumer-observes-claimed-passed")
      let assert Ok(initial) = process.receive(capacity, within: 5000)
      initial.maximum |> should.equal(10)
      env.mark("consumer-observes-capacity-passed")
      let _ = sinal.detach(claimed_handler)
      let _ = sinal.detach(capacity_handler)
      Nil
    }
  }
}

pub fn public_consumer_observes_acknowledged_test() {
  case env.database_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let double = double()
      let acknowledged = process.new_subject()
      let handler =
        sinal.observe(telemetry.acknowledged(), fn(measurements, metadata) {
          case metadata.ref.queue == double.queue {
            True -> process.send(acknowledged, #(measurements, metadata))
            False -> Nil
          }
        })
      use jobs <- env.with_grind(url, grind.with_worker(_, double))
      let assert Ok(order) = correlation.from_string(env.unique("order"))
      let assert Ok(grind.Inserted(handle)) =
        grind.submit(jobs, job.new(double, 4) |> job.with_correlation(order))
      grind.await(jobs, handle, within: duration.seconds(10))
      |> should.equal(Ok(grind.Succeeded(8)))
      let assert Ok(#(measurements, ack)) =
        process.receive(acknowledged, within: 5000)
      measurements.count |> should.equal(1)
      ack.ref.correlation |> should.equal(order)
      ack.committed_state |> should.equal(job.Succeeded)
      ack.confirmation |> should.equal(telemetry.Replied)
      env.mark("consumer-observes-acknowledged-passed")
      let _ = sinal.detach(handler)
      Nil
    }
  }
}
