//// Observations: attach to Grind's Sinal descriptors with plain
//// `sinal.observe`. Events are best-effort, so outcomes are read from the
//// durable rows, never from an event.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/io
import gleam/json
import gleam/time/duration
import grind
import grind/job
import grind/telemetry
import grind/worker
import grind_usage/support
import sinal
import sinal/correlation

fn amount() -> worker.Codec(Int) {
  worker.codec(worker.infallible(json.int), decode.int)
}

pub fn run(url: String) -> Nil {
  let double =
    worker.new(
      support.unique("observed.double"),
      input: amount(),
      output: amount(),
      perform: fn(value) { Ok(value * 2) },
    )
    |> worker.with_queue(support.unique("observed"))

  let acknowledged = process.new_subject()
  let capacity = process.new_subject()
  // A lifecycle event: one committed disposition for one claimed attempt.
  let ack_handler =
    sinal.observe(telemetry.acknowledged(), fn(_measurements, metadata) {
      case metadata.ref.queue == double.queue {
        True -> process.send(acknowledged, metadata)
        False -> Nil
      }
    })
  // A diagnostic event: the coordinator's local capacity ledger.
  let capacity_handler =
    sinal.observe(telemetry.capacity(), fn(measurements, metadata) {
      case metadata.queue.queue == double.queue {
        True -> process.send(capacity, measurements)
        False -> Nil
      }
    })

  use jobs <- support.with_grind(url, grind.with_worker(_, double))
  let assert Ok(order) = correlation.from_string(support.unique("order"))
  let assert Ok(grind.Inserted(handle)) =
    grind.submit(jobs, job.new(double, 4) |> job.with_correlation(order))
  let assert Ok(grind.Succeeded(8)) =
    grind.await(jobs, handle, within: duration.seconds(10))

  let assert Ok(ack) = process.receive(acknowledged, 5000)
  let assert True = ack.ref.correlation == order
  let assert job.Succeeded = ack.committed_state
  let assert telemetry.Replied = ack.confirmation

  let assert Ok(initial) = process.receive(capacity, 5000)
  let assert 10 = initial.maximum

  let _ = sinal.detach(ack_handler)
  let _ = sinal.detach(capacity_handler)
  io.println("observations: acknowledged and capacity events observed")
  support.mark("usage-observations-passed")
}
