//// Handlers that need more than a `Result`: the job context, a snooze, a
//// discard, and cooperative cancellation of a running attempt.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/io
import gleam/json
import gleam/time/duration
import grind
import grind/job
import grind/queue
import grind/worker
import grind_usage/support

fn amount() -> worker.Codec(Int) {
  worker.codec(worker.infallible(json.int), decode.int)
}

pub fn run(url: String) -> Nil {
  let within = duration.seconds(10)

  // `worker.responding` hands the handler a context and takes a `Response`.
  // The first run snoozes: the job runs again later without using an attempt.
  let snoozing =
    worker.responding(
      support.unique("handlers.snooze"),
      input: amount(),
      output: amount(),
      handle: fn(context, _value) {
        case worker.snooze_count(context) {
          0 ->
            worker.Snoozed(
              after: duration.milliseconds(50),
              reason: "waiting for a dependency",
            )
          snoozes -> worker.Succeeded(snoozes)
        }
      },
    )
    |> worker.with_queue(support.unique("handlers-snooze"))

  // A handler can end a job as discarded, without retrying.
  let discarding =
    worker.responding(
      support.unique("handlers.discard"),
      input: amount(),
      output: amount(),
      handle: fn(_context, _value) { worker.Discarded("not applicable") },
    )
    |> worker.with_queue(support.unique("handlers-discard"))

  // A running handler learns of `grind.cancel` through its context.
  let running = process.new_subject()
  let blocking =
    worker.responding(
      support.unique("handlers.block"),
      input: amount(),
      output: amount(),
      handle: fn(context, value) {
        process.send(running, Nil)
        case process.selector_receive(worker.cancellation(context), 20_000) {
          Ok(Nil) -> worker.Cancelled("caller cancelled")
          Error(Nil) -> worker.Succeeded(value)
        }
      },
    )
    |> worker.with_queue(support.unique("handlers-block"))

  use jobs <- support.with_grind(url, fn(config) {
    config
    |> grind.with_statement_deadline(duration.milliseconds(1500))
    |> grind.with_unique_lock_wait(duration.milliseconds(400))
    |> grind.with_worker(snoozing)
    |> grind.with_worker(discarding)
    |> grind.with_worker(blocking)
    |> grind.with_queue(
      queue.new(blocking.queue) |> queue.with_lease(duration.seconds(6)),
    )
  })

  let assert Ok(grind.Inserted(snoozed)) =
    grind.submit(jobs, job.new(snoozing, 0))
  let assert Ok(grind.Succeeded(1)) = grind.await(jobs, snoozed, within:)

  let assert Ok(grind.Inserted(discarded)) =
    grind.submit(jobs, job.new(discarding, 0))
  let assert Ok(grind.Discarded("not applicable")) =
    grind.await(jobs, discarded, within:)

  let assert Ok(grind.Inserted(cancelled)) =
    grind.submit(jobs, job.new(blocking, 1))
  let assert Ok(Nil) = process.receive(running, 10_000)
  let assert Ok(grind.CancellationRequested) = grind.cancel(jobs, cancelled)
  // The committed reason records that the caller asked for it.
  let assert Ok(grind.Cancelled("cancelled by caller")) =
    grind.await(jobs, cancelled, within:)

  io.println("handlers: snoozed, discarded and cancelled")
  support.mark("usage-handlers-passed")
}
