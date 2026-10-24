//// Workers and codecs: typed input, output and error, a codec that refuses
//// a value, and what each refusal does.

import gleam/dynamic/decode
import gleam/io
import gleam/json
import gleam/option.{Some}
import gleam/time/duration
import grind
import grind/job
import grind/worker
import grind_usage/support

pub type Invoice {
  Invoice(id: String, cents: Int)
}

pub type ChargeError {
  Declined(invoice: String)
}

/// A codec's encoder may refuse a value, so a codec can validate.
fn invoice_codec() -> worker.Codec(Invoice) {
  worker.codec(
    fn(invoice: Invoice) {
      case invoice.cents >= 0 {
        True ->
          Ok(
            json.object([
              #("id", json.string(invoice.id)),
              #("cents", json.int(invoice.cents)),
            ]),
          )
        False -> Error("an invoice total must not be negative")
      }
    },
    {
      use id <- decode.field("id", decode.string)
      use cents <- decode.field("cents", decode.int)
      decode.success(Invoice(id:, cents:))
    },
  )
}

fn declined_codec() -> worker.Codec(ChargeError) {
  worker.codec(
    worker.infallible(fn(error: ChargeError) {
      json.object([#("declined", json.string(error.invoice))])
    }),
    {
      use invoice <- decode.field("declined", decode.string)
      decode.success(Declined(invoice))
    },
  )
}

fn receipt_codec() -> worker.Codec(Int) {
  worker.codec(worker.infallible(json.int), decode.int)
}

/// A handler is a function from the decoded input to a `Result`. The error
/// type is stored through the worker's error codec.
pub fn charge(queue: String) -> worker.Worker(Invoice, Int, ChargeError) {
  worker.new(
    support.unique("billing.charge"),
    input: invoice_codec(),
    output: receipt_codec(),
    perform: fn(invoice) {
      case invoice.cents {
        13 -> Error(Declined(invoice.id))
        cents -> Ok(cents)
      }
    },
  )
  |> worker.with_queue(queue)
  |> worker.with_error_codec(declined_codec())
  |> worker.with_max_attempts(1)
}

pub fn run(url: String) -> Nil {
  let charge = charge(support.unique("billing"))
  use jobs <- support.with_grind(url, grind.with_worker(_, charge))
  let within = duration.seconds(10)

  // A valid input runs, and the output comes back typed.
  let assert Ok(grind.Inserted(paid)) =
    grind.submit(jobs, job.new(charge, Invoice("inv-1", 4200)))
  let assert Ok(grind.Succeeded(4200)) = grind.await(jobs, paid, within:)

  // The handler's typed error ends the job once its attempts are used.
  let assert Ok(grind.Inserted(declined)) =
    grind.submit(jobs, job.new(charge, Invoice("inv-2", 13)))
  let assert Ok(grind.Failed(
    grind.Business(Declined("inv-2")),
    Some(job.BudgetExhausted),
    _,
  )) = grind.await(jobs, declined, within:)

  // A codec that refuses the value stops the submit before anything is written.
  let assert Error(error) =
    grind.submit(jobs, job.new(charge, Invoice("inv-3", -1)))
  let assert grind.InvalidInput("an invoice total must not be negative") = error
  io.println("workers: " <> grind.describe_submit_error(error))
  support.mark("usage-workers-passed")
}
