//// Copy cost: a worker that keeps its static data in a module constant and
//// captures nothing large, and a test that bounds its size. Grind copies a
//// worker's whole definition, handler and codecs with everything they
//// capture, into each job's processes.

import gleam/dynamic/decode
import gleam/io
import gleam/json
import gleam/list
import grind/testing
import grind/worker
import grind_usage/support

pub type Order {
  Order(country: String, cents: Int)
}

fn order_codec() -> worker.Codec(Order) {
  worker.codec(
    worker.infallible(fn(order: Order) {
      json.object([
        #("country", json.string(order.country)),
        #("cents", json.int(order.cents)),
      ])
    }),
    {
      use country <- decode.field("country", decode.string)
      use cents <- decode.field("cents", decode.int)
      decode.success(Order(country:, cents:))
    },
  )
}

// snips-start: copy-cost
/// Static data lives in a module constant. Gleam inlines a constant where
/// it is used, so the handler below captures nothing and the runtime does
/// not copy the table with each job.
const vat_percent = [#("DE", 19), #("ES", 21), #("FR", 20), #("PT", 23)]

pub fn pricing() -> worker.Worker(Order, Int, Nil) {
  worker.new(
    "pricing.total",
    input: order_codec(),
    output: worker.codec(worker.infallible(json.int), decode.int),
    perform: fn(order) {
      case list.key_find(vat_percent, order.country) {
        Ok(percent) -> Ok(order.cents * { 100 + percent } / 100)
        Error(Nil) -> Ok(order.cents)
      }
    },
  )
}

@external(erlang, "grind_usage_copy_size", "flat_bytes")
fn flat_bytes(term: a) -> Int

/// Bound the worker value in a test, so a later capture of a large value
/// fails here instead of in production memory.
pub fn worker_stays_small() -> Nil {
  let assert True = flat_bytes(pricing()) < 4096
  Nil
}

// snips-end: copy-cost

/// No database: the size check and one run of the handler.
pub fn offline() -> Nil {
  worker_stays_small()
  let assert Ok(worker.Succeeded(12_300)) =
    testing.perform(pricing(), Order(country: "PT", cents: 10_000))
  io.println("copy_cost: the pricing worker stays under 4 kB")
  support.mark("usage-copy-cost-passed")
}
