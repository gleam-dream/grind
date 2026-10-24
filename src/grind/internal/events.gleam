//// Shared pieces of the job events the engine emits.

import gleam/dynamic/decode
import gleam/int
import gleam/option.{type Option, None, Some}
import grind/internal/store
import grind/telemetry
import pog
import sinal/correlation.{type Correlation}

/// The correlation stored with a job, or one derived from its id for a row
/// admitted before correlations were stored.
pub fn correlation(job_id: Int, stored: Option(String)) -> Correlation {
  case stored {
    Some(value) -> correlation.from_key(value)
    None -> correlation.from_key("grind-job-" <> int.to_string(job_id))
  }
}

/// Reads a job's correlation for an event emitted after a write that did
/// not return it. A failed read falls back to the id-derived correlation.
pub fn read_correlation(
  connection: pog.Connection,
  job_id: Int,
) -> Correlation {
  let query =
    pog.query("SELECT correlation FROM grind_jobs WHERE id = $1")
    |> pog.parameter(pog.int(job_id))
    |> pog.returning({
      use stored <- decode.field(0, decode.optional(decode.string))
      decode.success(stored)
    })
  case store.execute_safely(query, on: connection) {
    Ok(pog.Returned(rows: [stored], ..)) -> correlation(job_id, stored)
    _ -> correlation(job_id, None)
  }
}

/// One event's measurements, stamped with this node's monotonic clock.
pub fn job_measurements() -> telemetry.JobMeasurements {
  telemetry.JobMeasurements(count: 1, monotonic_ms: monotonic_ms())
}

@external(erlang, "grind_queue_ffi", "monotonic_ms")
fn monotonic_ms() -> Int
