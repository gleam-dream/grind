//// Percentile summarizer: turns a raw per-line-JSON evidence file (a load
//// scenario's own latency samples, or one of `grind_bench/sampler_beam`'s /
//// `grind_bench/sampler_db`'s JSONL files) into one committed CSV summary
//// row per numeric field -- the raw JSONL itself is gitignored
//// (`bench/results/*/raw/`); only this rollup is committed
//// (`bench/results/<date>-<commit>/*.csv`), per the plan's own "commit only
//// CSV summaries" rule.

import gleam/dynamic/decode
import gleam/float
import gleam/int
import gleam/json
import gleam/list
import gleam/result
import gleam/string
import simplifile

pub type Percentiles {
  Percentiles(
    count: Int,
    min: Float,
    p50: Float,
    p90: Float,
    p95: Float,
    p99: Float,
    max: Float,
    mean: Float,
  )
}

/// Nearest-rank percentiles over `values` (order does not matter -- this
/// sorts first). `Error(Nil)` for an empty sample; there is no meaningful
/// percentile of nothing.
pub fn percentiles(values: List(Float)) -> Result(Percentiles, Nil) {
  case values {
    [] -> Error(Nil)
    _ -> {
      let sorted = list.sort(values, float.compare)
      let count = list.length(sorted)
      let at = fn(fraction: Float) {
        let index =
          float.round(fraction *. int.to_float(count - 1))
          |> int.clamp(0, count - 1)
        case list_at(sorted, index) {
          Ok(value) -> value
          Error(Nil) -> 0.0
        }
      }
      let assert Ok(min) = list.first(sorted)
      let assert Ok(max) = list.last(sorted)
      let mean =
        list.fold(sorted, 0.0, fn(acc, value) { acc +. value })
        /. int.to_float(count)
      Ok(Percentiles(
        count:,
        min:,
        p50: at(0.5),
        p90: at(0.9),
        p95: at(0.95),
        p99: at(0.99),
        max:,
        mean:,
      ))
    }
  }
}

fn list_at(values: List(Float), index: Int) -> Result(Float, Nil) {
  case values, index {
    [], _ -> Error(Nil)
    [head, ..], 0 -> Ok(head)
    [_, ..rest], n -> list_at(rest, n - 1)
  }
}

pub type ReadError {
  FileUnreadable
  NoValuesFound
}

/// Reads `path` (one JSON object per line, blank lines skipped) and pulls
/// `field` out of each line as a number (int or float in the source JSON --
/// either decodes). A line that fails to decode, or that lacks `field`, is
/// silently skipped rather than failing the whole read: the BEAM/DB samplers
/// each write several different field sets, and a caller reads one field at
/// a time.
pub fn read_field_values(
  path: String,
  field: String,
) -> Result(List(Float), ReadError) {
  use contents <- result.try(
    simplifile.read(path) |> result.replace_error(FileUnreadable),
  )
  let decoder = {
    use value <- decode.field(
      field,
      decode.one_of(decode.float, or: [decode.int |> decode.map(int.to_float)]),
    )
    decode.success(value)
  }
  let values =
    contents
    |> string.split("\n")
    |> list.map(string.trim)
    |> list.filter(fn(line) { line != "" })
    |> list.filter_map(fn(line) {
      json.parse(line, decoder) |> result.replace_error(Nil)
    })
  case values {
    [] -> Error(NoValuesFound)
    _ -> Ok(values)
  }
}

/// One CSV row: `scenario,field,count,min,p50,p90,p99,max,mean`. `write_csv`
/// appends this row (writing the header first if `path` does not exist yet),
/// so several fields/scenarios can accumulate into one committed summary
/// file across a run.
pub fn csv_row(scenario: String, field: String, stats: Percentiles) -> String {
  let Percentiles(count:, min:, p50:, p90:, p99:, max:, mean:, ..) = stats
  [
    scenario,
    field,
    int.to_string(count),
    float.to_string(min),
    float.to_string(p50),
    float.to_string(p90),
    float.to_string(p99),
    float.to_string(max),
    float.to_string(mean),
  ]
  |> string.join(",")
}

pub fn csv_header() -> String {
  "scenario,field,count,min,p50,p90,p99,max,mean"
}

pub fn write_csv(
  path: String,
  rows: List(String),
) -> Result(Nil, simplifile.FileError) {
  let exists = case simplifile.read(path) {
    Ok(_) -> True
    Error(_) -> False
  }
  let header = case exists {
    True -> ""
    False -> csv_header() <> "\n"
  }
  simplifile.append(
    to: path,
    contents: header <> string.join(rows, "\n") <> "\n",
  )
}
