//// Item 9: `pg_stat_statements` total_exec_time/calls, bucketed by which
//// part of Grind (or the bench harness's own ledger) a statement belongs
//// to -- claim, quarantine, ack, or ledger -- diffed before/after a run so
//// the harness's own statements (the `ledger` bucket) can be subtracted
//// from Grind's own cost. One aggregate `GROUP BY` query per snapshot,
//// classified by a `CASE` over `query` text rather than per-`queryid`
//// bookkeeping (`grind_bench/sampler_db`'s own per-tick sampler already
//// does that finer-grained job for the raw JSONL evidence; this module is
//// the coarser before/after rollup item 9 asks for).
////
//// Classification is necessarily heuristic (SQL text substring matching,
//// not a query planner): a statement touching
//// `grind_job_acknowledgements` is "ack"; one setting `state` to
//// `'uncertain'` on `grind_jobs` is "quarantine" (the lease-expiry scan);
//// any other statement touching `grind_jobs` is "claim_or_other_grind";
//// one touching only `bench_effects`/`bench_submissions` is "ledger" (the
//// bench harness's own writes, never Grind's).

import gleam/dict.{type Dict}
import gleam/dynamic/decode
import gleam/result
import grind_bench/harness_db
import pog

pub type Bucket {
  Ack
  Quarantine
  ClaimOrOtherGrind
  Ledger
  Other
}

pub fn bucket_name(bucket: Bucket) -> String {
  case bucket {
    Ack -> "ack"
    Quarantine -> "quarantine"
    ClaimOrOtherGrind -> "claim_or_other_grind"
    Ledger -> "ledger"
    Other -> "other"
  }
}

fn bucket_from_name(name: String) -> Bucket {
  case name {
    "ack" -> Ack
    "quarantine" -> Quarantine
    "claim_or_other_grind" -> ClaimOrOtherGrind
    "ledger" -> Ledger
    _ -> Other
  }
}

pub type Totals =
  Dict(Bucket, #(Int, Float))

/// One before/after snapshot. `Error(Nil)` means `pg_stat_statements` is
/// unavailable (extension not installed) -- gracefully skipped, matching
/// `grind_bench/sampler_db`'s own best-effort design, never a hard failure.
pub fn snapshot(connection: pog.Connection) -> Result(Totals, Nil) {
  let sql =
    "SELECT "
    <> "CASE "
    <> "WHEN query ILIKE '%grind_job_acknowledgements%' THEN 'ack' "
    <> "WHEN query ILIKE '%grind_jobs%' AND query ILIKE '%uncertain%' THEN 'quarantine' "
    <> "WHEN query ILIKE '%grind_jobs%' THEN 'claim_or_other_grind' "
    <> "WHEN query ILIKE '%bench_effects%' OR query ILIKE '%bench_submissions%' THEN 'ledger' "
    <> "ELSE 'other' END AS bucket, "
    <> "sum(calls)::bigint AS calls, sum(total_exec_time) AS total_ms "
    <> "FROM public.pg_stat_statements "
    <> "WHERE query ILIKE '%grind_%' OR query ILIKE '%bench_%' "
    <> "GROUP BY 1"
  let query =
    pog.query(sql)
    |> pog.returning({
      use bucket <- decode.field(0, decode.string)
      use calls <- decode.field(1, decode.int)
      use total_ms <- decode.field(2, decode.float)
      decode.success(#(bucket_from_name(bucket), calls, total_ms))
    })
  case harness_db.execute(query, connection) {
    Error(_) -> Error(Nil)
    Ok(returned) ->
      Ok(
        returned.rows
        |> list_fold_into_dict,
      )
  }
}

fn list_fold_into_dict(rows: List(#(Bucket, Int, Float))) -> Totals {
  rows
  |> fold_rows(dict.new())
}

fn fold_rows(rows: List(#(Bucket, Int, Float)), acc: Totals) -> Totals {
  case rows {
    [] -> acc
    [#(bucket, calls, total_ms), ..rest] ->
      fold_rows(rest, dict.insert(acc, bucket, #(calls, total_ms)))
  }
}

/// `#(bucket, calls_delta, total_ms_delta)` for every bucket seen in
/// `after` (a bucket absent from `before` is treated as starting at zero --
/// e.g. the first run against a freshly created `pg_stat_statements`).
/// `before: Error(Nil)` (extension unavailable at snapshot time) is treated
/// the same way, so a caller can always call `diff` unconditionally.
pub fn diff(
  before: Result(Totals, Nil),
  after: Result(Totals, Nil),
) -> List(#(Bucket, Int, Float)) {
  case after {
    Error(Nil) -> []
    Ok(after_totals) -> {
      let before_totals = result.unwrap(before, dict.new())
      after_totals
      |> dict.to_list
      |> list_map_diff(before_totals)
    }
  }
}

fn list_map_diff(
  rows: List(#(Bucket, #(Int, Float))),
  before: Totals,
) -> List(#(Bucket, Int, Float)) {
  case rows {
    [] -> []
    [#(bucket, #(calls, total_ms)), ..rest] -> {
      let #(before_calls, before_total_ms) =
        result.unwrap(dict.get(before, bucket), #(0, 0.0))
      [
        #(bucket, calls - before_calls, total_ms -. before_total_ms),
        ..list_map_diff(rest, before)
      ]
    }
  }
}
