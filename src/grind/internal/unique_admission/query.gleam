//// Builds the policy-dependent admission SQL and its bound queries.

import gleam/dynamic/decode
import gleam/int
import grind/internal/unique
import grind/internal/unique_admission/request.{type PolicyPart}
import pog

pub fn lock_key_sql(first_parameter: Int) -> String {
  "hashtextextended(jsonb_build_array('grind-unique-v1', "
  <> sql_parameter(first_parameter - 1, "text")
  <> ", "
  <> sql_parameter(first_parameter, "text")
  <> ", "
  <> sql_parameter(first_parameter + 1, "text")
  <> ", "
  <> sql_parameter(first_parameter + 2, "text")
  <> ", encode("
  <> key_digest_sql(first_parameter + 3)
  <> ", 'hex'))::text, 0)"
}

/// Builds the same domain-wide lock query for admission and contention tests.
/// The configured schema is bound explicitly. `pg_advisory_xact_lock` returns
/// `void`, which the driver cannot decode, so the query returns `true` instead.
pub fn lock_query(
  schema: String,
  worker_id: String,
  worker_version: String,
  key_contract: String,
  encoded_key: String,
) -> pog.Query(Bool) {
  pog.query(
    "SELECT true FROM (SELECT pg_advisory_xact_lock("
    <> lock_key_sql(2)
    <> ")) AS grind_unique_lock",
  )
  |> pog.parameter(pog.text(schema))
  |> pog.parameter(pog.text(worker_id))
  |> pog.parameter(pog.text(worker_version))
  |> pog.parameter(pog.text(key_contract))
  |> pog.parameter(pog.text(encoded_key))
  |> pog.returning({
    use acquired <- decode.field(0, decode.bool)
    decode.success(acquired)
  })
}

fn sql_parameter(index: Int, cast: String) -> String {
  "$" <> int.to_string(index) <> "::" <> cast
}

/// Hashes PostgreSQL's jsonb text representation of the uniqueness key.
/// See docs/adr/0003-separate-command-receipts-from-uniqueness.md.
pub fn key_digest_sql(key_json_parameter: Int) -> String {
  "sha256(convert_to(("
  <> sql_parameter(key_json_parameter, "jsonb")
  <> ")::text, 'UTF8'))"
}

/// Converts a bound millisecond (`1000.0`) or microsecond (`1000000.0`) integer
/// to timestamptz. A bound NULL remains NULL; no timestamp decoder is needed.
pub fn to_timestamptz_sql(param_index: Int, divisor: String) -> String {
  "to_timestamp("
  <> sql_parameter(param_index, "double precision")
  <> " / "
  <> divisor
  <> ")"
}

/// Includes rows whose selected time is on or after the occupancy cutoff.
/// `column` and `now_expression` are trusted SQL fragments, never caller input.
pub fn period_predicate(
  column: String,
  now_expression: String,
  period_ms_expression: String,
) -> String {
  column
  <> " >= "
  <> now_expression
  <> " - ("
  <> period_ms_expression
  <> "::double precision * interval '1 millisecond')"
}

pub fn is_reschedule(action: unique.ConflictAction) -> Bool {
  case action {
    unique.RescheduleScheduledTo(_) -> True
    unique.KeepExisting -> False
  }
}

/// Locks the earliest matching candidate. Rescheduling needs FOR UPDATE;
/// KeepExisting uses FOR KEY SHARE to block deletion while allowing ordinary
/// claim, cancellation, quarantine and resolution updates using FOR NO KEY UPDATE.
///
/// Pruning uses FOR UPDATE SKIP LOCKED and skips candidates admission holds.
/// If pruning locks first, admission waits under its lock_timeout and can report
/// AdmissionContended. Without admission's row lock, pruning could take a snapshot
/// before the receipt committed and delete its job afterward.
///
/// The ON DELETE CASCADE foreign keys independently prevent permanent receipt
/// orphans when a pruning statement locked the job before admission became visible.
/// See docs/adr/0007-validate-forward-migrations-and-bound-recovery-retention.md.
pub fn candidate_sql(
  scope: unique.QueueScope,
  period: unique.PeriodSpec,
  is_reschedule: Bool,
) -> String {
  let base =
    "SELECT id, queue, state, (extract(epoch FROM available_at) * 1000000)::bigint FROM grind_jobs WHERE worker_id = $1 AND worker_version = $2 AND unique_key_contract = $3 AND unique_key_sha256 = "
    <> key_digest_sql(4)
    <> " AND state = ANY($5::text[])"
  let #(scoped, next) = case scope {
    unique.WithinQueue -> #(base <> " AND queue = $6", 7)
    unique.AcrossQueues -> #(base, 6)
  }
  let with_period = case period {
    unique.Unbounded -> scoped
    unique.FinitePeriod(_, from) ->
      scoped
      <> " AND "
      <> period_predicate(
        unique.period_column(from),
        to_timestamptz_sql(next, "1000000.0"),
        sql_parameter(next + 1, "bigint"),
      )
  }
  with_period
  <> " ORDER BY id LIMIT 1"
  <> case is_reschedule {
    True -> " FOR UPDATE"
    False -> " FOR KEY SHARE"
  }
}

pub fn bind_candidate_params(
  query: pog.Query(a),
  worker_id: String,
  worker_version: String,
  queue: String,
  policy_part: PolicyPart,
  eligible_states: List(String),
  now_us: Int,
  period: unique.PeriodSpec,
) -> pog.Query(a) {
  let base =
    query
    |> pog.parameter(pog.text(worker_id))
    |> pog.parameter(pog.text(worker_version))
    |> pog.parameter(pog.text(policy_part.key_contract))
    |> pog.parameter(pog.text(policy_part.encoded_key))
    |> pog.parameter(pog.array(pog.text, eligible_states))
  let scoped = case policy_part.scope {
    unique.WithinQueue -> base |> pog.parameter(pog.text(queue))
    unique.AcrossQueues -> base
  }
  case period {
    unique.Unbounded -> scoped
    unique.FinitePeriod(ms, _) ->
      scoped |> pog.parameter(pog.int(now_us)) |> pog.parameter(pog.int(ms))
  }
}
