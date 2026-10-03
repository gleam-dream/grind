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

/// The domain-wide advisory lock query itself — SQL text, parameter
/// binding, and the `Bool` decoder together — built once here so
/// `acquire_lock` and any test that needs to hold this exact same lock (the
/// forced-overlap and contention tests under `test/grind/unique/`) never
/// re-encode it by hand; `pg_advisory_xact_lock` itself returns `void`,
/// which `pg_types` cannot decode (see `docs/UNIQUENESS-CONTRACT.md`'s
/// PostgreSQL driver note), hence the `SELECT true FROM (...)` wrapping.
/// `schema` is the configured schema (see `lock_key_sql`'s own doc comment
/// for why it is bound here rather than resolved server-side).
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

/// The uniqueness key digest; see `docs/UNIQUENESS-CONTRACT.md`, Decision 1.
pub fn key_digest_sql(key_json_parameter: Int) -> String {
  "sha256(convert_to(("
  <> sql_parameter(key_json_parameter, "jsonb")
  <> ")::text, 'UTF8'))"
}

/// Converts a bound millisecond (`divisor` `1000.0`) or microsecond
/// (`1000000.0`) integer into `timestamptz` (also correct for a bound
/// `NULL`). See `docs/UNIQUENESS-CONTRACT.md`'s PostgreSQL driver note for
/// why time round-trips through a bound integer instead of a decoded value.
pub fn to_timestamptz_sql(param_index: Int, divisor: String) -> String {
  "to_timestamp("
  <> sql_parameter(param_index, "double precision")
  <> " / "
  <> divisor
  <> ")"
}

/// The uniqueness period predicate; see `docs/UNIQUENESS-CONTRACT.md`,
/// admission transaction step 6. `column` and `now_expression` are trusted
/// SQL fragments spliced verbatim, never caller input.
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

/// Every candidate this admission transaction reads is locked, never merely
/// read: a `RescheduleScheduledTo` action needs `FOR UPDATE` (it is about to
/// write `available_at`), and every other action still needs `FOR KEY
/// SHARE` — the weakest lock mode that still conflicts with a `DELETE`
/// (`postgres.prune_finished` locks its own candidates at `FOR UPDATE`
/// strength). `prune_finished` itself never blocks on this: its own scan is
/// `FOR UPDATE SKIP LOCKED`, so a row this transaction already holds is
/// simply skipped, never waited on. The direction that *can* block is this
/// transaction's own read, when `prune_finished` instead reaches and locks
/// this row first — held for as long as that one `DELETE` statement, batch
/// and all, takes to run — this read then waits behind it, and reports
/// `AdmissionContended` if that wait exceeds this transaction's own
/// `lock_timeout`: a correct outcome, bounded to however long that single
/// prune batch holds the row, not a bug. `FOR KEY SHARE` deliberately does
/// *not* conflict with `FOR NO KEY UPDATE`: `attempt.claim_registered_job`,
/// `postgres.cancel_lock`, `lease`'s own quarantine scan, and
/// `postgres.apply_uncertain_resolution`'s own row lock all lock this same
/// table at that weaker strength precisely so an unrelated claim, cancel,
/// quarantine sweep, or resolution racing a `KeepExisting` read of the
/// identical row never spuriously contends (`AdmissionContended`) for a
/// reason that was never actually a write conflict — see
/// `docs/UNIQUENESS-CONTRACT.md`, "Admission transaction" step 6, for the
/// full contention picture.
///
/// The window this lock actually closes: this transaction committing (its
/// own `INSERT` and receipt) *after* `prune_finished`'s `DELETE` statement
/// already took its snapshot but *before* that statement's own scan reaches
/// and locks this exact row — without this lock, `prune_finished`'s `SKIP
/// LOCKED` search would find the row still unlocked at that point and
/// delete it out from under the read this transaction just performed.
/// `grind_v12`'s own `ON DELETE CASCADE` foreign keys are the second,
/// independent backstop for the one narrower window this lock alone cannot
/// close (a prune statement that already locked this row, under its own
/// fixed snapshot, strictly before this admission's own commit becomes
/// visible to it) — see `docs/RECOVERY-EVIDENCE.md`, Increment 24, for why
/// a receipt referencing an already-deleted job can still never become a
/// permanent orphan either way.
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
