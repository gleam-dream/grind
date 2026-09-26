//// The six terminal `grind_jobs.state` values, shared as one SQL fragment
//// rather than six independent copies. `grind_v12`'s `grind_jobs_finished_at_check`
//// constraint and its own backfill `UPDATE` reference this fragment, and it
//// is spliced into the resolution `UPDATE` in `grind/postgres` for the same
//// reason — every write site that needs to ask "is this state one that
//// leaves `finished_at` non-null" asks it the same way. Every per-outcome
//// acknowledgement write in `grind/internal/attempt` already knows
//// statically whether its own proposed state is terminal (unconditionally,
//// or exactly when a concurrent cancellation wins), so those sites do not
//// need this fragment at all — see each `finished_at` assignment in
//// `attempt.acknowledge_transaction`'s own SQL for the reasoning.
////
//// Kept as raw SQL text, not a `List(String)` or a `grind/job.State` list,
//// since every call site splices it directly into a `CHECK`/`WHERE`/`CASE`
//// clause; `grind/job.state_to_stored`'s six terminal variants
//// (`Succeeded`, `BusinessFailed`, `RuntimeFailed`, `ContractMismatch`,
//// `Discarded`, `Cancelled`) are the typed source of truth this text must
//// stay in lockstep with.

/// The `state IN (...)` SQL fragment (no surrounding parentheses) listing
/// every terminal `grind_jobs.state` value exactly once, in the same order
/// `grind_jobs_state_check` (`priv/migrations/*-grind_v11.sql`) lists them.
pub fn states_sql() -> String {
  "'succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'discarded', 'cancelled'"
}
