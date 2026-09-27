//// The bench audit checker: I1-I7, as far as they are measurable from a
//// single-node, single-schema-pair bench run.
////
//// Each invariant is proven by seeding a deliberate violation against a
//// live database and showing the matching check function turns red, then
//// green on the same data once the seed is undone or the checker runs
//// against an unmutated ledger -- see `bench/test/grind_bench_audit_test.gleam`.
////
//// Every query in this module runs against a **fresh, run-scoped Grind
//// schema** (`grind_bench.default_config`'s own `"bench_jobs_" <>
//// fresh_run_id()`, see item 1) -- this is what makes `check_no_extra_jobs`
//// sound: with a shared, long-lived schema, an unrelated job from some
//// other run would look identical to a genuinely lost ledger row, and this
//// check could never be trusted.
////
//// `AuditError` (not a bare `pog.QueryError`) is every check function's own
//// error type: a query that returns a row shape this module did not expect
//// (e.g. `count_query`'s own single-row/single-column contract) is itself
//// an audit failure, not a value silently coerced to `-1` and folded into
//// "zero problems found" by an unlucky caller.

import gleam/dynamic/decode
import gleam/list
import gleam/result
import gleam/string
import pog

/// The six terminal `grind_jobs.state` values, kept as a bench-owned literal
/// rather than importing `grind/internal/terminal` (out of scope for a
/// public-API-plus-@internal-accessors consumer) — must stay in lockstep
/// with `grind/internal/terminal.states_sql`'s own list; this is exactly the
/// concern `grind_migrations_conformance_test` etc. already guard on the
/// grind side, not something bench needs to re-guard.
const terminal_states_sql = "'succeeded', 'business_failed', 'runtime_failed', 'contract_mismatch', 'discarded', 'cancelled'"

pub type Violation {
  /// I1a: a ledger-recorded submission whose `job_id` no longer has a
  /// matching `grind_jobs` row at all (pruning is expected to be off for
  /// every scenario this checker runs against).
  MissingJobRows(bench_indices: List(Int))
  /// Item 2, bullet 3: a `grind_jobs` row in this run's own schema that no
  /// `bench_submissions` row claims -- an extra or duplicate job the
  /// preload/submission path produced without a matching ledger record.
  /// Only sound against a fresh, run-scoped schema (see this module's own
  /// doc comment) -- every row in `grind_jobs` belongs to this run.
  ExtraJobRows(job_ids: List(Int))
  /// Item 2, bullet 2: `count(bench_submissions)` does not equal the number
  /// of jobs the driver believes it preloaded/submitted.
  SubmissionCountMismatch(expected: Int, actual: Int)
  /// I1b: a ledger-recorded submission whose job has not reached
  /// `succeeded` with the expected `bench_index` as its output. Only
  /// meaningful once the driver believes the run has drained; calling this
  /// mid-run is expected to report violations that are not real defects.
  /// Stronger than "any terminal state" (item 2, bullet 4) -- this bench
  /// worker never fails on purpose, so a healthy drained run must show every
  /// job succeeded with exactly the output it submitted, not merely
  /// "terminal".
  NotSucceededWithExpectedOutput(bench_indices: List(Int))
  /// I2: a job whose `bench_effects` row count does not equal
  /// `1 + (authorized replays recorded for it in grind_job_resolutions)`.
  EffectCountMismatch(job_ids: List(Int))
  /// I3 (healthy runs only): at least one bench job sits in `uncertain`.
  UncertainJobsPresent(count: Int)
  /// I3 (healthy runs only): at least one `[grind, job, quarantined]`
  /// observation fired during the run (a lease actually expired).
  QuarantineObserved(count: Int)
  /// I4: at least one bench job is still `executing` after drain.
  ExecutingAfterDrain(count: Int)
  /// I5, direction A: a bench job currently in a terminal state has no
  /// committed, terminal-state acknowledgement at all (pruning is off, so
  /// this should never happen for a real committed terminal job).
  TerminalJobsMissingAck(job_ids: List(Int))
  /// I5, direction B: a committed, terminal-state acknowledgement exists for
  /// a bench job that is not (or no longer) itself in a terminal state --
  /// the anti-join in the other direction from `TerminalJobsMissingAck`.
  AckWithoutTerminalJob(job_ids: List(Int))
  /// I5: more than one committed, terminal-state acknowledgement recorded
  /// for the same job (pruning off; at most one terminal commit per job is
  /// expected).
  DuplicateTerminalAck(job_ids: List(Int))
  /// I5: a job's committed terminal acknowledgement disagrees with the
  /// job's own current `state` (`grind_job_acknowledgements.committed_state
  /// != grind_jobs.state`).
  AckStateMismatch(job_ids: List(Int))
  /// I6: at least one ledger write failed (observed by the bench worker
  /// itself via `grind_bench_counter_ffi`).
  LedgerWriteErrorsObserved(count: Int)
  /// I6: the PostgreSQL server log recorded at least one deadlock, lock
  /// wait, or ERROR-severity line during this run's own window. See
  /// `check_postgres_log`'s own doc comment for what counts as
  /// "attributable to Grind".
  PostgresLogAnomaliesObserved(lines: List(String))
  /// I7: at least one `[sinal, forwarder, dropped]` event fired during the
  /// run.
  ForwarderDropsObserved(count: Int)
}

pub type Report {
  Report(violations: List(Violation))
}

pub fn passed(report: Report) -> Bool {
  report.violations == []
}

/// Every query failure this module can produce. `UnexpectedQueryShape` is
/// itself an audit failure (item 3, bullet 4): a single-row/single-column
/// aggregate query that comes back some other shape means this module's own
/// assumption about the query no longer holds, which must stop the audit,
/// not silently read as "zero problems".
pub type AuditError {
  QueryFailed(pog.QueryError)
  UnexpectedQueryShape(context: String)
}

fn qualify(schema: String, table: String) -> String {
  "\"" <> schema <> "\".\"" <> table <> "\""
}

fn rows_query(
  sql: String,
  ledger: pog.Connection,
) -> Result(List(Int), AuditError) {
  pog.query(sql)
  |> pog.returning({
    use value <- decode.field(0, decode.int)
    decode.success(value)
  })
  |> pog.execute(ledger)
  |> result.map_error(QueryFailed)
  |> result.map(fn(returned) { returned.rows })
}

/// Item 3, bullet 4: a single-row, single-`int`-column aggregate query
/// (`SELECT count(*) ...`) whose result comes back some other shape is
/// itself an `AuditError`, never a silently-coerced `-1`.
fn count_query(sql: String, ledger: pog.Connection) -> Result(Int, AuditError) {
  let query =
    pog.query(sql)
    |> pog.returning({
      use count <- decode.field(0, decode.int)
      decode.success(count)
    })
  use returned <- result.try(
    pog.execute(query, ledger) |> result.map_error(QueryFailed),
  )
  case returned.rows {
    [count] -> Ok(count)
    _ -> Error(UnexpectedQueryShape(sql))
  }
}

/// I1a: every `bench_submissions` row's `job_id` still has a matching
/// `grind_jobs` row.
pub fn check_no_missing_jobs(
  ledger: pog.Connection,
  grind_schema: String,
) -> Result(Result(Nil, Violation), AuditError) {
  let sql =
    "SELECT bs.bench_index FROM bench_submissions bs "
    <> "LEFT JOIN "
    <> qualify(grind_schema, "grind_jobs")
    <> " j ON j.id = bs.job_id WHERE j.id IS NULL ORDER BY bs.bench_index"
  use rows <- result.map(rows_query(sql, ledger))
  case rows {
    [] -> Ok(Nil)
    missing -> Error(MissingJobRows(missing))
  }
}

/// Item 2, bullet 3: every `grind_jobs` row in this run's own schema has a
/// matching `bench_submissions` row. Only sound against a fresh, run-scoped
/// schema (see this module's own doc comment).
pub fn check_no_extra_jobs(
  ledger: pog.Connection,
  grind_schema: String,
) -> Result(Result(Nil, Violation), AuditError) {
  let sql =
    "SELECT j.id FROM "
    <> qualify(grind_schema, "grind_jobs")
    <> " j LEFT JOIN bench_submissions bs ON bs.job_id = j.id WHERE bs.job_id IS NULL ORDER BY j.id"
  use rows <- result.map(rows_query(sql, ledger))
  case rows {
    [] -> Ok(Nil)
    extra -> Error(ExtraJobRows(extra))
  }
}

/// Item 2, bullet 2: `count(bench_submissions)` equals `expected` (the
/// number of jobs the driver believes it preloaded/submitted this run).
pub fn check_submission_count(
  ledger: pog.Connection,
  expected: Int,
) -> Result(Result(Nil, Violation), AuditError) {
  use actual <- result.map(count_query(
    "SELECT count(*) FROM bench_submissions",
    ledger,
  ))
  case actual == expected {
    True -> Ok(Nil)
    False -> Error(SubmissionCountMismatch(expected, actual))
  }
}

/// I1b: every `bench_submissions` row's job has reached `succeeded` with
/// output equal to its own `bench_index` (see this module's own doc comment
/// for why "succeeded with the right output", not merely "terminal"). Only
/// meaningful once the driver believes the run has drained; calling this
/// mid-run is expected to report violations that are not real defects.
pub fn check_all_succeeded_with_expected_output(
  ledger: pog.Connection,
  grind_schema: String,
) -> Result(Result(Nil, Violation), AuditError) {
  let sql =
    "SELECT bs.bench_index FROM bench_submissions bs JOIN "
    <> qualify(grind_schema, "grind_jobs")
    <> " j ON j.id = bs.job_id WHERE NOT ("
    // Three boolean (never-NULL) conjuncts, deliberately -- `j.output IS
    // NOT NULL` short-circuits PostgreSQL's own three-valued `AND` to a
    // definite `FALSE` (not `NULL`) whenever `output` is NULL, so a queued
    // job with no output yet is correctly `NOT (FALSE) = TRUE` (a
    // violation), not silently excluded by `WHERE`'s own "NULL is not
    // TRUE" rule the way `NOT (state = 'succeeded' AND <null comparison>)`
    // would be.
    <> "j.state = 'succeeded' AND j.output IS NOT NULL AND (j.output #>> '{}')::bigint = bs.bench_index"
    <> ") ORDER BY bs.bench_index"
  use rows <- result.map(rows_query(sql, ledger))
  case rows {
    [] -> Ok(Nil)
    unfinished -> Error(NotSucceededWithExpectedOutput(unfinished))
  }
}

/// I2: `bench_effects` count per job equals `1 + authorized replays`
/// recorded for that job in Grind's own `grind_job_resolutions`.
pub fn check_effect_counts(
  ledger: pog.Connection,
  grind_schema: String,
) -> Result(Result(Nil, Violation), AuditError) {
  let resolutions = qualify(grind_schema, "grind_job_resolutions")
  let sql =
    "SELECT bs.job_id, count(be.id) AS effect_count, "
    <> "coalesce((SELECT count(*) FROM "
    <> resolutions
    <> " r WHERE r.job_id = bs.job_id AND r.decision = 'authorize_replay'), 0) AS replay_count "
    <> "FROM bench_submissions bs "
    <> "LEFT JOIN bench_effects be ON be.bench_index = bs.bench_index "
    <> "GROUP BY bs.job_id, bs.bench_index "
    <> "HAVING count(be.id) != 1 + coalesce((SELECT count(*) FROM "
    <> resolutions
    <> " r WHERE r.job_id = bs.job_id AND r.decision = 'authorize_replay'), 0) "
    <> "ORDER BY bs.job_id"
  use rows <- result.map(rows_query(sql, ledger))
  case rows {
    [] -> Ok(Nil)
    mismatched -> Error(EffectCountMismatch(mismatched))
  }
}

/// I3 (first half): zero bench jobs sitting in `uncertain`. Only meaningful
/// for a healthy run (no deliberate fault injection).
pub fn check_no_uncertain(
  ledger: pog.Connection,
  grind_schema: String,
) -> Result(Result(Nil, Violation), AuditError) {
  let sql =
    "SELECT count(*) FROM "
    <> qualify(grind_schema, "grind_jobs")
    <> " j JOIN bench_submissions bs ON bs.job_id = j.id WHERE j.state = 'uncertain'"
  use count <- result.map(count_query(sql, ledger))
  case count {
    0 -> Ok(Nil)
    n -> Error(UncertainJobsPresent(n))
  }
}

/// I4: zero bench jobs still `executing`. Only meaningful once the run has
/// drained.
pub fn check_no_executing_after_drain(
  ledger: pog.Connection,
  grind_schema: String,
) -> Result(Result(Nil, Violation), AuditError) {
  let sql =
    "SELECT count(*) FROM "
    <> qualify(grind_schema, "grind_jobs")
    <> " j JOIN bench_submissions bs ON bs.job_id = j.id WHERE j.state = 'executing'"
  use count <- result.map(count_query(sql, ledger))
  case count {
    0 -> Ok(Nil)
    n -> Error(ExecutingAfterDrain(n))
  }
}

/// I5, direction A: every bench job currently in a terminal state has at
/// least one committed, terminal-state acknowledgement.
pub fn check_terminal_jobs_have_ack(
  ledger: pog.Connection,
  grind_schema: String,
) -> Result(Result(Nil, Violation), AuditError) {
  let jobs = qualify(grind_schema, "grind_jobs")
  let acks = qualify(grind_schema, "grind_job_acknowledgements")
  let sql =
    "SELECT bs.job_id FROM bench_submissions bs "
    <> "JOIN "
    <> jobs
    <> " j ON j.id = bs.job_id "
    <> "LEFT JOIN "
    <> acks
    <> " a ON a.job_id = j.id AND a.committed_state IN ("
    <> terminal_states_sql
    <> ") "
    <> "WHERE j.state IN ("
    <> terminal_states_sql
    <> ") AND a.job_id IS NULL ORDER BY bs.job_id"
  use rows <- result.map(rows_query(sql, ledger))
  case rows {
    [] -> Ok(Nil)
    missing -> Error(TerminalJobsMissingAck(missing))
  }
}

/// I5, direction B: every committed, terminal-state acknowledgement belongs
/// to a bench job that is currently in a terminal state itself -- the
/// anti-join in the other direction from `check_terminal_jobs_have_ack`.
pub fn check_acks_have_terminal_job(
  ledger: pog.Connection,
  grind_schema: String,
) -> Result(Result(Nil, Violation), AuditError) {
  let jobs = qualify(grind_schema, "grind_jobs")
  let acks = qualify(grind_schema, "grind_job_acknowledgements")
  let sql =
    "SELECT DISTINCT a.job_id FROM "
    <> acks
    <> " a JOIN bench_submissions bs ON bs.job_id = a.job_id "
    <> "LEFT JOIN "
    <> jobs
    <> " j ON j.id = a.job_id AND j.state IN ("
    <> terminal_states_sql
    <> ") "
    <> "WHERE a.committed_state IN ("
    <> terminal_states_sql
    <> ") AND j.id IS NULL ORDER BY a.job_id"
  use rows <- result.map(rows_query(sql, ledger))
  case rows {
    [] -> Ok(Nil)
    orphaned -> Error(AckWithoutTerminalJob(orphaned))
  }
}

/// I5: at most one committed, terminal-state acknowledgement per job.
pub fn check_at_most_one_terminal_ack(
  ledger: pog.Connection,
  grind_schema: String,
) -> Result(Result(Nil, Violation), AuditError) {
  let acks = qualify(grind_schema, "grind_job_acknowledgements")
  let sql =
    "SELECT a.job_id FROM "
    <> acks
    <> " a JOIN bench_submissions bs ON bs.job_id = a.job_id "
    <> "WHERE a.committed_state IN ("
    <> terminal_states_sql
    <> ") GROUP BY a.job_id HAVING count(*) > 1 ORDER BY a.job_id"
  use rows <- result.map(rows_query(sql, ledger))
  case rows {
    [] -> Ok(Nil)
    duplicated -> Error(DuplicateTerminalAck(duplicated))
  }
}

/// I5: a job's committed terminal acknowledgement's `committed_state`
/// matches the job's own current `state`.
pub fn check_ack_state_matches_job(
  ledger: pog.Connection,
  grind_schema: String,
) -> Result(Result(Nil, Violation), AuditError) {
  let jobs = qualify(grind_schema, "grind_jobs")
  let acks = qualify(grind_schema, "grind_job_acknowledgements")
  let sql =
    "SELECT DISTINCT j.id FROM "
    <> jobs
    <> " j JOIN bench_submissions bs ON bs.job_id = j.id "
    <> "JOIN "
    <> acks
    <> " a ON a.job_id = j.id "
    <> "WHERE j.state IN ("
    <> terminal_states_sql
    <> ") AND a.committed_state IN ("
    <> terminal_states_sql
    <> ") AND a.committed_state != j.state ORDER BY j.id"
  use rows <- result.map(rows_query(sql, ledger))
  case rows {
    [] -> Ok(Nil)
    mismatched -> Error(AckStateMismatch(mismatched))
  }
}

/// I6 (partial): zero ledger-write failures observed by the bench worker
/// itself during this run.
pub fn check_no_ledger_write_errors(
  error_count: Int,
) -> Result(Nil, Violation) {
  case error_count {
    0 -> Ok(Nil)
    n -> Error(LedgerWriteErrorsObserved(n))
  }
}

/// I6: scans `log_window` (the PostgreSQL server log lines written strictly
/// during this run -- see `grind_bench/load`'s own capture of
/// `GRIND_BENCH_POSTGRES_LOG`, which slices by line count rather than
/// timestamp so no `log_line_prefix` configuration is required) for
/// deadlocks, lock waits, or ERROR-severity lines. "Attributable to Grind"
/// here means: this disposable cluster (`scripts/bench-postgres.sh`) runs
/// nothing else, so any such line during the run's own window is Grind's
/// (or the bench harness's own SQL against Grind's schema).
///
/// Matches (case-sensitive, PostgreSQL's own wording):
///   - `"deadlock detected"` (the deadlock detector fired)
///   - `"still waiting for"` (`log_lock_waits=on`'s own message)
///   - `"ERROR:"` (PostgreSQL's own severity marker, not a bare substring
///     match on the word "error" -- avoids matching an application's own
///     log line that merely mentions the word)
pub fn check_postgres_log(log_window: List(String)) -> Result(Nil, Violation) {
  let matches =
    list.filter(log_window, fn(line) {
      string.contains(line, "deadlock detected")
      || string.contains(line, "still waiting for")
      || string.contains(line, "ERROR:")
    })
  case matches {
    [] -> Ok(Nil)
    lines -> Error(PostgresLogAnomaliesObserved(lines))
  }
}

/// I7: zero `[sinal, forwarder, dropped]` events observed during this run.
/// `drop_count` is supplied by the driver, which attaches
/// `sinal.observe(id, sinal/forwarder.dropped_event(), ...)` before starting
/// the queue under test.
pub fn check_no_forwarder_drops(drop_count: Int) -> Result(Nil, Violation) {
  case drop_count {
    0 -> Ok(Nil)
    n -> Error(ForwarderDropsObserved(n))
  }
}

/// I3 (second half): zero `[grind, job, quarantined]` observations during a
/// healthy run. `quarantine_count` is supplied by the driver the same way
/// `drop_count` is for I7.
pub fn check_no_quarantine_observed(
  quarantine_count: Int,
) -> Result(Nil, Violation) {
  case quarantine_count {
    0 -> Ok(Nil)
    n -> Error(QuarantineObserved(n))
  }
}

/// Runs every check this module implements against a drained, healthy run
/// and folds the results into one `Report`. `expected_job_count` is the
/// number of jobs the driver believes it preloaded/submitted this run
/// (item 2, bullet 2). `ledger_error_count`, `forwarder_drop_count`, and
/// `quarantine_count` are counters the driver collects independently of SQL
/// (see each check's own doc comment). `postgres_log_window`, if `Some`, is
/// this run's own slice of the PostgreSQL server log (item 6/I6); `None`
/// skips that check entirely (e.g. the log path is unknown).
pub fn run(
  ledger: pog.Connection,
  grind_schema: String,
  expected_job_count: Int,
  ledger_error_count: Int,
  forwarder_drop_count: Int,
  quarantine_count: Int,
  postgres_log_window: Result(List(String), Nil),
) -> Result(Report, AuditError) {
  use missing <- result.try(check_no_missing_jobs(ledger, grind_schema))
  use extra <- result.try(check_no_extra_jobs(ledger, grind_schema))
  use submission_count <- result.try(check_submission_count(
    ledger,
    expected_job_count,
  ))
  use succeeded <- result.try(check_all_succeeded_with_expected_output(
    ledger,
    grind_schema,
  ))
  use effects <- result.try(check_effect_counts(ledger, grind_schema))
  use uncertain <- result.try(check_no_uncertain(ledger, grind_schema))
  use executing <- result.try(check_no_executing_after_drain(
    ledger,
    grind_schema,
  ))
  use ack_missing <- result.try(check_terminal_jobs_have_ack(
    ledger,
    grind_schema,
  ))
  use ack_orphaned <- result.try(check_acks_have_terminal_job(
    ledger,
    grind_schema,
  ))
  use ack_duplicate <- result.try(check_at_most_one_terminal_ack(
    ledger,
    grind_schema,
  ))
  use ack_mismatch <- result.map(check_ack_state_matches_job(
    ledger,
    grind_schema,
  ))
  let log_violations = case postgres_log_window {
    Error(Nil) -> []
    Ok(window) -> result_to_violations(check_postgres_log(window))
  }
  let checks = [
    result_to_violations(missing),
    result_to_violations(extra),
    result_to_violations(submission_count),
    result_to_violations(succeeded),
    result_to_violations(effects),
    result_to_violations(uncertain),
    result_to_violations(executing),
    result_to_violations(ack_missing),
    result_to_violations(ack_orphaned),
    result_to_violations(ack_duplicate),
    result_to_violations(ack_mismatch),
    log_violations,
    result_to_violations(check_no_ledger_write_errors(ledger_error_count)),
    result_to_violations(check_no_forwarder_drops(forwarder_drop_count)),
    result_to_violations(check_no_quarantine_observed(quarantine_count)),
  ]
  Report(list.flatten(checks))
}

fn result_to_violations(outcome: Result(Nil, Violation)) -> List(Violation) {
  case outcome {
    Ok(Nil) -> []
    Error(violation) -> [violation]
  }
}
