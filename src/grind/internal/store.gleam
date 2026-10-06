//// The shared, checkout-guarded storage-call wrappers every Grind module
//// that touches PostgreSQL funnels through — the one source of truth for
//// `execute_safely`/`call_safely`/`transaction_safely` and the transaction
//// wrappers `grind/postgres` and `grind/internal/unique_admission` both
//// call into.

import pog

/// Whether a managed storage call entered its callback after obtaining a
/// usable connection. This does not describe whether SQL committed.
pub type CheckoutOutcome {
  CheckedOut
  CheckoutUnavailable
}

pub type CheckoutTiming {
  /// This call reused an enclosing call's connection and did not check out.
  NoCheckout
  /// Wait covers only pgo checkout calls, summed across stale candidates.
  /// Owner rejection has zero wait and candidates. Pool waiting can exceed D.
  CheckoutTiming(wait_us: Int, candidates: Int, outcome: CheckoutOutcome)
}

pub type Measured(a) {
  /// The unchanged return value, with time from owner admission through
  /// connection cleanup and lifecycle-token release. No subscriber runs here.
  Measured(value: a, call_duration_us: Int, checkout: CheckoutTiming)
}

@external(erlang, "grind_postgres_ffi", "execute_measured")
pub fn execute_measured(
  query: pog.Query(a),
  on connection: pog.Connection,
) -> Measured(Result(pog.Returned(a), pog.QueryError))

@external(erlang, "grind_postgres_ffi", "call_measured")
pub fn call_measured(
  connection: pog.Connection,
  run: fn(pog.Connection) -> Result(pog.Returned(a), pog.QueryError),
) -> Measured(Result(pog.Returned(a), pog.QueryError))

@external(erlang, "grind_postgres_ffi", "transaction_measured")
pub fn transaction_measured(
  connection: pog.Connection,
  callback: fn(pog.Connection) -> Result(a, b),
) -> Measured(Result(a, pog.TransactionError(b)))

// Every Grind storage call funnels through one of these four wrappers,
// which check out their own connection under a deadline
// owned by `grind/internal/pool`, and run against the pog `Connection` shape
// `{single_connection, Conn}` rather than letting pog re-checkout with its
// own unconfigurable, hardcoded default — see `src/grind_postgres_ffi.erl`'s
// module documentation for the mechanism. It also converts unsupported
// checkout-error shapes to typed errors rather than letting
// `pog_ffi:convert_error` crash the caller.
@external(erlang, "grind_postgres_ffi", "execute_safely")
pub fn execute_safely(
  query: pog.Query(a),
  on connection: pog.Connection,
) -> Result(pog.Returned(a), pog.QueryError)

/// Generic form of `execute_safely`, for calling a Squirrel-generated query
/// function (`grind/internal/sql`) that invokes `pog.execute` itself rather
/// than going through `execute_safely`. `run` receives the deadline-checked-
/// out connection to actually issue that call against — never the
/// `connection` passed in here, which may still be a pool.
@external(erlang, "grind_postgres_ffi", "call_safely")
pub fn call_safely(
  connection: pog.Connection,
  run: fn(pog.Connection) -> Result(pog.Returned(a), pog.QueryError),
) -> Result(pog.Returned(a), pog.QueryError)

@external(erlang, "grind_postgres_ffi", "transaction_safely")
pub fn transaction_safely(
  connection: pog.Connection,
  callback: fn(pog.Connection) -> Result(a, b),
) -> Result(a, pog.TransactionError(b))

/// `postgres.migrate`'s own transaction wrapper, under
/// `Settings.migration_deadline_ms` instead of the shared per-pool deadline
/// — a schema migration's DDL step can legitimately need longer than an
/// ordinary job-lifecycle statement.
@external(erlang, "grind_postgres_ffi", "migration_transaction_safely")
pub fn migration_transaction_safely(
  connection: pog.Connection,
  deadline_ms: Int,
  callback: fn(pog.Connection) -> Result(a, b),
) -> Result(a, pog.TransactionError(b))

/// Separates a failed checkout from a transaction that ran and returned an
/// outcome. `Error(Nil)` means nothing was sent; `Ok(Result)` retains the
/// transaction's result, including an unknown commit. Admission uses this
/// distinction. Acknowledgement and resolution wrappers conservatively report
/// unknown outcomes for checkout failures too.
@external(erlang, "grind_postgres_ffi", "transaction_or_checkout_failure")
pub fn transaction_or_checkout_failure(
  connection: pog.Connection,
  callback: fn(pog.Connection) -> Result(a, b),
) -> Result(Result(a, pog.TransactionError(b)), Nil)
