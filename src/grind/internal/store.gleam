//// The shared, checkout-guarded storage-call wrappers every Grind module
//// that touches PostgreSQL funnels through — the one source of truth for
//// `execute_safely`/`call_safely`/`transaction_safely` and the transaction
//// wrappers `grind/postgres` and `grind/internal/unique_admission` both
//// call into.

import pog

// Every Grind storage call funnels through one of these four wrappers,
// which check out their own connection under a deadline
// (`set_deadline`/`clear_deadline`, called once from `postgres.start`/
// `postgres.close`) and run against the pog `Connection` shape
// `{single_connection, Conn}` rather than letting pog re-checkout with its
// own unconfigurable, hardcoded default — see `src/grind_postgres_ffi.erl`'s
// own module documentation for the full mechanism and why it also fixes
// DEFECT 2 (a `pog_ffi:convert_error` checkout-error shape with no matching
// clause, which could otherwise crash the caller with `error:function_clause`
// instead of a typed error).
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

/// Distinguishes a checkout failure (definitely not committed) from a
/// genuinely uncertain post-checkout outcome (checked out fine, then lost
/// the connection during the callback or its own `COMMIT`; might have
/// committed). `Error(Nil)` means checkout itself failed — nothing was ever
/// sent; `Ok(Result)` means the transaction actually ran to completion
/// (successfully, rolled back, or with its own `TransactionError`), and
/// `Result` is exactly what it returned. `grind/internal/unique_admission`'s
/// own `run` is the only caller that needs this distinction today; other
/// `transaction_safely` callers (acknowledgement, audited resolution)
/// conservatively still report their own "unknown" outcome for a checkout
/// failure too (`docs/IMPLEMENTATION-SCOPE.md` backlog) — safe, only less
/// precise. See "Admission transaction" in `docs/UNIQUENESS-CONTRACT.md` for
/// the full rationale.
@external(erlang, "grind_postgres_ffi", "transaction_or_checkout_failure")
pub fn transaction_or_checkout_failure(
  connection: pog.Connection,
  callback: fn(pog.Connection) -> Result(a, b),
) -> Result(Result(a, pog.TransactionError(b)), Nil)

/// Attaches the checkout deadline (milliseconds) `execute_safely`/
/// `call_safely`/`transaction_safely` bound every storage call against this
/// pool by, keyed on the pool's own atom name via `persistent_term`. Called
/// once from `postgres.start`, with the pool's freshly named `Connection`
/// (always the `{pool, Name}` shape at that point) — *before* that pool's
/// own supervisor is started, so no in-flight checkout can ever observe
/// this name with no deadline attached yet.
@external(erlang, "grind_postgres_ffi", "set_deadline")
pub fn set_deadline(connection: pog.Connection, deadline_ms: Int) -> Nil

/// Erases the deadline `set_deadline` attached. Called from `postgres.close`
/// only once its own `stop_supervisor` call confirms this `close` actually
/// stopped a still-live pool process — never for a stale `Database` handle
/// whose supervisor had already stopped, since the same pool name may since
/// have been reused by a fresh `start` of the same `ValidatedSettings`; see
/// `postgres.close`'s own doc comment.
@external(erlang, "grind_postgres_ffi", "clear_deadline")
pub fn clear_deadline(connection: pog.Connection) -> Nil
