# Borrow transaction authority without claiming business commit

<a id="adr-0004"></a>

- **Decision.** submit_in validates an open READ COMMITTED SingleConnection for the same database, scopes and restores local settings, and performs only admission statements. The caller owns commit/rollback. No committed admitted observation is emitted from staging.

- **Rationale.** Issuing BEGIN/COMMIT inside an application transaction could prematurely commit business writes. A matching old admission receipt proves that job admission happened, not that this new surrounding transaction committed.

- **Alternatives.** A Grind-owned transaction callback would create another transaction/connection owner. Nested transactions without savepoint or ownership semantics are unsafe. Treating staged admission as durable conflates a statement reply with COMMIT.

- **Evidence and history.** 749c0dcef14500dd05719d3769b871ea64aa380f and follow-up 9ccbfb57dd01fe736632e2463df14225b383928f capture the public facade/shared pool fixes. src/grind.gleam submit_in and consumer/test/grind_consumer/admission_test.gleam prove current behavior. oversight/apps/checkout/src/checkout/orders.gleam composes business writes and enqueue. Old RELEASE-EXECUTION before-1.0 alternatives are resolved by the checked borrowed-connection API.

- **Source revisions.** [749c0dce](https://github.com/gleam-dream/grind/commit/749c0dcef14500dd05719d3769b871ea64aa380f), [9ccbfb57](https://github.com/gleam-dream/grind/commit/9ccbfb57dd01fe736632e2463df14225b383928f).
