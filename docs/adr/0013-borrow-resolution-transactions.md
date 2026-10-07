# Borrow resolution transactions for application acknowledgment

<a id="adr-0013"></a>

## Decision

- On 7 October 2026, the owner approved the narrow transactional capability after separate consumer prototypes compared it with retained receipts and archival before deletion. Add `admin.resolve_uncertain_in` using the caller's transaction and the existing resolution protocol.
- Return `Staged(Resolved)` and retain exact-command equality, typed value validation, attempt fencing and cancellation rules. The caller owns commit, rollback and recovery from an unknown outer commit.
- Keep financial investigation and settlement application-owned. Introduce no archive, callback protocol, background process, schema migration or independent receipt lifetime.

## Rationale and alternatives

- Standalone resolution commits independently of a surrounding application transaction. A crash before the application saves acknowledgment can leave an unresolved local record; pruning then removes the job and its resolution receipt.
- Borrowing the transaction lets the application commit queue resolution and its acknowledgment together. The application acknowledgment survives pruning under application retention; it does not prove an external effect without the application's own evidence.
- Independently retained receipts can instead recover the gap within their retention period. They require a lifetime, expiry policy, migration and public lookup that does not depend on a live job.
- Archival and deletion hooks remain compatible alternatives for other consumers. They add callback duration, failure, retry and retention obligations; external archival requires durable delivery beyond a PostgreSQL transaction. Defer that broader capability until a concrete consumer establishes its contract.

## Consequences

- READ COMMITTED and same-database constraints match the existing borrowed admission boundary. Database-OID checking inherits its documented cluster-identity limits; it is not authentication.
- Resolution bounds each lock and statement wait by the configured storage deadline without raising a stricter caller limit. It restores search_path and both timeout settings after successful statements; the caller still bounds its whole transaction and network waits.
- A fresh resolution holds its job lock until the caller commits or rolls back. Short application writes can share that transaction; provider calls and slow investigation cannot.
- No committed resolved observation is emitted by staging. Even an exact prior receipt does not prove that the current outer application transaction committed.
- No migration changes stored jobs or receipts. Proof already deleted under the previous separate-commit procedure remains unavailable.

## Evidence

- The separate public consumer first failed with a control delegating to standalone resolution: outer rollback removed the application acknowledgment but left the job succeeded. The same regression passes through the borrowed transaction.
- Retained PostgreSQL consumer tests cover native values, rollback, process death before and after commit, exact-command conflicts, cancellation, scoped settings, concurrent same-job resolution and unrelated pruning. A TCP proxy forwards COMMIT and suppresses its reply; independent readback proves the application acknowledgment survives caller loss and pruning.
- The reply-loss test establishes durable readback after caller loss, not automatic driver retry classification. These checks do not qualify whole-node restart, PostgreSQL failover, remote latency, pool saturation or production throughput.
- Preceding disposable prototypes retained inputs and logs under `/tmp/grind-resolution-candidates`; those local measurements informed the direction and are not release performance guarantees. The maintained consumer tests are the reproducible acceptance evidence.
