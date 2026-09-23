# Implementation scope

## Delivered in the current PostgreSQL slice

- Caller-defined worker inputs, outputs, and errors use ordinary versioned JSON
  codecs. Worker identity and codec metadata come from the definition; registry
  selection binds types before heterogeneous storage.
- PostgreSQL settings are validated before the package starts a connection pool.
  Schema migrations upgrade v1 through v4, preserve live attempt fields and
  historical resolution rows, and reject incompatible columns, state
  constraints, ID defaults, primary keys, and attempt sequences. Historical v2
  resolution rows remain payload-unverifiable because that version did not store
  the typed decision payload; reusing one of those command IDs fails closed.
- Grind owns the pool under a package supervisor, and the admission test closes
  and reopens the same pool name before reading persisted state. PGO may log a
  shutdown error when its asynchronous connection starter is blocked reloading
  type metadata during pool teardown; active-query draining and quiet shutdown
  remain unverified.
- Admission returns typed handles without running handlers. Handles retain the
  storage owner, queue, worker contract, and result codecs. Committed success and
  caller-codec-backed business failure can be read through typed outcomes. A
  process can rebuild a typed handle from a durable numeric ID and the worker
  definition after verifying the stored worker, codec, and storage route.
  Numeric IDs are scoped to the PostgreSQL storage owner, not globally unique.
- Absolute one-time scheduling accepts an opaque nonnegative `job.AvailableAt`.
  Due checks use PostgreSQL time. Automatic polling is the wakeup fallback.
- A supervised OTP consumer runs each attempt in a `Temporary` worker child and
  monitors that exact process before activation. Its pure policy validates a
  positive polling interval, jobs-per-poll limit, lease duration, and local
  concurrency limit. Manual and automatic polling enforce configured capacity;
  capacity is per consumer, not global across consumers. Worker death releases
  local capacity without restarting the effect; default recovery later
  quarantines its expired claim. Stop pauses new claims, keeps renewing active
  leases, and waits for active work through the configured drain grace. If the
  grace expires, it reports the active count and leaves those rows executing
  for reconciliation. The separate bounded supervisor teardown can still
  return a timeout. The integration test verifies that automatic polling stays
  paused and the active lease advances during drain. A stopped batch reports
  earlier calls that returned committed acknowledgements; the failing call is
  excluded even when it committed an operational disposition. A deterministic
  manual consumer is available as a distinct stepping capability.
- Ownership claims have attempt IDs, epochs, owners, and database-time leases;
  acknowledgements fence on those fields and live expiry. Output/error codec
  mismatches are rejected before invocation and persisted as nonclaimable
  `contract_mismatch` rows, so an incompatible head job does not starve later
  compatible work. Queue expiry policy is durable and immutable per storage
  owner and queue;
  default expiry quarantines one old attempt per poll, while explicit
  `ReplayAtLeastOnce` may create a new fenced attempt.
- An uncertain attempt retains its claim metadata until an operator applies an
  audited resolution. The receipt binds its ID to the job route, worker
  contract, decision, actor/details, typed payload codec version, canonical
  JSONB payload, and committed target state. Repeating the command returns its
  original target state even after later execution changes the job. A successful
  acknowledgement writes the job transition and a payload-bound command receipt
  in one transaction. On a transaction error Grind checks that receipt before
  returning `QueueAckUnknown` with the command ID and exact in-memory proposal.
  A real backend termination during `COMMIT` returns Unknown, leaves the row
  executing without a receipt, and does not trigger a blind worker rerun. The
  test does not prove the separate case where PostgreSQL commits but its reply
  is lost; that remains covered only by a simulated result-boundary test. If the
  process crashes after an external effect and before it can retain or commit
  the proposal, the typed proposal may be unrecoverable.
- The external `consumer/` package uses only public Grind modules for two
  differently typed workers, tagged application errors, a configured queue, and
  typed committed outcomes. Its local synthetic effect has an application-owned
  deduplication key; this demonstrates caller-side idempotency only.
- `oracle/` runs pinned Oban OSS `v2.24.1` against a separate disposable database.
  Success is differentially observed; business-failure coverage is inspired and
  records intentional state and error-contract differences.

## Retained backlog

The current slice narrows implementation work; it does not remove any capability
from the Grind design or ecosystem inventory.

| Capability family                          | Remaining work                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Job lifecycle and attempt history          | Retry and retry exhaustion policy, backoff/jitter, snooze accounting, discard, cancellation intent and races, complete persisted attempt history, and committed transition observations. A prepared proposal log after process death remains optional follow-up; the unavoidable external-effect/ack crash window requires application-owned idempotency or a transaction protocol.                                             |
| Claims, leases, workers, and shutdown      | Production exact-expiry rejection still needs an integration test at the acknowledgement boundary. A forced row-lock test proves a second independently pooled consumer skips a row while the first claim transaction remains uncommitted. Orphan rescue and recovery of a running worker after process/queue-owner loss remain open. A real aborted `COMMIT` is tested; successful commit with a lost reply remains simulated. |
| Queue management and capacity              | Pause/resume/scale, live configuration revisions, global capacity, fairness, and atomic cluster occupancy remain open. Configured local concurrency is enforced by manual and automatic polling; the jobs-per-poll setting remains a separate bound on each batch.                                                                                                                                                              |
| Smart queue limits                         | Global limits, rate limits, typed partitions, cluster counters, and failure recovery. Pro-equivalent behavior is deferred without an original observable Grind contract or legitimately available licensed oracle.                                                                                                                                                                                                              |
| Uniqueness                                 | Conservative typed equality, retained receipts, concurrent admission, period boundaries, eligibility groups, queue scope, lock contention, and uncertain-commit reconciliation remain unimplemented. Broader field replacement, arbitrary states, cross-worker uniqueness, bulk uniqueness, and backend-specific policies remain retained too. Admission uniqueness will not imply exactly-once execution.                      |
| Recurring schedules and plugins            | Cron parsing, recurring intervals, timezone and missed-run policy, dynamic schedules, pruner, stager, lifeline, reindexer, plugin lifecycle, and leader/failure behavior.                                                                                                                                                                                                                                                       |
| Storage and notification                   | SQLite, storage/notifier abstractions, later PostgreSQL migration history, LISTEN/NOTIFY with polling recovery, uncertain admission reconciliation, and transactional cancellation behavior. Committed ACK receipts are implemented; post-commit reply-loss proof, prepared proposals across process crashes, and uncertain admission receipts remain open.                                                                     |
| Lower-level workflows and Saga integration | Raw dependency-based job workflows remain a Grind capability independent of Saga. Durable Saga execution remains an optional `saga_grind` integration; Grind does not require Saga or Fabric.                                                                                                                                                                                                                                   |
| Batches, callbacks, and chunking           | Typed batch entries, aggregation, callback triggers and deduplication, homogeneous chunking, size/timeout flush, partial failures, cancellation, and recovery. Per-poll serial draining is not a job-batch implementation.                                                                                                                                                                                                      |
| Recording and request/response             | Result recorder extensions and bounded synchronous relay/waiting APIs with timeout, late-completion, process-death, and cleanup behavior.                                                                                                                                                                                                                                                                                       |
| Lifecycle observations                     | Grind-owned Sinal facts for committed job, queue, attempt, and plugin transitions; event ordering, duplicates, handler failures, and native telemetry conversion. Observations must never drive policy.                                                                                                                                                                                                                         |
| Testing support                            | Enqueued, inline, and manual modes with explicit persistence, serialization, time, retry, side-effect, and cleanup differences.                                                                                                                                                                                                                                                                                                 |
| Oban OSS coverage                          | Retry, snooze, cancellation, stale acknowledgement, uniqueness, and broader scheduling behavior require separate source-mapped tests and differential runs.                                                                                                                                                                                                                                                                     |
| Oban Pro equivalence                       | Deferred. Public Pro descriptions are not an oracle and do not justify parity claims.                                                                                                                                                                                                                                                                                                                                           |

The complete capability names correspond to the Grind section of Oversight's
`API-COVERAGE.md`: typed workers; immediate/scheduled admission and typed
handles; lifecycle/retry/snooze/discard/backoff; claims/leases/heartbeats and
supervision; queue configuration; global/rate/partition limits; uniqueness;
recurring schedules/plugins; storage/notifier abstraction; raw workflows; Saga
workflows; batches/callbacks; chunks; recorded results/relay; lifecycle
observations; Sinal cancellation facts; and testing modes/helpers.
