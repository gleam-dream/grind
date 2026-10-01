# Operational diagnostics

Owner-approved implementation scope, 2026-09-28, following release item 8.
Starting implementation: `1e87d2c`. This work adds operational observations;
it does not change admission, retry, lease, fencing, quarantine or replay policy.
No new commit, push, tag or publication is part of this implementation request.

## Contract

The governing anchors are Oversight `grind-design.md` section 15 (package-owned
Sinal descriptors, bounded forwarding, observations never control policy) and
`docs/RELEASE-EXECUTION.md`, "Before 1.0 follow-ups". The owner's operational
scope explicitly extends the former post-commit-only description: new
`grind/diagnostic` events describe observed runtime activity, separately from
`grind/observation`'s existing durable lifecycle contracts. Oversight is read-only.

Every new event uses the same Database-owned bounded Sinal forwarder as lifecycle
events, including the reserved renewal pool. No subscriber runs in a checkout,
coordinator, renewer or attempt. Encoding and handoff have finite local cost;
overflow or unavailable forwarding drops observations without changing work.
Ordering holds only per producer. Missing events never prove absence of work.
Names and field keys are fixed literals; identifiers remain strings/integers.
Metadata excludes job input/output/errors, SQL, connection settings and raw
exception messages. Node and consumer-incarnation identity accompany queue and
attempt identity. Metrics describe local consumers, not global queue depth.

| Descriptor under `[grind, diagnostic, …]` | Observation and owner                                                                                                                                                                                                                                                                                                                                                                                      |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `renewal`                                 | Renewer reports each returned attempt result: renewed, skipped lock, live fence unavailable, storage failure, or once-only local completion-renewal budget exhaustion. Signed remaining lease milliseconds come from the database clock in the existing renewal statement; unavailable measurements remain absent. A successful sample describes database evaluation time, not delivery time.              |
| `acknowledgement`                         | ACK transaction owner reports replied/reconciled completion, observed transaction rollback, unresolved outcome, stale fence or command conflict after storage returns. Rollback is reported only from the actual transaction result, never inferred from a general QueueAckFailed. Existing `acknowledged(Reconciled)` remains durable receipt proof.                                                      |
| `acknowledgement_retry`                   | Attempt reports scheduling another ACK/reconciliation call, its observed failure/unknown classification, retry number, delay and elapsed local pending time. No handler retry is implied.                                                                                                                                                                                                                  |
| `checkout`                                | Selected queue storage operations report actual checkout-call wait, candidate count, total wrapper duration and returned success/error, with operation and main/reserved role. Wait sums actual pgo checkout calls across stale candidates; socket probes, SQL and cleanup are outside that wait. Total includes owner admission and cleanup. Already checked-out nested calls produce no duplicate event. |
| `claim_failed`                            | Claim owner reports quarantine-scan or candidate-claim failure with queue/consumer identity and duration; no job identity is invented.                                                                                                                                                                                                                                                                     |
| `capacity`                                | Coordinator reports its observed ledger transitions: configured maximum, active, handler-running, ACK-pending, available slots and draining status. A fenced phase message before the first ACK distinguishes running from pending. Pending attempts retain capacity; stale/removed notifications are ignored.                                                                                             |

A skipped renewal identifies a matching live row not renewed by this statement;
it does not identify the locker. A missing live fence can mean completion,
cancellation, expiry or changed ownership. Budget exhaustion is a local choice
and proves neither database expiry nor quarantine. Existing committed quarantine
observations and durable rows remain authoritative.

Checkout measurement preserves callback count, exceptions, result classification,
absolute deadlines, connection cleanup and lifecycle-token release. A completed
sample is emitted only after cleanup. Unexpected callback crashes and processes
killed before return may produce no completed sample. Pool waiting may exceed
the storage deadline; measurement never clamps it or installs a fresh deadline.
Coverage is the queue's quarantine/claim, renewal, ACK transaction and reconciliation
paths. It excludes other public PostgreSQL APIs, proposal validation before
storage and contract-mismatch parking. Renewal duration is the shared batch-call
time repeated for each returned attempt; checkout events count storage calls.

## Acceptance

1. Wire tests round-trip every closed outcome, reject unknown tags and verify
   fixed payload-free native keys; an external consumer imports the descriptors.
2. Real first-ACK rollback and lost-COMMIT-reply tests observe the relevant
   failure/retry/reconciliation signals, preserve one handler invocation, and
   retain durable acknowledgement semantics.
3. Real lock, expiry and connection-failure tests distinguish renewal outcomes;
   successful database-time headroom and absent failure measurements are explicit.
   Completion-budget exhaustion reports once without inventing quarantine.
4. Actual pool contention separates checkout wait from full operation duration,
   preserves deadlines and callback count, and does not starve reserved renewal.
5. Held handlers and ACKs produce accurate local capacity transitions. Completion,
   death and shutdown update occupancy; stale notifications cannot corrupt it.
6. Slow, overflowing and unavailable diagnostic subscribers do not stall work or
   cleanup. Existing bounded-forwarder delivery regressions remain valid.

Use existing real PostgreSQL fault barriers where possible. Add a failing public
observation assertion before its emission path, then implement and verify it.
Serialize builds and database gates in this tree. Applicable final gates:

- `nix develop --command bash scripts/test-postgres.sh`
- `nix develop --command bash scripts/bench-postgres.sh`
- `nix flake check`

The prior two-hour and matrix evidence remains tied to its archived source. This
slice requires focused fault evidence and the gates above; a repeat of the full
soak or throughput matrix is not implied. Publication and release qualification
remain separate work.

## Evidence map

| Contract                                                                                                                 | Executable evidence                                                                                                                  |
| ------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| Native names, payload-free field maps, all closed tags and decode rejection                                              | `test/grind/observations/diagnostics_wire_test.gleam`; real native telemetry probe; external consumer imports and observations       |
| Rollback, unknown outcome, retry identity and reconciled commit                                                          | `test/grind/queue/executor_test.gleam`, `test/grind/queue/ack_failure_test.gleam`, `test/grind/observations/acknowledged_test.gleam` |
| Database-time headroom, expired fence, storage failure and recovery                                                      | `test/grind/queue/leases_test.gleam`                                                                                                 |
| Held row skipped, same-fence renewal after unlock, budget reported once before independent quarantine                    | `test/grind/observations/diagnostics_runtime_test.gleam`                                                                             |
| Real pool wait versus SQL time, original deadline, nested calls, rollback, original exception stack and stale candidates | `test/grind/database/measured_test.gleam`                                                                                            |
| Local capacity while running, ACK-pending, draining, completed or killed                                                 | `test/grind/queue/capacity_test.gleam`, `test/grind/queue/executor_test.gleam`, `test/grind/queue/claims_test.gleam`                 |
| Blocked diagnostic subscriber, exact overflow and continued commits; unavailable forwarder isolation                     | `test/grind/observations/delivery_test.gleam`                                                                                        |

The pre-implementation run failed on absent measured functions and absent runtime
signals. Focused validation caught a SQL alias ambiguity in the new renewal
projection; distinct returned-column naming and an explicit row qualifier fixed
it. The next focused run passed all 20 tests. Native-wire assertions also passed.
The lock fixture uses a separate observer connection so its deliberate hold does
not race the tested consumer's shorter idle-transaction timeout.

Independent reviews covered codecs, checkout cleanup and exception preservation,
ACK classification, lease semantics, capacity fencing and fault-test causality.
The SQL issue was caught by execution and parent verification, not the first
static review. Final acceptance requires the complete gates listed above.

One full run passed 260 tests and failed the new lock fixture's cleanup because
its 2004 ms idle-transaction timeout raced the 2000 ms renewal tick. The separate
observer connection fixed that fixture, and its focused rerun passed. Another
full run passed 260 tests but failed the existing long-handler ACK fence check.
The host slept from 18:25:08 to 18:25:36 local time on 2026-09-28, overlapping
that test. The wall/native clock gap was consistent with the sleep interval;
the exact missed renewal was not retained. The isolated long-handler rerun
passed without code or expectation changes. Final gates use `caffeinate -i`
only for the command lifetime to prevent idle sleep during lease tests.

## Validation at commit `75e50ae`

- PostgreSQL gate: **261 root tests and 12 external-consumer tests pass**.
  All diagnostic execution markers are required by the gate. The pinned Oban
  harness, 55-row ledger check, 12 core pairs and 14 oracle evidence checks pass.
- Benchmark gate: **38 Gleam tests, 5 Python tests and the 1000-job smoke audit
  pass**, followed by query-plan, open-loop, pruning and full-drain sampling
  activation checks.
- Formatting: `nix flake check` passes on `aarch64-darwin`. An uncached
  filesystem formatting check also passes for the new, untracked files.
- The first benchmark execution rejected an open-loop generator
  maximum lag of 31 ms against its unchanged 20 ms limit; all 50 jobs were
  admitted. The unchanged rerun recorded 6 ms maximum lag for that scenario.
  The failed run is not accepted performance evidence.
- Both benchmark executions record source digest
  `c375838e11d356e17941385446b5d4995533e70aa039960da04e1c7adb072807`.
  Only documentation/evidence status was updated afterward. Sinal was clean at
  `858dfa360e260a3e12cf27c71f9adc40d4f0c3c8`. Its added `Dropped.unavailable`
  field required one benchmark fixture to supply zero; no Sinal source changed.

These results were collected before the implementation was committed as
`75e50ae`. The generated benchmark outputs were removed during the 2026-10-01
cleanup after preserving this summary. The historical findings cannot be
re-audited from raw files here. They establish the selected diagnostic contracts
at the recorded inputs; they do not repeat the full matrix or two-hour soak, or
qualify today's changed dependency set. See [release readiness](RELEASE-READINESS.md).
