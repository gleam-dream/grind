# Recovery evidence

This document tracks, per recovery claim, how it was proven: the fault
injection mechanism, how the test synchronizes with the fault instead of
sleeping past it, and either genuine red-before-green evidence or a named
mutation for a characterization test that passed on first write. It is
deliberately narrower than `oracle/ORACLE-LEDGER.md`: the ledger tracks
Oban-comparison categories; this document tracks _how each recovery claim was
actually forced to happen and observed_, for claims that have no Oban
oracle to compare against.

Three facts hold across every row below and are not repeated per row:

- **Absent receipt does not prove an external effect did not occur.** A
  worker may have performed its effect and the process may have died, or the
  network may have failed, before any receipt was durably committed. Grind's
  receipt table proves an acknowledgement transaction _committed_; it never
  proves the negative case.
- **Database ownership fencing cannot make external effects exactly once.**
  Attempt IDs, epochs, and lease expiry prevent two live claims from both
  believing they own a row at the same time, and prevent a stale claim's
  acknowledgement from overwriting a newer one. None of that constrains what
  a worker did to the outside world (an HTTP call, a charge, an email)
  before or after that fencing decision.
- **The crash window between an effect and its acknowledgement is
  unavoidable.** A worker can perform its effect, and the process (or the
  connection, or the host) can die before the effect's outcome is
  durably recorded. No amount of database fencing closes this window;
  only the caller's own idempotency key, or a two-phase external protocol,
  can.

## Topic index

The entries below retain the implementation names, counts, and commit references
from the runs they describe. Later entries may supersede earlier mechanisms.
Use the [module map](MODULE-MAP.md) for current code locations and the
[oracle ledger](../oracle/ORACLE-LEDGER.md#last-full-run) for the current gate baseline.

| Topic                                            | Evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Coordinator recovery and shutdown                | [Owner recovery](#increment-1--coordinatorowner-recovery-commit-399838f); [Scheduled wakeup and forced shutdown](#increment-4--real-scheduled-wakeup-forced-shutdown-with-pool-cleanup)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| Acknowledgement and audited resolution           | [Lost commit replies](#increment-2--a-genuinely-successful-ack-commit-whose-reply-disappears); [Expired leases](#increment-3--lease-expiry-through-the-production-acknowledgement); [Concurrent resolution](#concurrent-audited-resolution)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Unique admission                                 | [Sequential identity](#increment-6--uniqueness-admission-schema-v11-pure-policy-validation-sequential-identity); [Scope, states, periods and receipts](#increment-7--uniqueness-admission-queue-scope-state-eligibility-period-boundaries-at-database-time-and-receipt-idempotency-approved-plan-increments-47); [Concurrent admission](#increment-8--concurrent-admission-under-a-real-barrier-forced-overlap); [Contention](#increment-9--contention); [Rescheduling](#increment-10--rescheduling); [Uncertain commits](#increment-11--uncertain-admission-commits); [Selected keys](#increment-12--uniqueness-selected-keys)                                                                                                                                                                                                                                                           |
| Observations                                     | [Acknowledged events](#acknowledged-observation--grind-job-acknowledged-round-1); [Other lifecycle events](#round-2-observations--grind-job-admittedclaimedquarantinedresolvedcancellationreleasedcontract_mismatch)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Automatic acknowledgement retry                  | [Retry and renewal bounds](#independent-review-follow-up-reconcile_unique-conflict-passthrough-free-capacity-polling-and-automatic-ack-unknown-retry)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| Connection faults and deadlines                  | [Fault-proxy scenarios](#increment-15--acknowledgement-deadline-a-real-fault-proxy-a-grind-owned-checkout-deadline-and-three-defects-it-surfaced); [Checkout restoration](#increment-18--pog-dependency-dropping-the-fork-restoring-grinds-own-checkout); [Pool close ownership](#increment-19--close-erasing-a-live-pools-checkout-deadline-independent-review-at-f42e6c0)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| Migrations and retention                         | [Migration protocol](#increment-16--migration-mechanism-versioned-steps-advisory-lock-upgrade-harness); [Migration lock deadlines](#increment-20--migration-steps-own-lock-waits-are-now-bounded); [Terminal timestamps](#increment-21--schema-v12-grind_jobsfinished_at-records-when-a-job-finished); [Pruning and admission races](#increment-23--retention-prune_finished-the-supervised-pruner-and-the-for-key-share-admission-race-it-exposed); [Cascade and timer review](#increment-24--independent-review-of-increment-23-on-delete-cascade-lock-mode-contention-and-a-real-timer-leak); [Cigogne and upgrade faults](#increment-34--end-to-end-cigogne-interop-and-a-genuine-upgrade-boundary-lost-reply-reconcile_unique-docsrelease-readinessmd-migration-gaps-docsrisksmd-risk-16); [Controlled lock barrier](#cigogne-migration-serialization--test-controlled-lock-barrier) |
| Capacity and schema isolation                    | [Fill free slots](#increment-26--automatic-polling-fills-every-free-slot-docsrisksmd-risk-6); [Yield between claims](#increment-28--automatic-filling-yields-to-the-mailbox-one-claim-at-a-time-docsrisksmd-risks-4-and-5); [Schema isolation](#increment-29--storage_owner-removed-entirely-isolation-is-the-postgresql-schema-docsrisksmd-risk-7-superseding-increment-27); [Handle installation binding](#increment-30--explicit-settingsschema-handle-installation-binding-an-automated-migration-collision-test-and-a-forbidden-columns-shape-check-independent-review-fixes-to-increment-29-docsrisksmd-risk-7); [Cluster identity and schema races](#increment-31--cluster-identifier-disambiguation-a-concurrent-schema-creation-race-fix-schema-name-validation-and-pooler-documentation-second-round-review-fixes-to-increment-30)                                              |
| Gate and CI                                      | [PostgreSQL CI gate](#increment-32--ci-actually-runs-the-postgresql-gate-docsrelease-readinessmd-packaging-and-documentation); [Gate robustness](#increment-33--ciscript-robustness-and-test-first-coverage-for-increment-3132s-own-review-findings)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Executor ACK, reserved renewal and pool lifetime | [Current runtime and regressions](#executor-acknowledgements-reserved-renewal-and-resource-lifetime--2026-09-28)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| Endurance and host suspension                    | [Failed first long soak and prospective duration](#host-suspension-during-the-first-full-soak--2026-09-28)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Two-hour soak and audit correction               | [Final duration, accounting, resources and audit](#two-hour-mixed-soak-and-final-audit--2026-09-28)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

## Increment 1 — coordinator/owner recovery (commit `399838f`)

### Claim: a coordinator killed mid-attempt never auto-replays; the orphaned attempt is only recoverable through audited resolution

- **Test**: `postgres_coordinator_loss_with_active_work_quarantines_without_replay_test`
  (`test/grind_test.gleam`; marker `coordinator-loss-quarantined-no-replay`).
- **Fault injection**: `process.kill` on the coordinator's own actor pid
  while a worker attempt is blocked mid-execution (the coordinator and its
  worker are linked, not merely supervised, so the kill cascades to the
  worker without a separate kill call — confirmed empirically beforehand
  with a standalone `:supervisor`/link probe, not assumed).
- **Synchronization**: `await_new_coordinator_pid` polls the consumer's
  registered name until it resolves to a pid different from the killed one,
  bounded by a check budget, rather than sleeping a fixed duration.
- **Red before fix** (this is genuine red-before-green, not a
  characterization test):
  ```
  let assert  test/grind_test.gleam:955
   test: grind_test.postgres_coordinator_loss_with_active_work_quarantines_without_replay_test
   code: let assert Ok(second_coordinator) =
      await_new_coordinator_pid(consumer, first_coordinator, 500)
  value: Error(Nil)
   info: Pattern match failed, no pattern matched the value.
  ```
  Root cause: pre-fix, the consumer handle held a pid-bound `Subject`, so
  after the supervisor restarted the coordinator under the same registered
  name, the handle kept resolving the dead pid forever and could never
  observe the restart.
- **Fix**: the coordinator registers a consumer-scoped name each
  incarnation; the client resolves the live pid through that name instead of
  a pid-bound subject.
- **Limits**: this proves the restarted incarnation does not itself replay
  the orphaned attempt. It does not prove no replay ever happens — that is
  a property of lease expiry plus the audited `AuthorizeReplay` gate, tested
  separately. The worker's own external effect (if any) before the kill is
  not observed by this test.

### Claim: after the owning process and its pool both die, a fresh consumer on a reopened pool only ever observes the orphaned attempt as `Uncertain`

- **Test**: `postgres_owner_loss_recovers_on_fresh_consumer_after_pool_restart_test`
  (marker `owner-loss-pool-restart-quarantined-no-replay`).
- **Fault injection**: kill the process that owns the consumer; only once
  that process and its cascaded worker are both confirmed dead is the
  PostgreSQL pool separately closed and reopened, then a fresh consumer
  started against the same row.
- **Characterization test, proven by mutation** (the test passed on first
  write, so red-before-green does not apply; a named mutation stands in for
  it). Mutation: in `postgres.gleam`'s quarantine query
  (`quarantine_expired`), changed the target state from `'uncertain'` to
  `'queued'`. Result:
  ```
  panic  src/gleeunit/should.gleam:10
   test: grind_test.postgres_owner_process_and_pool_loss_recovers_on_fresh_consumer_test
   info:
  Ok(True)
  should equal
  Ok(False)
  ```
  (test was renamed to its current name after this evidence was recorded;
  the code path and assertion are unchanged). The mutated `'queued'` row was
  reclaimed and actually re-executed by the fresh consumer inside the same
  `process_one` call, which is exactly the double-execution this test
  exists to catch. The mutation was reverted and the file diffed
  byte-for-byte against `git show HEAD:...` to confirm a clean revert.
- **Limits**: this does not prove the worker's external effect (if any)
  before the owner died was or was not performed; it only proves the fresh
  consumer never re-invokes the handler without an audited replay.

### Claim: a repeated `stop` (or a `stop` racing a coordinator that already crashed) reports it did not drain, instead of falsely claiming a clean stop

- **Test**: `postgres_repeated_stop_after_coordinator_gone_reports_without_drain_test`
  (marker `stop-after-coordinator-gone-without-drain`).
- **Fault injection**: call `queue.stop` once (a genuine clean stop that
  blocks until the supervisor is actually dead), then call `queue.stop`
  again on the same handle, with no coordinator left to reach.
- **Red before fix**, using a scaffold-then-wire sequence (the new
  `StoppedWithoutDrain` variant was added first so the test would compile
  against a real value rather than a nonexistent constructor):
  ```
  panic  src/gleeunit/should.gleam:10
   test: grind_test.postgres_repeated_stop_after_coordinator_gone_reports_without_drain_test
   info:
  Ok(StoppedCleanly)
  should equal
  Ok(StoppedWithoutDrain)
  ```
- **Fix**: `stop` now distinguishes "no coordinator was reachable at all"
  from "reached a live coordinator and it drained cleanly," returning
  `StoppedWithoutDrain` for the former.
- **Limits**: `StoppedCleanly`/`StoppedWithoutDrain` describe only the
  incarnation this call actually reached. Work abandoned by an earlier,
  already-crashed incarnation is recovered solely through lease expiry and
  audited resolution, regardless of what a later `stop` reports.

### Claim: a shutdown grace timer scheduled by one coordinator incarnation cannot end a later incarnation's drain early

- **Test**: `postgres_stale_shutdown_grace_timer_does_not_end_a_later_drain_early_test`
  (marker `stale-shutdown-grace-timer-scoped-to-incarnation`).
- **Fault injection**: begin a drain on incarnation #1 (scheduling its grace
  timer for `+4000ms`), kill the coordinator before that timer fires
  (orphaning it), let the supervisor restart a new incarnation #2, then
  begin a second, independent drain on #2 with its own `+4000ms` timer.
  Both incarnations' _first-ever_ drain is guaranteed to reach internal
  shutdown generation 1 (a generation only advances once per incarnation's
  lifetime), so the orphaned timer and the live timer are numerically
  indistinguishable unless timers are also scoped per incarnation.
- **Synchronization**: a deterministic lower-bound timing check, not a sleep
  used as the assertion — `elapsed_ms >= shutdown_grace_ms - 600` (i.e.
  `>= 3400`) distinguishes a genuine ~4000ms deadline from a wrongly-early
  ~2800ms cutoff with roughly 600ms of margin on each side, clear of local
  scheduling jitter.
- **Red before fix** (only the timer-routing call sites reverted to the
  named, incarnation-independent subject; everything else left fixed to
  isolate this one change):
  ```
  panic  src/gleeunit/should.gleam:10
   test: grind_test.postgres_stale_shutdown_grace_timer_does_not_end_a_later_drain_early_test
   info:
  False
  should equal
  True
  ```
  (the outcome _type_ — `ShutdownForced(1)` — was already correct in that
  red run; only its timing was wrong, exactly as designed).
- **Fix**: every self-scheduled timer (`Poll`, `ShutdownGraceExpired`,
  `Renew`) now targets a plain, pid-bound subject created fresh per
  incarnation instead of the named, registered subject, so a timer an old
  incarnation scheduled becomes an inert send to a dead pid once that
  incarnation is gone.
- **Limits**: this proves timer isolation specifically; it does not
  simulate a real supervisor restart _race_ against an in-flight `stop`
  call (the helper test drives both incarnations sequentially through a
  test-only hook, `begin_shutdown_for_test`, not through two overlapping
  real `stop` calls).

## Increment 2 — a genuinely successful ack commit whose reply disappears

Both tests below share one fault-injection mechanism. `scripts/test-postgres.sh`
starts the disposable cluster with `-c synchronous_standby_names=grind_never_standby
-c synchronous_commit=local`, so an ordinary commit stays local (fast, no
behavior change for any other test). Each test installs a deferred
constraint trigger on `grind_job_acknowledgements`, filtered to its own
`job_id`, whose function body is `PERFORM set_config('synchronous_commit',
'on', true)` — raising _only that one transaction's_ synchronous_commit
back to `on`. Because the configured standby name never connects, that one
COMMIT reaches PostgreSQL's `SyncRep` wait _after_ its WAL record is already
flushed locally — a genuine commit, with the reply not yet sent. The test
polls `pg_stat_activity` for `wait_event = 'SyncRep'` (the same polling
shape as the pre-existing `wait_for_commit_trigger_backend` helper, which
polls for `wait_event = 'PgSleep'` for the already-committed aborted-commit
test) and then calls `pg_terminate_backend` on that exact backend.

**Confirmed in `postgres.log`** (captured from a standalone run of the
mutated cluster settings, one line pair per test):

```
WARNING:  canceling the wait for synchronous replication and terminating connection due to administrator command
DETAIL:  The transaction has already committed locally, but might not have been replicated to the standby.
```

This is PostgreSQL's own confirmation that the backend had already
committed the transaction locally before its connection was severed — the
exact fault this increment claims to reproduce, not merely an aborted or
rolled-back commit.

**Visibility while parked in `SyncRep`, and why 2a needs no extra barrier
but 2b does.** PostgreSQL's `RecordTransactionCommit` flushes the WAL record
and then calls `SyncRepWaitForLSN` _before_ it calls `ProcArrayEndTransaction`
(the step that actually removes the transaction from the proc array and
makes it visible to other snapshots). So while a backend is parked in
`SyncRep`, its commit is durable (survives a crash) but **invisible to every
other session** — a concurrent `SELECT` on `grind_job_acknowledgements`
would not see the row yet. When `pg_terminate_backend` sets `ProcDiePending`,
`SyncRepWaitForLSN` cancels its wait and returns control to
`RecordTransactionCommit`, which runs `ProcArrayEndTransaction` (making the
commit visible) and only _afterward_, back up the call stack, hits the next
interrupt check that raises the FATAL error and closes the backend's socket.
That sequencing is why **2a is deterministic with no extra barrier**: the
same backend that is committing is also the one whose socket closure Grind's
own coordinator is blocked reading from, so by the time the coordinator
observes `{error, closed}` and runs its receipt lookup, `ProcArrayEndTransaction`
has already completed on that exact backend, in that exact process, before
its socket closed. **2b is different**: Grind's own pool is closed
client-side while the backend is still parked in `SyncRep` (the backend
never notices; nothing server-side has run `ProcArrayEndTransaction` yet).
`pg_terminate_backend` is fire-and-forget — it returns as soon as the
signal is sent, not once the target has actually exited — so nothing
orders "reopen Grind's pool and query" after "the target backend actually
finished committing." The test therefore polls `pg_stat_activity` on the
independent observer connection until that exact backend pid is gone
(bounded checks, not a sleep) before reopening Grind's pool, matching the
same ordering 2a gets naturally.

### Claim: a committed ack whose reply is lost reconciles from the receipt on the very next call, with no rerun

- **Test**: `postgres_ack_committed_reply_lost_reconciles_from_receipt_test`
  (marker `ack-committed-reply-lost-reconciled-passed`).
- **Path exercised**: this is the _production_ path, with no test-only
  seam. The client sees its COMMIT connection close
  (`pog.TransactionQueryError`), which `resolve_ack_transaction_result`
  already unconditionally routes to `reconcile_unknown_ack`, which queries
  `grind_job_acknowledgements` by the stable command ID and finds the
  receipt the transaction actually committed, returning `Ok(True)`.
- **Observed via the real manual `queue.process_one` call** (not a
  simulated boundary function): `process.receive(reply, ...)` equals
  `Ok(Ok(True))`; `postgres.state` reads `Succeeded`; `postgres.outcome`
  reads `SucceededWith("reply-lost-33")`; `reconcile_acknowledgement`
  returns the receipt with `committed_state: Succeeded`; a second
  `process_one` call returns `Ok(False)` (nothing left to claim); the
  worker's invocation subject received exactly one message across the
  whole test.
- **Mutation evidence** (this is a genuinely successful first real run —
  75 passed, 0 failures against a real disposable cluster — so mutation
  evidence, not red-before-green, proves the test discriminates the
  claim). Mutation: in `resolve_ack_transaction_result`, changed
  `Error(pog.TransactionQueryError(_)) -> reconcile_unknown_ack(...)` to
  `Error(pog.TransactionQueryError(_)) -> Error(QueueAckUnknown(command_id, execution))`,
  i.e. skip the receipt lookup and always report Unknown. Result: exactly
  one failure, this test, everything else (including the store-unavailable
  test below and the pre-existing aborted-commit test) unaffected:
  ```
  panic src/gleeunit/should.gleam:10
   test: grind_test.postgres_ack_committed_reply_lost_reconciles_from_receipt_test
   info:
  Ok(Error(QueueProcessFailed(QueueAckUnknown("grind-ack:49:51:1", ExecutedSuccess("ack-reply-lost-output-v1", "\"reply-lost-33\"")))))
  should equal
  Ok(Ok(True))
  74 passed, 1 failures
  ```
  The mutation was reverted immediately after capturing this; `git diff`
  against the reverted file shows no trace of the mutated line, and
  `gleam check` recompiles clean with the original warnings gone.

### Claim: if the store is also unavailable when a committed ack's reply is lost, the caller sees `QueueAckUnknown` (not a crash), and the commit still reconciles once the store comes back

- **Test**: `postgres_ack_committed_reply_lost_with_store_unavailable_is_unknown_test`
  (marker `ack-committed-reply-lost-store-unavailable-unknown-passed`).
- **Fault injection**: identical SyncRep setup, but instead of terminating
  the stuck backend immediately, the test first calls `postgres.close`
  on Grind's own pool while that backend is still parked in `SyncRep`. A
  second, independent observer pool (`pool_size: 1`, its own pool name) is
  used throughout to poll for the SyncRep wait, read the committed attempt
  identity (`grind_jobs.attempt_id`/`attempt_epoch`, unchanged by a
  successful-state commit), and — only after the store-unavailable
  assertion below — terminate the stuck backend.
- **Observed**: `process.receive(reply, ...)` yields
  `Ok(Error(QueueProcessFailed(QueueAckUnknown(command_id, proposed))))`
  with the exact command ID computed independently from the observed
  attempt identity, and the exact proposed `worker.ExecutedSuccess(...)`
  value — not `QueueActorExited`, and no coordinator crash. This ran
  against real Grind code with no change to
  `src/grind_postgres_ffi.erl`: closing the pool while the coordinator's
  checked-out connection is mid-COMMIT unblocks that connection's pending
  read with an ordinary `{error, closed}`-derived `pog.QueryError`, which
  `transaction_safely` already returns as
  `Error({transaction_query_error, ...})` without needing to catch any new
  process-exit shape. This was not assumed; it was confirmed by running the
  test for real (see "Full-gate confirmation" below) with the FFI file
  untouched.
- After that assertion, the test terminates the still-stuck backend from
  the observer pool, then — because `pg_terminate_backend` is
  fire-and-forget and returns before the target has actually finished
  committing and exited — polls `pg_stat_activity` on the observer
  connection (bounded checks, not a sleep) until that exact backend pid is
  gone before doing anything else. Only then does it reopen Grind's own
  pool against the same settings and confirm `reconcile_acknowledgement`
  now returns the receipt as `Succeeded` and `outcome` reads
  `SucceededWith("unavailable-34")`. A same-command retry through the
  reopened pool (`postgres.acknowledge_claim` with the identical execution)
  returns `Ok(True)` — the idempotent-retry-after-Unknown path, exercised
  end to end through the reopened store, not just inferred from the receipt
  read. A fresh manual consumer's `process_one` then returns `Ok(False)` —
  the worker's invocation subject still shows exactly one call across the
  entire test, proving no rerun occurred despite the store having been
  briefly unavailable mid-acknowledgement.
- **Full-gate confirmation**: `nix develop --command bash
scripts/test-postgres.sh` passed end to end (75 root tests, 0 failures,
  all required markers present, oracle and consumer suites unaffected)
  with `src/grind_postgres_ffi.erl` at its pre-existing content — `git diff
--stat` for that file shows no changes for this task. The task's
  contingency ("fix minimally by mapping that exit to the query-error path
  ... if the pool-close exit shape is not caught") was evaluated and found
  unnecessary in this environment/PostgreSQL/OTP version combination; it
  remains a real risk worth re-checking if `pog`'s connection-checkout
  architecture changes, since it depends on that library closing the
  underlying socket (not merely killing a process the caller is
  synchronously blocked calling into) when its owning connection process is
  torn down mid-query.

### Honest limits of Increment 2

- The reply loss here is produced by **backend termination after a local
  commit**, not a network partition. A half-open TCP connection — where the
  server believes it sent the reply and the client's socket never observes
  a `closed` or `tcp_closed` event because the network dropped packets
  silently rather than resetting the connection — is **not covered**. It is
  **unverified whether pog/pgo's own query timeout bounds a COMMIT stuck on
  a half-open socket**, and no Grind-level ack deadline exists either way:
  if the client's TCP stack never learns the connection is dead and pog has
  no timeout that fires first, `resolve_ack_transaction_result` never runs
  at all and the caller hangs indefinitely. Reproducing the half-open case
  for real, and checking pog's timeout behavior against it, would need a
  TCP proxy that drops the reply packets without closing either end, which
  this increment deliberately did not build.
- This depends on **PostgreSQL's synchronous-replication semantics**
  specifically (the documented behavior of a backend killed while parked in
  `SyncRepWaitForLSN` after a local WAL flush) and on the **test cluster's
  `synchronous_standby_names`/`synchronous_commit` settings**. It is not a
  general statement about every possible "commit succeeded, reply lost"
  mechanism (e.g., it says nothing about a reverse proxy or connection
  pooler in front of PostgreSQL swallowing a reply after the database
  itself replied normally).
- Neither test proves anything about the **worker's own external effect**
  beyond what its return value encodes. The three facts at the top of this
  document apply here without qualification: the receipt proves the
  acknowledgement transaction committed, not that the effect the worker
  code performed (if any, outside the test's synthetic string return) was
  itself exactly-once, idempotent, or even completed.
- The **process-crash-before-commit-attempt window remains unaddressed**:
  if the process performing the ack dies (not merely loses a connection)
  after the worker's external effect but before `acknowledge_claim` is even
  called, no proposal is ever retained anywhere, and this is unrecoverable
  by Grind alone — this was already documented for Increment 0/1's
  connection-loss test and is unchanged by this increment.

## Increment 3 — lease expiry through the production acknowledgement

### Claim: the fenced "lease is still live" predicate is one shared fragment, so the predicate-only test tracks the exact production text

- **Refactor** (justified duplication removal, no behavior change): `live_lease_predicate`/`expired_lease_predicate` in `src/grind/postgres.gleam`
  (just after `claim_previous_state`), each taking the SQL time expression as
  a parameter (a trusted SQL fragment such as `clock_timestamp()` or a named
  CTE column — spliced verbatim, never bound as a query parameter, so this
  is never a place to pass caller/user input). `live_lease_predicate("clock_timestamp()")`
  now backs renewal (`renew_claim`), contract-mismatch release (`mark_contract_mismatch`),
  all eight acknowledgement-disposition statements in
  `acknowledge_transaction` (succeeded, business_failed, retryable, snoozed,
  discarded, cancelled, uncertain, runtime_failed), and the ack-rejection
  reader (`current_ack_rejection`). `expired_lease_predicate("clock_timestamp()")`
  backs the quarantine scan (`quarantine_expired`). The complement is kept
  as its own mirrored fragment rather than `NOT (` <> `live_lease_predicate` <> `)`,
  so the production SQL text is byte-identical to before the refactor.
  Confirmed with a full green gate (78 root tests) both before and after.
- `postgres_acknowledgement_rejects_exact_database_expiry_test` now builds
  its `SELECT` from both `postgres.live_lease_predicate("instant")` and
  `postgres.expired_lease_predicate("instant")` (rather than hand-writing
  either), and additionally asserts that at the exact tie the two fragments
  are exact complements of each other (`quarantine_eligible == !acknowledgement_allowed`,
  and both resolve to the expected values at that boundary). "Exact expiry
  rejects" is now proven against the identical fragment production code
  calls, and the quarantine scan's own complement fragment is proven to
  agree with it at that boundary, not a parallel hand-copy that could
  silently drift.

### Claim: at the production acknowledgement boundary, a lease already expired by the ack transaction's own database time is rejected as stale — no receipt, no rerun

- **Test**: `postgres_ack_after_database_expiry_is_stale_without_receipt_test`
  (marker `ack-after-database-expiry-stale-no-receipt-passed`).
- **Fault injection**: a manual consumer claims a job with a 30-second
  lease. `renewal_interval_ms = lease_duration_ms / 3` (10s) is far outside
  this test's whole run, so no automatic renewal tick can fire and confound
  the result with a renewal-detected loss instead of the forced write. The
  handler blocks on a barrier; while blocked, `lease_expires_at` is forced
  to `clock_timestamp()` directly via a manual `UPDATE` (bypassing the
  coordinator entirely), asserting exactly one row updated — the tightest
  reachable forced value, rather than something further in the past.
- **Synchronization**: the worker is released only after the forced write's
  row count is confirmed and after `queue.renewal_status(consumer)` reads
  back `LeaseRenewalConfirmed` (proving the coordinator has not itself
  already detected a renewal loss); the reply is read with a bounded
  `process.receive`.
- **Characterization test, proven by mutation** (passed on first write, so
  red-before-green does not apply). Mutation: in `acknowledge_transaction`'s
  `"succeeded"` branch, replaced `<> live_lease_predicate("clock_timestamp()") <>`
  with `<> "TRUE" <>`. Result:
  ```
  let assert  test/grind_test.gleam:4925
   test: grind_test.postgres_ack_after_database_expiry_is_stale_without_receipt_test
   code: let assert Ok(Error(queue.QueueProcessFailed(postgres.QueueAckStale(
      proposed,
      postgres.AckLeaseExpired(stale_attempt_id, stale_epoch, stale_owner),
    )))) = process.receive(reply, within: 5000)
  value: Ok(Ok(True))
  75 passed, 3 failures
  ```
  The same mutation also failed two pre-existing tests that exercise the
  identical succeeded-ack fragment
  (`postgres_cancelled_expired_attempt_is_quarantined_test`,
  `postgres_expired_renewal_keeps_worker_fenced_and_returns_proposal_test`),
  and nothing else — confirming the mutation's blast radius is exactly the
  statements sharing this one fragment. The mutation was reverted
  immediately; `gleam check` recompiled clean and `git diff` for
  `postgres.gleam` showed no trace of the mutated line.
- **Observed**: `QueueAckStale(proposed, AckLeaseExpired(attempt_id, epoch, owner))`
  with the exact attempt identity read from the row before the forced
  write (via `attempt_snapshot`, which also now returns `attempt_owner`
  directly instead of a separate query); zero rows in
  `grind_job_acknowledgements` for the independently computed command ID
  (`postgres.acknowledgement_command_id`); `reconcile_acknowledgement`
  returns `AckReceiptNotFound`; the row is still `Executing`; the next
  `process_one` quarantines it to `Uncertain` through the existing bounded
  quarantine scan, with zero further worker invocations.
- **Limits**: this proves the "lease already expired" side of the boundary
  on the real acknowledgement path, not exact-instant equality. Real time
  elapses between the forced write and the ack transaction's own later
  `clock_timestamp()` call, so by the time the production predicate
  evaluates, the lease is already in the past, not tied to that instant —
  two separate `clock_timestamp()` evaluations in different statements
  cannot be forced from the client side to land on the identical
  microsecond. Exact equality at a single instant is what the predicate-only
  test above proves, against this same shared fragment.

## Increment 4 — real scheduled wakeup; forced shutdown with pool cleanup

### Claim: the automatic consumer claims a scheduled job only once the database's own clock has reached its `available_at`

- **Test**: `postgres_automatic_consumer_wakes_for_database_deadline_test`
  (marker `automatic-wakeup-database-deadline-passed`).
- **Mechanism**: `available_at` is computed from the database itself as
  `clock_timestamp() + 300ms` and submitted with `submit_at`. Immediately
  after the short-poll-interval (20ms) automatic consumer is started, the
  test asserts (not retried) that the database clock is still before
  `available_at` and that the job is still `Scheduled` — direct evidence
  that at least one pre-deadline poll tick has already run and correctly
  skipped the row, rather than merely inferring it from the eventual
  outcome. A too-slow environment (already past the ~300ms deadline by the
  time this runs) fails this assertion honestly instead of the test
  silently passing without having proven anything. The handler itself, at
  the moment it actually executes, compares `available_at` against the
  row's own recorded _claim_ time — `lease_expires_at - lease_duration`,
  with `lease_duration` fixed at 5000ms for both the policy and this
  computation — rather than its own later `clock_timestamp()` call, so the
  observation is pinned to the instant the claim SQL admitted the row, not
  to whatever moment the handler happens to run afterward. It reports the
  resulting boolean over a subject.
- **Synchronization**: a bounded `process.receive` (5s) on the reported
  boolean, then `wait_for_job_state` polls the job's committed state for
  `Succeeded` (bounded, 250 checks), never a fixed sleep used as the
  assertion.
- **Mutation**: dropped the due-time condition from the claim SQL's
  eligible-state clause (`eligible_state = "state IN ('queued',
'scheduled', 'retryable')"`, removing `AND available_at <=
clock_timestamp()`). Result: this test's handler reports `False`, and
  four other pre-existing tests that depend on the same due-time gate also
  fail (`postgres_worker_snooze_commits_scheduled_state_test`,
  `postgres_scheduled_jobs_observe_database_due_time_test`,
  `postgres_business_failure_is_scheduled_before_retry_test`,
  `postgres_default_retry_backoff_is_persisted_at_database_time_test`); 73
  passed, 5 failures. Reverted; `gleam check` recompiled clean; clean `git
diff`.
- **Honest limit**: Grind has no LISTEN/NOTIFY wakeup path — this is
  confirmed by source inspection alone (reading `src/grind/postgres.gleam`
  and `src/grind/queue.gleam`: the only ways a row is ever claimed are the
  coordinator's own self-scheduled `Poll` timer and an explicit manual
  `process_one` call; no PostgreSQL channel is ever subscribed to). What
  the mutation-proven test itself establishes is narrower and separate from
  that source-reading claim: that a scheduled job's claim genuinely waits
  for database time via polling, not that no notification path exists (the
  test cannot prove a negative like that; only reading every call site
  can, and does). This test proves the consumer's next poll tick after the
  deadline elapses picks the row up — polling-after-deadline wakeup — not a
  notification-driven wakeup that claims the row at the instant the
  deadline is reached. A longer poll interval would show proportionally
  more latency after the deadline; this is not measured here.

### Claim: a forced shutdown (grace 0) releases both the active worker and Grind's own connection pool, and the orphaned attempt recovers as `Uncertain` — never replayed — once a fresh pool and consumer take over

- **Test**: `postgres_forced_stop_releases_worker_and_pool_then_recovers_test`
  (marker `forced-stop-pool-cleanup-recovered`).
- **Mechanism**: shutdown grace 0. The active worker's pid is monitored
  directly; after `queue.stop` returns `StoppedWithActiveWork(1)`, the
  monitor's `ProcessDown` is awaited (bounded) to confirm the worker is
  actually gone, not merely assumed from the stop outcome. A size-1
  observer pool — a second, independent `postgres.start` against the same
  database — polls `pg_stat_activity` for backends against this database
  and user other than its own (`pid <> pg_backend_pid()`) and other than
  non-client backends (`backend_type = 'client backend'`), since Grind sets
  no `application_name` on its connections to filter on directly.
  **Before** `postgres.close` runs, this same query is checked once
  (`poll_leftover_grind_backends(observer_connection, 0)`, reusing the
  bounded-poll helper with zero retries instead of a bespoke one-off) and
  asserted `>= 1` — proving the query is not vacuously always zero, since
  Grind's own pool has just run `migrate`/`submit`/`state` through real
  connections. `postgres.close` is then called on Grind's own pool while
  the row is still `executing` with no ack ever having run (the worker died
  mid-attempt), and the same query is polled (bounded, 20ms steps) until it
  reaches `0`. The same pool name is then reopened, a fresh manual consumer
  is started on it, the lease is driven to expiry at the database boundary
  (a direct `UPDATE`, not a sleep), and `process_one` is called.
- **Observed** (real run against a disposable PostgreSQL cluster): at least
  one leftover backend was present immediately before `postgres.close` (the
  vacuous-zero check passed), and the leftover-backend poll converged to
  `0` well inside its bound afterward — no leftover Grind backend was found
  once `postgres.close` returned in this run. This is a measured finding,
  not an assumption: `postgres.close`
  calls `gen_server:stop(Pid, shutdown, 6000)` synchronously on the pool's
  top-level supervisor (`src/grind_postgres_ffi.erl`), so by the time it
  returns, the whole pool supervision tree — including its pooled
  PostgreSQL sockets — had already been torn down in this
  environment/PostgreSQL/OTP/pog combination. `postgres.state` on the
  reopened pool reads `Executing` immediately after reopen (the row
  survived the pool restart untouched), then `Uncertain` after the
  forced-expiry `process_one` call, with zero further worker invocations.
- A confirmatory mutation (not required by name, run for extra assurance
  since this test re-exercises the same no-replay quarantine contract as
  several existing tests) reused the existing `'uncertain'` → `'queued'`
  quarantine-target mutation already recorded under
  `owner-loss-pool-restart-quarantined-no-replay` in
  `oracle/ORACLE-LEDGER.md`. Result: this test fails alongside eleven other
  tests that share the same quarantine contract (66 passed, 12 failures);
  reverted, clean `git diff`.
- **Limits**: this run observed zero leftover backends within the bounded
  poll; it does not prove pool teardown is always synchronous or immediate
  in every environment. In the same full-gate run, an unrelated Increment 2
  test's own pool close independently produced the pre-existing, already
  documented (`docs/IMPLEMENTATION-SCOPE.md`) PGO
  `pgo_connection_sup`/`pgo_connection` `SUPERVISOR REPORT` log line for
  its asynchronous connection starter — this test does not gate on log
  quietness, and that log line is not evidence of a leftover backend (the
  `pg_stat_activity` poll is the actual evidence, and it converged to
  zero). A future run that fails to converge within the bound would be a
  genuine finding to report, not something this test papers over by
  widening the bound silently.

## Increment 5 — public-API consumer coverage: retry, running cancellation, and audited uncertainty recovery

All three claims below are exercised from `consumer/`, the separate package
that imports only public Grind modules (`oversight`'s public-API-acceptance
rule). Nothing here reads `@internal` functions, `Dynamic`, or Grind's
private state; each mechanism below is either a real wall-clock retry delay,
a real worker crash, or a real lease-expiry quarantine observed entirely
through `grind/postgres` and `grind/queue`'s public surface.

Every synthetic receipt minted by `consumer_effect.erl`'s `apply/2` (the
application's own effect table, used by the dedup-exercising success test
above and by the crash/uncertainty claim below) now carries a unique token
(`erlang:unique_integer/1`) alongside the key, rather than being a pure,
predictable function of the key alone. Every test that asserts a job's
committed outcome equals a receipt reads that exact value back from the
app's own table first (`consumer_effect:receipt/1`) and compares against
that, never against a hand-written string. This makes "the committed
outcome equals the app's receipt" evidence of actual provenance — the value
genuinely came from that one table — rather than a coincidental match with
a value the test could have computed independently.

### Claim: a definition-bound retry policy's single retry actually reaches a second delivery, and the job commits `Succeeded`

- **Test**: `public_consumer_retry_and_running_cancellation_test` (first
  half), `consumer/test/grind_consumer_test.gleam`; marker
  `consumer-retry-and-cancellation-passed`.
- **Mechanism**: `worker.with_retry_policy` is bound to a worker whose
  handler fails on its first invocation and succeeds on every later one. The
  policy callback itself receives Grind's own `RetryContext` and reports its
  `current_attempt` on a probe (`retry_context_probe`) before always
  returning `RetryAfter(worker.retry_delay(50))` — a genuine 50ms wall-clock
  delay, not a test-only seam. A manual consumer (`queue.start_manual`)
  drives each attempt explicitly through `queue.process_one`.
- **Synchronization**: the bound retry policy's own report proves the real
  `RetryContext.current_attempt` is `1` on the first failed delivery,
  observed through this public callback. The `perform` handler itself has no
  such access — only a bound retry policy callback ever receives a
  `RetryContext` (recorded as a backlog finding in
  `docs/IMPLEMENTATION-SCOPE.md`, "Job lifecycle and attempt history") — so
  it separately tracks its own invocation count through a small counter FFI
  (`consumer_counter.erl`) and reports it on its own probe subject
  (`attempt_probe`), which in this single-worker, no-concurrent-claims
  scenario advances in lockstep with Grind's persisted attempt count. A
  bounded polling helper (`await_claim`, 20ms steps) waits for the real
  retry delay to become due instead of sleeping a fixed duration as the
  assertion itself.
- **Characterization test, proven by mutation** (passed on first write, so
  red-before-green does not apply). Mutation: in
  `retry_then_succeed_worker`'s handler, replaced the attempt-1-fails/later-
  succeeds `case` with an unconditional `Error(Nil)` (never succeed).
  Result, against a real disposable cluster:
  ```
  panic src/gleeunit/should.gleam:10
   test: grind_consumer_test.public_consumer_retry_and_running_cancellation_test
   info:
  False
  should equal
  True
  4 passed, 1 failures
  ```
  Only this test failed (the job never reached `Succeeded` within the
  bounded poll). Reverted immediately; `gleam check` recompiled clean and
  `git diff` for the test file showed no trace of the mutated line.
- **Limits**: this proves the test's own assertions discriminate a genuine
  retry-then-succeed outcome from a worker that never recovers; it is a
  consumer-level demonstration of an already-implemented mechanism, not a
  new proof of Grind's internal retry accounting (that is proven at the root
  level: `postgres_business_failure_is_scheduled_before_retry_test` and
  related rows in `oracle/ORACLE-LEDGER.md`).

### Claim: cancellation requested against a genuinely running attempt commits `Cancelled` regardless of what the handler returns, and does not undo the handler's own effect

- **Test**: `public_consumer_retry_and_running_cancellation_test` (second
  half); same marker.
- **Mechanism**: a second worker's handler reports it has started (handing
  back a release subject over a probe) and blocks on `process.receive` until
  released. The test calls `queue.process_one` for this job from a
  `process.spawn_unlinked` process (so it can block independently), waits
  for the handler's own start signal, then confirms the row is genuinely
  `Executing` via `postgres.state` before calling `postgres.cancel` — the
  same deterministic barrier shape as the root suite's
  `postgres_cancel_running_worker_overrides_proposal_on_ack_test`. After
  `postgres.cancel` returns `CancellationRequested`, the barrier is
  released; the handler applies its synthetic effect and returns normally,
  but the acknowledgement still commits `Cancelled`.
- **Characterization test, proven by mutation** (passed on first write).
  Mutation: removed the `postgres.cancel` call in the test itself (replaced
  it with an unused reference to the function, so nothing is ever
  requested). Result, against a real disposable cluster:
  ```
  panic src/gleeunit/should.gleam:10
   test: grind_consumer_test.public_consumer_retry_and_running_cancellation_test
   info:
  Ok(Succeeded)
  should equal
  Ok(Cancelled)
  4 passed, 1 failures
  ```
  Only this test failed. Reverted immediately; `gleam check` recompiled
  clean and `git diff` for the test file showed no trace of the mutated
  line.
- **Observed**: after release, `postgres.state` reads `Cancelled`,
  `postgres.outcome` reads `CancelledWithReason("cancelled by caller")`, and
  the application's own synthetic-effect table
  (`consumer_effect:count/1`) still shows exactly one recorded application
  of the effect for that job's key — direct evidence that Grind's
  cancellation overwrote the committed _job_ outcome without reaching into,
  or undoing, whatever the handler had already done to the application's own
  state.
- **Limits**: this is a consumer-level demonstration of an already-proven
  root mechanism (the `cancel_requested_at` override at acknowledgement,
  proven at the root level under the `cancel-running-*` markers); it adds no
  new claim about Grind's internal acknowledgement SQL, only that the
  behavior is reachable and observable entirely through public imports.

### Claim: a worker crash right after its effect surfaces as worker death and conservative `Uncertain` recovery — never as a business or invalid-input outcome — and an audited resolution is the only way either job runs again

- **Test**: `public_consumer_effect_crash_uncertainty_audited_recovery_test`,
  `consumer/test/grind_consumer_test.gleam`; marker
  `consumer-uncertainty-audited-recovery-passed`.
- **Fault injection**: a one-shot fault plan owned entirely by the test
  application, not by Grind (`consumer_effect.erl`:
  `arm_crash_after_effect/1` / `take_fault/1`). Arming a key makes the very
  next `apply/2` call for that exact key apply and retain its synthetic
  effect first, then raise a genuine Erlang error — killing the calling
  worker process before it can return anything to Grind, so no
  acknowledgement is ever attempted for that attempt. The flag is consumed
  by that one call; a later call for the same key (the authorized replay's
  rerun) applies normally with no crash. Two jobs are each admitted once and
  armed once, with an automatic consumer (`queue.start_with_policy`)
  configured with a short lease (`queue.with_lease_duration(500)`) and a
  short poll interval (`queue.with_poll_interval(20)`), so the crashed
  attempt's expiry is found by the coordinator's own quarantine scan on a
  later tick, exactly as for the root suite's `postgres_temporary_worker_death_quarantines_without_replay_test`
  (`docs/IMPLEMENTATION-SCOPE.md`; `handle_worker_down` in
  `src/grind/queue.gleam` treats any monitor-`DOWN` reason for an active
  attempt identically, whether from `process.kill` or an uncaught exception
  raised inside the handler itself — confirmed here by a real BEAM crash
  report captured during the run, not assumed from source reading alone).
- **Synchronization**: bounded polling of `postgres.state` (250 checks, 20ms
  steps — the same `await_state` helper used by the existing consumer
  tests) waits for each row to reach `Uncertain`.
- **Observed** (real run against a disposable cluster): both jobs' committed
  outcome reads
  `Ok(job.ReconciliationRequired("expired attempt requires outcome reconciliation"))`
  — never a business failure or invalid-input outcome — confirming a
  handler crash is recovered the same conservative way as any other
  worker death. Each key's synthetic effect was applied exactly once
  (`consumer_effect:count/1` reads 1), each handler ran exactly once so far
  (`consumer_counter:value/1` reads 1), and the one-shot fault is already
  consumed (`consumer_effect:take_fault/1` now reads `false` for both keys).
  The application then reads its own dedup table
  (`consumer_effect:receipt/1`, not a Grind API) and rebinds a typed handle
  from each durable job ID with `postgres.bind_handle`:
  - Job 1 is resolved with `postgres.resolve_uncertain(..., ConfirmSuccess(receipt))`,
    committing `Succeeded` directly; its handler is never invoked again
    (`consumer_counter:value/1` stays at 1).
  - Job 2 is resolved with `AuthorizeReplay`, committing `Queued`; the same
    automatic consumer, still running, reclaims the row on its own next
    poll tick and reruns the handler a second time
    (`consumer_counter:value/1` reaches 2). That rerun calls the synthetic
    effect with the same key and receives the original receipt
    (`consumer_effect:count/1` stays at 1); the job commits `Succeeded` with
    that same receipt.
- **Mutation 1 — non-idempotent application effect** (characterization test,
  proven by mutation; passed on first write). Mutation: in
  `consumer_effect.erl`'s `apply/2`, replaced the retained-receipt lookup on
  a repeat call with an unconditional re-insert that bumps the call counter
  every time, so the app's own table stops being idempotent. Result, against
  a real disposable cluster with a freshly recreated database (to rule out
  cross-run contamination):
  ```
  panic src/gleeunit/should.gleam:10
   test: grind_consumer_test.public_consumer_executes_typed_workers_test
   info:
  2
  should equal
  1

  panic src/gleeunit/should.gleam:10
   test: grind_consumer_test.public_consumer_effect_crash_uncertainty_audited_recovery_test
   info:
  2
  should equal
  1
  3 passed, 2 failures
  ```
  Exactly the two tests whose assertions depend on the app's own dedup
  idempotency failed, nothing else. Reverted immediately; `gleam check`
  recompiled clean and `git diff` for `consumer_effect.erl` showed no trace
  of the mutated lines.
- **Mutation 2 — quarantine target state (`uncertain` → `queued`)**
  (confirmatory, reusing the same production mutation already recorded
  under `owner-loss-pool-restart-quarantined-no-replay` in
  `oracle/ORACLE-LEDGER.md`, applied here to prove it also breaks the
  public-API-level uncertainty/replay contract, not just the root-level
  one). Mutation: in `quarantine_expired` (`src/grind/postgres.gleam`),
  changed the quarantine `UPDATE`'s target state from `'uncertain'` to
  `'queued'`. Result, against a real disposable cluster:
  ```
  panic src/gleeunit/should.gleam:10
   test: grind_consumer_test.public_consumer_effect_crash_uncertainty_audited_recovery_test
   info:
  False
  should equal
  True
  4 passed, 1 failures
  ```
  With the row quarantined straight back to `queued`, the automatic
  consumer reclaimed and reran it before the test ever reached its audited
  resolution calls, so the row never stayed `Uncertain` long enough for the
  bounded poll to observe it there — exactly the double-execution risk this
  claim exists to rule out. Only this test failed. Reverted immediately;
  `git diff` showed no trace of the mutated line.
- **Limits**: the crash here is a synchronous, in-process Erlang exception
  raised from inside the handler's own call stack, not a host, VM, or
  connection-level failure — it exercises the "worker dies mid-attempt"
  path (already proven at the root level under
  `temporary-worker-death-quarantined-no-replay`), not the coordinator- or
  owner-loss paths covered by Increment 1. As documented at the top of this
  file, the receipt/counter evidence above proves the _application's own_
  effect and handler-invocation bookkeeping, not that Grind itself
  guarantees anything about external effects; the app's dedup table is
  doing the idempotency work, exactly as `consumer/README.md` already says
  for the crash-window case.

## Increment 6 — uniqueness admission: schema v11, pure policy validation, sequential identity

Full contract, decisions, and admission-transaction step order:
`docs/UNIQUENESS-CONTRACT.md`. This increment covers schema v11 install/reject
behavior, `grind/unique`'s pure validation, and sequential (non-concurrent)
`submit_unique` admission and identity. Concurrent admission, period-boundary
timing, live rescheduling, lock contention, and uncertain-commit
reconciliation are implemented by the same production code but not yet
tested — see the "Status" section of `docs/UNIQUENESS-CONTRACT.md`. The
admission transaction lives in `grind/internal/unique_admission.gleam`;
`submit_unique`/`reconcile_unique` in `grind/postgres.gleam` are thin
wrappers around it (see `docs/UNIQUENESS-CONTRACT.md`, "Module placement").

### Claim: `submit_unique` rejects an empty queue before touching storage

- **Test**: `postgres_submit_unique_rejects_before_touching_storage_test`
  (marker `unique-pre-storage-rejections-passed`).
- **Mechanism**: the pool is started, then immediately closed
  (`postgres.close`) before the `submit_unique` call. A passing test
  therefore proves the rejection is a pure `case` branch that returns before
  `unique_admission.submit` ever reaches the database — a query attempt
  against a closed pool would surface as a storage-shaped failure
  (`AdmissionFailed`), not the exact `EmptyQueueName` value
  asserted. (An earlier draft of this test also asserted an
  `Immediately`/`RescheduleScheduled` pairing was rejected before storage;
  that combination is no longer constructible at all once the coordinator's
  reschedule-target-on-the-action API decision landed — see
  `docs/UNIQUENESS-CONTRACT.md`'s `ConflictAction` note — so this test now
  covers only the empty-queue case.)
- **Limits**: this is a characterization test (it passed on first write, so
  red-before-green does not apply); its own construction — proving the exact
  pure error survives a closed pool — is the evidence, not a separate
  mutation.

### Claim: a same-worker-id, different-version submission never conflicts with an earlier admission

- **Test**: `postgres_submit_unique_scopes_key_to_worker_identity_test`
  (marker `unique-worker-identity-isolation-passed`).
- **Characterization test, proven by mutation** (passed on first write, so
  red-before-green does not apply). Mutation: removed `worker_version = $3`
  from the candidate-selection SQL and its matching bound parameter
  (`candidate_sql`/`bind_candidate_params`,
  `src/grind/internal/unique_admission.gleam`, renumbering the remaining
  positional parameters). Re-run after the module split (R1) that moved this
  code out of `grind/postgres.gleam`, against a **freshly recreated**
  disposable database — reusing a database across repeated manual runs of
  this test with the same fixed `SubmissionId`s the first time produced a
  false negative (the receipt-lookup step, working exactly as designed,
  returned each submission's already-committed decision from the _previous_
  run before the mutated candidate query was ever reached at all; recreating
  the database before the mutated run is what actually exercises the
  candidate path). Result, against a real disposable cluster (85 passed, 1
  failure):
  ```
  let assert  test/grind_test.gleam:7399
   test: grind_test.postgres_submit_unique_scopes_key_to_worker_identity_test
   code: let assert Ok(postgres.Inserted(_)) =
      postgres.submit_unique(
        database,
        "identity",
        submission_v2,
        worker_v2,
        5,
        unique.Immediately,
        policy,
        unique.KeepExisting,
      )
  value: Ok(Existing(Conflict(13, "127.0.0.1:55995/grind_refactor_test", "identity", "unique.identity", "v2", Queued)))
  ```
  The same-id, different-version submission (`worker_v2`) incorrectly saw
  the first submission (`worker_v1`, same id, different version) as a
  candidate and returned `Existing` — note the conflict's own reported
  worker version is `"v2"`, i.e. the row it actually matched belongs to
  `worker_v1`'s admission but is being read back through `worker_v2`'s
  identity, exactly the cross-version confusion this test exists to catch —
  instead of the expected `Inserted`. Only this test failed. Reverted
  immediately; `gleam build --warnings-as-errors` recompiled clean and `git
diff` for `unique_admission.gleam` showed no trace of the mutated lines.
- **Limits**: this proves worker-version isolation specifically; it does not
  exercise concurrent admission of the two versions (a sequential-call test,
  per this increment's scope). The false-negative-then-corrected run above
  is itself recorded as a caution for anyone re-running this mutation
  manually: use a fresh database, not a reused one, when the test's
  `SubmissionId`s are fixed literals.

### Claim: a plain-submitted row never participates in uniqueness admission, at any eligible-states width

- **Test**: `postgres_submit_unique_ignores_plain_submitted_rows_test`
  (marker `unique-plain-submit-non-participation-passed`).
- **Mechanism**: the policy under test deliberately uses `unique.AllRetained`
  (every persisted state), the widest possible eligibility, so a states
  filter cannot be the reason the plain row is invisible — only its `NULL`
  `unique_key_contract`/`unique_key_sha256` (columns `submit`/`submit_at`
  never set) can explain the result.
- **Observed**: the `submit_unique` call following the plain `submit`, with
  an identical key/worker/queue, returns `Inserted` (a new row), not
  `Existing`. Full gate passed (see `oracle/ORACLE-LEDGER.md` header).
- **Limits**: characterization evidence (passed on first write); the
  candidate query's `unique_key_contract = $n` equality can structurally
  never match a `NULL` column, so this is closer to a proof-by-construction
  than a mutation-discovered regression — recorded here because it is the
  test increment 3 of the approved plan named explicitly ("plain-submit
  non-participation").

### Claim: PostgreSQL's own `jsonb::text` equality, not a cross-runtime canonical JSON, governs key conflict

- **Test**: `postgres_submit_unique_json_equality_matches_postgres_jsonb_test`
  (marker `unique-json-equality-cases-passed`).
- **Mechanism**: a `RawInput` wrapper whose codec's `encode` returns exactly
  the `json.Json` value the test constructs, submitted as a full-input key,
  so each case controls the encoded JSON precisely: reordered object fields,
  `json.int(1)` vs. `json.float(1.0)`, reordered arrays, `{"id":1}` vs.
  `{"id":1,"extra":2}`, and an empty vs. a non-empty object. `key_digest_sql`
  (`grind/internal/unique_admission`) casts the bound key text to `jsonb` and
  hashes its `::text` rendering in the same SQL statement every time
  (candidate select, insert, and lock key), so this exercises the actual
  production digest path, not a parallel hand-written comparison. This
  digest path is unaffected by the request-fingerprint-in-Gleam change (R1.2):
  key equality was, and remains, computed entirely by PostgreSQL.
- **Observed**: reordered object fields conflict (`Existing`); every other
  pair admits both sides (`Inserted` twice) — `1`/`1.0`, reordered arrays,
  subset/superset objects, and empty/non-empty objects are all treated as
  distinct keys. Full gate passed (see header).
- **Limits**: this is a deliberate, documented departure from Oban's own
  selected-field containment semantics (`docs/UNIQUENESS-CONTRACT.md`,
  Decisions 1–2), not an attempt to reproduce it; it does not exercise
  `unique.selected` (a projected key), only `unique.full_input()`.

### Claim: `state = ANY($n::text[])` array binding works against the pinned `pog`/`pgo` version

- Not a named test; a standalone probe run during review, recorded here
  because an earlier draft of this document (and of
  `docs/UNIQUENESS-CONTRACT.md`) incorrectly attributed a `FunctionClause`
  crash to array-parameter binding.
- **Mechanism**: `pog.query("SELECT 'queued' = ANY($1::text[])") |> pog.parameter(pog.array(pog.text, ["queued", "scheduled"])) |> pog.execute(...)`
  against a real disposable cluster.
- **Observed**: `Ok(Returned(1, [True]))` — no crash, correct result.
- **Correction**: the original crash (`PgTypes(Decode, ["", UnknownOid], [])`,
  captured via a stacktrace-preserving `catch Class:Reason:Stack` FFI probe)
  came from decoding `pg_advisory_xact_lock`'s `void` return type, not from
  array binding — `pg_types` has no registered decoder for `void`. Once the
  lock-acquisition query was wrapped (`SELECT true FROM (SELECT
pg_advisory_xact_lock(...)) AS ...`) to give it a decodable result, the
  candidate query was switched from a literal `state IN ('queued', ...)` list
  back to `state = ANY($n::text[])` with `pog.array`, per this probe's result.

## Increment 7 — uniqueness admission: queue scope, state eligibility, period boundaries at database time, and receipt idempotency (approved plan increments 4–7)

Full contract: `docs/UNIQUENESS-CONTRACT.md`. All mutations below were applied
against a **freshly recreated** disposable cluster per mutation (via a
minimal harness that starts its own throwaway PostgreSQL cluster and runs
`gleam test` with only `GRIND_TEST_DATABASE_URL` set), per the caution
recorded in Increment 6: reusing a database across repeated manual runs with
fixed `SubmissionId` literals can produce a false negative, because the
receipt-lookup step (working exactly as designed) would return an
already-committed decision from an earlier run before the mutated code path
is ever reached. Each mutation below was reverted immediately after
capturing its failure, and `gleam build --warnings-as-errors` recompiled
clean after every revert.

### Claim: `WithinQueue` admits the same key independently per queue; `AcrossQueues` conflicts with the earliest matching row regardless of which queue holds it

- **Test**: `postgres_submit_unique_respects_queue_scope_test` (marker
  `unique-queue-scope-passed`).
- **Characterization test, proven by mutation** (passed on first write).
  Mutation: in `candidate_sql`/`bind_candidate_params`
  (`src/grind/internal/unique_admission.gleam`), made the `AcrossQueues`
  branch of both `case` expressions take the same `queue = $7` clause (and
  matching bound parameter) as `WithinQueue`, i.e. "always add
  `queue = $q`". Result, against a fresh disposable cluster (97 passed, 1
  failure):
  ```
  test: grind_test.postgres_submit_unique_respects_queue_scope_test
  info:
  19
  should equal
  18
  ```
  With the mutation, the `AcrossQueues` submission against queue `"q2"`
  incorrectly matched the `WithinQueue` row already sitting in `"q2"` (job 19) instead of the earlier row in `"q1"` (job 18) that `AcrossQueues` is
  supposed to reach — still reported `Existing`, but pointed at the wrong
  row, which is exactly the failure mode this test's `conflict_job_id`
  assertion exists to catch. Only this test failed. Reverted immediately;
  `git diff` for `unique_admission.gleam` showed no trace of the mutated
  lines.

### Claim: the state-eligibility matrix — all 11 persisted states against all four `States` groups — matches `grind/unique`'s `eligible_states` table exactly

- **Test**: `postgres_submit_unique_state_eligibility_matrix_test` (marker
  `unique-state-eligibility-matrix-passed`). For each of the 11 states
  (`queued`, `scheduled`, `retryable`, `executing`, `succeeded`,
  `business_failed`, `runtime_failed`, `contract_mismatch`, `uncertain`,
  `discarded`, `cancelled`), a fresh row is admitted under a unique key,
  forced into that state by raw SQL, and then checked against each of
  `Incomplete`, `ScheduledOnly`, `IncompleteOrSucceeded`, and `AllRetained`
  (44 checks in one test) — every combination is proven, not just the ones
  named in the plan.
- **Characterization test, proven by two mutations** (passed on first
  write), each reverted before the next was applied, each against a fresh
  disposable cluster:
  1. **Remove `uncertain` from `Incomplete`**
     (`Incomplete -> ["queued", "scheduled", "retryable", "executing"]` in
     `eligible_states`, `src/grind/unique.gleam`). Result (97 passed, 1
     failure):
     ```
     test: grind_test.postgres_submit_unique_state_eligibility_matrix_test
     code: let assert Ok(unique.Existing(conflict)) = result
     value: Ok(Inserted(JobHandle(67, ...)))
     info: Pattern match failed, no pattern matched the value.
     ```
     The `uncertain`/`Incomplete` cell (expected `Existing`) silently
     admitted a second row instead. Only this test failed.
  2. **Add `cancelled` to `Incomplete`**
     (`Incomplete -> [..., "uncertain", "cancelled"]`). Result (97 passed, 1
     failure):
     ```
     test: grind_test.postgres_submit_unique_state_eligibility_matrix_test
     code: let assert Ok(unique.Inserted(_)) = result
     value: Ok(Existing(Conflict(78, ..., Cancelled)))
     info: Pattern match failed, no pattern matched the value.
     ```
     The `cancelled`/`Incomplete` cell (expected `Inserted`, since
     `cancelled` is terminal and outside `Incomplete`) incorrectly reported
     `Existing` against the cancelled row. Only this test failed.
     Both mutations reverted immediately; `git diff` for `unique.gleam` showed
     no trace of either mutated line after reverting.

### Claim: a live transition through a real, manually-driven consumer — `Incomplete` sees a genuinely `queued` row, stops seeing it once the job genuinely succeeds, and `IncompleteOrSucceeded` still matches the original succeeded row

- **Test**: `postgres_submit_unique_state_live_transition_test` (marker
  `unique-state-live-transition-passed`).
- **Mechanism**: a real `queue.start_manual`/`queue.process_one` consumer
  claims and commits the row to `job.Succeeded` through the production
  execution path (not a forced SQL update), between two `submit_unique`
  calls under `Incomplete` and two under `IncompleteOrSucceeded`.
- **Limits**: characterization evidence (passed on first write); the
  state-forcing matrix above already proves every state/group cell by
  mutation, so this test's own evidence is that the _transition_ (a real
  row moving from `queued` to `succeeded` through the production queue) is
  observed correctly, not a second mutation-discovered regression over the
  same cells.

### Claim: the shared `@internal` period predicate is inclusive at the exact database-time boundary and false one microsecond past it

- **Test**: `postgres_unique_period_predicate_matches_the_exact_instant_test`
  (marker `unique-period-predicate-exact-instant-passed`), against two
  literal `timestamptz` expressions only (no table involved): `now = ts +
5000ms` (expected `true`) and `now = ts + 5000ms + 1µs` (expected
  `false`).
- **Characterization test, proven by mutation** (passed on first write).
  Mutation: changed `period_predicate`'s comparison from `>=` to `>`
  (`src/grind/internal/unique_admission.gleam`). Result, against a fresh
  disposable cluster (97 passed, 1 failure):
  ```
  test: grind_test.postgres_unique_period_predicate_matches_the_exact_instant_test
  info:
  False
  should equal
  True
  ```
  The exact-boundary case flipped to `false`, exactly the inclusive-vs-exclusive
  distinction this test exists to catch. Only this test failed. Reverted
  immediately; `git diff` showed no trace of the mutated operator.
- **Limits**: proves the predicate fragment itself at an exact instant;
  the exact-instant claim is not (and cannot cheaply be) reproduced through
  the live `submit_unique` path, which samples `now` at an uncontrolled
  real time — see the live boundary tests below for that side, honestly
  bounded to a multi-second margin instead of an exact tie.

### Claim: `FromInsertion` matches database-time reality through the live `submit_unique` path — 58 seconds inside a 60-second window still conflicts; 62 seconds outside it does not

- **Test**:
  `postgres_submit_unique_from_insertion_period_matches_database_time_test`
  (marker `unique-period-from-insertion-boundary-passed`). `inserted_at` is
  forced by raw SQL to `clock_timestamp() - interval '58 seconds'` and
  `'62 seconds'` on two separately-keyed rows, then a real `submit_unique`
  call (which samples its own `now` moments later) checks each.
- **Limits**: characterization evidence (passed on first write); the exact
  instant is proven only by the predicate-only test above. A 2-second
  margin on each side of the 60-second boundary is deliberately generous
  against ordinary test-runtime latency between the forced write and the
  live call's own `clock_timestamp()` sample.

### Claim: `FromSchedule` is "compared to the scheduled time" (Oban's own framing) — 121 seconds past a schedule falls outside a 120-second period; 119 seconds is still inside it

- **Test**: `postgres_submit_unique_from_schedule_period_past_boundary_test`
  (marker `unique-period-from-schedule-past-boundary-passed`).
  `available_at` is forced to `clock_timestamp() - interval '121 seconds'`
  (outside) and `'119 seconds'` (inside) on two separately-keyed rows.
- **Limits**: characterization evidence (passed on first write); same
  margin-vs-exact-instant relationship as the `FromInsertion` boundary test
  above.

### Claim: a future `FromSchedule` deadline extends the occupancy window well beyond what the same period length would already have let expire under `FromInsertion`

- **Test**:
  `postgres_submit_unique_from_schedule_future_extends_window_test` (marker
  `unique-period-from-schedule-future-extends-window-passed`). One row is
  forced to `inserted_at = now - 300s` and `available_at = now + 300s`; a
  60-second `FromInsertion` policy no longer matches it (long past its
  window), while the same 60-second `FromSchedule` policy still matches it
  (its schedule is 5 minutes in the future, so the period counted from
  there has not even started to elapse).
- **Limits**: characterization evidence (passed on first write); this is an
  original Grind observation about how the two `UniqueTimestamp` origins
  interact with the same period length, not an upstream-derived claim.

### Claim: `while_retained()` has no time boundary — a row inserted a year ago still conflicts

- **Test**:
  `postgres_submit_unique_while_retained_matches_a_year_old_row_test`
  (marker `unique-period-while-retained-old-row-passed`). `inserted_at` is
  forced to `clock_timestamp() - interval '1 year'`.
- **Limits**: characterization evidence (passed on first write); `Unbounded`
  is structurally a no-time-predicate SQL branch (`candidate_sql`), so this
  is closer to proof-by-construction than a mutation-discovered regression.

### Claim: replaying the same `SubmissionId` and request after the original row genuinely succeeded and its period has elapsed returns the original `Inserted` handle (same job id), not a second row

- **Test**:
  `postgres_submit_unique_receipt_replay_is_idempotent_after_period_elapses_test`
  (marker `unique-receipt-idempotent-replay-passed`). A real
  `queue.process_one` commits the row to `succeeded`; `inserted_at` is then
  forced 6 seconds into the past against a 5-second `FromInsertion` period
  (so a receipt-blind candidate lookup for this key would find nothing
  eligible); the exact same `submit_unique` call is repeated with the same
  `SubmissionId` and input.
- **Characterization test, proven by mutation** (passed on first write).
  Mutation: in `admission_transaction`
  (`src/grind/internal/unique_admission.gleam`), removed the receipt check
  entirely — after acquiring the lock, it called `admit_candidate`
  unconditionally instead of calling `find_receipt` at all. **This mutation
  proves the receipt check is necessary for idempotent replay; it does not
  prove the receipt-lookup-before-candidate-selection _ordering_ the
  contract documents** (`docs/UNIQUENESS-CONTRACT.md`, admission
  transaction step 3) — that ordering exists to correctly resolve a
  concurrent submitter's SyncRep-parked-but-already-committed receipt, a
  race this increment does not exercise (sequential calls only; the
  concurrency scenario is increments 8/11, still untested). Removing the
  check outright is a stronger, simpler mutation that answers a different,
  narrower question — "is the receipt consulted at all before every
  admission attempt" — not "is it consulted _before_ candidate selection
  specifically." Result, against a fresh disposable cluster (96 passed, 2
  failures):
  ```
  test: grind_test.postgres_submit_unique_receipt_replay_is_idempotent_after_period_elapses_test
  code: let assert Ok(unique.Inserted(replayed_handle)) = postgres.submit_unique(...)
  value: Error(SubmissionConflict)
  ```
  Without the receipt check, the replayed call ran a fresh candidate lookup
  (found nothing eligible, since the period had elapsed) and attempted an
  `INSERT` of a new job row, then hit the receipt table's own primary-key
  constraint (`storage_owner`, `submission_id`) trying to record a second
  receipt for the same id. **That constraint violation rolled back the
  whole admission transaction** — `run` in `unique_admission.gleam` maps
  a callback `Error` to `pog.TransactionRolledBack`, which never commits
  anything the callback did — so the attempted `INSERT` was rolled back
  along with the receipt write; no second row persisted. This was verified
  empirically, not just reasoned: a temporary diagnostic added to the test
  under this same mutation printed `count_jobs_in_queue(connection,
test_queue)` immediately after the failing call, which read `1`, not
  `2`. The net effect of losing the receipt check is exactly the primary
  key already protecting against a genuinely duplicated row on its own
  (the same protection Increment 2's ack-receipt work already established
  for a different table) — what the receipt check specifically adds is the
  **idempotent return**: without it, a legitimate replay after the period
  elapses surfaces as `SubmissionConflict` (an error) instead of quietly
  handing back the original `Inserted` handle, exactly the value this test
  asserts. A second, independent test (below) failed the same way in the
  same run, for the same reason. Both failures reverted immediately by
  restoring the receipt check (and the diagnostic print removed); `git
diff` showed no trace of either.

### Claim: the same `SubmissionId` with a different input conflicts

- **Test**:
  `postgres_submit_unique_receipt_replay_with_different_input_conflicts_test`
  (marker `unique-receipt-different-input-conflict-passed`).
- **Limits**: characterization evidence (passed on first write); this
  exercises the request-fingerprint mismatch path directly (Decision 9),
  not a new mechanism of its own.

### Claim: a replayed `Existing` decision returns the state recorded in the receipt at decision time, not the row's current (possibly since-progressed) state

- **Test**:
  `postgres_submit_unique_receipt_replay_returns_originally_observed_state_test`
  (marker `unique-receipt-replay-returns-observed-state-passed`). The
  conflicting row's state is forced to `succeeded` by raw SQL _after_ its
  `Existing` receipt was recorded (observed state `queued`); the exact same
  `submit_unique` call is repeated.
- **Same mutation as the idempotent-replay claim above failed this test
  too** (same caveat: this proves the receipt check is necessary, not that
  its ordering relative to candidate selection is — see above), in the
  same run (96 passed, 2 failures):
  ```
  test: grind_test.postgres_submit_unique_receipt_replay_returns_originally_observed_state_test
  code: let assert Ok(unique.Existing(conflict_replayed)) = postgres.submit_unique(...)
  value: Error(SubmissionConflict)
  ```
  Without the receipt check, the replay ran a fresh candidate lookup (which
  still found the row — its state is unaffected by this mutation, only
  whether the receipt is consulted at all) but then tried to record a
  second receipt for the same `(storage_owner, submission_id)` and hit the
  same primary-key conflict, rolling back that transaction (no state
  change persisted from this second call). Reverted with the same fix as
  above.
- **Observed (unmutated)**: `conflict_state(conflict_replayed)` reads
  `job.Queued` — the receipt's recorded observation — even though the row's
  actual current state is `succeeded` by the time of the replay.

### Claim: replaying the same `SubmissionId` and input against a worker whose output codec version has changed conflicts (proves R2, Decision 9)

- **Test**:
  `postgres_submit_unique_receipt_replay_with_changed_output_codec_conflicts_test`
  (marker `unique-receipt-output-codec-change-conflict-passed`). Two
  `Worker` values share the same id, version, input codec, and handler, but
  differ only in output codec version; the second submission (same
  `SubmissionId`, same input, the recoded worker) conflicts.
- **Proven by mutation** (passed on first write). Mutation: removed
  `json.string(request.output_version)` from the fingerprint envelope
  (`fingerprint`, `src/grind/internal/unique_admission.gleam`) — the
  one-line change R2 names. Result, against a fresh disposable cluster (97
  passed, 1 failure):
  ```
  test: grind_test.postgres_submit_unique_receipt_replay_with_changed_output_codec_conflicts_test
  info:
  Ok(Inserted(JobHandle(99, ..., Codec("...-output-...-v2", ...), None)))
  should equal
  Error(SubmissionConflict)
  ```
  Without the output codec version in the fingerprint, the replay silently
  returned `Inserted`, bound to the _recoded_ worker's `-v2` output codec —
  exactly the stale-typed-handle failure Decision 9 exists to prevent, not
  merely an unasserted possibility. Only this test failed. Reverted
  immediately; `gleam build --warnings-as-errors` recompiled clean and
  `git diff` showed no trace of the mutated line.
- **Limits**: this exercises the same request-fingerprint mechanism as the
  different-input test above, specifically its output-codec-version field;
  it does not exercise the input or error codec version fields of the same
  envelope, which are unverified by mutation (asserted only by
  construction — the envelope's shape includes them symmetrically with the
  output version).

## Increment 8 — concurrent admission under a real, barrier-forced overlap

Full contract: `docs/UNIQUENESS-CONTRACT.md`. All mutations below were
applied against a **freshly recreated** disposable cluster per mutation (the
same minimal single-database harness used in Increment 7), each reverted
immediately after capturing its failure, `gleam check` recompiling clean
after every revert, and `git diff` on `src/grind/internal/unique_admission.gleam`
confirming no trace of the mutated lines.

**Mechanism.** A test-only `BEFORE INSERT` trigger on `grind_jobs`
(`install_unique_insert_barrier`, `test/grind_test.gleam`), scoped by
`NEW.worker_id = '<this test's worker id>'`, calls
`pg_advisory_xact_lock(<a literal test lock key>)` — the same
held-then-released-on-cue barrier shape `run_overlapping_claim_test`'s
`grind_test_claim_overlap` trigger already uses for a claim `UPDATE`,
generalized to an `INSERT` (`spawn_lock_holder` factors the
hold-a-lock-in-an-open-transaction-then-release-on-a-message shape both
share). The test itself holds that literal lock in an open transaction
before starting any concurrent `submit_unique` call, so every submitter for
that worker that reaches its own `INSERT` genuinely blocks until the test
releases it — a real, deterministic overlap, not a timing race.

**Synchronization.** Every barrier test polls `pg_stat_activity`
(`await_overlap_shape`) for an _exact_ count of other backends whose active
query text matches the real `INSERT INTO grind_jobs (storage_owner, queue,
worker_id, worker_version, input_version...` text `insert_job` issues, and a
separate exact count matching the real `SELECT true FROM (SELECT
pg_advisory_xact_lock(hashtextextended...` text `acquire_lock` issues — both
counted from **one** query over one snapshot of `pg_stat_activity`, so the
two figures are never read at two different instants. Only once the exact
expected shape is observed does the test release the barrier; there is no
sleep anywhere in these tests standing in for a synchronization point.

### Claim: three concurrent `submit_unique` calls forced to genuinely overlap, same key, `KeepExisting`, distinct `SubmissionId`s — exactly one settles `Inserted`, the other two settle `Existing` against that same job id, and exactly one row persists

- **Test**: `postgres_submit_unique_concurrent_admission_forced_overlap_test`
  (marker `unique-concurrent-forced-overlap-passed`). Three separate pools
  (separate physical connections), same worker/key/queue, `KeepExisting`,
  distinct `SubmissionId`s, submitted from three separate BEAM processes.
  The test polls until it observes exactly one backend blocked inserting
  behind the barrier and exactly two blocked acquiring the domain lock,
  then releases.
- **Genuine red first, by mutation**: commented out
  `use _ <- result.try(acquire_lock(connection, request))` in
  `admission_transaction` (`src/grind/internal/unique_admission.gleam`) —
  the domain lock skipped entirely. With no lock serializing them, all
  three submitters pass candidate selection concurrently (each sees zero
  existing rows) and all three then contend for the _same_ trigger-held
  lock instead of the domain lock the test is polling for — the expected
  shape (one inserting, two on the domain lock) never materializes. Result,
  against a fresh disposable cluster (99 passed, 5 failures; this test and
  four others that depend on the same domain lock — see their own claims
  below — all failed the same way):
  ```
  test: grind_test.postgres_submit_unique_concurrent_admission_forced_overlap_test
  info:
  False
  should equal
  True
  ```
  (`await_overlap_shape(...) |> should.equal(True)` timed out after 10
  seconds of polling — the real state stayed at three backends waiting on
  the trigger's lock and zero on the domain lock the whole time, never
  reaching the one-inserting/two-domain-lock-waiting shape a working domain
  lock guarantees.) Reverted immediately; `gleam check` recompiled clean
  (the mutation left `acquire_lock` unused, producing only a compiler
  warning, not an error) and `git diff` showed no trace of the mutated
  line.
- **Proven correct** (passed on first write, and on every rerun — 11
  consecutive green full-suite runs against fresh disposable clusters while
  developing this section, 0 flakes): with the domain lock restored, the
  exact expected shape is reached and held (nothing else can happen until
  the test releases it), release yields exactly one `Inserted` and two
  `Existing` values whose `conflict_job_id` both equal the inserted job's
  id, and `count_jobs_in_queue` confirms exactly one row.
- **Limits**: this proves the shape for three same-scope (`WithinQueue`)
  submitters sharing one queue; the mixed-scope variant below proves the
  domain lock also serializes across `QueueScope` values on the same key.

### Claim: the domain lock's deliberate exclusion of queue from its own key (Decision — admission transaction step 3) actually serializes a `WithinQueue` submission against a concurrent `AcrossQueues` submission on the same key, in different queues

- **Test**: `postgres_submit_unique_concurrent_admission_mixed_scope_test`
  (marker `unique-concurrent-mixed-scope-passed`). A `WithinQueue`
  submission in queue `q1` is started first and confirmed blocked inserting
  behind the barrier before an `AcrossQueues` submission in queue `q2`,
  same key, is started — `WithinQueue`'s own candidate query only ever
  looks inside its own queue, so (unlike the same-queue overlap test above)
  which of the two reaches the barrier first is not incidental: if the
  `AcrossQueues` submission inserted into `q2` first, the `WithinQueue`
  submission's `q1`-scoped candidate query would never see it and would
  correctly insert its own row too — two rows, correctly, per the
  per-queue semantics `WithinQueue` already promises (Increment 4). Forcing
  the `WithinQueue` submission first removes that ambiguity without
  weakening the concurrency being proved: the `AcrossQueues` submission
  still arrives while the first submission's transaction is genuinely open
  and still needs the very same domain lock.
- **Genuine red first, by mutation**: widened the domain lock's own hash
  input to include `request.queue` (`lock_key_sql`/`acquire_lock`,
  `src/grind/internal/unique_admission.gleam`) — the literal "add queue to
  the lock key" mutation. `WithinQueue`/`q1` and `AcrossQueues`/`q2` then
  compute _different_ domain lock keys, so the second submission no longer
  waits behind the first's domain lock; both instead race independently to
  the shared barrier trigger (scoped only by worker id) and end up
  contending _there_ instead — never producing the one-inserting/one-
  domain-lock-waiting shape the test polls for. Result, against a fresh
  disposable cluster (101 passed, 3 failures):
  ```
  test: grind_test.postgres_submit_unique_concurrent_admission_mixed_scope_test
  info:
  False
  should equal
  True
  ```
  The other two failures in that run
  (`postgres_submit_unique_contended_lock_wait_test`,
  `postgres_unique_lock_timeout_does_not_leak_to_later_statements_test`)
  are **collateral, not evidence for this claim**: both share the
  `unique_domain_lock_query` test helper, which binds five parameters to
  `lock_key_sql`'s SQL text; with the mutation, that text expects six,
  so PostgreSQL's own parameter-count mismatch made those two tests'
  lock-holder transactions fail to acquire anything at all (`Error(Nil)`
  from `pog.execute`), which they correctly report as a hard pattern-match
  panic rather than a silent false pass — an artifact of a shared test
  helper coincidentally depending on `lock_key_sql`'s arity, not a
  statement about Increment 9's own contract. Reverted immediately;
  `gleam check` recompiled clean and `git diff` showed no trace of the
  mutated lines.
- **Proven correct** (passed on first write once the test itself was fixed
  to start the `WithinQueue` submission first — see "Limits" below): with
  the domain lock restored to excluding queue, the second submission
  reliably waits behind the first's domain lock, the first settles
  `Inserted`, the second settles `Existing` referencing the first's job id
  and its actual queue (`q1`), and exactly one row exists across both
  queues.
- **Limits**: the very first version of this test started both submissions
  at once (a genuine coin-flip race for the domain lock) and was
  observably flaky under real PostgreSQL — roughly 3 of 4 runs failed with
  a row count of 2, not because of a concurrency defect, but because
  `AcrossQueues` sometimes won the race and inserted into `q2` first, at
  which point `WithinQueue`'s own `q1`-scoped candidate query correctly
  found no conflict and correctly inserted a second row. That flake, and
  its explanation, is recorded here rather than only silently fixed,
  because it is itself evidence of a real, documented asymmetry between
  the two scopes (Increment 4's per-queue `WithinQueue` semantics), not a
  defect in the domain lock this increment is otherwise about.

### Claim: the receipt lookup's real invariant is "after the domain lock, before this transaction's own write" — not "before candidate selection" specifically, which is a coincidence of the unmutated code and was shown to be unobservable on its own

- **Test**:
  `postgres_submit_unique_receipt_ordering_returns_committed_decision_test`
  (marker `unique-receipt-ordering-b-returns-a-decision-passed`) — the
  receipt-ordering evidence deferred from Increment 7. Submitter A is
  confirmed blocked inserting behind the barrier; submitter B — the _same_
  `SubmissionId` and the same input, hence the same request fingerprint —
  is then started and confirmed waiting on the domain lock A holds.
  Releasing the barrier lets A finish: it inserts, records its `inserted`
  receipt, and commits, which releases the domain lock. B then acquires it.
  B's result is asserted to be `Inserted`, with the _same_ job id as A's —
  not `Existing` against the row A just committed, which is what B would
  get if it ran a fresh candidate selection instead of finding and
  returning A's receipt.
- **First mutation attempted, no observable difference — recorded
  honestly, as the approved plan allows**: moved the receipt lookup from
  immediately after the lock to immediately after candidate selection
  (still strictly before the insert/decide/record-receipt write), i.e.
  inside `admit_candidate` right after `find_candidate`, rather than in
  `admission_transaction` before `admit_candidate` is even called
  (`src/grind/internal/unique_admission.gleam`). Result, against a fresh
  disposable cluster, run twice: **104 passed, no failures** both times —
  identical to the unmutated baseline.
  - **Why this specific reordering is genuinely unobservable here**: by
    the time B can even acquire the domain lock, A's commit has already
    fully finished — PostgreSQL only releases a transaction's advisory
    locks (and ordinary row locks) as part of finishing `COMMIT`, strictly
    after the transaction's changes are already marked committed and
    visible to other backends' snapshots. So A's receipt row is visible to
    B's very first statement in its own transaction regardless of whether
    B's receipt lookup runs immediately after the lock or one read-only
    `SELECT` later (`find_candidate` takes no row lock under
    `KeepExisting`, so inserting it before the receipt lookup has no side
    effect to race against). This result prompted a more precise statement
    of the actual invariant (below), checked by two further mutations that
    genuinely do move the lookup somewhere observably wrong.
- **Second mutation, genuinely red — moved the lookup _before_ the domain
  lock** (`admission_transaction`: `find_receipt` now runs immediately
  after `pin_read_committed`, before `set_lock_timeout`/`acquire_lock`, and
  its result is used unconditionally rather than re-checked after the
  lock). Against a fresh disposable cluster: **104 passed, 1 failure**:
  ```
  test: grind_test.postgres_submit_unique_receipt_ordering_returns_committed_decision_test
  code: let assert Ok(unique.Inserted(handle_b)) = outcome_b
  value: Error(SubmissionConflict)
  ```
  With the lookup moved earlier, B's find-nothing result is captured
  _before_ B ever waits on the domain lock, so it is stale by the time B
  is unblocked: B proceeds straight to candidate selection using that
  stale "no receipt" answer, finds A's committed row, and its own
  `record_receipt` collides with A's already-committed row on
  `grind_unique_submissions`'s primary key — exactly the different-key
  `23505` race documented in "Admission transaction" step 4, except forced
  here onto the _same_ key by the mutation itself. Reverted immediately;
  `gleam check` recompiled clean and `git diff` showed no trace of the
  mutated lines.
- **Third mutation, genuinely red — moved the lookup _after_ `insert_job`'s
  `INSERT`, before `record_receipt`** (`admission_transaction` calls
  `admit_candidate` directly, with no early receipt check at all;
  `insert_job` now runs its `INSERT` unconditionally, then checks
  `find_receipt`, returning the existing decision if found instead of
  recording a new receipt — but the just-inserted row is never rolled
  back). Against a fresh disposable cluster, this single mutation was
  caught by **two** existing tests, **103 passed, 2 failures**:
  ```
  test: grind_test.postgres_submit_unique_receipt_replay_is_idempotent_after_period_elapses_test
  info:
  2
  should equal
  1

  test: grind_test.postgres_submit_unique_receipt_ordering_returns_committed_decision_test
  code: let assert Ok(unique.Inserted(handle_b)) = outcome_b
  value: Error(SubmissionConflict)
  ```
  The first failure is the more telling one for this mutation, and is
  exactly why "before any write" — not merely "returns the right value" —
  is the real invariant: Increment 7's sequential replay test (the same
  `SubmissionId` retried after the original row's period has elapsed, so
  candidate selection legitimately finds nothing) still gets back the
  _correct_ `Inserted` handle from the pre-existing receipt, because the
  post-insert receipt check still finds and returns it — but the mutated
  code already committed a _second_, orphaned job row before making that
  check, which only `count_jobs_in_queue` catches (the returned value looks
  right; the database does not match it). The second failure is the same
  `23505` symptom as the previous mutation, for the concurrent case: B's
  `find_candidate` now correctly finds A's row (candidate selection was
  never skipped here), takes the `decide_conflict`/`Existing` path, and its
  own `record_receipt` for the shared `SubmissionId` collides with A's.
  Reverted immediately; `gleam check` recompiled clean and `git diff`
  showed no trace of the mutated lines.
- **Limits**: this proves the ordering claim for the specific interleaving
  the barrier forces (B waits on the domain lock for the entire duration of
  A's transaction); it does not exercise the `23505` different-key,
  same-`SubmissionId` race documented as out of scope in
  `docs/UNIQUENESS-CONTRACT.md` (two submitters that never contend the same
  domain lock at all) — though the second mutation above incidentally
  demonstrates that exact failure mode's _symptom_, just triggered by a
  code defect rather than by two submitters genuinely using different keys.

## Isolation-level pinning

Several of Grind's transactions depend on `READ COMMITTED` semantics: the
uniqueness admission transaction's plain reads after its domain lock must
see whatever a fellow submitter committed while it waited; the
acknowledgement and audited-resolution paths' fenced, locking `UPDATE`s
must not surface PostgreSQL's `REPEATABLE READ`/`SERIALIZABLE` conflict
handling in place of the idempotent result a legitimate concurrent retry is
supposed to get. `pog.transaction` issues a plain `BEGIN`, which takes on
whatever the connecting role or database's own
`default_transaction_isolation` is configured to — never audited or pinned
before this round.

**The fix is uniform, not per-path.** An initial version of this section
pinned isolation only inside the uniqueness admission transaction
(`pin_read_committed`, a `SET TRANSACTION ISOLATION LEVEL READ COMMITTED`
as that transaction's own first statement) and separately argued that every
_other_ transaction was safe under a non-default isolation level because
its only wait happens inside a _locking_ statement (`UPDATE`/`SELECT FOR
UPDATE`), which PostgreSQL itself protects with `40001
serialization_failure` rather than silently misreading — "at worst a no-op
becomes an error," that draft said. That framing was wrong: a normal,
legitimate concurrent retry of an idempotent command (a duplicate
acknowledgement, a duplicate audited resolution) _surfacing as an
unhandled query failure_ is itself a real bug, not merely an acceptable
degraded case — proven directly below. The fix instead pins isolation once,
for every connection in the pool, at the moment it starts:
`postgres.validate` now adds `default_transaction_isolation = 'read
committed'` as a `pog.connection_parameter` (a PostgreSQL startup
parameter, sent once per physical connection), so every transaction on
every pooled connection is `READ COMMITTED` regardless of what the
connecting role or database is configured to default to.
`pin_read_committed` is kept in the admission transaction as defense in
depth on top of this — a connection pooler between Grind and PostgreSQL
could drop or ignore a startup parameter, where an in-transaction `SET
TRANSACTION` cannot be silently dropped the same way.

### Claim: without pinning `READ COMMITTED`, a role or database configured with `default_transaction_isolation = 'repeatable read'` makes the forced-overlap admission transaction silently duplicate a row

- **Test**:
  `postgres_submit_unique_admission_safe_under_repeatable_read_test`
  (marker `unique-admission-safe-under-repeatable-read-passed`). A
  dedicated disposable database
  (`grind_repeatable_read_test`/`GRIND_TEST_REPEATABLE_READ_URL`,
  `scripts/test-postgres.sh`) is configured with `ALTER DATABASE
grind_repeatable_read_test SET default_transaction_isolation =
'repeatable read'` at cluster setup — a real, differently-configured
  PostgreSQL session, not a simulated one. The test reads this database's
  own _persisted, configured_ default back from `pg_db_role_setting`
  joined to `pg_database` (not `SHOW default_transaction_isolation` on a
  Grind-managed connection — once the pool-level pin below exists, a Grind
  connection's own _active_ session setting always reads `read committed`
  regardless of what the database is configured to default to, so a `SHOW`
  on it would prove nothing). It then runs the same two-submitter
  forced-overlap barrier as Increment 8's main test (same key,
  `KeepExisting`, distinct `SubmissionId`s) against this database, and
  asserts exactly one `Inserted` and one `Existing` referencing the same
  job id — one row.
- **Genuine red first**: run against the code _before_ either pin existed
  (`admission_transaction`'s first statement was `set_lock_timeout`, and
  `postgres.validate` did not yet set any connection parameter). Against a
  fresh disposable cluster (with the dedicated `repeatable read` database),
  run twice, both times deterministically:
  ```
  test: grind_test.postgres_submit_unique_admission_safe_under_repeatable_read_test
  info:
  2
  should equal
  1
  ```
  Both submitters reported `Inserted`, each with its own job id — the
  waiting submitter's plain reads, using the snapshot frozen before it
  ever started waiting on the domain lock, never saw the other's commit.
- **Fix and proven correct, in two layers**: first added `pin_read_committed`
  (`src/grind/internal/unique_admission.gleam`) alone and reran the
  identical test three times against a fresh cluster each time: **105
  passed, no failures** every time — the in-transaction pin alone is
  sufficient for this specific claim. Then added the pool-level
  `pog.connection_parameter` pin in `postgres.validate`
  (`src/grind/postgres.gleam`) on top, and reran again: still green. The
  full uniqueness suite was also rerun repeatedly during development with
  zero flakes (11 additional runs after Increment 8/9, 5 more after this
  round).

### Claim: a duplicate acknowledgement — the _same_ command, retried while the first attempt's commit is still in flight — reports `Ok(True)` (idempotent success), not a query failure, regardless of the connecting session's isolation level

- **Test**: `postgres_ack_duplicate_reports_ok_under_pinned_isolation_test`
  (marker `ack-duplicate-ok-under-pinned-isolation-passed`), run against
  the dedicated `repeatable read` database. A's fenced acknowledgement
  `UPDATE` is blocked behind a test-only `BEFORE UPDATE` barrier trigger
  scoped to this job (`OLD.state = 'executing' AND NEW.state <>
'executing'`); B — the _identical_ acknowledgement command (same claim,
  same execution outcome), from a separate pool — starts while A is
  blocked, and B's own fenced `UPDATE` then genuinely waits on the row lock
  A's in-flight `UPDATE` holds. `pg_stat_activity` confirms the exact
  shape before releasing: one backend waiting on the trigger's _advisory_
  lock (A), one waiting on the _row_ lock (B, `wait_event = 'transactionid'`
  — a real PostgreSQL tuple-lock wait, not a second advisory wait, since
  the two backends' query text is byte-identical and only `wait_event`
  tells them apart here). Once A completes and commits, B's `UPDATE` no
  longer matches (the row is no longer `executing`), so B falls through to
  `acknowledge_transaction`'s existing re-read of the acknowledgement
  receipt — and both A and B must report `Ok(True)`, with exactly one
  acknowledgement row persisted.
- **Genuine red first**: run against the code before either isolation pin
  existed. Against a fresh disposable cluster, reproduced deterministically
  twice:
  ```
  test: grind_test.postgres_ack_duplicate_reports_ok_under_pinned_isolation_test
  info:
  Error(QueueAckFailed(PostgresqlError("40001", "serialization_failure", "could not serialize access due to concurrent update")))
  should equal
  Ok(True)
  ```
  B's `UPDATE`, waiting on the row lock A held, found (once unblocked) that
  the row had been modified by a transaction that committed after B's own
  `REPEATABLE READ` snapshot was taken — PostgreSQL's own conflict
  handling for a locking statement under that isolation level, raising
  `40001` instead of silently re-fetching. A legitimate, idempotent retry
  of an already-successful command surfaced as an unhandled query failure.
- **Fix and proven correct**: the pool-level `pog.connection_parameter`
  pin in `postgres.validate` (this path has no advisory lock and no
  in-transaction `SET TRANSACTION` of its own — the pool-level pin is the
  _only_ fix available to it, which is exactly why the fix had to be
  uniform rather than per-path). Reran the identical test against a fresh
  disposable cluster: **106 passed, no failures**; reran twice more with
  the same result.
- **Limits**: this proves the duplicate-acknowledgement race specifically;
  the equivalent race for audited resolution is a separate claim (below),
  because it is a genuinely different, pre-existing defect independent of
  isolation level, not merely the same isolation-level fix applied to a
  second path.

### Audit of every other transaction in `src/grind`, and what actually happens to each under `REPEATABLE READ`/`SERIALIZABLE` without the pin

Checked every `transaction_safely`/`pog.transaction` call site and every
`FOR UPDATE` in `src/grind/postgres.gleam`:

- `claim_registered_job`/`quarantine_expired`/`renew_claim`
  (`grind/postgres.gleam`) issue a single `WITH candidate AS (... FOR
UPDATE SKIP LOCKED ...) UPDATE ...`/`UPDATE ...` statement each via
  `execute_safely`, with no explicit `BEGIN` — PostgreSQL's implicit
  single-statement transaction takes its snapshot at that one statement
  regardless of isolation level, and `SKIP LOCKED` never waits at all. Not
  reachable by this class of race at all (no wait, so nothing to surface).
- `acknowledge_transaction` (`acknowledge`) — proven directly above: a
  duplicate acknowledgement that must wait on the row lock the first
  attempt holds reports `QueueAckFailed(PostgresqlError("40001",
"serialization_failure", ...))` instead of `Ok(True)`, without the pin.
- `reconcile_matching_owner`/`apply_uncertain_resolution` (`resolve_uncertain`)
  — `find_resolution` (a plain read, before the lock) → `SELECT ... FOR
UPDATE` on the job row → (a separate, deferred `INSERT` into
  `grind_job_resolutions`, then an `UPDATE` on the job row to leave the
  `uncertain` state). A concurrent, identical resolution command that must
  wait on the `SELECT ... FOR UPDATE` or the later `UPDATE` would, without
  the pin, surface the wait's conflict as a `40001` query failure exactly
  like the acknowledgement path — `Error(ReconciliationQueryFailed(...))`
  from whichever statement's row version changed underneath it. This path
  also has a **second, distinct defect independent of isolation level**,
  documented as its own claim below (the same `40001`-vs-`READ COMMITTED`
  distinction still applies to _that_ fixed code, once it does complete a
  normal `READ COMMITTED` wait-then-refetch instead of throwing).
- `cancel_transaction` (`cancel`) issues `SELECT ... FOR UPDATE` as its
  transaction's own first statement; a concurrent `cancel` on the same job
  waiting on that row lock would, without the pin, get a `40001` query
  failure instead of PostgreSQL's `READ COMMITTED` re-fetch-and-continue
  behavior.
- `migrate_transaction` (`migrate`) only reads system catalogs and issues
  DDL, with no advisory or row-lock wait step. Not reachable by this class
  of race (schema install is not run concurrently against the same
  not-yet-existing schema by design).
- No other module calls `pg_advisory_xact_lock`; `grep -n "pg_advisory\|advisory_lock"
src/grind/postgres.gleam src/grind/queue.gleam` returns nothing — the
  uniqueness admission transaction is the only one where the _wait itself_
  (an advisory lock, with no PostgreSQL-native conflict detection tied to
  it) is the failure surface; every other path's wait is a row lock, whose
  failure surface without the pin is a `40001` query failure rather than a
  silently wrong decision — worth fixing (an idempotent retry should not
  fail loudly either) but categorically different from admission's
  silent-duplicate risk, which is why both this section's tests were
  needed to characterize the fix's actual effect on both.

## Concurrent audited resolution

A separate, pre-existing defect in `reconcile_matching_owner`/
`apply_uncertain_resolution`, independent of isolation level (reproducible
under this cluster's default `READ COMMITTED`), found while auditing the
isolation-pinning fix above.

### Claim: two concurrent, identical `resolve_uncertain` calls for the same uncertain job — the same `resolution_id` and payload — both report success, with only one resolution row and one state transition

`reconcile_matching_owner` checks `find_resolution` (a plain read, by
`resolution_id`) once, _before_ `apply_uncertain_resolution`'s `SELECT ...
FOR UPDATE` on the job row. If that first check finds nothing (no receipt
yet), `apply_uncertain_resolution` proceeds to lock the row and, if its
`state` is not `uncertain`, previously returned `Error(ReconciliationNotRequired)`
unconditionally — with no re-check of `find_resolution`. Two concurrent
calls sharing the same `resolution_id` and payload both miss the early
check (neither has committed yet); the second then waits on the `FOR
UPDATE` behind the first, and once unblocked re-fetches a job row the
first call has already moved out of `uncertain` — misreporting a
legitimate, already-applied retry as "reconciliation not required," the
audited-resolution equivalent of the acknowledgement path's "duplicate
retry reported as stale" failure mode — `acknowledge_transaction`'s own
existing comment in `src/grind/postgres.gleam` names the identical
shape: "a duplicate ACK can wait behind the first writer's row lock;
re-read its receipt... instead of misreporting that exact retry as
stale."

- **Test**: `postgres_resolution_concurrent_same_outcome_applied_once_test`
  (marker `resolution-concurrent-same-outcome-applied-once`), run against
  the _default_ database (`GRIND_TEST_DATABASE_URL` — this defect is not
  an isolation-level issue). A's `write_resolution` `UPDATE` (the one that
  moves the job out of `uncertain`) is blocked behind a test-only `BEFORE
UPDATE` barrier trigger scoped to this job (`OLD.state = 'uncertain'`);
  B — the identical `AuthorizeReplay` resolution command, same
  `resolution_id`, same details, from a separate pool — starts while A is
  blocked, and B's own `SELECT ... FOR UPDATE` then genuinely waits on the
  row lock A already holds (confirmed via `pg_stat_activity`'s
  `transactionid` wait event, the same discipline the acknowledgement test
  above uses). Once A completes and commits, B must report
  `Ok(ResolutionAlreadyApplied(Queued))` — not
  `Error(ReconciliationNotRequired)` — and exactly one
  `grind_job_resolutions` row and one final job state (`queued`) must
  exist.
- **Genuine red first**: against a fresh disposable cluster, reproduced
  deterministically twice:
  ```
  test: grind_test.postgres_resolution_concurrent_same_outcome_applied_once_test
  info:
  Error(ReconciliationNotRequired)
  should equal
  Ok(ResolutionAlreadyApplied(Queued))
  ```
- **Fix, minimal, chosen over the alternative and justified**: extracted
  the exact receipt-matching logic `reconcile_matching_owner`'s early
  check already used into its own function,
  `resolution_receipt_outcome` (`src/grind/postgres.gleam`), and called it
  a _second_ time from `apply_uncertain_resolution`'s `stored_state ==
"uncertain"` `False` branch — the same "re-read the receipt instead of
  assuming the earlier miss is still accurate" pattern the acknowledgement
  path already uses. The alternative the review raised — moving the
  _first_ `find_resolution` check to run after the lock instead of before
  — was not taken: it would require restructuring
  `reconcile_matching_owner` to always take the job's row lock even for a
  resolution command that turns out to need no lock at all (a route
  mismatch, a worker contract mismatch, or a job that was never
  `uncertain`), taking a lock this function does not otherwise need for
  those cases, for every call. Re-checking only in the one branch that
  discovers it needs to is the smaller change and does not alter the
  locking footprint of any other path through this function.
- **Proven correct**: reran the identical test against a fresh disposable
  cluster: **107 passed, no failures**; reran twice more with the same
  result. The existing sequential resolution tests
  (`audited-uncertain-resolution-passed` and others using
  `postgres.resolve_uncertain` twice in a row) are unaffected — a
  _sequential_ replay after the first call has already committed is still
  caught entirely by `reconcile_matching_owner`'s original early check,
  which never reaches `apply_uncertain_resolution` at all.
- **Limits**: this proves the concurrent-resolution race for
  `AuthorizeReplay`; `ConfirmSuccess`/`ConfirmBusinessFailure` share the
  same code path (the `False` branch re-check is decision-agnostic) but
  are not independently exercised under this exact forced overlap.

## Increment 9 — contention

Full contract: `docs/UNIQUENESS-CONTRACT.md`, Decision 6 (a blocking,
bounded lock wait — `AdmissionContended` on PostgreSQL `55P03`, never a
persisted-conflict implication). Mutations below follow the same
fresh-disposable-cluster-per-mutation discipline as Increment 8.

**Mechanism.** The test itself holds the _real_ lock a concurrent
`submit_unique` call would need — either the domain advisory lock, built
from the same `@internal unique_admission.lock_key_sql` expression
production code uses (`unique_domain_lock_query`, so the test never
re-encodes the key by hand), or a real PostgreSQL row lock
(`SELECT ... FOR UPDATE` in an open transaction) — via the same
`spawn_lock_holder` hold-then-release-on-cue helper Increment 8 uses. A
concurrent `submit_unique` call against a pool configured with
`postgres.unique_lock_wait(200)` then genuinely waits on that real lock for
up to 200ms before PostgreSQL itself raises `55P03`.

### Claim: a `submit_unique` call contending the real domain lock for 200ms reports `AdmissionContended`, with no job row and no receipt; the same `SubmissionId` succeeds once the lock is released

- **Test**: `postgres_submit_unique_contended_lock_wait_test` (marker
  `unique-contended-lock-wait-passed`).
- **Proven correct** (passed on first write): while the test holds the
  domain lock for this worker/key, a `submit_unique` call with a 200ms
  `unique_lock_wait` reports `Error(AdmissionContended)`; `grind_jobs` has
  no row for the queue and `grind_unique_submissions` has no receipt for
  that `SubmissionId`. Releasing the lock and retrying the identical
  `SubmissionId` succeeds with `Inserted`.
- **Proven by mutation**: removed the `55P03` special case from
  `classify_query_error` (`src/grind/internal/unique_admission.gleam`), so
  every PostgreSQL error — including the bounded lock wait elapsing —
  became `AdmissionFailed(error)`. Result, against a fresh disposable
  cluster (101 passed, 3 failures; this test and the two other Increment 9
  tests below, which share the same classification function, all failed
  the same way):
  ```
  test: grind_test.postgres_submit_unique_contended_lock_wait_test
  info:
  Error(AdmissionFailed(PostgresqlError("55P03", "lock_not_available", "canceling statement due to lock timeout")))
  should equal
  Error(AdmissionContended)
  ```
  Reverted immediately; `gleam check` recompiled clean and `git diff`
  showed no trace of the mutated lines.

### Claim: a `RescheduleScheduledTo` submission whose candidate row lock (`FOR UPDATE`) is held by the test contends the same way; the row is left completely unchanged; the same request succeeds once the lock is released

- **Test**: `postgres_submit_unique_reschedule_row_lock_contention_test`
  (marker `unique-reschedule-row-lock-contention-passed`). A row is first
  admitted `scheduled` far in the future; the test then holds that row's
  own lock via an open `SELECT ... FOR UPDATE` transaction (not the domain
  lock) — the lock `docs/UNIQUENESS-CONTRACT.md`'s admission transaction
  step 5 takes on a `RescheduleScheduledTo` candidate. A concurrent
  reschedule attempt against the same key, 200ms `unique_lock_wait`,
  reports `AdmissionContended`; `available_at` is read back and confirmed
  byte-for-byte unchanged from before the attempt. Releasing the row lock
  and retrying the identical reschedule request succeeds with
  `Rescheduled`, and `available_at` is read back and confirmed to equal
  the new target exactly.
- **Proven correct** (passed on first write); **proven by the same
  `classify_query_error` mutation above** (101 passed, 3 failures,
  identical `AdmissionFailed(PostgresqlError("55P03", ...))` vs.
  `AdmissionContended` mismatch, reverted the same way) — this test
  specifically proves the row-lock path (not just the domain-lock path
  Increment 9's first claim covers) is classified through the same
  function, exactly as `docs/UNIQUENESS-CONTRACT.md` documents ("every
  query in the admission transaction ... is classified through the same
  function").
- **Limits**: this proves contention on the row lock a _reschedule_
  candidate takes; it does not exercise a race between this row lock and a
  concurrent claim already in progress on the same row (a long-held row
  lock causing "false contention" against an unrelated in-progress
  acknowledgement is a documented failure mode in
  `docs/UNIQUENESS-CONTRACT.md`, not itself proven by a dedicated test).

### Claim: the bounded `lock_timeout` `set_config(..., true)` sets (`is_local`, transaction-local) does not leak into a later statement that reuses the same pooled physical connection

- **Test**:
  `postgres_unique_lock_timeout_does_not_leak_to_later_statements_test`
  (marker `unique-lock-timeout-no-leak-passed`). A dedicated pool of
  exactly one physical connection (`postgres.pool_size(1)`) guarantees
  every statement on that pool reuses the identical PostgreSQL session the
  contended attempt's `set_config('lock_timeout', ..., true)` ran on —
  there is only one connection in the pool, so there is no other
  connection it could route to. The test checks `SHOW lock_timeout` on
  that pool at two points: (1) immediately after a _contended_ attempt
  (domain lock held by a separate connection, 200ms `unique_lock_wait`),
  and (2) immediately after a _committed_ attempt on the same pool once the
  domain lock is released. Both read back `"0"` — the disposable test
  cluster's own default (`scripts/test-postgres.sh` never sets
  `lock_timeout`) — not `200ms`.
- **The first checkpoint alone cannot distinguish `is_local`**: PostgreSQL
  reverts a `SET`/`set_config` change made inside an aborted transaction
  regardless of whether it was transaction-local (`is_local: true`) or
  session-level (`is_local: false`) — only a _committed_ transaction's
  change actually depends on `is_local` to know whether it should survive
  past that commit. The first version of this test checked only the
  contended (hence rolled-back) case and did not catch the mutation below
  — an empirical finding, not merely a theoretical one (see next bullet) —
  so the test was extended with the second, post-commit checkpoint.
- **Proven by mutation, both checkpoints run**: flipped `set_config`'s
  third argument, `true` -> `false`
  (`set_lock_timeout`, `src/grind/internal/unique_admission.gleam`).
  First run (before adding the post-commit checkpoint), against a fresh
  disposable cluster: **105 passed, no failures** — the mutation was
  invisible to the contended-only assertion, exactly as the reasoning
  above predicts. After extending the test with the post-commit checkpoint
  and rerunning the same mutation, against a fresh disposable cluster:
  **104 passed, 1 failure**:
  ```
  test: grind_test.postgres_unique_lock_timeout_does_not_leak_to_later_statements_test
  info:
  "200ms"
  should equal
  "0"
  ```
  A session-level `set_config` survives the committed transaction's
  `COMMIT`, leaving `lock_timeout` at `200ms` for every later statement on
  that pooled connection — exactly the leak `is_local: true` exists to
  prevent. Reverted immediately; `gleam check` recompiled clean and `git
diff` showed no trace of the mutated line.
- **Also proven by the same `classify_query_error` mutation from the claim
  above** (101 passed, 3 failures, reverted the same way) — this test's
  contended-attempt precondition depends on `AdmissionContended` being
  reported at all, so that mutation breaks this test's setup too.
- **Limits**: this proves non-leakage across one contended attempt followed
  by one committed attempt on the same connection; it does not prove
  non-leakage across a longer sequence of mixed contended/committed
  attempts, nor across a connection that outlives many unrelated
  `submit_unique` calls in production use.

## Increment 10 — rescheduling

Full contract: `docs/UNIQUENESS-CONTRACT.md`, `ConflictAction`,
`RescheduleScheduledTo`, and admission transaction steps 5-6.
`postgres_submit_unique_reschedule_row_lock_contention_test` (Increment 9)
already proves lock contention on the reschedule candidate's row; this
increment proves the reschedule decision itself.

### Claim: a scheduled conflict rescheduled to `t2` settles `Rescheduled`; `available_at` equals `t2` exactly; the job id, worker, and input are unchanged; the receipt records both the previous and new `available_at`

- **Test**: `postgres_submit_unique_reschedule_moves_available_at_test`
  (marker `unique-reschedule-moves-available-at-passed`).
- **Limits**: characterization evidence (passed on first write); the
  reschedule mechanism itself is proven by mutation below, against the
  companion "non-scheduled states unchanged" and "live race" tests, which
  exercise the same `decide_conflict` branch this test also reaches.

### Claim: a `RescheduleScheduledTo` submission against a conflict in `queued`, `retryable`, `executing`, or `uncertain` settles `Existing` with the row completely unchanged, never `Rescheduled`

- **Test**: `postgres_submit_unique_reschedule_leaves_non_scheduled_states_unchanged_test`
  (marker `unique-reschedule-non-scheduled-unchanged-passed`). Each of the
  four states is forced by raw SQL onto a freshly-keyed row (the same
  `force_job_state` helper the Increment 7 state-eligibility matrix uses),
  then a `RescheduleScheduledTo` submission against that same key is
  checked.
- **Proven by mutation** — see "Mutation: the Gleam-level state guard" below
  (shared with the live-race test).

### Claim: rescheduling a `scheduled` row to a due (past) database time makes it genuinely claimable by a real manually-driven consumer on its very next `process_one` call

- **Test**: `postgres_submit_unique_reschedule_to_due_time_makes_row_claimable_test`
  (marker `unique-reschedule-due-time-claimable-passed`). The reschedule
  target itself is a due (past) database time (`future_available_at` with a
  negative offset), so no separate forced write or wait is needed beyond the
  reschedule call itself.
- **Limits**: characterization evidence (passed on first write); this is a
  straight-line production-path test, not a race.

### Claim: the live race between a real claim (which locks the scheduled row as part of its own claim `UPDATE`) and a concurrent `RescheduleScheduledTo` submission for the same key resolves through the row's _fresh_ post-claim state, not the stale value the wait began with

- **Tests**:
  `postgres_submit_unique_reschedule_race_incomplete_returns_existing_test`
  (marker `unique-reschedule-race-incomplete-existing-passed`) and
  `postgres_submit_unique_reschedule_race_scheduled_only_returns_inserted_test`
  (marker `unique-reschedule-race-scheduled-only-inserted-passed`), sharing
  one runner (`run_unique_reschedule_claim_race_test`) parameterized only by
  the policy's `States` group.
- **Mechanism**: a `scheduled` row, forced due by raw SQL
  (`force_available_at_due`, leaving `state` untouched). A `BEFORE UPDATE`
  trigger on `grind_jobs`, scoped to this job id and `NEW.state =
'executing'`, blocks on a fresh test-only advisory lock — the exact
  `grind_test_claim_overlap` shape `run_overlapping_claim_test` already uses
  for the same claim-`UPDATE` barrier, generalized here with a per-test lock
  key (`unique_test_lock_key`). The test itself holds that advisory lock via
  `spawn_lock_holder` before spawning a real manual consumer's
  `queue.process_one` call, which claims the due row (locking it via its own
  `FOR UPDATE SKIP LOCKED` candidate CTE), sets `state = 'executing'`, and
  blocks in the trigger — the claim's row lock is held for the rest of that
  still-open transaction. A concurrent `RescheduleScheduledTo` submission for
  the same key is then spawned; its own candidate `SELECT ... FOR UPDATE`
  genuinely waits on that row lock.
- **Synchronization**: `await_lock_wait_counts(connection, 1, 1, 500)` polls
  one `pg_stat_activity` snapshot for exactly one backend waiting on an
  _advisory_ lock (the claim, parked in its trigger) and exactly one waiting
  on a _row_ lock (`transactionid` — the reschedule submission's `FOR
UPDATE`) before releasing the barrier — the same discipline
  `postgres_ack_duplicate_reports_ok_under_pinned_isolation_test` and
  `postgres_resolution_concurrent_same_outcome_applied_once_test` already
  use to distinguish an advisory wait from a genuine tuple-lock wait.
- **Determinism fix (found by independent review)**: the claimed job's
  worker originally returned immediately (`unique_test_worker`). Once the
  claim's `UPDATE` commits (releasing the row lock) and the barrier is
  released, that worker's handler runs and returns instantly, letting the
  coordinator's own subsequent acknowledgement race ahead to `succeeded`
  (which `Incomplete` does not admit either) _before_ the reschedule
  submission's blocked row lock is necessarily granted — an
  environment-dependent race, not a deterministic proof, since nothing
  ordered "the reschedule's `FOR UPDATE` re-acquires and re-evaluates" ahead
  of "the worker returns and the ack commits." Fixed with a dedicated
  blocking worker (`unique_test_blocking_worker`) whose handler reports
  `FirstAttemptStarted(release)` and then blocks; the test now waits for
  that signal (proof the claim's row lock has already been released, since
  the claim's `UPDATE` commits, as a single-statement transaction, strictly
  before the coordinator invokes the handler) and for the reschedule
  submission's own result _before_ releasing the handler's barrier, so the
  row is deterministically still `executing` — no acknowledgement has run
  yet — for the entire window the reschedule's re-acquisition needs.
- **Observed**: releasing the barrier lets the claim finish and commit
  (`process.receive(claim_reply, ...) == Ok(Ok(True))`), which releases the
  row lock. Under `Incomplete` (which admits `executing`), PostgreSQL's own
  `EvalPlanQual` re-check — the same mechanism a concurrently-updated `SELECT
... FOR UPDATE` target always gets — hands the reschedule submission the
  row's fresh `executing` state, not the stale `scheduled` value it started
  waiting behind; the Gleam-level `RescheduleScheduledTo(_), "scheduled"`
  pattern match does not fire, and the call settles `Existing` with
  `conflict_state` reading `Executing`, `available_at` completely untouched,
  and exactly one row for the key. Under `ScheduledOnly` (which does not
  admit `executing`), the same re-check finds no row matching the eligible-
  states filter at all, so `find_candidate` returns nothing and the call
  settles `Inserted` — a fresh row, distinct id, two rows for the key in
  total.
- **Mutation: the Gleam-level state guard** — widened
  `decide_conflict`'s `unique.RescheduleScheduledTo(at), "scheduled" -> ...`
  pattern match (`src/grind/internal/unique_admission.gleam`) to
  `unique.RescheduleScheduledTo(at), _ -> ...`, i.e. fire the reschedule
  branch for _any_ candidate state, not only `scheduled`. Against a fresh
  disposable cluster, re-run after the determinism fix above (115 passed, 2
  failures):
  ```
  test: grind_test.postgres_submit_unique_reschedule_leaves_non_scheduled_states_unchanged_test
  code: let assert Ok(unique.Existing(conflict)) =
      submit_reschedule(...)
  value: Ok(Rescheduled(Conflict(108, "127.0.0.1:36835/grind_test", "unique-reschedule-states-...", "unique.reschedule-states-...", "v1", Scheduled)))
  info: Pattern match failed, no pattern matched the value.

  test: grind_test.postgres_submit_unique_reschedule_race_incomplete_returns_existing_test
  code: let assert Ok(unique.Existing(conflict)) = reschedule_result
  value: Ok(Rescheduled(Conflict(110, "127.0.0.1:36835/grind_test", "unique-reschedule-race-...-6", "unique.reschedule-race-...-6", "v1", Scheduled)))
  info: Pattern match failed, no pattern matched the value.
  ```
  Both mutated calls fabricate a `Rescheduled` decision (falsely reporting
  the observed state as `Scheduled`) against a row whose real state is
  `queued`/`executing` and whose `available_at` the SQL-level `WHERE id = $2
AND storage_owner = $3 AND state = 'scheduled'` guard (still present,
  unmutated, in `reschedule_job`'s own `UPDATE`) silently leaves untouched —
  a receipt recording "rescheduled" against a row nothing actually changed,
  exactly the fabricated-success failure mode this test exists to catch.
  Only these two tests failed. Reverted immediately; `gleam build
--warnings-as-errors` recompiled clean and `git diff` for
  `unique_admission.gleam` showed no trace of the mutated line.
- **First mutation attempted, no observable difference — recorded honestly**:
  dropped only the SQL-level `AND state = 'scheduled'` guard from
  `reschedule_job`'s own `UPDATE` (leaving the Gleam-level pattern match
  intact). Against a fresh disposable cluster, re-run after the determinism
  fix above: **117 passed, no failures** — identical to the unmutated
  baseline. This guard is redundant given the
  code as written: `decide_conflict`'s Gleam-level match only ever calls
  `reschedule_job` when `candidate.state` already reads `"scheduled"`
  (freshly re-checked by the very `SELECT ... FOR UPDATE` that took the row
  lock this same transaction holds for the rest of its duration), so no
  other transaction can change that row's state between the read and the
  `UPDATE` — the same reasoning Increment 8's own "first mutation, no
  observable difference" entry documents for a structurally similar
  redundancy. `docs/UNIQUENESS-CONTRACT.md`'s own wording ("re-checked under
  the row lock from step 6, not assumed from the candidate read") is kept
  as defense in depth regardless, on the same rationale `pin_read_committed`
  is kept alongside the pool-level isolation pin.
- **Limits**: the race test proves this exact interleaving (the reschedule
  submission waits on the row lock for the claim's entire remaining
  transaction); it does not exercise a race where the reschedule submission
  arrives first and the claim must instead wait on _its_ domain lock (not
  applicable — a claim never takes the uniqueness domain lock at all, only a
  scheduled row's own row lock, which the claim's `FOR UPDATE SKIP LOCKED`
  never waits for in the first place — `SKIP LOCKED` skips rather than
  waits, so the only way to force this exact interleaving is what this test
  already does).

## Increment 11 — uncertain admission commits

Full contract: `docs/UNIQUENESS-CONTRACT.md`, "Admission transaction" (the
`pog.TransactionQueryError` classification and `CommitUnknown`/
`reconcile_unique`). `install_syncrep_reply_trigger` (Increment 2) is
generalized here to take a table and a predicate (a trusted SQL boolean
expression referencing `NEW`), in place of its earlier hard-coded
`grind_job_acknowledgements`/`job_id` scoping — both existing Increment 2
call sites were updated to pass `"grind_job_acknowledgements"` and `"NEW.job_id
= " <> int.to_string(job_id)` explicitly, with no behavior change (confirmed
by the full gate staying green). The tests below scope it to
`grind_unique_submissions` by `submission_id` instead: the submission id is
chosen by the caller and known _before_ the admission transaction that would
create a job id even starts, which the acknowledgement path's job-id scoping
could not offer here (a `submit_unique` call's job id does not exist until
the same transaction whose reply might be lost has already run).

**Cleanup fix (found by independent review)**: `install_syncrep_reply_trigger`'s
returned cleanup thunk ran `SET lock_timeout = '2s'` and both `DROP`
statements as three separate queries against `connection` — a pool value,
not one physical connection. Each separate query on a pool value checks out
and releases a connection for that query alone, so the `SET` could land on a
different physical connection than the one either `DROP` later happens to
check out, leaving the `DROP`s unbounded again (able to hang indefinitely
behind an unrelated lock, rather than failing after 2 seconds as intended).
Fixed by running `SET LOCAL lock_timeout = '2s'` and both `DROP`s inside one
`pog.transaction` call, pinning all three statements to the same checked-out
connection, where `SET LOCAL` actually scopes. Backend termination (for any
backend still parked in `SyncRep`) still runs first and separately, since it
must reach the specific stuck backend regardless of which connection the
cleanup transaction itself uses.

### Claim (a): a pool closed _before_ `submit_unique` ever sends anything

- **Test**:
  `postgres_submit_unique_closed_pool_before_send_is_admission_failed_test`
  (marker `unique-closed-before-send-recovers-passed`).
- **Mechanism**: `run` (`src/grind/internal/unique_admission.gleam`) calls a
  dedicated FFI wrapper, `transaction_or_checkout_failure`
  (`grind_postgres_ffi.erl`, added by the fix below), which distinguishes a
  checkout failure — the pool could not hand out a connection at all, so
  `BEGIN` never ran — from pog's own transaction outcome. A checkout failure
  is reported directly as `AdmissionFailed(ConnectionUnavailable)`, with no
  `PendingSubmission` constructed and no receipt lookup attempted.
- **Observed**: `submit_keep_existing(...) == Error(AdmissionFailed(ConnectionUnavailable))`.
  Reopening the same pool name and retrying the identical `SubmissionId` — a
  plain `submit_unique`, not `reconcile_unique` (there is no
  `PendingSubmission` to reconcile from) — succeeds normally: `Inserted`,
  exactly one row (`count_jobs_in_queue` on the reopened pool's own
  connection).
- **This test's own precision is proven, not coincidental, by Claim (d)'s own
  evidence below**: applying the receipt-lookup fix (R1) _alone_, without
  the checkout/mid-transaction distinction (R2), makes this exact test go
  red — `AdmissionFailed` becomes `CommitUnknown` too, imprecisely, because
  a checkout failure and a genuinely uncertain mid-transaction loss were
  still indistinguishable. See "the bug, found and fixed" under Claim (d)
  for the quoted output; R2 is what restores this test's precise
  `AdmissionFailed`.

### Claim (b): an aborted commit — a deferred trigger's `pg_sleep` fires during the admission transaction's own `COMMIT`

- **Test**: `postgres_submit_unique_aborted_commit_is_commit_unknown_test`
  (marker `unique-aborted-commit-is-commit-unknown-passed`).
- **Mechanism**: the same `pg_sleep(30)`-in-a-deferred-constraint-trigger
  shape the pre-existing `postgres_ack_commit_connection_loss_is_unknown_test`
  uses for the acknowledgement path (a literal, un-generalized trigger —
  distinct from `install_syncrep_reply_trigger`, which raises
  `synchronous_commit` rather than sleeping), scoped here by `NEW.submission_id
= '<submission text>'` on `grind_unique_submissions` instead of a job id.
  `wait_for_commit_trigger_backend` (already table-agnostic — it polls
  `pg_stat_activity` for `wait_event = 'PgSleep'` alone) needed no change.
  Terminating the backend while it sleeps aborts the whole transaction
  before it is ever marked committed — unlike (c)/(d) below, nothing is
  visible to any other connection, not even briefly.
- **Trap avoided**: the trigger is scoped by `submission_id` equality, and
  the test's own _recovery_ retries the _same_ `SubmissionId` — so the
  trigger must be dropped **before** that retry, not only deferred to test
  teardown, or the retry's own commit would fire the identical trigger again
  and hang for the full 30 seconds with nobody left to terminate it. An
  earlier draft of this test deferred cleanup only, and the retry hung
  the whole gate's subsequent tests behind a leftover `pg_sleep(30)`
  transaction still holding a lock relevant to `grind_unique_submissions`
  DDL — confirmed by two _other_, unrelated tests failing with
  `Error(QueryTimeout)` on their own `CREATE CONSTRAINT TRIGGER` statements
  in the same run. Fixed by extracting the drop into a named closure, called
  once explicitly before the retry and again (idempotently) via
  `exception.defer` as a safety net.
- **Observed**: `process.receive(reply, ...) == Ok(Error(unique.CommitUnknown(pending)))`;
  zero rows in `grind_jobs` for the queue; zero receipts for the submission
  id. `reconcile_unique(database, pending)` still reports `CommitUnknown` —
  nothing was ever committed, so the receipt lookup finds nothing, and would
  keep finding nothing forever; `reconcile_unique` alone can never recover
  this. Only a plain retry of the identical `SubmissionId` (after dropping
  the trigger) converges: `Inserted`, exactly one row.
- **Limits**: this is a characterization test for the classification and the
  zero-rows/zero-receipts observation (passed on first write); the shared
  "receipt lookup is essential" mutation below (Increment 11 (d)) also
  affects this test's own final retry step, but inconclusively — see that
  entry's own note on collateral timing noise.

### Claim (c): a genuinely committed admission whose reply is lost after PostgreSQL has already committed locally

- **Test**: `postgres_submit_unique_committed_reply_lost_returns_inserted_test`
  (marker `unique-committed-reply-lost-inserted-passed`).
- **Mechanism**: `install_syncrep_reply_trigger` (generalized, see above),
  installed on `grind_unique_submissions` with predicate `NEW.submission_id
= '<submission text>'`, on the same disposable cluster settings
  (`synchronous_standby_names=grind_never_standby`,
  `synchronous_commit=local`) Increment 2 requires
  (`require_syncrep_cluster_configured`). `submit_unique` is spawned in the
  background (it blocks for the duration of the park);
  `wait_for_syncrep_trigger_backend` confirms the park, then
  `terminate_backend` ends it.
- **Observed**: `process.receive(reply, ...) == Ok(Ok(unique.Inserted(handle)))`
  — `submit_unique` itself resolves the outcome via `run`'s own automatic
  `reconcile_from_receipt` fallback on `pog.TransactionQueryError`, with no
  separate `reconcile_unique` call needed; `backend_pid_is_alive == False`;
  `postgres.arguments` reads back the original input through both the
  returned handle and a handle freshly rebound with `bind_handle`; exactly
  one row; the receipt exists.
- **Proven by mutation** — see Claim (d)'s mutation entries below, both of
  which also turn this test red.

### Claim (d): committed, reply lost, _and_ Grind's own pool closed while the commit is still parked

- **Test**:
  `postgres_submit_unique_committed_reply_lost_store_unavailable_test`
  (marker `unique-committed-reply-lost-store-unavailable-passed`).
- **This was a correctness bug, found by independent review of an earlier
  draft of this test's own "the classification just differs" writeup — not
  a classification difference to document and move on from.** Root cause,
  in `run` (`src/grind/internal/unique_admission.gleam`, pre-fix): the
  admission transaction's own "commit" call correctly unblocked with
  `pog.TransactionQueryError` (checked out fine, then lost the connection —
  genuinely uncertain, might have committed), and `run` correctly routed it
  to `reconcile_from_receipt` to check. But that follow-up `find_receipt`
  query then _also_ failed — the pool was now fully closed — and the
  pre-fix `Error(error) -> Error(error)` branch returned that _lookup's
  own_ connectivity failure, `AdmissionFailed(ConnectionUnavailable)`, as if
  it were the _admission's_ outcome, silently discarding the
  `pending: PendingSubmission` already in hand. A caller told
  `AdmissionFailed` reasonably treats that as "did not happen, safe to
  retry independently" — but the zombie transaction can still commit later,
  and nothing told the caller to check.
- **Mechanism**: `install_syncrep_reply_trigger` on `grind_unique_submissions`
  (scoped by `submission_id`), installed via a size-1 _observer_ pool
  independent of Grind's own pool (the same shape
  `postgres_ack_committed_reply_lost_with_store_unavailable_is_unknown_test`
  uses); `submit_unique` spawned on Grind's own pool; `postgres.close` called
  on Grind's own pool once the observer confirms the park.
- **Red before the fix**: the test was first changed to assert the correct
  contract (`Ok(Error(unique.CommitUnknown(pending)))`) against the
  then-unmodified code. Against a fresh disposable cluster (116 passed, 1
  failure):
  ```
  test: grind_test.postgres_submit_unique_committed_reply_lost_store_unavailable_test
  code: let assert Ok(Error(unique.CommitUnknown(pending))) =
      process.receive(reply, within: 10_000)
  value: Ok(Error(AdmissionFailed(ConnectionUnavailable)))
  info: Pattern match failed, no pattern matched the value.
  ```
- **The fix, in two parts**:
  1. **(R1)** `reconcile_from_receipt` now maps a failed lookup to
     `Error(unique.CommitUnknown(pending))`, the same as finding no receipt
     yet (`Ok(None) | Error(_) -> Error(unique.CommitUnknown(pending))`) —
     mirroring `reconcile_unknown_ack`'s `Ok(None) | Error(_) ->
QueueAckUnknown` in `grind/postgres`. Applied alone, this makes the test
     above pass — but at a cost, checked directly: it also makes Claim (a)'s
     test go red, because a checkout failure (definitely not committed) and
     a mid-transaction connection loss (genuinely uncertain) were still
     indistinguishable at this point — both reached
     `reconcile_from_receipt` the same way. Against a fresh disposable
     cluster with R1 alone (116 passed, 1 failure):
     ```
     test: grind_test.postgres_submit_unique_closed_pool_before_send_is_admission_failed_test
     info:
     Error(CommitUnknown(PendingSubmission(...)))
     should equal
     Error(AdmissionFailed(ConnectionUnavailable))
     ```
  2. **(R2)** `run` now calls a new FFI wrapper, `transaction_or_checkout_failure`
     (`grind_postgres_ffi.erl`), added as a sibling to the existing
     `transaction_safely` (kept for other callers — see "Backlog" below).
     Unlike `transaction_safely`'s `try ... catch exit:{_, {pgo_pool,
checkout, _}} -> {error, {transaction_query_error, connection_unavailable}}`
     (which disguises a checkout failure as the same
     `transaction_query_error` shape a genuine mid-transaction loss
     produces), the new wrapper returns a distinct `{error, nil}` for that
     exact same catch, keeping `{ok, Result}` (pog's own transaction outcome,
     untouched) for everything else. `run` maps `Error(Nil)` straight to
     `Error(unique.AdmissionFailed(pog.ConnectionUnavailable))` — no
     `PendingSubmission`, no receipt lookup attempted — restoring Claim (a)'s
     precise classification on top of R1's fix.
     Reapplying both together: **117 passed, no failures** (reproduced three
     consecutive times against fresh disposable clusters).
- **Mutation (regression check): revert R1 alone, keep R2** — restored
  `reconcile_from_receipt`'s pre-fix `Error(error) -> Error(error)` branch
  while keeping the R2 FFI wrapper in `run`. Against a fresh disposable
  cluster (116 passed, 1 failure) — the exact original bug symptom recurs,
  and only this one test is affected (Claim (a) stays green, confirming R2
  alone does not fix (d); R1 is specifically what (d) needs):
  ```
  test: grind_test.postgres_submit_unique_committed_reply_lost_store_unavailable_test
  code: let assert Ok(Error(unique.CommitUnknown(pending))) =
      process.receive(reply, within: 10_000)
  value: Ok(Error(AdmissionFailed(ConnectionUnavailable)))
  info: Pattern match failed, no pattern matched the value.
  ```
  Reverted immediately; `gleam build --warnings-as-errors` recompiled clean
  and `git diff` showed no trace of the mutated lines.
- **Observed, with the fix**: `submit_unique` reports
  `Ok(Error(unique.CommitUnknown(pending)))`. `reconcile_unique(reopened,
pending)`, tried _while the zombie is still parked_, is a pure receipt
  lookup with no lock of its own — the zombie's receipt insert is not yet
  visible to any other session, so it still reports `CommitUnknown` (not a
  persisted-conflict inference). A second, independent recovery path — a
  _plain_ `submit_unique` retry of the same `SubmissionId`, tried while the
  zombie is still parked (via a third pool with `unique_lock_wait(200)`) —
  genuinely needs the domain lock the zombie's still-open transaction holds:
  `Error(AdmissionContended)`, and `count_jobs_in_queue` on the observer
  connection reads `0`. Only after the zombie backend is terminated and
  confirmed gone (`wait_for_backend_gone` on the _observer_ connection —
  Grind's own closed-then-reopened socket is not the ordering signal here,
  unlike Increment 2's 2a; an independent connection is what later reads
  visibility, matching Increment 2's 2b discipline) does
  `reconcile_unique(reopened, pending)` resolve from the now-visible
  receipt: `Ok(Inserted(handle))` with the original job id, exactly one row
  — and the plain-retry path, tried again on the reopened pool, converges on
  that same job id, still one row.
- **Proven by mutation (isolating each half of the recovery flow)**:
  1. **Skip `admission_transaction`'s own receipt lookup entirely** — the
     same removal Increment 7 already proved necessary for sequential
     replay (`admission_transaction` called `admit_candidate` unconditionally
     instead of calling `find_receipt` at all first,
     `src/grind/internal/unique_admission.gleam` — note this does **not**
     touch `reconcile_from_receipt`/`reconcile_unique`, a separate function).
     Against a fresh disposable cluster (113 passed, 4 failures): the two
     pre-existing Increment 7 receipt-replay failures and the pre-existing
     Increment 8 receipt-ordering failure recur exactly as documented there,
     plus:
     ```
     test: grind_test.postgres_submit_unique_committed_reply_lost_store_unavailable_test
     code: let assert Ok(unique.Inserted(retried_handle)) =
         submit_keep_existing(reopened, test_queue, submission_text, worker_def, 13, policy)
     value: Error(SubmissionConflict)
     info: Pattern match failed, no pattern matched the value.
     ```
     `reconcile_unique(reopened, pending)` itself is unaffected by this
     mutation (it calls `reconcile_from_receipt` directly, never
     `admission_transaction`) and still correctly resolves `Inserted` with
     the original job id — this mutation's failure is specifically on the
     _second_ recovery path, the plain `submit_unique` retry: once the
     zombie's row and receipt become visible, that retry's candidate
     selection (run unconditionally, receipt check skipped) finds the row as
     an ordinary conflict and attempts to record its own receipt for the
     same `(storage_owner, submission_id)`, colliding with the zombie's
     already-committed one — `23505` on the receipt table's primary key, the
     exact different-key-shaped symptom Increment 8 documents for a
     structurally similar mutation, here forced onto the identical key by
     genuine replay rather than by a code defect elsewhere. This proves the
     plain-retry recovery path also depends on `admission_transaction`'s own
     receipt lookup, not on candidate selection reinterpreting the
     now-visible row as a fresh conflict.
     Reverted immediately; `gleam build --warnings-as-errors` recompiled
     clean and `git diff` showed no trace of the mutated lines.
  2. **Skip `run`'s own automatic-reconciliation fallback** — replaced
     `Ok(Error(pog.TransactionQueryError(_))) -> reconcile_from_receipt(...)`
     with an unconditional `Error(unique.CommitUnknown(unique.new_pending_submission(...)))`
     in `run` (`src/grind/internal/unique_admission.gleam`), i.e. never even
     attempt the follow-up receipt lookup for a mid-transaction loss. Against
     a fresh disposable cluster (115 passed, 2 failures):
     ```
     test: grind_test.postgres_submit_unique_committed_reply_lost_returns_inserted_test
     code: let assert Ok(Ok(unique.Inserted(handle))) =
         process.receive(reply, within: 10_000)
     value: Ok(Error(CommitUnknown(PendingSubmission(...))))
     info: Pattern match failed, no pattern matched the value.

     test: grind_test.postgres_submit_unique_reschedule_reply_lost_returns_rescheduled_test
     code: let assert Ok(Ok(unique.Rescheduled(conflict))) =
         process.receive(reply, within: 10_000)
     value: Ok(Error(CommitUnknown(PendingSubmission(...))))
     info: Pattern match failed, no pattern matched the value.
     ```
     Claims (c) and (e) — the two cases whose admission genuinely committed
     and needs the lookup to discover that — turn red. Claim (a) is
     unaffected (it never reaches this branch at all: `Error(Nil)` short-
     circuits earlier). Claim (d) is also unaffected, but not because it is
     insensitive to this code path — its first assertion
     (`Ok(Error(unique.CommitUnknown(pending)))`) is satisfied by this
     mutation too, since skipping the lookup and finding no receipt both
     produce the identical `CommitUnknown(pending)` value with identical
     contents, and its later `reconcile_unique` calls go through the
     separate, unmutated `reconcile_from_receipt` function directly — this
     mutation is a coincidental false negative for claim (d) specifically,
     not evidence that (d) is correct independent of this code path (Claim
     (d)'s own dedicated red-before-fix and R1-revert evidence above already
     covers it directly). Only these two tests failed. Reverted immediately;
     `gleam build --warnings-as-errors` recompiled clean and `git diff`
     showed no trace of the mutated lines.
- **Backlog**: `postgres.gleam`'s other `transaction_safely` callers
  (acknowledgement, audited resolution) were left unchanged — they still
  conservatively report their own "unknown" outcome for a checkout failure
  too (safe, only less precise than R2's distinction), same as `run` did
  before this fix. Noted in `docs/IMPLEMENTATION-SCOPE.md` as backlog, not
  blocking: those paths do not retain an analogous typed "pending" value a
  caller could otherwise reconcile from more precisely, so there is no
  equivalent information being discarded today.
- **Limits**: this proves the checkout-vs-mid-transaction distinction and
  the two-path recovery for this exact fault sequence; it does not audit
  every other `transaction_safely` call site in `grind/postgres` for the
  same class of bug (a failed follow-up lookup discarding a retained typed
  value) — none of those paths currently retain one, so the same bug shape
  cannot occur there today, but this was not independently re-verified
  against each call site line by line.

### Claim (e): a reschedule whose commit reply is lost

- **Test**:
  `postgres_submit_unique_reschedule_reply_lost_returns_rescheduled_test`
  (marker `unique-reschedule-reply-lost-rescheduled-passed`).
- **Mechanism**: identical to (c), except the submission under the SyncRep
  trigger is a `RescheduleScheduledTo` request against a pre-existing
  scheduled row (inserted normally beforehand, no fault injection needed for
  that half).
- **Observed**: `process.receive(reply, ...) == Ok(Ok(unique.Rescheduled(conflict)))`,
  not `Existing` — even though the row's current state (`scheduled`, at its
  new `available_at`) looks exactly like an ordinary scheduled conflict
  either way; `conflict_job_id` matches the original job id;
  `job_available_at_ms` reads the new target exactly; exactly one row. The
  receipt's own recorded _decision_ column, not the row's current state, is
  what `find_receipt`/`outcome_of_receipt` decodes.
- **Proven by mutation**: both mutations in Claim (d) above also turn this
  test red (see their quoted output); no separate mutation was needed.

## Increment 12 — uniqueness: selected keys

Full contract: `docs/UNIQUENESS-CONTRACT.md`, "Status" (Increment 12) and
Decision 9's key-contract discussion. `unique.selected` was already
implemented (`src/grind/unique.gleam`'s `Selected` key variant and
`key_material`); this increment adds the tests proving its isolation and
equality semantics, so both tests below are characterization tests proven by
mutation rather than red-before-green.

### Claim: a selected key's contract isolates by name and by codec version, independently of the projected value; a full-input key never collides with a selected key

- **Test**: `postgres_submit_unique_selected_key_scoping_test`
  (`test/grind_test.gleam`; marker `unique-selected-key-scoping-passed`).
- **Mechanism**: an input type (`SelectedInput`) with one field the key
  projects (`account`, itself a raw JSON value) and one field it never sees
  (`other`). Five submissions against the same worker and queue: (1) admits
  under a selected key named `"account"`; (2) the identical projected value
  with a different `other` field, same policy — still `Existing`; (3) the
  identical projected value under a selected key with a different _name_,
  same projection and codec — `Inserted`; (4) the identical projected value
  under the same name and projection but a different codec _version_ --
  `Inserted`; (5) the identical whole input under a `full_input()` policy
  instead of any selected key — `Inserted`, never colliding with (1)-(4).
- **Proven by mutation**: in `key_material`
  (`src/grind/unique.gleam`), dropped the key name from the `Selected`
  branch's contract string (`#("selected:" <> name <> ":" <> codec_version,
...)` → `#("selected:" <> codec_version, ...)`, name unused). Against a
  real disposable cluster:
  ```
  let assert  test/grind_test.gleam:11466
   test: grind_test.postgres_submit_unique_selected_key_scoping_test
   code: let assert Ok(unique.Inserted(_)) =
      submit_keep_existing(
        database,
        test_queue,
        "unique-selected-3-" <> suffix,
        worker_def,
        shared_account,
        account_policy_other_name,
      )
  value: Ok(Existing(Conflict(122, "127.0.0.1:26420/grind_test", "selected-...", "unique.selected-...", "v1", Queued)))
  info: Pattern match failed, no pattern matched the value.
  118 passed, 1 failures
  ```
  With the name dropped from the contract, submission (3) (a differently-named
  selected key, same codec version) collapses onto submission (1)'s contract
  and falsely conflicts with its row. Only this test failed. Reverted
  immediately; `gleam check` recompiled clean and `git diff` showed no trace
  of the mutated line.
- **Limits**: no separate mutation was run for dropping the codec version
  instead of the name — the contract string concatenates both the same way,
  so the two omissions are structurally identical bugs (a missing
  disambiguating segment lets two distinct contracts collapse onto the same
  string); a single representative mutation was judged sufficient, matching
  the discipline already applied to Decision 1's key-digest and Decision
  3's worker-identity isolation claims elsewhere in this contract.

### Claim: a selected key compares by exact equality, not containment, for a projected value the same way a full-input key already does

- **Test**: `postgres_submit_unique_selected_key_equality_not_containment_test`
  (marker `unique-selected-key-equality-not-containment-passed`).
- **Mechanism**: two submissions under the same selected key (`"account"`,
  projecting a raw JSON value): one with `{"id":1}`, the next with
  `{"id":1,"extra":2}` — a JSON superset of the first. Both admit as
  `Inserted`; neither conflicts with the other.
- **Characterization test, proven by mutation** (passed on first write, so
  red-before-green does not apply; this exercises the same underlying
  `unique_key_sha256` equality machinery already proven at the digest level
  by `postgres_submit_unique_json_equality_matches_postgres_jsonb_test`'s
  subset/superset case, applied here specifically through `unique.selected`
  rather than `unique.full_input()`). No separate mutation was run beyond
  that existing coverage: the SQL predicate compared
  (`unique_key_sha256 = sha256(...)`) is identical regardless of which
  `Key` variant produced the encoded key text, and Decision 1's own
  digest-equality proof already covers that predicate directly.
- **Limits**: this proves selected-key equality behaves the same as
  full-input equality for one representative subset/superset pair; it does
  not re-run every JSON-equality case from
  `postgres_submit_unique_json_equality_matches_postgres_jsonb_test` (field
  order, numeric scale, array order) through a selected key, since those all
  reduce to the same `unique_key_sha256` comparison already proven
  independent of which `Key` variant produced the compared text.

## Increment 13 — public-API consumer coverage: uniqueness admission

All claims below are exercised from `consumer/`, the separate package that
imports only public Grind modules (`grind/unique`, `grind/postgres`,
`grind/job`, `grind/worker`, `grind/queue`, `grind/registry`), following the
same discipline as Increment 5. Nothing here reads `@internal` functions or
raw `pog`; the only addition to the consumer package itself is a dev
dependency on `gleam_time` (already transitively resolved through `pog`), so
a test can build a `job.AvailableAt` from a real wall-clock read without a
bespoke Erlang FFI.

### Claim: `submit_unique`, an `Existing` conflict rebound with `bind_handle`, and a `SubmissionId` replay are all reachable and correct through public imports alone

- **Test**: `public_consumer_unique_admission_existing_conflict_and_retry_test`
  (`consumer/test/grind_consumer_test.gleam`; marker
  `consumer-unique-admission-existing-conflict-retry-passed`).
- **Mechanism**: a plain `Int`/`String` echo worker is admitted once
  (`unique.Inserted`); a second, independently identified submission with the
  identical key observes `unique.Existing`, whose `conflict_job_id` matches
  the first handle's id. The conflict is rebound with the same
  `postgres.bind_handle` path used after a restart, and its state read as
  `Queued`. Replaying the _first_ submission's own `SubmissionId` a third
  time returns `unique.Inserted` again with the identical job id — the
  receipt's own recorded decision, not a fresh conflict against the
  still-present row. A manually driven consumer (`queue.start_manual`,
  `queue.process_one`) then actually runs the job; the rebound handle's
  typed outcome reads `SucceededWith("42")`.
- **Characterization test, proven by mutation** (passed on first write).
  In `admission_transaction`
  (`src/grind/internal/unique_admission.gleam`), replaced the `case existing
{ Some(outcome) -> Ok(outcome) None -> admit_candidate(...) }` dispatch
  with an unconditional `admit_candidate(connection, request)` — the same
  "drop the receipt lookup" mutation already recorded for sequential replay
  in `docs/UNIQUENESS-CONTRACT.md`'s Increment 8, applied here specifically
  to observe this consumer-level test. The consumer suite was run in
  isolation against its own fresh disposable cluster (`cd consumer && gleam
test`, its own `grind_consumer_test` database):
  ```
  let assert  test/grind_consumer_test.gleam:785
   test: grind_consumer_test.public_consumer_unique_admission_existing_conflict_and_retry_test
   code: let assert Ok(unique.Inserted(replayed)) =
      postgres.submit_unique(
        database,
        test_queue,
        first_submission,
        worker_def,
        42,
        unique.Immediately,
        policy,
        unique.KeepExisting,
      )
  value: Error(SubmissionConflict)
  info: Pattern match failed, no pattern matched the value.
  6 passed, 1 failures
  ```
  With the receipt lookup skipped, the replay of `first_submission` runs
  candidate selection directly, finds the still-`queued` row as an ordinary
  conflict, and attempts to record a _second_ receipt for the same
  `(storage_owner, first_submission)` primary key — colliding with the one
  the original admission already committed (`23505`, mapped to
  `SubmissionConflict` by `record_receipt`'s own constraint handling) —
  exactly the different-shaped symptom Increment 8 documents for the same
  mutation at the root level, here forced onto the identical key by genuine
  replay. Only this test failed. Reverted immediately; `gleam check`
  recompiled clean and `git diff` showed no trace of the mutated lines.
- **Limits**: this is a consumer-level demonstration of already-proven root
  mechanisms (existing-conflict detection: Increment 6/7's identity and
  json-equality tests; receipt replay: Increment 7); it adds no new claim
  about the admission transaction's own SQL, only that the full flow —
  admit, detect conflict, rebind, replay, run, read typed outcome — is
  reachable and correct entirely through public imports.

### Claim: an `AcrossQueues` policy's `RescheduleScheduledTo` moves a genuinely scheduled row's `available_at` from a submission made through a different queue, and the row is claimable and runs once due

- **Test**: `public_consumer_unique_reschedule_across_queues_test`; marker
  `consumer-unique-reschedule-across-queues-passed`.
- **Mechanism**: a job is seeded with `unique.At` an hour in the future
  (genuinely `Scheduled`, not already due) in queue `"consumer-unique-across-a"`.
  A second submission, through the _different_ queue
  `"consumer-unique-across-b"`, under an `AcrossQueues`/`ScheduledOnly`
  policy and `RescheduleScheduledTo` targeting 50ms in the future, observes
  `unique.Rescheduled`; `conflict_job_id` matches the original handle, and
  `conflict_queue` reports `"consumer-unique-across-a"` — the row's actual
  queue, not the rescheduling submission's own. The rebound handle is then
  actually claimed and run by a manually driven consumer once its new time
  is due (bounded polling, the same `await_claim` helper Increment 5 already
  uses for a real retry delay), and its typed outcome reads
  `SucceededWith("7")`.
- **Confirmatory mutation, reusing a production mutation already proven at
  the root level** (the same discipline as Increment 5's Mutation 2): in
  `candidate_sql` and `bind_candidate_params`
  (`src/grind/internal/unique_admission.gleam`), changed both `case scope`
  matches so `AcrossQueues` takes the `WithinQueue` branch too (always
  filtering candidate selection by `queue = $q`) — the identical mutation
  Increment 4's own root-level `AcrossQueues` proof already uses. Reproduced
  in two ways against a real disposable cluster:
  1. The full gate (`scripts/test-postgres.sh`): the root suite itself goes
     red first (117 passed, 2 failures —
     `postgres_submit_unique_respects_queue_scope_test` and
     `postgres_submit_unique_concurrent_admission_mixed_scope_test`, the
     same failures Increment 4/8's own evidence already documents), so the
     script's `set -euo pipefail` aborts before the consumer suite ever
     runs — expected, and not itself evidence for this claim.
  2. To observe the consumer-level failure directly, the consumer suite was
     run in isolation against its own disposable cluster (same mutation,
     `cd consumer && gleam test` against a freshly created
     `grind_consumer_test` database):
     ```
     let assert  test/grind_consumer_test.gleam:863
      test: grind_consumer_test.public_consumer_unique_reschedule_across_queues_test
      code: let assert Ok(unique.Rescheduled(conflict)) =
          postgres.submit_unique(
            database,
            "consumer-unique-across-b",
            reschedule_submission,
            worker_def,
            7,
            unique.Immediately,
            policy,
            unique.RescheduleScheduledTo(soon_at),
          )
     value: Ok(Inserted(JobHandle(11, "127.0.0.1:.../grind_consumer_test", "consumer-unique-across-b", "consumer.unique_reschedule_echo", "v1", ...)))
     info: Pattern match failed, no pattern matched the value.
     6 passed, 1 failures
     ```
     With every candidate query forced to filter by the submitting queue,
     the rescheduling submission (queue `"consumer-unique-across-b"`) never
     finds the seeded row (queue `"consumer-unique-across-a"`) as a
     candidate at all, and inserts a second row instead of rescheduling the
     first. Only this test failed. Reverted immediately; `gleam check`
     recompiled clean and `git diff` showed no trace of the mutated lines.
- **Limits**: this is a consumer-level demonstration of an already-proven
  root mechanism (`AcrossQueues` candidate selection: Increment 4;
  `RescheduleScheduledTo`: Increment 10); it adds no new claim about the
  admission transaction's own SQL, only that the full flow — seed a
  scheduled row, reschedule it across queues, observe the row's real queue
  on the conflict, and run the rescheduled job to a typed outcome — is
  reachable and correct entirely through public imports.

## Acknowledged observation — `[grind, job, acknowledged]` (Round 1)

Grind's first Sinal event descriptor (`grind/observation.acknowledged()`),
wired per the design decision that Grind owns no telemetry event sum type:
Grind depends on Sinal directly (path dependency, the same pattern as
`saga`/`relay`/`llm_wire`), starts one `sinal/forwarder.Forwarder` per
`Database` as a sibling child of its existing pool supervisor, and emits
through that forwarder — never a plain `sinal.emit` — only after
`grind/postgres.acknowledge`'s commit is proven. All tests below run against
a real disposable PostgreSQL cluster (`scripts/test-postgres.sh`); markers
are listed in the script.

### Claim: `InvalidObservationCapacity` is rejected before any process starts

- **Test**: `postgres_settings_reject_non_positive_observation_capacity_test`
  (pure — no database, no marker).
- **Observed**: `postgres.observation_capacity(settings, 0)` and `(-1)` both
  fail `validate` with `Error(postgres.InvalidObservationCapacity)`, checked
  ahead of `pog.url_config` in `validate`'s own multi-subject `case`, so an
  invalid capacity never reaches pool or forwarder construction.

### Claim: the observation is emitted strictly after the commit — reading `postgres.state` from inside the attached handler already observes the committed state

- **Test**: `postgres_acknowledged_observation_commit_ordering_test`
  (marker `acknowledged-observation-commit-ordering-passed`).
- **Mechanism**: a plain `sinal.observe` handler on `observation.acknowledged()`
  calls `postgres.state(database, handle)` from inside its own callback (which
  runs in the forwarder process, per Sinal's documented hand-off) and reports
  the read back to the test. A successful job is run through
  `queue.process_one`.
- **Observed**: the in-handler read is `Ok(job.Succeeded)`; the decoded
  metadata's `committed_state` is also `job.Succeeded`, `proposed` is
  `ProposedSuccess`, `confirmation` is `Replied`, `available_at_unix_ms` is
  `None`, and `command_id` equals
  `postgres.acknowledgement_command_id(job_id, attempt_id, epoch)` for the
  attempt actually stored. Exactly one event arrives.
- **Proven by mutation — actually run, both forms recorded** (a coordinator
  review caught the first attempt at this evidence as unrun/asserted rather
  than observed, and a real second attempt found the naive form does not
  reliably catch the bug): temporarily moved the `emit_acknowledged` call in
  `acknowledge` to run before `transaction_safely` (ahead of the commit),
  building its `AckCommit` from the proposal instead of a real commit.
  - **Naive form (no added delay)**: against a fresh disposable cluster
    (`gleam test`, `128 passed, 8 failures`), this test was _not_ among the
    failures. Reading the reason: `forwarder.emit`'s hand-off is a fire-and-
    forget async send, and the actual `transaction_safely` call — a local,
    synchronous PostgreSQL round trip — reliably completes before the BEAM
    scheduler gets around to running the forwarder's handler in practice.
    Moving the _call site_ earlier does not reliably move the _observed
    read_ earlier: this specific test, as written, cannot deterministically
    distinguish "emits before the transaction is issued" from "emits after",
    only "emits so much earlier that the handler's read loses the race to a
    synchronous local commit" — which the naive mutation does not achieve.
  - **Forced form (delay added to widen the race window)**: the same
    mutation, plus a `process.sleep(50)` immediately after the (mutated)
    early emit call and before `transaction_safely` runs — a deliberate
    synchronization delay whose only purpose is to force the already-real
    race open wide enough to observe, the standard technique for making an
    order-dependent bug reproducible rather than scheduler-luck-dependent.
    Against a fresh disposable cluster (`gleam test`, `127 passed, 9
failures`), this test is among the failures:
    ```
    panic src/gleeunit/should.gleam:10
     test: grind_test.postgres_acknowledged_observation_commit_ordering_test
     info:
    Ok(Executing)
    should equal
    Ok(Succeeded)
    ```
  - **What this does and does not prove**: it confirms the code path _can_
    observably violate the ordering claim if emission is moved early enough,
    and that this specific test's assertion is what would catch it. It does
    not claim the naive, undelayed mutation is itself caught — that claim
    (present in an earlier draft of this section) was false and has been
    removed, along with the unrelated claim that the "emit on Error path"
    mutation (below) also fails this ordering test; that mutation's own
    negative-path tests are what actually catch it, not this one.
  - Reverted immediately in both cases; `gleam check` recompiled clean and
    `git diff` showed no trace of the mutated lines.

### Claim: isolation — a gate-blocked handler for one job's observation does not stall a second job's lease renewal or completion under the same coordinator

- **Test**: `postgres_acknowledged_observation_isolation_test` (marker
  `acknowledged-observation-isolation-passed`).
- **Mechanism**: two jobs (A, B) run under one manually driven consumer,
  `maximum_concurrency: 2`, `lease_duration_ms: 300` (renewal every
  `lease_duration_ms / 3` ≈ 100ms). A `sinal.observe` handler on
  `observation.acknowledged()` blocks only for job A's event, on a gate
  created _inside_ the handler (so it is owned by the forwarder process that
  will `process.receive` it — a `process.Subject` created in the test process
  cannot be received on from a different process; this exact bug was hit and
  fixed while writing this test, see "Test-harness bug" below) and handed
  back to the test over a signal subject. A completes and acks while B is
  still executing; with A's own observation now gate-blocked in the
  forwarder, the test polls `queue.renewal_status` for
  `LeaseRenewalConfirmed` (proving at least one renewal tick reached the
  database while A's gate stayed shut), then releases B and asserts
  `postgres.state(database, handle_b) == Ok(job.Succeeded)` — all before
  releasing A's gate.
- **Observed**: B's renewal confirms and B succeeds while A's gate remains
  shut; A's gate is only released afterward, in cleanup.
- **Proven by mutation — "call `sinal.emit` directly in `acknowledge`"**,
  re-run against a fresh disposable cluster with a recorded clean baseline
  (see "Mutation evidence: clean baselines and deltas" below for the full
  table): temporarily replaced the `forwarder.emit` call in
  `emit_acknowledged` with a direct `sinal.emit` call (bypassing the
  forwarder entirely — the emission now runs synchronously in the
  coordinator, exactly the regression this test exists to catch). Clean
  baseline `136 passed, 0 failures`; mutated `133 passed, 3 failures` — this
  test, `postgres_acknowledged_observation_overflow_reports_dropped_test`,
  and `postgres_forwarder_crash_loop_does_not_stop_the_pool_test` (all three
  depend on genuine forwarder dispatch/capacity semantics), and no others.
  This test's own failure: `Error(Nil) should equal Ok(Ok(True))` (B's reply
  never arrives — the coordinator is now synchronously blocked running A's
  gate-blocked handler itself). Reverted immediately; `gleam check`
  recompiled clean and `git diff` showed no trace of the mutated lines.

### Claim: negative paths — a rolled-back, aborted, stale, or commit-unknown acknowledgement emits nothing

- **Tests** (deterministic sentinel pattern, not a fixed wall-clock wait —
  see "Sentinel pattern, not `within: 500`" below for why: a `sinal.observe`
  handler forwards every received event to a test subject; after the
  negative path, a distinct sentinel job is submitted and acknowledged
  through the _same_ consumer/producer, registered via a second, trivial,
  instantly-completing worker (`register_sentinel_worker`) so driving it to
  completion can never itself block on the original worker's own gate; the
  assertion is that the very next event received carries the sentinel's
  `job_id`, never the original job's):
  - `postgres_acknowledged_observation_absent_on_commit_unknown_test` (marker
    `acknowledged-observation-absent-on-commit-unknown-passed`) — the same
    "kill the backend mid-`pg_sleep` deferred trigger during `COMMIT`"
    technique as `postgres_ack_commit_connection_loss_is_unknown_test`:
    nothing is durably committed, `acknowledge_claim` reports
    `QueueAckUnknown`, `postgres.state` stays `Ok(job.Executing)`.
  - `postgres_acknowledged_observation_absent_on_stale_ack_test` (marker
    `acknowledged-observation-absent-on-stale-ack-passed`) — the same forced
    lease-expiry technique as
    `postgres_ack_after_database_expiry_is_stale_without_receipt_test`: the
    fenced `UPDATE` affects zero rows, `acknowledge_claim` reports
    `QueueAckStale`.
- **Note on scope**: in this codebase "rollback" and "stale" are the same
  mechanism (any `Error(..)` returned from inside `acknowledge_transaction`
  rolls the whole ack transaction back via `pog`'s own transaction wrapper),
  and "abort" and "unknown" are likewise the same mechanism (a connection
  lost during `COMMIT` is unconditionally reported `QueueAckUnknown`,
  whether or not the transaction actually reached commit) — two negative
  tests cover the four named categories from the plan, not four
  independently distinct code paths.
- **Proven by mutation — "emit on an Error path"**, re-run against a fresh
  disposable cluster with a recorded clean baseline: temporarily made
  `acknowledge`'s final `case` also call `emit_acknowledged` (with a
  synthesized fake `AckCommit`) on the `Error(error) -> Error(error)` branch
  before re-raising the same error. Clean baseline `136 passed, 0 failures`;
  mutated `134 passed, 2 failures` — exactly
  `postgres_acknowledged_observation_absent_on_commit_unknown_test` and
  `postgres_acknowledged_observation_absent_on_stale_ack_test`, and no
  others. Both failures show the sentinel assertion catching a genuine extra
  event precisely, by job id, e.g.:
  ```
  test: grind_test.postgres_acknowledged_observation_absent_on_commit_unknown_test
  info:
  75
  should equal
  76
  ```
  (`75` is the original job's own spurious mutated event, arriving ahead of
  sentinel job `76` — exactly what the FIFO-ordered sentinel check is
  designed to catch). Reverted immediately; `gleam check` recompiled clean
  and `git diff` showed no trace of the mutated lines.

### Claim: lost reply (`SyncRep` harness) — exactly one event, and its `confirmation` is `Reconciled`, never `Replied`

- **Test**:
  `postgres_acknowledged_observation_reconciled_after_lost_reply_test`
  (marker `acknowledged-observation-reconciled-after-lost-reply-passed`).
- **Mechanism**: the same `SyncRep`-park-then-terminate technique as
  `postgres_ack_committed_reply_lost_reconciles_from_receipt_test` (Increment
  2 above): the ack genuinely commits, but this call's own connection is
  severed while `COMMIT` is parked in `SyncRep`, so `acknowledge` only learns
  the outcome via `reconcile_unknown_ack` reading the receipt back.
- **Observed**: exactly one `[grind, job, acknowledged]` event arrives for
  this command, checked deterministically (a sentinel job's own
  acknowledgement, run through the same consumer afterward, must be the very
  next event — see "Sentinel pattern, not `within: 500`" below); `confirmation`
  is `Reconciled`; `committed_state` is `job.Succeeded`; `command_id` matches
  the attempt actually stored.
- **Proven by mutation — "label `Replied` always"**, re-run against a fresh
  disposable cluster with a recorded clean baseline: temporarily replaced
  `emit_acknowledged`'s `case commit.via_receipt_match { True -> Reconciled;
False -> Replied }` with a hard-coded `Replied`. Clean baseline `136
passed, 0 failures`; mutated `134 passed, 2 failures` — exactly this test
  and `postgres_acknowledged_observation_reconciled_on_sequential_duplicate_ack_test`
  (the round 2 addition proving the other receipt-match site, below), and no
  others:
  ```
  panic src/gleeunit/should.gleam:10
   test: grind_test.postgres_acknowledged_observation_reconciled_after_lost_reply_test
   info:
  Replied
  should equal
  Reconciled
  ```
  Reverted immediately; `gleam check` recompiled clean and `git diff` showed
  no trace of the mutated line.

### Claim: proposed vs. committed — cancel-while-running emits a `proposed: ProposedSuccess` / `committed_state: Cancelled` event, `committed_state` always taken from the commit, never re-derived from the proposal

- **Test**:
  `postgres_acknowledged_observation_committed_state_overrides_proposal_test`
  (marker
  `acknowledged-observation-committed-state-overrides-proposal-passed`).
- **Mechanism**: the same technique as
  `postgres_cancel_running_worker_overrides_proposal_on_ack_test`: a worker
  is mid-execution when `postgres.cancel` requests cancellation
  (`CancellationRequested`); the worker then completes with a proposed
  success, but the fenced ack's own `CASE WHEN cancel_requested_at IS NOT
NULL` commits `Cancelled` instead.
- **Observed**: the single received event has `proposed ==
observation.ProposedSuccess`, `committed_state == job.Cancelled`,
  `confirmation == observation.Replied`; checked deterministically, a
  sentinel job's own acknowledgement run afterward must be the very next
  event.
- **Proven by mutation — "take committed from the proposal"**, re-run
  against a fresh disposable cluster with a recorded clean baseline:
  temporarily replaced the metadata's `committed_state:` field with a value
  derived purely from `proposed_of_execution(execution)` (ignoring the
  actual `AckCommit`). Clean baseline `136 passed, 0 failures`; mutated `134
passed, 2 failures` — exactly this test and
  `postgres_acknowledged_observation_available_at_none_when_cancel_overrides_retry_test`
  (the round 2 addition below, a proposed-retry variant of the same
  override), and no others:
  ```
  panic src/gleeunit/should.gleam:10
   test: grind_test.postgres_acknowledged_observation_committed_state_overrides_proposal_test
   info:
  Succeeded
  should equal
  Cancelled

   test: grind_test.postgres_acknowledged_observation_available_at_none_when_cancel_overrides_retry_test
   info:
  Retryable
  should equal
  Cancelled
  ```
  Reverted immediately; `gleam check` recompiled clean and `git diff` showed
  no trace of the mutated lines.

### Claim: overflow — capacity 1 with a blocked handler reports `[sinal, forwarder, dropped]`, job states unchanged

- **Test**: `postgres_acknowledged_observation_overflow_reports_dropped_test`
  (marker `acknowledged-observation-overflow-reports-dropped-passed`).
- **Mechanism**: `postgres.observation_capacity(settings, 1)`. Job A's
  acknowledged handler holds the forwarder's only in-flight slot on a gate
  (again created inside the handler, per the isolation test's fix). Job B's
  own acknowledgement still commits normally through `queue.process_one`,
  but its forwarded observation exceeds capacity while A's slot is held and
  is dropped.
- **Observed**: `[sinal, forwarder, dropped]` reports `Dropped(rejected: 1,
lost: 0)`; both `postgres.state(database, handle_a)` and
  `postgres.state(database, handle_b)` are `Ok(job.Succeeded)` — the
  forwarder's own capacity accounting never touches either job's committed
  outcome. This exercises the exact mechanism `sinal/forwarder`'s own test
  suite proves in isolation (`capacity_exceeded_emits_single_dropped_event_from_forwarder_test`),
  wired through Grind's `Database`.
- **Test-harness bug found and fixed while writing this test**: the release
  gate was originally created in the _test_ process and handed to the
  handler by closure capture; since a `process.Subject` can only be received
  on by the process that created it (`gleam_erlang`), the forwarder's own
  `process.receive` on that subject could never actually match a message
  sent to it, so the handler always ran its own internal timeout instead of
  being released promptly. This did not produce a false pass — the assertion
  it fed (`process.receive(dropped_signal, ...)`) simply timed out and
  failed honestly — but it made the test far slower and less deterministic
  than intended. Fixed by creating the gate _inside_ the handler (owned by
  the forwarder) and handing it back to the test over a signal subject,
  matching the pattern `sinal/forwarder`'s own test suite already uses for
  exactly this reason.

### Claim: a raising handler leaves the job's own committed outcome unchanged

- **Test**: `postgres_acknowledged_observation_raising_handler_test` (marker
  `acknowledged-observation-raising-handler-outcome-unchanged-passed`).
- **Mechanism**: a `sinal.observe` handler on `observation.acknowledged()`
  unconditionally `panic`s. A job is run through `queue.process_one`.
- **Observed**: `queue.process_one(consumer) == Ok(True)`,
  `postgres.state(database, handle) == Ok(job.Succeeded)`,
  `postgres.outcome(database, handle) == Ok(job.SucceededWith("raising-7"))`
  — unaffected. Native `:telemetry` isolates the raise and auto-detaches the
  faulty handler; by the time any handler runs at all,
  `forwarder.emit`'s own hand-off to the forwarder has already returned,
  decoupled from the coordinator regardless.

### Claim: the descriptor is usable end to end from outside the package, using only public imports

- **Test**: `public_consumer_observes_acknowledged_test` (external
  `consumer/` package; marker `consumer-observes-acknowledged-passed`).
- **Mechanism**: `sinal.observe` attached to `grind/observation.acknowledged()`
  from the consumer package, importing only `grind/observation` and `sinal`
  (both public), running one typed job through the public consumer API.
- **Observed**: the decoded record's `job_id`, `queue`, `worker_id`,
  `committed_state`, `proposed`, and `confirmation` all match the run.

## Round 1 follow-up (coordinator review)

Independent review of round 1 found one genuine isolation hole, one
committed-value bug, two untested emission sites, an unrun mutation claim,
and stale mutation-evidence counts (recorded against a disposable cluster
already reused across several prior mutation runs in the same session,
rather than a clean one). Each is addressed below, with the review's own
wording as the section title.

### Claim: a handler that exits or is killed cannot exhaust the pool's own supervisor and stop the pool

- **Test**: `postgres_forwarder_crash_loop_does_not_stop_the_pool_test`
  (marker `forwarder-crash-loop-pool-survives-passed`).
- **Red first, against the unfixed round-1 code**: a minimal standalone
  reproduction (not the full suite, to get a clean signal fast) — a
  `sinal.observe` handler on `observation.acknowledged()` that
  `process.kill(process.self())`s on every invocation, driven by six
  acknowledged events in quick succession (80ms apart). Before the fix, the
  forwarder was a plain `Permanent` sibling of the PostgreSQL pool under one
  shared `OneForOne` supervisor at its OTP default restart intensity (2
  restarts / 5 seconds). Observed against a real disposable cluster:
  ```
  =SUPERVISOR REPORT====
      supervisor: {<0.129.0>,gleam@otp@static_supervisor}
      errorContext: shutdown
      reason: reached_max_restart_intensity
  ...
  {ok,true}                    // job 4's own process_one
  {ok,true}                    // job 5's own process_one
  {ok,true}                    // job 6's own process_one
  final submit
  {error,{submit_query_failed,{connection_unavailable}}}
  ```
  On the third crash the _shared_ supervisor exhausted its own restart
  budget and shut down — terminating the pool along with the forwarder — so
  the very next `postgres.submit` failed outright with
  `SubmitQueryFailed(ConnectionUnavailable)`. This is strictly worse than
  "acks fail": admission itself stops working.
- **The fix** (`grind/postgres.start`): the forwarder is now nested under
  its own dedicated `static_supervisor`, added to the root as a `Temporary`
  child (`supervision.restart(.., supervision.Temporary)`). A `Temporary`
  child's termination is never restarted by its parent and never counts
  toward the parent's own restart intensity — so if the _nested_ supervisor
  exhausts its own budget (still 2/5s by default) from a persistently
  crashing forwarder and terminates itself, the root supervisor simply drops
  it and moves on; the pool is never touched.
- **Green after the fix**, same standalone reproduction: the nested
  supervisor's own `reached_max_restart_intensity` shutdown is now the last
  supervisor report; every subsequent submit and `process_one` (including
  the final one, after the forwarder subtree is permanently gone) succeeds
  normally:
  ```
  =SUPERVISOR REPORT====
      supervisor: {<0.129.0>,gleam@otp@static_supervisor}   // the nested one
      errorContext: shutdown
      reason: reached_max_restart_intensity
  iteration begin
  submitted
  {ok,true}
  iteration begin
  submitted
  {ok,true}
  iteration begin
  submitted
  {ok,true}
  final submit
  {ok,true}
  {ok,succeeded}
  DONE
  ```
  Confirmed again as a permanent regression test against a clean disposable
  cluster (`gleam test`, `136 passed, 0 failures`, this test included).
- **Degraded-mode contract**: once the forwarder subtree is gone, further
  `forwarder.emit` calls report `ForwarderUnavailable`, which
  `grind/postgres` already discards — jobs keep being admitted, claimed, and
  acknowledged normally; only observations become unavailable. Documented in
  `README.md` and `docs/IMPLEMENTATION-SCOPE.md`.
- **Not applied**: the review's optional suggestion to derive the forwarder's
  `process.Name` from the pool's own name. `gleam_erlang`'s `process.Name`
  is fully opaque in the resolved version (`process.new_name(prefix:
String) -> Name(message)`, no reverse string accessor), so there is no
  public API to read a string back out of the caller-supplied `pool_name`
  to derive a related forwarder name from — not "simple" as the review
  anticipated, so left as its own independently generated name.

### Claim: `available_at_unix_ms` is chosen from the committed state, never the proposed state

- **The bug**: `available_at_for_observation` gated on `proposed_state` (the
  `AckProposal`'s own vocabulary — `"retryable"`/`"snoozed"`) instead of the
  actual `committed_state` read back from `RETURNING`. A proposed
  retry/snooze overridden by a concurrent cancellation commits `"cancelled"`
  and leaves the row's `available_at` column at its unrelated, stale pre-ack
  value — the bug would still report that stale value as `Some(ms)`, exactly
  contradicting the "committed state, never the proposal" invariant the rest
  of this descriptor's fields already uphold.
- **The fix**: `available_at_for_observation` now takes the committed-state
  string and gates on `"retryable" | "scheduled"` (the actual `RETURNING`
  vocabulary), not the proposal's.
- **Tests**:
  - `postgres_acknowledged_observation_available_at_for_committed_retry_test`
    (marker `acknowledged-observation-available-at-committed-retry-passed`):
    a proposed, genuinely committed retry — `available_at_unix_ms` is
    `Some(ms)`, within the default 15-second backoff's bounds
    (`before_ack_ms + 15_000` .. `after_ack_ms + 15_000`).
  - `postgres_acknowledged_observation_available_at_for_committed_snooze_test`
    (marker `acknowledged-observation-available-at-committed-snooze-passed`):
    a proposed, genuinely committed snooze (commits `scheduled`) —
    `Some(ms)`, within the requested 60-second delay's bounds.
  - `postgres_acknowledged_observation_available_at_none_when_cancel_overrides_retry_test`
    (marker
    `acknowledged-observation-available-at-none-cancel-overrides-retry-passed`):
    a proposed retry (`ProposedRetryable`) overridden by a concurrent
    cancellation — commits `Cancelled`, and `available_at_unix_ms` is
    `None`, never the stale pre-ack value.
- **Proven by mutation — "gate on the proposed state, the original bug"**,
  against a fresh disposable cluster with a recorded clean baseline: reverted
  `available_at_for_observation` to gate on `proposed_state` (`"retryable" |
"snoozed"`). Clean baseline `136 passed, 0 failures`; mutated `135 passed,
1 failure` — exactly the cancel-overrides-retry test:
  ```
  panic src/gleeunit/should.gleam:10
   test: grind_test.postgres_acknowledged_observation_available_at_none_when_cancel_overrides_retry_test
   info:
  Some(1790340070892)
  should equal
  None
  ```
  Reverted immediately; `gleam check` recompiled clean and `git diff` showed
  no trace of the mutated lines.

### Claim: both untested receipt-match ("early" and "post-0-row-`UPDATE`") sites are `Reconciled`

Round 1 only exercised `reconcile_unknown_ack`'s own receipt-match site (the
lost-reply claim above). Two further sites construct `AckCommit(...,
via_receipt_match: True)` and were untested: the check at the very top of
`acknowledge_transaction`, before any `UPDATE` is attempted, and the re-check
after a fenced `UPDATE` affects zero rows.

- **Test (early site)**:
  `postgres_acknowledged_observation_reconciled_on_sequential_duplicate_ack_test`
  (marker `acknowledged-observation-reconciled-on-sequential-duplicate-passed`).
  Reached deterministically, no concurrency needed: the exact same
  `ClaimedJob`/`Execution`, acknowledged a second time, finds the first
  call's own receipt already durably recorded before any `UPDATE` runs. The
  first ack's event is `Replied`; the second (duplicate) is `Reconciled`,
  same `command_id`. A sentinel ack afterward confirms exactly two events.
- **Test (post-0-row-`UPDATE` site)**:
  `postgres_acknowledged_observation_reconciled_on_concurrent_duplicate_ack_test`
  (marker `acknowledged-observation-reconciled-on-concurrent-duplicate-passed`,
  gated on `GRIND_TEST_REPEATABLE_READ_URL`). Reuses
  `run_ack_duplicate_repeatable_read_test`'s exact forced-overlap mechanism
  (`postgres_ack_duplicate_reports_ok_under_pinned_isolation_test`): a
  `BEFORE UPDATE` trigger parks acknowledgement A behind a held advisory
  lock while it still holds the row lock; acknowledgement B (the _same_
  claim, from a separate pool) genuinely waits on that row lock (confirmed
  via `pg_stat_activity` wait events, not inferred). A's `UPDATE` commits
  first (`Replied`); B's own `UPDATE` then affects zero rows against the
  now-committed row and re-checks the receipt, finding A's — the site under
  test. `count_acknowledgements_for_job == 1` confirms only one row was ever
  written. A sentinel ack through A's own pool afterward confirms exactly
  two events (B's separate pool is not exercised again, so nothing further
  could arrive from it either).
- **Proven by mutation — "`via_receipt_match: False` at both sites"**,
  against a fresh disposable cluster with a recorded clean baseline
  (`GRIND_TEST_REPEATABLE_READ_URL` set, so the concurrent test actually
  runs rather than skipping): hardcoded `False` at both the early and the
  post-0-row-`UPDATE` construction sites simultaneously. Clean baseline `136
passed, 0 failures`; mutated `134 passed, 2 failures` — exactly the two
  tests above, and no others:
  ```
  panic src/gleeunit/should.gleam:10
   test: grind_test.postgres_acknowledged_observation_reconciled_on_sequential_duplicate_ack_test
   info:
  Replied
  should equal
  Reconciled

   test: grind_test.postgres_acknowledged_observation_reconciled_on_concurrent_duplicate_ack_test
   info:
  False
  should equal
  True
  ```
  Reverted immediately; `gleam check` recompiled clean and `git diff` showed
  no trace of the mutated lines.

### Sentinel pattern, not `within: 500`

Every negative ("nothing arrived") and "exactly N" ("nothing _more_
arrived") assertion in this suite now uses a deterministic sentinel instead
of a fixed wall-clock wait: after the interesting event(s), a distinct,
known-good job is acknowledged through the _same_ consumer/producer (a
second, trivial, always-succeeding worker registered onto the same registry
via `register_sentinel_worker`, so it can never itself block on the original
worker's own gate), and the assertion is that the very next event received
carries the sentinel's `job_id`. `sinal/forwarder` guarantees per-producer
FIFO delivery (its own module documentation), so if the code under test had
wrongly emitted an extra event for the original job, it would have been
enqueued ahead of the sentinel's and would be the one actually received —
deterministically, not racily. A fixed-duration wait (the round 1 draft's
`process.receive(signal, within: 500)`) is either too short under load
(false pass) or wastes wall-clock time otherwise (true negative, but slow);
the sentinel pattern has neither failure mode. Applied to: both negative
tests above, the lost-reply "exactly one" check, the cancel-overrides
"exactly one" check, and the sequential/concurrent duplicate-ack "exactly
two" checks.

### Mutation evidence: clean baselines and deltas

Every mutation below was run against a freshly created disposable PostgreSQL
cluster (`initdb`/`pg_ctl` per run, the same shape `scripts/test-postgres.sh`
uses, with `GRIND_TEST_QUEUE_DATABASE_URL` and — for the one concurrent
duplicate-ack mutation — `GRIND_TEST_REPEATABLE_READ_URL` pointed at it), not
a cluster already reused across other test/mutation runs in the same
session; an earlier pass of this evidence recorded counts against a reused
cluster, whose accumulated cross-run state produced additional, unrelated
apparent failures that had nothing to do with the mutation under test. Each
row below is its own clean baseline immediately before its own mutation, in
the same session, on the same fresh cluster:

| Mutation                                              | Clean baseline         | Mutated                | Failing tests (only)                                                                                                                                                                                                    |
| ----------------------------------------------------- | ---------------------- | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Bypass forwarder (`sinal.emit` directly)              | 136 passed, 0 failures | 133 passed, 3 failures | isolation, overflow, forwarder-crash-loop                                                                                                                                                                               |
| Hardcode `Replied` always                             | 136 passed, 0 failures | 134 passed, 2 failures | reconciled-after-lost-reply, reconciled-on-sequential-duplicate                                                                                                                                                         |
| Committed state derived from proposal                 | 136 passed, 0 failures | 134 passed, 2 failures | committed-state-overrides-proposal, available-at-none-cancel-overrides-retry                                                                                                                                            |
| Emit on an `Error` path                               | 136 passed, 0 failures | 134 passed, 2 failures | absent-on-commit-unknown, absent-on-stale-ack                                                                                                                                                                           |
| `available_at` gated on proposed state                | 136 passed, 0 failures | 135 passed, 1 failure  | available-at-none-cancel-overrides-retry                                                                                                                                                                                |
| `via_receipt_match: False` at both untested sites     | 136 passed, 0 failures | 134 passed, 2 failures | reconciled-on-sequential-duplicate, reconciled-on-concurrent-duplicate                                                                                                                                                  |
| Emit before the transaction (naive)                   | 136 passed, 0 failures | 128 passed, 8 failures | none of the observation tests (see the commit-ordering claim above — this naive form does not reliably catch the bug); the 8 failures there are unrelated flakiness under a cluster already reused once in that session |
| Emit before the transaction, with a forced 50ms delay | 136 passed, 0 failures | 127 passed, 9 failures | commit-ordering, plus the same 8 unrelated failures as the row above (same reused cluster)                                                                                                                              |

The last two rows are the one pair not run on a maximally clean cluster
(each reused the cluster from the row immediately above it in the same
short investigation); the commit-ordering claim's own section above quotes
the specific panic and treats only that test's result as evidence, not the
raw counts, for exactly this reason.

### Limits and deliberate decisions

- **`reconcile_acknowledgement` does not emit — including when `acknowledge`
  itself never did either.** The plan allows the public
  `reconcile_acknowledgement`/`reconcile_unique` reconciliation reads to
  optionally emit with `confirmation: Reconciled`, "decide consistently;
  document". This round emits only from the two internal call sites that are
  the actual first proof of a commit (`acknowledge`'s own
  `resolve_ack_transaction_result`/`reconcile_unknown_ack`), not from
  `reconcile_acknowledgement` itself. When `acknowledge`'s own call already
  emitted (its transaction reply came back normally, or `reconcile_unknown_ack`
  resolved a lost reply within that same call), this correctly avoids
  multiplying observation events for the same commit when an operator or test
  calls `reconcile_acknowledgement` repeatedly afterward (as several existing
  tests already do, to prove idempotent reads). But when `acknowledge` itself
  returned `QueueAckUnknown` (its own reply lost _and_ `reconcile_unknown_ack`
  could not resolve it within that call, e.g. the store was unavailable right
  then), nothing was ever emitted for that commit — and a later
  `reconcile_acknowledgement` call that does resolve it still does not emit.
  That committed disposition can end up with no observation at all, ever,
  even though `grind_job_acknowledgements` holds a fully correct receipt.
  This is a genuine, accepted gap in delivery (see README.md's "best-effort"
  bullet), not merely a double-reporting safeguard.
- **`available_at_unix_ms` is `None` on every `Reconciled` event.** It is
  only ever known from a fresh write's own `RETURNING` (the
  `acknowledge_transaction` UPDATE was extended with one additional
  `RETURNING` column, `(extract(epoch FROM available_at) * 1000)::bigint`,
  present on every proposed-state branch); a duplicate-receipt match
  (`Reconciled`) cannot recover it, since `grind_job_acknowledgements` does
  not retain `available_at`. Inventing a value there was rejected as
  misleading; `None` documents the genuine gap instead. Separately from
  `Reconciled` vs. `Replied`, the value is also gated on the _committed_
  state (`"retryable" | "scheduled"`), never the proposed one — see "Round 1
  follow-up" above for the bug this was and its mutation evidence.
- **Retry-budget exhaustion does not change a committed outcome away from
  what was proposed.** An earlier draft of this documentation claimed
  "retry-budget exhaustion can turn a proposed `Retryable` into a committed
  `RuntimeFailed`/`Discarded` outcome" — this was wrong and has been
  corrected in `grind/observation` and `README.md`. Exhaustion is decided by
  the worker's own business retry policy _before_ the acknowledgement ever
  runs: an exhausted retry is proposed as a business failure
  (`worker.ExecutedBusinessFailure(.., BudgetExhausted)`, so `proposed` is
  already `ProposedBusinessFailure`), never as a `Retryable` proposal the
  acknowledgement later reinterprets. The acknowledgement's own `retryable`
  commit path re-checks `attempt_count < max_attempts` as a defensive
  consistency guard, not a policy decision: if that guard fails for a
  genuinely proposed retry, the whole acknowledgement is rejected as stale
  (no commit, no observation), not silently committed as something else. The
  only thing that actually overrides a proposal in this descriptor is a
  concurrent cancellation, covered above.
- Round 2 (`[grind, job, admitted]`, `[grind, job, claimed]`,
  `[grind, job, quarantined]`, `[grind, job, resolved]`,
  `[grind, job, cancellation]`, `[grind, job, released]`,
  `[grind, job, contract_mismatch]`) is delivered; see "Round 2
  observations" below for its own evidence and mutation table.

## Round 2 observations — `[grind, job, admitted/claimed/quarantined/resolved/cancellation/released/contract_mismatch]`

Every descriptor below shares `grind/observation`'s `JobRef`/`AttemptRef`
codecs and the same delivery discipline `acknowledged` established: emitted
only once a commit is proven, never from inside a transaction callback,
through the one `Forwarder` a `Database` owns (shared across every
`[grind, job, *]` event — see the overflow claim below for what that implies
for capacity). Each has at least one emission test asserting the exact
metadata a consumer would read, one no-emission test for its read-only or
error outcomes (proven with the same "the very next observation on this
channel is a known sentinel" technique the Round 1 evidence above documents,
never a fixed wall-clock wait), and a named mutation.

### Claim: `[grind, job, admitted]` — a plain `submit`/`submit_at` is always `Replied`, with `committed_state`/`available_at_unix_ms` from its own `RETURNING`

`postgres_admitted_observation_plain_submit_test` submits immediately (state
`queued`) and at a future time (`submit_at`, state `scheduled`), reading both
`committed_state` and `available_at_unix_ms` from the insert's own extended
`RETURNING id, state, (extract(epoch FROM available_at) * 1000)::bigint` —
the same "commit reply, never the proposal" discipline `acknowledged`
documents, applied to admission. `submission_id` is `None` for both, and
`confirmation` is `Replied` for both (a plain submission has no receipt to
reconcile from). The same test also submits at a target time already in the
past _by the database's own clock_: the insert's `CASE ... <=
clock_timestamp()` still commits `queued`, not `scheduled` — proving
`committed_state` is read back from that same `RETURNING`, never inferred
client-side from the request's own "immediate vs. future" intent (which a
client clock could disagree with the database's about, at the boundary).
**Named mutation**: deriving `committed_state` from
`available_at_unix_ms`'s request-side presence (`Some` → `Scheduled`, `None`
→ `Queued`) instead of the `RETURNING`-decoded `state` is caught by this
exact past-time assertion.

### Claim: `[grind, job, admitted]` — unique admission: `Inserted` is `Replied`; replaying the exact same `submission_id` is `Reconciled` via the in-transaction receipt hit

`postgres_admitted_observation_unique_inserted_and_reconciled_test` submits
once (`Inserted`, `Replied`, a known `available_at_unix_ms`), then replays
the identical `submission_id`/request through `submit_keep_existing`.
`admission_transaction`'s own leading `find_receipt` call finds the first
attempt's receipt before any candidate row is even looked up; that decision
is proven by a receipt read, not a fresh write, so the second observation is
`Reconciled` with `available_at_unix_ms: None` — mirroring the limitation
`acknowledged` already documents for its own receipt-matched commits.
**Named mutation**: hardcoding `confirmation: Replied` regardless of
`via_receipt_match` in `postgres.submit_unique` turns the second
observation's `Reconciled` into `Replied` — red exactly on this test.

### Claim: `[grind, job, admitted]` — the other `Reconciled` path: a commit reply lost after PostgreSQL already committed, resolved transparently within the same `submit_unique` call

`postgres_admitted_observation_in_call_post_commit_unknown_reconciled_test`
reuses `run_unique_committed_reply_lost_test`'s `SyncRep`
park-then-terminate harness (scenario (c) in "Increment 11" above):
`submit_unique`'s own transaction reply is lost, so its result comes back as
`pog.TransactionQueryError` — but `run`'s own follow-up
`reconcile_from_receipt` call, made within this exact same `submit_unique`
call before it ever returns to the caller, finds the now-visible receipt and
resolves `Ok(Inserted(handle))` transparently. This is distinct from the
in-transaction receipt hit above (that one never even reaches a
`TransactionQueryError`): here the whole transaction result actually came
back uncertain, and it is `run`'s _own_ recovery, not `admission_transaction`'s
leading lookup, that establishes the commit. Exactly one `admitted` event is
emitted, `Reconciled`, `available_at_unix_ms: None`. **Named mutation**:
flipping `via_receipt_match` to `False` at this exact fallback site (`run`,
`unique_admission.gleam`) is caught by this test alone.

### Claim: `[grind, job, admitted]` — a distinct `submission_id` landing on an occupied key (`Existing`) is its own fresh, `Replied` commit

`postgres_admitted_observation_unique_existing_conflict_test` proves both the
first (`Inserted`) and second (`Existing`) submissions each emit their own
`Replied` observation, tagged with their own `submission_id` — the `Existing`
receipt row is a genuine write for this exact submission, even though the
job row itself is untouched.

### Claim: `[grind, job, admitted]` — `available_at_unix_ms` is `None` for an `Existing` conflict against a non-eligibility state, even though the decision is `Replied`

`postgres_admitted_observation_existing_over_executing_available_at_none_test`
forces a job into `executing` directly, then submits a distinct
`submission_id` under an `Incomplete` policy (which treats `executing` as
still occupying the key). The resulting `Existing` conflict is a fresh,
`Replied` commit — but `committed_state: Executing` is not `Queued`,
`Scheduled`, or `Retryable`, so `available_at_unix_ms` must still be `None`:
`Replied` alone does not imply a meaningful next-run time. **Named
mutation**: removing `admitted_available_at`'s state gate
(`unique_admission.gleam`), so it reports `Some` unconditionally, is caught
by this test.

### Claim: `[grind, job, admitted]` — `SubmissionConflict` never commits and never emits

`postgres_admitted_observation_absent_on_submission_conflict_test` replays a
`submission_id` with a materially different request (`SubmissionConflict`)
and asserts nothing arrives on the `admitted` channel, then submits a fresh,
distinct submission through the same producer as the sentinel.

### Claim: `[grind, job, admitted]` — public `reconcile_unique` never emits, even when it recovers a genuinely committed `Inserted` outcome

`postgres_admitted_observation_absent_from_reconcile_unique_test` reuses the
exact "(d) committed, reply lost, pool closed" `SyncRep` scenario from
`run_unique_committed_reply_lost_store_unavailable_test`
(`docs/UNIQUENESS-CONTRACT.md`): `submit_unique` itself reports
`CommitUnknown`; while the zombie transaction is still parked,
`reconcile_unique` reports `CommitUnknown` again (no receipt visible yet, no
emission); once the zombie backend is confirmed terminated,
`reconcile_unique` resolves the genuine `Inserted` outcome from the
now-visible receipt — and still emits nothing. A subsequent fresh submission
through the same producer arrives as the very next `admitted` observation,
proving the channel itself is unaffected. This is the round 1 "does
`reconcile_acknowledgement` emit?" decision applied consistently to
`reconcile_unique`: both are pure receipt reads offered for a caller's own
return value, independent of whatever call originally produced the commit.
In this exact test's own scenario the originating `submit_unique` call did
_not_ emit anything — it returned `CommitUnknown`, precisely because its own
attempt to prove the commit (including its in-call receipt-lookup fallback)
failed. Since `reconcile_unique` also never emits, this genuinely committed
`Inserted` admission ends up with no `admitted` observation at all, ever —
a real gap, not a double-reporting safeguard; see README.md's "best-effort"
bullet and the round 1 "reconcile_acknowledgement" limits bullet above,
which document the same gap for `acknowledge`/`reconcile_acknowledgement`.

### Claim: `[grind, job, claimed]` — the claim's own autocommitted `RETURNING` is the proof of commit

`postgres_claimed_observation_emission_test` claims a freshly submitted job
and reads `attempt_id`/`epoch` back off `postgres.claim_identity` for direct
comparison against the observation's own `AttemptRef`, plus `attempt: 1` and
`previous_state: Queued`. **Named mutation**: swapping the `attempt_id`/
`epoch` arguments in `postgres.gleam`'s `emit_claimed` call site is caught by
both this test and the ordering test below (attempt_id, a global sequence
value, is never equal to epoch in practice).

### Claim: `[grind, job, claimed]` — nothing due (`Ok(None)`) never emits

`postgres_claimed_observation_absent_when_nothing_due_test` claims an empty
queue, asserts nothing arrives, then claims a genuine job through the same
producer as the sentinel.

### Claim: `[grind, job, quarantined]` — one event per row the quarantine scan's own `RETURNING` reports, with `cancellation_was_requested` distinguishing an ordinary abandoned attempt from one with a pending cancellation

`postgres_quarantined_observation_emission_test` forces two jobs into
`executing` with an already-expired lease — one plain, one also
`cancel_requested_at`-set — and drives the scan twice (`LIMIT 1` quarantines
at most one row per `claim_one` call), asserting `cancellation_was_requested`
is `False` then `True`. **Named mutation**: hardcoding
`cancellation_was_requested: False` in `emit_quarantined` is caught by the
second assertion.

### Claim: `[grind, job, quarantined]` — an ordinary claim with nothing expired never emits

`postgres_quarantined_observation_absent_when_nothing_expired_test` claims
one job normally (no expiry), asserts nothing arrives, then expires its
lease and re-runs the scan as the sentinel.

### Claim: `[grind, job, resolved]` — the first audited resolution is `Replied`; replaying the same `resolution_id` is `Reconciled` via `resolution_receipt_outcome`'s own receipt read

`postgres_resolved_observation_replied_and_reconciled_test` reuses the
`run_uncertain_resolution_test` shape (force a row `executing` with a stale
lease, quarantine it via a `queue.process_one` poll, then
`resolve_uncertain` with `AuthorizeReplay` twice for the same
`resolution_id`), asserting `Replied` then `Reconciled`. **Named mutation**:
swapping the two `Confirmation` values in `emit_resolved`'s `case result` is
caught by this test's own two assertions (which read `Reconciled` where
`Replied` was expected, and vice versa).

### Claim: `[grind, job, resolved]` — `ReconciliationNotRequired` never emits

`postgres_resolved_observation_absent_on_reconciliation_not_required_test`
calls `resolve_uncertain` against a job that was never `uncertain`, asserts
nothing arrives, then resolves a genuinely `uncertain` job through the same
producer as the sentinel.

### Claim: `[grind, job, resolved]` — a genuinely aborted commit (`ResolutionCommitUnknown`) never emits

`postgres_resolved_observation_absent_on_commit_unknown_test` reuses the
deferred-constraint-trigger-plus-`pg_terminate_backend` abort mechanism
`postgres_submit_unique_aborted_commit_is_commit_unknown_test` uses for
admission, applied here to an `AFTER INSERT` trigger on
`grind_job_resolutions`: the whole `resolve_uncertain` transaction — its
receipt insert and its `grind_jobs` update alike — is genuinely rolled back,
so `ResolutionCommitUnknown` is reported and the job is still `uncertain`
afterward. The sentinel is a _second_, distinct uncertain job resolved
through the same producer, not a retry of the same job — reusing the same
job would not distinguish a stray wrongly-emitted event (which would carry
that job's own id) from the real sentinel event, since both would carry an
identical id either way. **Named mutation**: emitting on
`resolve_uncertain`'s `TransactionQueryError` branch instead of staying
silent is caught by the sentinel's job-id mismatch.

### Claim: `[grind, job, cancellation]` — `CancelledBeforeRun` and `CancellationRequested` are the only genuine writes; `CancellationRequested` can repeat for an idempotent re-request

`postgres_cancellation_observation_before_run_and_requested_test` cancels a
queued job (`CancelledBeforeRun`) and an executing job twice
(`CancellationRequested` both times, proving the documented repeat), each
tagged with the row's own `previous_state`. **Named mutation**: also
emitting for `AlreadyCancelled` in `emit_cancellation`'s `case outcome` is
caught by the companion absence test below (the sentinel's job id no longer
matches the very next observation once the unwanted `AlreadyCancelled` event
lands ahead of it).

### Claim: `[grind, job, cancellation]` — the read-only outcomes (`AlreadyCancelled`, `AlreadyUncertain`, `AlreadyFinished`) never emit

`postgres_cancellation_observation_absent_on_already_cancelled_test` cancels
an already-cancelled job, asserts nothing arrives, then cancels a distinct
queued job through the same producer as the sentinel.

### Claim: `[grind, job, cancellation]` — a genuinely aborted commit (`CancellationCommitUnknown`) never emits

`postgres_cancellation_observation_absent_on_commit_unknown_test` uses the
same deferred-trigger-plus-terminate abort mechanism, applied to an
`AFTER UPDATE` trigger on `grind_jobs` matching the cancelled job's own
`id`: killing the backend mid-`COMMIT` rolls back `cancel_before_run`'s own
`UPDATE` too, so the job is still `queued` afterward and
`CancellationCommitUnknown` is reported. The sentinel cancels a second,
distinct queued job (not the same one again) for the same reason the
`resolved` commit-unknown test above uses a distinct job: cancelling the
same job again would emit the identical `CancelledBeforeRunOutcome`/job-id
pair either way, unable to distinguish a stray wrongly-emitted event from
the real one. **Named mutation**: emitting on `cancel`'s
`TransactionQueryError` branch instead of staying silent is caught by the
sentinel's job-id mismatch.

### Claim: `[grind, job, released]` — `release_unstarted_claim`'s own `RETURNING` is the proof of commit

`postgres_released_observation_emission_test` claims then releases before
any execution, asserting `attempt_id`/`epoch` against `claim_identity` and
`restored_state: Queued`. **Named mutation**: hardcoding
`restored_state: job.Retryable` (ignoring the real previous state) in
`emit_released` is caught by this test.

### Claim: `[grind, job, released]` — `Ok(False)` (the attempt fence no longer matches) never emits

`postgres_released_observation_absent_when_not_unstarted_test` acknowledges a
claim successfully, then calls `release_unstarted_claim` against that same
now-stale `ClaimedJob` (`Ok(False)`, since the row is no longer `executing`
under that attempt), asserts nothing arrives, then releases a genuinely
unstarted claim through the same producer as the sentinel.

### Claim: `[grind, job, contract_mismatch]` — a claimed attempt released for a codec version no longer matching the registered worker

`postgres_contract_mismatch_observation_emission_test` reuses
`run_batch_partial_error_test`'s forced `output_version` mismatch, asserting
`kind: OutputCodec`, `expected_version`/`actual_version` from the stored vs.
registered codec versions. **Named mutation**: hardcoding `kind: InputCodec`
in `emit_contract_mismatch` is caught by both this test and the companion
absence test below (the sentinel's mismatch is also on `output`).

### Claim: `[grind, job, contract_mismatch]` — an ordinary, matching-codec claim never emits

`postgres_contract_mismatch_observation_absent_on_matching_codec_test` claims
and runs a normal job to completion (no mismatch), asserts nothing arrives,
then forces a genuine mismatch on a second job through the same producer as
the sentinel.

### Claim: a coordinator's `[grind, job, claimed]` for one attempt always arrives before that same attempt's `[grind, job, acknowledged]`

`postgres_claimed_observation_precedes_acknowledged_test` attaches to both
descriptors before claiming, tags each received event with its own
`attempt_id`, and asserts the `claimed` event for that `attempt_id` is
received strictly before the `acknowledged` one — both are emitted by the
same producer (the queue actor claiming, then later acknowledging, one
attempt) through the one `Forwarder` a `Database` owns, and `sinal/forwarder`
guarantees per-producer FIFO delivery.

### Claim: the shared forwarder's capacity is exceeded by round 2 traffic the same way it already was for `acknowledged` alone

Adding `[grind, job, claimed]` changed
`postgres_acknowledged_observation_overflow_reports_dropped_test`'s own
expected drop count: with a gate-blocked `acknowledged` handler for job A
holding the forwarder's single in-flight slot, job B's own `claimed` _and_
`acknowledged` observations are both forwarded while that slot is held (one
`Forwarder` per `Database`, not one per event kind) and both are dropped,
coalesced into one `[sinal, forwarder, dropped]` report — `rejected` moved
from `1` to `2`. Job A's own `claimed` observation is unaffected: it is
emitted and drained before A's `acknowledged` handler ever blocks the
forwarder. This is a real, documented behavior change (more observation
traffic shares the same bounded forwarder), not a regression in either
descriptor's own correctness — job outcomes for both A and B are unchanged
either way.

### Claim: the external `consumer/` package can attach to a Round 2 descriptor using only public imports

`public_consumer_observes_claimed_test` (`consumer/test/grind_consumer_test.gleam`)
attaches `sinal.observe` to `grind/observation.claimed()` from outside the
package, submits and claims a job through `postgres`/`queue`, and decodes a
real `ClaimedMeasurements`/`ClaimedMetadata` pair — the same proof round 1
already gave for `acknowledged`.

### Mutation evidence: clean baselines and deltas (Round 2)

Every mutation below was applied to `src/grind/postgres.gleam` or
`src/grind/internal/unique_admission.gleam`, run against a freshly created
disposable PostgreSQL cluster (`initdb`/`pg_ctl` per run,
`GRIND_TEST_QUEUE_DATABASE_URL` and `GRIND_TEST_DATABASE_URL` pointed at
separate fresh databases on it — the same shape `scripts/test-postgres.sh`
uses), then reverted. Each row is its own clean baseline immediately before
its own mutation, in the same session, on the same fresh cluster; the full
`scripts/test-postgres.sh` gate (disposable cluster, squirrel check, the
pinned Oban oracle, the `grind` suite, and the external `consumer/` suite)
was run once more after every mutation in this table was reverted, with all
contract markers present.

The first seven rows were captured in an earlier pass, against a
154-passed baseline (before the coordinator-review follow-up below added
five more tests). The remaining five rows were captured together in one
later pass, against the resulting 158-passed baseline; both passes are
equally valid clean-baseline evidence, just at different points in the same
round.

| Mutation                                                                                                                                                  | Clean baseline         | Mutated                | Failing tests (only)                                                                           |
| --------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------- | ---------------------- | ---------------------------------------------------------------------------------------------- |
| `admitted`: hardcode `confirmation: Replied` in `submit_unique`'s emit call                                                                               | 154 passed, 0 failures | 153 passed, 1 failure  | admitted-observation-unique-inserted-reconciled                                                |
| `claimed`: swap `attempt_id`/`epoch` args in `emit_claimed`'s call site                                                                                   | 154 passed, 0 failures | 152 passed, 2 failures | claimed-observation-emission, claimed-precedes-acknowledged-ordering                           |
| `quarantined`: hardcode `cancellation_was_requested: False` in `emit_quarantined`                                                                         | 154 passed, 0 failures | 153 passed, 1 failure  | quarantined-observation-emission                                                               |
| `resolved`: swap `Replied`/`Reconciled` in `emit_resolved`'s `case result`                                                                                | 154 passed, 0 failures | 153 passed, 1 failure  | resolved-observation-replied-and-reconciled                                                    |
| `cancellation`: also emit for `AlreadyCancelled` in `emit_cancellation`                                                                                   | 154 passed, 0 failures | 153 passed, 1 failure  | cancellation-observation-absent-on-already-cancelled                                           |
| `released`: hardcode `restored_state: Retryable` in `emit_released`                                                                                       | 154 passed, 0 failures | 153 passed, 1 failure  | released-observation-emission                                                                  |
| `contract_mismatch`: hardcode `kind: InputCodec` in `emit_contract_mismatch`                                                                              | 154 passed, 0 failures | 152 passed, 2 failures | contract-mismatch-observation-emission, contract-mismatch-observation-absent-on-matching-codec |
| `admitted`, in-call post-`CommitUnknown` path: flip `via_receipt_match` to `False` in `run`'s `TransactionQueryError` fallback (`unique_admission.gleam`) | 158 passed, 0 failures | 157 passed, 1 failure  | admitted-observation-in-call-post-commit-unknown-reconciled                                    |
| `resolved`: emit on `resolve_uncertain`'s `TransactionQueryError` branch (aborted-commit case) instead of staying silent                                  | 158 passed, 0 failures | 157 passed, 1 failure  | resolved-observation-absent-on-commit-unknown                                                  |
| `cancellation`: emit on `cancel`'s `TransactionQueryError` branch (aborted-commit case) instead of staying silent                                         | 158 passed, 0 failures | 157 passed, 1 failure  | cancellation-observation-absent-on-commit-unknown                                              |
| `admitted`, `available_at_unix_ms` gating: remove `admitted_available_at`'s state gate (`unique_admission.gleam`), reporting `Some` unconditionally       | 158 passed, 0 failures | 157 passed, 1 failure  | admitted-observation-existing-over-executing-available-at-none                                 |
| `admitted`, plain submit: derive `committed_state` from the request's `available_at_unix_ms` presence instead of the insert's own `RETURNING`             | 158 passed, 0 failures | 157 passed, 1 failure  | admitted-observation-plain-submit                                                              |

### Limits and deliberate decisions (Round 2)

- **`quarantine_expired`'s `RETURNING` and `submit`/`submit_at`'s extra
  `RETURNING` columns are both plain inline SQL, not Squirrel-managed** —
  neither function was ever routed through `grind/internal/sql`
  (Squirrel-generated), so no `scripts/generate-sql.sh` regeneration was
  needed for round 2; `gleam run -m squirrel check` (run as part of
  `scripts/test-postgres.sh`) stays green unchanged.
- **`available_at_unix_ms` is `None` on every unique-admission `Reconciled`
  event**, the same limitation `acknowledged` documents for its own
  receipt-matched commits: `grind_unique_submissions` does not retain a
  generally reusable `available_at`, so a receipt-proven decision (an
  in-transaction receipt hit, or a post-`CommitUnknown` reconciliation inside
  `submit_unique`'s own `run`) cannot re-derive it. A `Rescheduled` receipt
  does retain its own `rescheduled_to` column, which could in principle
  recover this one case — left as `None` uniformly for simplicity and
  consistency, since a `Reconciled` observation is already documented as
  best-effort and not the durable source of this value.
- **The ordinary quarantine scan's `RETURNING` decodes `attempt_id` as
  `Option(Int)` defensively**, even though every row this scan can match is
  already `state = 'executing'` (which always has an attempt on record) —
  consistent with this module's existing fail-closed handling of every other
  stored-state mapping (`job.state_of_stored`), never an `assert` on
  data read back from storage.
- **`available_at_unix_ms` on `admitted` is `Some` only for `Queued`,
  `Scheduled`, or `Retryable`**, never for any other policy-eligible state an
  `Existing` conflict can land on (`Incomplete` reaches `Executing`/
  `Uncertain`; `AllRetained` reaches every terminal state too) —
  `admitted_available_at` (`grind/internal/unique_admission`) gates on the
  committed/observed state itself, not on which `Admission` variant produced
  it, so `Inserted`/`Rescheduled` (always `Queued`/`Scheduled`) and `Existing`
  share one definition. Proven by
  `postgres_admitted_observation_existing_over_executing_available_at_none_test`
  (an `Existing` conflict against a row forced `executing`) and by mutation
  (removing the gate reports `Some` unconditionally — red on that test).
- **A claim whose commit reply is lost (not "detected as an error", but
  genuinely never reaches the calling process at all — the OTP process
  making the claim call exits between PostgreSQL committing the claim
  `UPDATE` and this code reaching its own `emit_claimed` call) has no
  `claimed` event, ever.** Nothing in this design retries or reconciles a
  `claimed` observation after the fact the way `acknowledged`/`admitted`/
  `resolved` reconcile a lost _transaction_ reply — a claim's own
  `RETURNING` is read synchronously in the same call that decides whether to
  emit, with no separate "was it committed?" question to later resolve. If
  the lease is never renewed after such a crash, the claim-time quarantine
  scan later emits a `[grind, job, quarantined]` event for that exact
  `attempt_id`/`epoch` with no matching `[grind, job, claimed]` ever having
  been observed — a consumer that expects one `claimed` per `quarantined`
  (by `attempt_id`) cannot assume that pairing holds. Not tested here (it
  requires killing the claiming OTP process itself mid-flight, not a
  database-level fault); documented as a real, accepted gap.

## Independent-review follow-up: `reconcile_unique` conflict passthrough, free-capacity polling, and automatic ack-unknown retry

Three fixes from an independent correctness review, each proven red before
its fix and green after, against the 161-passed baseline this pass leaves
behind (158 baseline plus these three tests). All three were exercised both
in isolation (a disposable local cluster with only `grind_test`/
`grind_queue_test`) and as part of the full `scripts/test-postgres.sh` gate.

### Claim: `reconcile_unique` must not turn a genuine `SubmissionConflict` into `CommitUnknown`

`grind/internal/unique_admission.gleam`'s `reconcile_from_receipt` (reached
by both public `reconcile_unique` and `run`'s own post-`TransactionQueryError`
fallback) collapsed every `find_receipt` error — including
`unique.SubmissionConflict` from a genuine fingerprint mismatch (Decision 9;
`docs/UNIQUENESS-CONTRACT.md`) — into `Error(unique.CommitUnknown(pending))`.
A caller whose `SubmissionId` was already committed by a _different_ request
would get `CommitUnknown` forever from `reconcile_unique`, never the
`SubmissionConflict` a fresh `submit_unique` call against the same id would
have reported immediately.

**Test**: `postgres_reconcile_unique_mismatched_pending_reports_conflict_test`
(`test/grind_test.gleam`) commits request A under a `submission_id`, then
calls `reconcile_unique` with a `PendingSubmission` built via the `@internal`
`unique.new_pending_submission` carrying an arbitrary, non-matching
`request_sha256` — standing in for a different request B's `CommitUnknown`
being reconciled against the same id. Red before the fix:
`postgres.reconcile_unique` returned `Error(CommitUnknown(pending))`; the
test asserts `Error(SubmissionConflict)`.

**Fix**: `reconcile_from_receipt` now matches `find_receipt`'s result
explicitly — `Ok(None)`, `AdmissionContended`, and `AdmissionFailed` still
report `CommitUnknown(pending)` (genuinely unknown, or the check itself could
not run); `SubmissionConflict` passes through unchanged, mirroring
`grind/postgres`'s own `reconcile_unknown_ack` → `QueueAckCommandConflict`
passthrough for the acknowledgement path.

**Red/green** (isolated cluster, `grind_test` only): red — 158 passed, 1
failure (`postgres_reconcile_unique_mismatched_pending_reports_conflict_test`,
`Error(CommitUnknown(...))` where `Error(SubmissionConflict)` was expected).
Green after the fix — 159 passed, no failures. Reverted with `Edit`, not
`git checkout`, and reapplied after confirming red; final state proven green
again against the same isolated cluster and again as part of the full gate
(161 passed overall, all contract markers present).

Not separately covered: a fingerprint mismatch surfacing through `run`'s own
post-connection-loss path (both faults — a lost connection mid-transaction
_and_ a different request's receipt already present — forced simultaneously)
is not exercised by a dedicated test. `run`'s fallback calls the exact same
`reconcile_from_receipt` this test already proves correct, so the fix is
structurally covered either way; a combined-fault test was judged not to add
distinct evidence for the added harness complexity.

### Claim: an automatic consumer with free capacity must keep polling while another attempt is still active

`grind/queue.gleam`'s `continue_if_idle` armed the next `Poll` timer only
when `active` was completely empty and the poll batch was drained
(`poll_remaining_jobs == 0`). With `maximum_concurrency > 1`, one long-running
attempt left every other slot idle for as long as it kept running: a newly
submitted, already-due job — and the claim-time expired-lease quarantine
scan, which piggybacks on the same claim query — had to wait for every
active attempt to finish before the next poll was even scheduled, no matter
how much capacity was free in the meantime.

**Test**: `postgres_automatic_consumer_polls_while_capacity_free_test`
(`test/grind_test.gleam`). An automatic consumer with
`maximum_concurrency: 2`, `maximum_jobs_per_poll: 1`, and a 50ms poll
interval claims job 1, which blocks on its own handler-owned gate. Only once
job 1 is confirmed running is job 2 submitted — it did not exist at the poll
that claimed job 1, so only a _later_, freshly scheduled poll can find it
due. The test asserts job 2's worker starts within 2 seconds while job 1 is
still blocked, then releases both and confirms both succeed.

**Fix**: `continue_if_idle` now arms the next poll whenever the consumer is
not shutting down, this round is done claiming, and `active` is below
`maximum_concurrency` — not only when `active` is empty. A new
`ConsumerState.poll_scheduled: Bool` field is the single-outstanding-timer
guard this relies on: `continue_if_idle` only arms a timer when one is not
already pending, and the `Poll` message handler clears the flag as the very
first thing it does (before any other branch), so the one timer this design
intentionally keeps outstanding is never allowed to become two overlapping
ones.

**Red/green** (isolated cluster, `grind_test` + `grind_queue_test`): red —
`let assert Ok(CapacityWorkerStarted(22, second_release)) = process.receive(started, within: 2000)`
panicked with "Pattern match failed" (job 2 never started while job 1 was
blocked). Green after the fix. Reverted with `Edit` and reapplied after
confirming red; existing shutdown/drain tests
(`automatic-drain-paused-poll-and-renewed`,
`stale-shutdown-grace-timer-scoped-to-incarnation`, and the rest of the
`consumer-stop-*`/`automatic-*` suite) stayed green throughout, including in
the final full-gate run.

### Claim: automatic mode must not silently drop a `QueueAckUnknown` acknowledgement

`finish_completion`'s `Automatic` branch treated every `ProcessError` —
including `QueueProcessFailed(QueueAckUnknown(command_id, proposed))` — the
same as an ordinary "nothing due" result: the claim was simply dropped, with
no caller left holding the `ClaimedJob`/`Execution` to retry. The row was
left `executing` until its lease naturally expired and some consumer's
claim-time scan quarantined it to `uncertain` — recoverable only by an
operator's audited resolution, never on its own, even though the
acknowledgement's own `command_id` fencing
(`postgres.acknowledgement_command_id`, deterministic in job id, attempt id,
and epoch) already makes a retry of the exact same `acknowledge_claim` call
safe.

**Decision**: the coordinator now keeps a `QueueAckUnknown` attempt in
`active` (`ActiveAttempt.pending_ack: Some(execution)`) and retries the exact
same `acknowledge_claim` call on this incarnation's own renewal-interval
timer — reusing the `Renew` message and timer instead of adding new
machinery — until a non-`QueueAckUnknown` result (`Ok`, or one of the ack's
own known-outcome errors such as `QueueAckStale`/`QueueAckCommandConflict`)
resolves it. **Scoped to `Automatic` completion only**: a `Manual`
`process_one` caller keeps getting `QueueAckUnknown` back synchronously,
exactly as before — it already has a live reply channel and can call
`reconcile_acknowledgement` itself; only automatic mode had no caller left to
hand an unknown ack to. This was proven the hard way: an early version of
the fix retried under `Manual` completion too, which made
`postgres_ack_commit_connection_loss_is_unknown_test` (and two `acknowledged`
observation tests sharing the same fault mechanism) hang past their own
10-second reply timeout, since `process_one`'s caller stopped getting an
immediate answer. Scoping the retry to `Automatic` fixed all four at once.

**Corrected after independent re-review (R1/R2/R3 below)**: an earlier
version of this fix deliberately did _not_ renew the lease while an ack was
pending, on the claim that "nothing is still running that a lease protects,
and every retry is fenced regardless of lease currency". That claim is
wrong: a not-yet-committed retry's own acknowledgement `UPDATE` is _itself_
gated on `postgres.live_lease_predicate` (only the already-committed,
receipt-matched replay path is lease-independent), so never renewing meant a
merely-transient outage could exhaust the lease and strand the job after
only ~3 renewal intervals, well before a genuinely transient fault would
have cleared. Fixed by renewing first, then retrying, bounded — see R1.

**R1 — renew while pending, bounded** (`retry_pending_ack`): each pending
tick calls the same fenced `postgres.renew_claim` first (fenced to
`executing`, this exact `attempt_id`/`epoch`/`attempt_owner`, and a still
live lease — identical to an ordinary in-progress attempt's own renewal),
then retries `acknowledge_claim` regardless of the renewal's own result. If
the original attempt's transaction had actually committed already (a lost
reply, not an abort), `renew_claim`'s fence no longer matches (state is no
longer `executing` under this attempt) and it harmlessly reports `Ok(False)`
— the acknowledgement retry right after it still reconciles correctly from
the now-visible receipt, since that path never depended on the lease at all.
**Bounded** by `ConsumerState.pending_ack_retry_budget` (approximately one
lease duration's worth of ticks — `lease_duration_ms / renewal_interval_ms`,
computed once, at least 1): once `ActiveAttempt.pending_ack_ticks` reaches
it, ticks stop renewing (but keep retrying the acknowledgement, which stays
cheap and safe) and the lease is left to lapse — a persistently failing
commit then converges on a known `QueueAckStale(_, AckLeaseExpired(..))`
once the lease is truly gone (or on whatever a claim-time quarantine scan's
own `state` change surfaces as, if that runs first), ending the retry chain
rather than renewing forever.

**R2 — exactly one timer chain per pending attempt** (`renewal_generation`):
transitioning an attempt into `pending_ack` always finds exactly one
ordinary-renewal timer already outstanding (the chain `start_attempt`/
`renew_lease` maintains) — arming a second, pending-retry timer on top of it
without invalidating the first would double the retry/renewal rate. Fixed
by adding `ActiveAttempt.renewal_generation`, bumped only the first time
`retry_ack_until_known` moves an attempt from `pending_ack: None` to `Some`,
carried in the `Renew(attempt_id, epoch, generation)` message, and checked
by `renew_active_attempt` against the attempt's current stored generation —
a mismatch means a stale, invalidated chain, dropped with no further
scheduling. A later re-arm for the same still-pending attempt keeps the same
generation (the leftover ordinary timer has already fired and been consumed
by this exact chain by then).

**R3 corrected `unique.AdmissionFailed` doc wording**: "a controlled
rollback the database itself confirms" overclaimed what is actually known.
The accurate claim (now in the doc comment) is narrower: the transaction
callback failed, so this code never sent `COMMIT`, and therefore the
admission cannot have committed — regardless of whether the resulting
`ROLLBACK` itself ever reached the server (it may not, if the connection was
already lost). The same corrected reasoning is now also documented on
`postgres.QueueAckFailed` (previously undocumented), since it is produced by
the exact same "callback returned `Error`, pog reports a controlled
`TransactionRolledBack`" mechanism on the acknowledgement path.

**Optional fixes also applied**: (1) a `QueueAckFailed` on a retry already
in flight (`pending_ack: Some`) is now treated the same as another
`QueueAckUnknown` — retried within the same bound — since it is exactly as
safe to retry (`command_id` idempotency) and surfacing it immediately would
silently drop the claim in `Automatic` mode the same way an unhandled
`QueueAckUnknown` did; a _first_ attempt's own `QueueAckFailed` is
unaffected. (2) "call `fill_automatic_slots`/`continue_if_idle` after a
pending retry resolves" was already true by construction: a resolved retry
still flows through the shared `finalize_ack_result` → `finish_completion`,
whose `Automatic` branch already calls `fill_automatic_slots` (`Ok(True)`)
or `continue_if_idle` (`Ok(False)`/`Error(_)`) exactly as it does for an
ordinary first-attempt result — no separate change was needed.

**Test**: `postgres_automatic_ack_commit_connection_loss_recovers_test`
(`test/grind_test.gleam`), the same deferred-constraint-trigger
aborted-commit harness `run_ack_commit_connection_loss_test` uses (a
`DEFERRABLE INITIALLY DEFERRED` trigger on `grind_job_acknowledgements`
scoped to this job's id sleeps at commit time; terminating that backend
aborts the whole transaction before it ever commits — proven by
`postgres.reconcile_acknowledgement` finding no receipt), driven against an
**automatic** consumer instead of a manually stepped one. After the trigger
is dropped, the job reaches `succeeded` on its own, with the coordinator's
own retried `acknowledge_claim` performing the acknowledgement fresh (the
aborted transaction left nothing to reconcile from) — without ever passing
through `uncertain`. `lease_duration: 1000` (checked against the R1/R2
redesign too, not just the original fix) stayed robust across 5+ consecutive
full local runs; not raised.

**Red/green** (isolated cluster, `grind_test` + `grind_queue_test`): red —
`wait_for_job_state_tolerating_errors(database, handle, job.Succeeded, 750)`
returned `False` (the job stayed `executing` for the whole wait, exactly the
pre-fix drop-and-wait-for-lease-expiry behavior). Green after the fix.
Reverted with `Edit` (only the `finalize_ack_result` branch, keeping the
surrounding `pending_ack`/`retry_ack_until_known`/`retry_pending_ack`
machinery in place so the revert exercises the same code shape a reviewer
would see) and reapplied after confirming red.

Two flakiness fixes were needed to make this test's own harness reliable,
independent of the production fix: `wait_for_job_state` bails out on the
first `postgres.state` error rather than retrying, which is wrong
immediately after this same test kills a connection on the pool it is about
to read from again — `wait_for_job_state_tolerating_errors` (a copy that
treats a transient read error as "not yet" instead of "never") is used
instead, for both the `Executing`-then-`Succeeded` wait and (via
`retry_transient_query`) the final `postgres.arguments`/`postgres.outcome`
reads.

### Claim (R1): a persistently aborting commit does not retry forever — bounded renewal ends it as `uncertain`

`postgres_automatic_ack_retry_bounded_eventually_uncertain_test`
(`test/grind_test.gleam`) keeps the same deferred-constraint-trigger abort
installed for the _entire_ test (never dropped mid-test, unlike the recovery
test above) — every acknowledgement attempt for this job, the first and
every retry alike, hits it. `maximum_concurrency: 2` keeps this consumer
polling throughout (the free-capacity fix), so its own claim-time quarantine
scan keeps running. A helper
(`kill_ack_backends_until_uncertain`) repeatedly finds and kills this job's
own sleeping backend via `pg_stat_activity` (a real database-time barrier,
not a wall-clock guess), checking the job's own state between kills, up to a
generous 40-iteration cap; once the ack retry loop itself gives up (a known
`QueueAckStale` once the lease lapses), no further backend ever sleeps for
this job (`current_ack_rejection` reads the row directly once it is no
longer `executing`, never reaching the trigger's `INSERT`), so a "miss" just
means waiting for a poll's quarantine scan to catch up.

**Red/green**: red — with the retry budget forced to always renew (`case
pending_ack_ticks < state.pending_ack_retry_budget` replaced with `case True`
in `retry_pending_ack`, `src/grind/queue.gleam`), the loop exhausted its
40-iteration cap with the job still `executing`, never reaching `uncertain`
(`False` where `True` was expected) — proving that without a bound, this
exact persistent-failure scenario retries indefinitely. Green after
reverting to the real budget check. Stable across 3 consecutive full local
runs after the fix.

### Claim (R2): the generation guard prevents a doubled retry/renewal rate

Not committed as a permanent asserting test (the exact rate is inherently
timing-sensitive across machines and load, so a hard pass/fail threshold
here would be a flaky-test liability far out of proportion to what it
proves); instead, mutation evidence, following this document's own
established pattern for a claim whose exact effect size is not worth
threshold-asserting.

**Mechanism**: a temporary experiment (not part of the retained suite)
reused the persistent-abort harness from R1's test above but with
`maximum_concurrency: 1`, `lease_duration_ms: 2000` (long enough that the
retry budget never runs out mid-measurement, isolating the renewal-rate
question from the separate bounded-exhaustion one), and counted how many
distinct sleeping backends it found and killed inside a fixed 2000ms
wall-clock window starting right after the worker released.

**Clean baseline** (the real `retry_ack_until_known`, generation bumped on
first transition into `pending_ack`): 3, 2, 2 kills across three runs
(≈1 every ~666ms, matching `renewal_interval_ms = lease_duration_ms / 3`).

**Mutated** (`retry_ack_until_known`'s `generation` always kept as
`renewal_generation` unchanged, never bumped — reintroducing R2's bug: the
leftover ordinary-renewal timer chain and the new pending-retry chain both
stay "current" and both keep re-arming): 5, 8, 5 kills across three runs —
roughly 2–3× the clean baseline's rate, consistent with two overlapping
timer chains both firing on the same cadence (`wait_for_commit_trigger_backend`
only ever returns the single most-recent sleeping backend, so two
near-simultaneous sleepers show up as two kills in quick succession rather
than one, which is why the ratio is noisy rather than exactly 2×).

### Full-gate confirmation

`nix develop --command bash scripts/test-postgres.sh` was run to completion
after all fixes above (the original three fixes, R1/R2/R3, the two optional
fixes, plus the doc-only and dead-code-removal items from the same overall
pass): root package 162 passed, 0 failures; external consumer 9 passed, 0
failures; the pinned Oban oracle harness and `gleam run -m squirrel check`
both green; every contract marker in both `for contract in ...` loops
present. `gleam check` (root and `consumer/`), `nix fmt`, and `nix flake
check` all clean; `git diff --check` reports no whitespace errors.

## Increment 14 — cross-version quarantine coverage and retry-safe plain submit (approved contract decisions, 2026-09-25; unified after independent review)

### Claim: a consumer's own per-queue quarantine scan covers an expired executing row left by a worker version no consumer currently registers

- **Test**: `postgres_quarantine_covers_unregistered_worker_version_test`
  (`test/grind_test.gleam`; marker
  `quarantine-covers-unregistered-worker-version-passed`).
- **Fault injection**: a job is submitted under worker `v1`, then forced
  directly to `executing` with an already-expired lease (`lease_expires_at =
clock_timestamp()`, the same direct-`UPDATE` shape every other quarantine
  test in this file uses) and an owner naming a consumer that no longer
  exists. Only worker `v2` of the same worker id is registered from then on,
  through a fresh `queue.start_manual` consumer — nothing in that registry
  can ever claim the `v1` row.
- **Genuine red against the real pre-fix filter**: the original bug was
  reproduced exactly, not with a literal stand-in. `quarantine_expired_in_queue`
  and its caller `claim_one` were temporarily reverted (via `Edit`, not `git
checkout`) to the real pre-fix shape: `quarantine_expired_in_queue` took an
  `identities: List(#(String, String))` parameter, `claim_one` passed
  `registry.identities(workers)`, and the candidate `SELECT` spliced in the
  original `(worker_id = $n AND worker_version = $n+1) OR ...` eligibility
  clause built from those identities. Run against a fresh disposable
  PostgreSQL cluster (`nix develop --command gleam test`,
  `GRIND_TEST_DATABASE_URL`/`GRIND_TEST_QUEUE_DATABASE_URL`/
  `GRIND_TEST_REPEATABLE_READ_URL`/`GRIND_TEST_QUARANTINE_URL` pointed at a
  cluster started the same way `scripts/test-postgres.sh` does):
  **168 passed, 1 failure — exactly and only
  `postgres_quarantine_covers_unregistered_worker_version_test`**, with the
  precise assertion failure `Ok(Executing) should equal Ok(Uncertain)`. No
  other test in the 169-test suite failed: the real bug's blast radius is
  narrow (every other test registers the same worker version that claimed
  its own row, so the identity filter matches trivially there), unlike a
  cruder stand-in mutation (see below). Reverted immediately via `Edit`;
  `gleam check` recompiled clean and `git diff` showed no trace of the
  reverted lines.
- **Fix**: `quarantine_expired_in_queue` (`src/grind/postgres.gleam`) drops
  the identity filter entirely — its candidate `SELECT` is scoped only by
  `storage_owner` and `queue`, matching every `executing` row with an
  expired lease regardless of which worker id/version claimed it.
  Quarantining never decodes or runs any worker code (it is a single
  `UPDATE ... SET state = 'uncertain'`), so there is no codec or
  registration reason to restrict it.
- **Secondary, broader mutation (kept as a second, distinct data point, not
  a substitute for the real-filter reproduction above)**: `AND
worker_version = 'v2'` added to the fixed `quarantine_expired_in_queue`'s
  candidate `SELECT` — a cruder stand-in that hardcodes one literal version
  rather than reproducing the real per-consumer registry filter. This
  broke far more broadly (137 passed, 32 failures) because most other tests
  in the suite use worker version `v1`, never `v2`, so this stand-in
  disagrees with nearly every other quarantine-dependent test's own setup —
  expected collateral from a cruder mutation of shared infrastructure, kept
  here only to show the contrast with the precise, real-filter reproduction
  above (which is the actual evidence for this claim). Reverted via `Edit`.

### Claim: a queue no consumer ever polls still has its expired executing rows quarantined by the public `quarantine_expired` operation, bounded by `limit`, and the sweep genuinely crosses queues

- **Test**: `postgres_quarantine_expired_global_operation_test`
  (`test/grind_test.gleam`; marker
  `quarantine-expired-global-operation-passed`). Runs against a database
  dedicated to this test alone (`GRIND_TEST_QUARANTINE_URL`, added to
  `scripts/test-postgres.sh` and `test/grind_test_env.erl` as
  `grind_quarantine_test`), not the shared `GRIND_TEST_DATABASE_URL`
  database every other test in this file uses: `quarantine_expired` sweeps
  every expired `executing` row for its whole storage owner (not scoped to
  one queue), and storage owner is derived from `host:port/database`
  (`postgres.validate`), so sharing a database would make this test's own
  row and observation counts depend on whatever unrelated expired rows other
  tests happen to leave behind at the moment this one runs, in `id` order —
  a dedicated database is a dedicated storage owner, immune to that
  ordering.
- **Mechanism**: two jobs are submitted to two _different_ queues neither
  `queue.start_manual`/`queue.start` consumer ever polls, both forced
  directly to `executing` with an already-expired lease. A `sinal.observe`
  handler on `observation.quarantined()` captures each event's own metadata.
  `limit: 0` and `limit: -1` are asserted to reject with
  `Error(NonPositiveLimit)` before touching storage (both rows still
  `Executing` afterward, no observation emitted); `limit: 1` quarantines
  exactly one of the two, and the captured event's own `ref.queue` is
  asserted to match that exact row's real queue (not a hardcoded or swapped
  one — the global sweep, unlike the per-queue scan, spans more than one
  queue in a single call, so `emit_quarantined`'s row-carried `queue` field
  is genuinely exercised here); `limit: 10` then quarantines the remainder,
  its own event's queue asserted the same way; the two captured queues
  together are asserted to be exactly `{queue_a, queue_b}` — the sweep
  genuinely crossed queues, not two events both reporting the same one. A
  further `limit: 10` call reports `Ok(0)` with no further observation
  (idempotent, nothing left).
- **This is new code, not a red-before-green claim** (`postgres.quarantine_expired`
  did not exist before this change) — compile-error "red" is not evidence,
  so the claim is instead proven by mutation. **Mutation**: `limit > 0`
  relaxed to `limit >= 0` in `quarantine_expired`. Run against a fresh
  disposable cluster: 168 passed, 1 failure — exactly
  `postgres_quarantine_expired_global_operation_test`, with zero collateral
  damage. Reverted via `Edit`; `gleam check` recompiled clean.

### Claim: `submit_with_id` gives a plain admission the same retry safety `submit_unique` has, with no uniqueness policy — one unified admission transaction, not two

- **Tests** (`test/grind_test.gleam`):
  `postgres_submit_with_id_first_submit_inserted_test`,
  `postgres_submit_with_id_same_request_retry_returns_original_test`,
  `postgres_submit_with_id_different_input_same_id_conflict_test`,
  `postgres_submit_with_id_committed_reply_lost_returns_inserted_test`,
  `postgres_submit_with_id_concurrent_same_id_one_row_test`,
  `postgres_submit_with_id_concurrent_different_input_conflict_test`,
  `postgres_admitted_observation_submit_with_id_test`; markers
  `submit-with-id-first-submit-inserted-passed`,
  `submit-with-id-retry-returns-original-passed`,
  `submit-with-id-different-input-conflict-passed`,
  `submit-with-id-committed-reply-lost-inserted-passed`,
  `submit-with-id-concurrent-one-row-passed`,
  `submit-with-id-concurrent-different-input-conflict-passed`,
  `admitted-observation-submit-with-id-passed`. Consumer package:
  `public_consumer_submit_with_id_retry_test`
  (`consumer/test/grind_consumer_test.gleam`; marker
  `consumer-submit-with-id-retry-passed`), public-imports only.
- **This is new code**: `postgres.submit_with_id` and the "no policy"
  (`policy: None`) path through `grind/internal/unique_admission` did not
  exist before this change, so there is no red-before-green baseline —
  proven instead by mutation, and by a real forced fault for the
  reply-lost/concurrency claims.
- **Post-review unification**: after independent review, the initial
  implementation's parallel `PlainRequest`/`plain_fingerprint`/`run_plain`/
  `plain_admission_transaction`/`insert_plain_job` functions were deleted.
  `grind/internal/unique_admission` now has exactly one `Request` type
  (`policy: Option(PolicyPart)`, `Some` for `submit_unique`'s uniqueness
  policy, `None` for `submit_with_id`), one `fingerprint` function (tag and
  policy-specific fields chosen by `policy`), and one `run`/
  `admission_transaction`/`insert_job` used by both `submit` and
  `submit_plain` — the domain-wide advisory lock and candidate selection run
  only when `policy` is `Some`; `record_receipt` takes a small `ReceiptWrite`
  record instead of 13 positional arguments. This is a pure refactor with no
  intended behavior change: the fingerprint envelope's field order and
  content are byte-for-byte identical to the pre-unification version for
  both the `Some` and `None` cases (verified by inspection of `fingerprint`'s
  field list against the deleted `fingerprint`/`plain_fingerprint`
  functions), and the full suite (below) confirms every pre-existing
  `submit_unique` test still passes unchanged after unification.
- **Committed-reply-lost fault injection**: identical mechanism to
  Increment 11 (`install_syncrep_reply_trigger`, generalized to any table
  and predicate), scoped by `NEW.submission_id = '<this test's submission
text>'` on `grind_unique_submissions` — the same receipt table
  `submit_unique` uses, since `submit_with_id` reuses `record_receipt`
  directly. The disposable cluster's `synchronous_standby_names =
grind_never_standby` / `synchronous_commit = local` configuration lets the
  test's own transaction raise `synchronous_commit` to `on` just before
  `COMMIT`, parking it in `SyncRep` after the WAL record is already locally
  flushed (genuinely committed, reply not yet sent); terminating that
  backend (found by polling `pg_stat_activity` for `wait_event = 'SyncRep'`,
  never a fixed sleep) reproduces "committed, but the client's connection
  closed before it saw the reply." `submit_with_id` itself still returns
  `Ok(Inserted(handle))` directly — the shared `run`'s follow-up receipt
  lookup on the `TransactionQueryError` branch resolves it, the exact same
  code `submit_unique` runs through.
- **Concurrent-overlap fault injection, same-input**: the same `BEFORE
INSERT` barrier trigger Increment 8 uses (`install_unique_insert_barrier`,
  scoped by `worker_id`, blocking behind a held `pg_advisory_xact_lock`),
  applied to the "no policy" path's own `grind_jobs` insert. Two
  `submit_with_id` callers, identical `SubmissionId` and request, are
  launched concurrently while a third process holds the barrier's lock;
  `await_overlap_shape` polls `pg_stat_activity` (never a fixed sleep) until
  both callers are genuinely parked on that lock (`wait_event = 'advisory'`,
  matching the "no policy" insert's own query text) before the barrier is
  released. Unlike a `Some` (`submit_unique`) request, a `None` request
  acquires no domain-wide advisory lock (see
  `docs/UNIQUENESS-CONTRACT.md`, "Admission receipts", for the full
  justification), so both callers reach their own `grind_jobs` insert;
  whichever the barrier releases first commits its row and receipt, and the
  other's own `record_receipt` then hits a real `23505` against that
  just-committed row. The shared `run`'s
  `TransactionRolledBack(SubmissionConflict)` arm resolves this by
  re-reading the exact same receipt (`reconcile_from_receipt`) rather than
  surfacing a bare `SubmissionConflict` for what is, from that caller's own
  perspective, an ordinary successful retry — both callers observe
  `Ok(Inserted(handle))` with the identical job id, and exactly one row is
  ever persisted (`count_jobs_in_queue`).
- **Concurrent-overlap fault injection, different-input**: the identical
  barrier setup, but the two concurrent callers submit _different_ inputs
  (`42` vs. `99`) under the same `SubmissionId`. Whichever commits first is
  `Inserted`; the other still hits the same real `23505`, but this time
  `reconcile_from_receipt`'s fingerprint check does not match (a different
  `encoded_input` produces a different `request_sha256`) — it stays
  `SubmissionConflict` rather than converging, exactly like a sequential
  different-input-same-id retry, and exactly one row is ever persisted
  (`postgres_submit_with_id_concurrent_different_input_conflict_test`).
- **Mutation 1 (fingerprint envelope)**: the shared `fingerprint`'s `None`
  branch had `json.string(request.encoded_input)` removed (so two different
  inputs under the same `SubmissionId` would fingerprint-match). Against a
  fresh disposable cluster: 170 passed, 1 failure — exactly
  `postgres_submit_with_id_different_input_same_id_conflict_test`, no
  collateral damage (in particular, no `submit_unique` test observed this
  change, since its own `Some` branch of `fingerprint` was untouched).
  Reverted via `Edit`.
- **Mutation 2 (concurrent-race convergence, re-verified after
  unification)**: the `Ok(Error(pog.TransactionRolledBack(unique.SubmissionConflict)))`
  arm was removed from the now-shared `run`'s match (used by both `submit`
  and `submit_plain`), leaving only the `TransactionQueryError` fallback.
  Against a fresh disposable cluster: **170 passed, 1 failure — exactly and
  only `postgres_submit_with_id_concurrent_same_id_one_row_test`**; every
  `submit_unique` test, including its own forced-overlap and contention
  tests (Increments 8–13), still passed. This empirically confirms the
  "harmless on the unique path" claim in `docs/UNIQUENESS-CONTRACT.md`,
  "One admission transaction, not two": `submit_unique`'s own domain lock
  already prevents a losing same-key concurrent submitter from ever
  reaching this arm in the first place (its `find_receipt` resolves before
  `record_receipt` is ever attempted), so removing the arm from the now
  _shared_ function still only breaks the "no policy" path that actually
  depends on it. Reverted via `Edit`.
- **Aborted-commit coverage**: no separate aborted-commit
  (`pg_sleep`-during-`COMMIT`) test was written for `submit_with_id`. This
  is not "inherited by code sharing" in the sense of two similar but
  separate implementations happening to behave the same way — after the
  post-review unification above, `submit_with_id` and `submit_unique` run
  through the literal same `run`/`admission_transaction` function objects,
  differing only in `Request.policy` (`None` vs. `Some`), and an aborted
  commit is caught by the `pog.TransactionQueryError` branch before
  `admission_transaction` itself ever inspects `policy` (`transaction_or_checkout_failure`'s
  own classification happens above and outside the `policy` branch
  entirely). `postgres_submit_unique_aborted_commit_is_commit_unknown_test`
  (Increment 11) already forces and proves this exact branch, for the
  identical code `submit_with_id` runs through — the claim for
  `submit_with_id` is proven by construction (same code, no `policy`-
  dependent branch between the fault and its handling), not merely assumed.
  No dedicated `submit_with_id`-specific aborted-commit test was added on
  top of that, since it would exercise no additional code path.

### Full-suite confirmation (fresh disposable clusters, all mutations reverted)

`nix develop --command gleam test` against fresh disposable PostgreSQL
clusters started the same way `scripts/test-postgres.sh` configures its own
(`synchronous_standby_names=grind_never_standby`,
`synchronous_commit=local`), with `GRIND_TEST_DATABASE_URL`,
`GRIND_TEST_QUEUE_DATABASE_URL`, `GRIND_TEST_REPEATABLE_READ_URL`, and
`GRIND_TEST_QUARANTINE_URL` set: 171 passed, 0 failures, including all nine
new tests above (two more than the pre-unification round: the
different-input concurrent race and the `submit_with_id` admitted
observation). See the gate run recorded at the end of this document (or the
coordinator's own `scripts/test-postgres.sh` run) for the full
official-script confirmation, external-consumer package, and pinned Oban
oracle harness together.

## Increment 15 — Acknowledgement deadline: a real fault proxy, a Grind-owned checkout deadline, and three defects it surfaced

Answers the release-readiness open item verbatim: "Verify with a TCP-proxy
fault test whether pog/pgo bounds a `COMMIT` on a half-open socket." Source
reading alone (confirmed against `build/packages/pog/src/pog.gleam`,
`pog_ffi.erl`, and `build/packages/pgo/src/pgo_pool.erl`) had suggested
`pgo_pool`'s own absolute checkout-deadline timer (armed at checkout time,
independent of what statement is later in flight) would bound a stuck
connection at pog's hardcoded, unconfigurable 5000ms — but that mechanism is
an implementation detail of a dependency two levels down, not a documented
contract, and per-call it never applies uniformly (a single-connection
`pog.execute`/`query_extended` call ignores `Query.timeout` entirely once
checked out). This increment builds the proxy, proves the source reading
empirically, then makes the bound a first-class, Grind-owned, configurable
setting instead of an accident of pog's internals.

### The fault proxy (`test/grind_fault_proxy.erl` + `test/fault_proxy.gleam`)

A small Erlang TCP relay sits between a test `Database` and the real
disposable cluster (new database `grind_fault_proxy`,
`GRIND_TEST_FAULT_PROXY_URL`, wired into `scripts/test-postgres.sh`). A
controller process owns the listen socket and a one-shot arming
(`pass | {armed, on_commit | on_begin | {on_sql, Pattern}, drop_reply |
drop_request}`); each accepted connection gets its own relay process owning
both sockets (`{active, once}`), detecting the extended-query-protocol
`Parse` message for an unnamed `begin`/`commit` statement by the byte
pattern `<<0, "commit", 0>>`/`<<0, "begin", 0>>` (pog always sends these
lowercase), or an arbitrary caller-supplied substring for a specific
statement (`{on_sql, Pattern}`, used for the lease-renewal `UPDATE`).
`drop_reply` forwards the triggering request (it genuinely executes) then
silently discards every reply afterward; `drop_request` never forwards the
triggering request, or anything after it, at all. Neither ever closes a
socket — a true half-open fault, not a disguised close (unlike
`pg_terminate_backend`, which the existing `postgres_ack_committed_reply_lost_*`
tests already use and which closes the _server_ side, letting the client
observe a fast, clean close rather than genuine silence).

Two real bugs were found and fixed building this harness, both confirming
Erlang basics rather than anything Grind-specific: `gen_tcp:accept/1` makes
the _acceptor_ the accepted socket's controlling process, so the spawned
relay process never received its `{tcp, ...}` messages until ownership was
explicitly transferred via `gen_tcp:controlling_process/2` before the relay
touched the socket; and the harness's own `start/2` return shape had to be
`{ok, {Controller, Port}}` (a 2-tuple `Result` payload), not a flat 3-tuple,
to match the Gleam `Result(#(Proxy, Int), Nil)` it is declared as.
`fault_proxy_pass_through_test` proves the harness itself first: a `Database`
behind the proxy connects, authenticates, and runs the full multi-statement,
multi-transaction `migrate` and one real job end to end — byte-for-byte
transparent relaying, not just "a socket accepted a connection" — before any
other test relies on it to prove something about Grind.

### T1–T5: verification against the code as found (before this increment's fix)

All five ran against the code exactly as it stood before any FFI change in
this increment (pog's own hardcoded, unconfigurable checkout deadline was
the only bound in effect). Every one is **bounded**, confirming the source
reading — but only via a side effect of pgo's internals Grind never asked
for and could not tune:

| Test | Fault                                                                            | Result before this increment                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ---- | -------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| T1   | `drop_reply` on manual ack `COMMIT`                                              | bounded, **~5012ms**, `acknowledge_claim` itself returned `Ok(True)` (pgo's checkout-deadline timer force-closed the socket; the client-observed `closed` maps through pog's `convert_error` to `query_timeout`, but the coordinator's own retry-on-`QueueAckUnknown` path — the exact one the acknowledgement side already relies on — resolved it inline before this call even returned)                                                                                                                                   |
| T2   | `drop_request` on manual ack `COMMIT`                                            | bounded, **~5005–5010ms**, `Error(QueueAckUnknown(...))` — the request never reached PostgreSQL, so pgo's checkout deadline (armed at checkout time, independent of the half-open socket) still force-closed the _client_ side on schedule; the _server_ session was left genuinely idle in transaction holding the row lock, requiring this test's own observer backstop (`pg_terminate_backend`) to clear it before a retried `acknowledge_claim` could succeed                                                            |
| T3   | automatic, `maximum_concurrency: 2`, A's ack `COMMIT` `drop_request`'d           | B (unaffected sibling) succeeded in **~5021ms**; A converged to `Succeeded` via the automatic pending-ack retry in **~15029ms** total, but only after this test's own observer backstop cleared A's stuck idle-in-transaction backend (no server-side timeout existed yet to do it automatically)                                                                                                                                                                                                                            |
| T4   | `drop_reply` on the coordinator's own lease-renewal `UPDATE` (not a transaction) | bounded, **~5000ms**, `Error(QueueClaimFailed(pog.QueryTimeout))`                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| T5   | `drop_reply` on manual ack's implicit `BEGIN`                                    | bounded, **~5011–5014ms**, `Error(QueueAckUnknown(...))` — `pog.transaction`'s own `use _ <- result.try(do(conn, "begin"))` short-circuits before the callback (the actual ack work) ever runs, so no receipt exists and the job stays `Executing`, exactly like a `TransactionQueryError` at any other step; pog exposes no way to tell "BEGIN failed" from "COMMIT failed after the callback ran", so Grind conservatively reports `QueueAckUnknown` for both rather than the more precise (but unproven) `QueueAckFailed` |

**Verification answer: bounded, but only by an accident of a dependency two
levels down** — pog's own hardcoded 5000ms, never surfaced as
`postgres.Settings`, never validated against `unique_lock_wait_ms`, and
silent about the one non-transactional call shape (T4) it also happens to
cover only because that call still goes through a pool checkout.

### DEFECT 2 probe: attempted, not reproduced — fixed anyway

A dedicated, no-proxy-needed test (`fault_proxy_defect2_queue_deadline_test`)
holds a `pool_size: 1` pool's sole connection with `SELECT pg_sleep(8)` (its
own `pog.timeout` raised so _that_ checkout's own deadline does not fire
first), then issues a single contended `postgres.state` call, and — finding
that pgo's CoDel-style overload shedding returned the already-handled
`none_available` (→ `ConnectionUnavailable`) well before the narrower race
this defect targets — a second version bursts eight concurrent contended
callers instead. Both variants, run repeatedly against a real cluster,
consistently observed graceful `Error(StateQueryFailed(ConnectionUnavailable))`
for every caller (~2675–2690ms), never the `error:function_clause` crash
`pog_ffi:convert_error/1`'s missing clause for `pgo_pool`'s "connection not
available because deadline reached while in queue" string (confirmed to
exist in `pgo_pool.erl`'s `checkout_info/2`) would raise. The fix (below)
is applied regardless, on the strength of the source-level confirmation, not
this empirical attempt — CoDel's overload-shedding heuristic evidently wins
the race under ordinary contention in this environment, but nothing
guarantees it always will (heavier concurrent load, a different
`queue_target`/`queue_interval`, or a genuine network partition instead of
local contention could still hit the narrower window). Per independent
review, the test now _asserts_ every contended caller's own monitor never
reports an abnormal exit (`panic` if one does), rather than printing
"CONFIRMED" and letting the test pass regardless of what it observed — a
real defect reproduced this way would now fail the test, not just narrate
itself into the log.

### Decision 1: a Grind-owned checkout deadline (`src/grind_postgres_ffi.erl`)

Every Grind storage call already funneled through one of four Erlang
wrappers (`execute_safely/2`, `call_safely/2`, `transaction_safely/2`,
`transaction_or_checkout_failure/2`). Each now checks out its own connection
directly via the public `pgo:checkout/2` (`pgo.erl`'s own arity-2 form,
`checkout(Pool, Options) -> pgo_pool:checkout(Pool, Options)`) with an
explicit, Grind-chosen `{timeout, DeadlineMs}` option — _not_ the same
function pog itself calls to check out a `{pool, Name}` connection
(`pog_ffi:checkout/1`, which always calls the arity-1 `pgo:checkout/1` with
no options at all, so `pgo_pool`'s own `?TIMEOUT` constant, 5000ms,
applies unconditionally and unconfigurably) — and runs the actual work
against the pog `Connection` shape `{single_connection, Conn}` (confirmed by
reading the compiled `pog.erl`/`pog_ffi.erl`: `pog.transaction`'s
`SingleConnection` branch calls `transaction_layer` directly with no further
checkout, and `pog.execute`'s `{single_connection, _}` branch calls
`pgo_handler:extended_query` directly) — so `pog:execute`/`pog:transaction`
never re-checkout with pog's own unconfigurable default again. The deadline
is attached to a pool by its atom name (`postgres.Database`'s
`pog.Connection` is always `{pool, PoolName}` at this boundary) via
`persistent_term`, set once in `postgres.start` and cleared in
`postgres.close`, rather than threaded as an explicit parameter through
every one of Grind's ~40 call sites; `call_safely`'s own Gleam signature
changed from a zero-arity closure to `fn(connection, fn(connection) ->
result)` so the closure actually runs against the deadline-checked-out
connection instead of the original (possibly still-a-pool) one. A dedicated
`migration_transaction_safely/3` takes an explicit deadline
(`Settings.migration_deadline_ms`, default 30000ms) instead of the shared
per-pool one, since a schema migration's DDL step can legitimately need
longer than an ordinary job-lifecycle statement.

`postgres.Settings` gained `statement_deadline_ms` (default 4000, setter
`postgres.statement_deadline`) and `migration_deadline_ms` (default 30000,
setter `postgres.migration_deadline`), both validated positive by
`postgres.validate`. This also structurally fixes half of DEFECT 2: since
Grind's own wrapper now always checks out first and never lets pog's
`{pool, Name}` branches run their own checkout, the exact call sites
`pog_ffi:convert_error/1`'s missing clause could previously crash from
(`pog_ffi:checkout/1`, `pgo:query/3`'s pool path) are no longer reached at
all through Grind.

The _other_ half — a post-checkout query on an already-`{single_connection,
_}` connection still calling `pgo_handler:extended_query`, which can still
return an error shape `convert_error` has no clause for (`econnreset`/
`etimedout`, distinct from the `closed` shape it does handle) — is defended
by a `guarded_query`/`guarded_transaction` wrapper around the actual call.
Per independent review, this catch is narrowed to a `function_clause`
crash whose own top stack frame is genuinely `pog_ffi:convert_error`
(`is_convert_error_crash/1`, checked against the captured stacktrace), so an
unrelated `function_clause` bug elsewhere in the callback still crashes its
caller instead of being silently absorbed into an endless `QueueAckUnknown`
retry. It is mapped to `query_timeout` (an _uncertain_ outcome — the same
one a genuine query timeout already produces, feeding the existing
`QueueAckUnknown`/retry path), not `connection_unavailable`: the crash
happens _after_ the request was already sent, so an `econnreset`/`etimedout`
on `recv` gives no proof PostgreSQL never received or applied it, unlike a
checkout failure (nothing was ever sent, correctly still
`connection_unavailable`, unchanged, in `with_deadline_ms`'s own checkout-
failure branch). Because `Conn`'s own protocol state is unknown after a
crash mid-decode, it is `pgo:break/1`'d (disconnected and replaced) before
being checked back in, rather than risking a corrupted connection being
handed to the next caller.

`gleam.toml` now pins `pog = ">= 4.1.0 and < 4.2.0"` (tightened from
`< 5.0.0`) and declares `pgo` as a direct dependency (`>= 0.20.0 and <
0.21.0`) rather than relying on it only being present transitively through
pog's own `rebar.config` — this wrapper depends on `pog.Connection`'s exact
`{pool, _} | {single_connection, _}` compiled shape and on `pgo`'s own
`checkout/2`/`checkin/2`/`break/1` API directly, not just on pog's checkout
timeout being 5000ms. `pog_connection_pool_shape_test` (no database needed)
is a standing regression guard for the `{pool, Name}` shape specifically —
`pog.named_connection(name)` is pattern-matched against it directly, so a
future pog release changing that compiled representation fails this test
immediately instead of surfacing as a silent `function_clause` inside
`grind_postgres_ffi`'s own pattern matches.

### T1–T5 re-verified after the fix, plus the mutation

Same tests, same fault proxy, against the fixed code (default
`statement_deadline_ms` 4000):

| Test | Result after the fix                                                                                                                                            | Mutation (`deadline_for/1` forced to return `infinity`)                                                                                                                                                                                                     |
| ---- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| T1   | bounded, **~4011ms**, `Ok(True)`                                                                                                                                | **UNBOUNDED** — did not return within the test's own 20000ms bound (panics as designed); the `COMMIT` had already genuinely succeeded server-side, so the session is plain `idle`, not idle-in-transaction — DEFECT 3's backstop does not apply here either |
| T2   | bounded, **~4010ms**, `Error(QueueAckUnknown(...))`                                                                                                             | still bounded at **~8011ms** — _not_ via the (disabled) checkout deadline, but via DEFECT 3's independent `idle_in_transaction_session_timeout` (8000ms, `2 ×` the deadline) killing the genuinely-idle-in-transaction server session on its own            |
| T3   | B **~4021ms**; A converges automatically in **~14009ms** total — the observer backstop is no longer load-bearing (DEFECT 3 clears A's stuck backend on its own) | still converges — B **~8047ms**, A **~28033ms** — again via DEFECT 3's independent backstop, not the checkout deadline                                                                                                                                      |
| T4   | bounded, **~4000ms**, `Error(QueueClaimFailed(pog.QueryTimeout))`                                                                                               | **UNBOUNDED** — did not return within 20000ms; a single autocommit statement is never "in a transaction" at all, so DEFECT 3 cannot apply                                                                                                                   |
| T5   | bounded, **~4011ms**, `Error(QueueAckUnknown(...))`                                                                                                             | still bounded at **~8009ms** via DEFECT 3 (the dropped `BEGIN` reply still leaves a real, genuinely open transaction server-side)                                                                                                                           |

The mutation result is the more interesting evidence than a plain "still
passes": it shows the checkout deadline and DEFECT 3's server-side idle
timeout are two genuinely independent, complementary mechanisms, not one
disguised as two — T1 (already-committed, session idle) and T4 (never in a
transaction) have no exposure to `idle_in_transaction_session_timeout` at
all and are honestly unbounded once the checkout deadline is disabled, while
T2/T3/T5 (a real open transaction left stranded) are covered by _either_
mechanism on its own. Both are required for full coverage; neither
subsumes the other. The mutation's own collateral (T2 and T5 additionally
observed a transient `AckReceiptQueryFailed(QueryTimeout)` from
`reconcile_acknowledgement`/`reconcile_unique` immediately after DEFECT 3's
more abrupt server-initiated kill, where the real fix's own client-owned
deadline would have let the pool recover more gracefully on its own terms)
is not itself evidence of a defect in the real, unmutated code — it is
exactly the kind of degraded behavior the real fix exists to avoid, observed
only because the mutation removed it. Reverted via `Edit`.

**Tightened per independent review**: T1/T2/T4/T5 now _assert_
`elapsed < 2 * postgres.statement_deadline_ms(database)` (reading the
pool's own configured deadline, not a hardcoded constant), not merely
"returned at all before the test's own generous 20000ms outer wait" — and
T2's own "no reply within 20000ms" branch is now a hard `panic`, not a
printed observation, since after this fix that call must always be bounded
on its own. Mutation evidence that this tightened bound is not vacuous:
T1's pool temporarily given `postgres.statement_deadline(15_000)` while its
own assertion was temporarily hardcoded to the old default
(`elapsed < 2 * 4000`, simulating the exact mistake of forgetting to read
the deadline dynamically) — against a real cluster, `elapsed` came back
**~15015ms** (proving it genuinely tracks the configured deadline, not some
unrelated fixed timing) and the stale hardcoded bound correctly failed
(`15015 < 8000` is `False`). Reverted via `Edit`; re-confirmed green
afterward at `elapsed < 2 * 4000` with the real assertion restored.

### DEFECT 1: `unique_lock_wait_ms` racing the checkout deadline (red, then green)

Before this increment, `unique_lock_wait_ms` defaulted to 5000ms — the same
value as pog's own hardcoded checkout deadline. A new test using entirely
_default_ `postgres.settings` (no `unique_lock_wait` override —
`postgres_submit_unique_contended_lock_wait_default_settings_test`;
the existing `postgres_submit_unique_contended_lock_wait_test` always
overrode `unique_lock_wait` to 200ms and never exercised the shipped
default) holds the real domain lock in one connection while a second,
default-settings `submit_unique` call contends for it. Mutation for red
evidence: `unique_lock_wait_ms`'s default temporarily restored to 5000 and
`postgres.validate`'s new margin check temporarily disabled (`Edit`, not a
config toggle) — against a real cluster, the contended call returned
**`Error(CommitUnknown(PendingSubmission(...)))`**, not the typed
`AdmissionContended` a caller could actually branch on: with the checkout
deadline now smaller (4000ms) than the old lock-wait default (5000ms), the
connection was force-closed _before_ PostgreSQL's own `lock_timeout` error
(55P03, raised after `unique_lock_wait_ms`) ever had a chance to surface.
Reverted via `Edit` (`unique_lock_wait_ms` default back to 2000;
`postgres.validate`'s margin check restored). Green after: the same test now
deterministically observes `AdmissionContended`, no job row, no receipt.

The fix: `postgres.validate` now rejects any `Settings` where
`unique_lock_wait_ms + 1000 >= statement_deadline_ms`
(`UniqueLockWaitTooCloseToDeadline`), and the shipped defaults
(`unique_lock_wait_ms` 2000, `statement_deadline_ms` 4000) clear that margin
by construction — this is no longer a race a caller could reintroduce by
accident through the public API at all, not merely a better default number.

**The `+1000` margin is a heuristic, not a proof.** It covers the specific
race this defect names — PostgreSQL's own `lock_timeout` error surfacing
before the checkout deadline force-closes the connection — but
`statement_deadline_ms` also has to cover whatever time a caller's checkout
itself spends queued under real pool contention (bounded separately by
pgo's own overload shedding, not by this margin) and any statement the
admission transaction runs _before_ it ever takes the domain lock (`SET
lock_timeout`, `pin_read_committed`, the candidate read). Under genuine pool
contention — several callers checked out at once, not just one lock held —
the checkout deadline can still legitimately elapse before
`unique_lock_wait_ms` does, and `CommitUnknown`/`AdmissionFailed` is still a
possible outcome even with the margin satisfied; the margin only rules out
the _specific_, previously-undefended race where the two values were simply
too close together by construction.

### DEFECT 3: `idle_in_transaction_session_timeout`

`postgres.validate` now also sets `idle_in_transaction_session_timeout` to
`2 × statement_deadline_ms` (8000ms by default) as a pooled connection
startup parameter, alongside the existing `default_transaction_isolation`
pin. This is what actually clears a request whose _reply_ was lost but
which never even reached PostgreSQL (T2, T3, T5's `drop_request`/dropped-
`BEGIN` scenarios above): the checkout deadline bounds the _client_, but
without this setting the _server_ session is left genuinely idle in
transaction, holding whatever row locks that transaction already took,
until something else (an operator, or — before this fix — a test's own
`pg_terminate_backend` observer backstop) clears it. TCP keepalive
(`connection_parameters`' `tcp_keepalives_idle`/`_interval`/`_count`, or the
equivalent client-side socket options) is a real, complementary defense for
a genuine network partition (as opposed to a proxy that keeps the socket
open but silent) but is not exercised here — loopback gives no way to
distinguish a partitioned link from a merely idle one, and this deadline
work does not attempt it; see "Limits" below.

This setting is a connection _startup parameter_, so it applies to _every_
session Grind opens on the named pool — not only the sessions that happen
to run through `execute_safely`/`transaction_safely`/etc. A caller doing
its own raw `pog.transaction(pog.named_connection(pool_name), callback)`
directly (bypassing Grind's own storage functions entirely, as several
tests in this file do for setup/observation) gets the identical
`idle_in_transaction_session_timeout` on that same pool, whether or not
that particular call goes through Grind's deadline wrapper. See "No
poolers" under "Limits" below for the corresponding risk of a pooler
silently dropping this parameter.

### Lease rule: `queue.LeaseTooShortForDeadline`

`queue.start`/`queue.start_with_policy`/`_manual`/`_manual_with_policy` now
reject a lease too short relative to `database`'s own
`postgres.statement_deadline_ms` (`D`) before starting any process:
`lease_duration_ms < 6 × D` at `maximum_concurrency > 1`, or `< 1.5 × D` at
exactly 1 (`queue.minimum_lease_for_deadline`, `@internal`, exposed for this
reasoning to be checked directly; the full derivation now also lives in
`queue.LeaseTooShortForDeadline`'s own doc comment).

**Derivation.** A live attempt's own renewal timer fires every `L / 3`
(`renewal_interval_ms`), so once a renewal succeeds there is `(2 / 3) × L`
of slack before that same lease would otherwise expire. A stalled pending
acknowledgement can occupy the coordinator's single message loop for up to
roughly `3 × D` (`ConsumerState.pending_ack_retry_budget`, ~3 ticks), during
which a _sibling_ attempt's own renewal tick sits queued behind it before it
can even start; that queued renewal call is itself now bounded by `D`. At
`maximum_concurrency > 1`, the slack must cover both stages:
`(2 / 3) × L` at least `3 × D + D`, giving `L` at least `6 × D` — with _zero_
margin left at that exact minimum (the queued renewal starts the instant the
stall clears and takes the full `D` to finish, landing exactly at the
lease's own expiry). At `maximum_concurrency` of exactly 1 there is no
sibling to queue behind anything, so only the renewal's own `D` needs to
fit: `(2 / 3) × L` at least `D`, giving `L` at least `1.5 × D`, again with
zero margin at that minimum. Neither bound carries deployment headroom
beyond exact algebraic sufficiency; a real deployment should clear it with
real margin (the shipped `default_policy` lease of 30000 clears the
`maximum_concurrency > 1` minimum of `6 × 4000 = 24000` — the shipped
default `D` — by 6000ms, `1.5 × D` of headroom, not by design margin baked
into the rule itself).

**Default choice, decided and justified**: `statement_deadline_ms` defaults
to 4000, not pog's old 5000, specifically so `queue.default_policy`'s
existing 30000ms lease default clears `6 × D` (24000) at
`maximum_concurrency > 1` with headroom, without having to raise the lease
default itself (a lease is a worker-execution-time budget; tying it to a
storage-mechanics constant would conflate two different concerns). Lowering
`D` also let `unique_lock_wait_ms`'s own default drop from 5000 to 2000
while still clearing DEFECT 1's margin.

**A hard floor this rule collides with**: `unique_lock_wait_ms` must be a
positive integer, so DEFECT 1's `+1000` margin forces `statement_deadline_ms`
to at least 1002 for any pool at all (not just ones using uniqueness
features) — a pool cannot pick an arbitrarily small `D` to keep an
arbitrarily small lease valid. Eight existing tests using a short,
fast-iteration lease (a manually forced expiry, not a wall-clock wait, in
every case) collided with the lease rule once it shipped: each now sets its
own small, explicit `D` on its pool — `postgres.statement_deadline(1002)`
paired with `postgres.unique_lock_wait(1)` (the smallest `D` the DEFECT-1
margin permits at all, since none of these tests exercise uniqueness
contention and the exact wait value is otherwise irrelevant to them) —
and bumps its lease to the smallest value clearing the rule against that
`D`: 1600ms at `maximum_concurrency` 1 (minimum `1.5 × 1002 = 1503`), 6100ms
at `maximum_concurrency > 1` (minimum `6 × 1002 = 6012`). This trades a
comfortable default-`D` margin for a genuinely tight one: at `D = 1002`,
_any_ Grind storage call on that pool — including time spent queued for a
connection under contention, not just the query itself — that happens to
take longer than 1002ms now also times out on that same pool, and its
`idle_in_transaction_session_timeout` is `2 × 1002 = 2004ms`. These tests
were chosen deliberately for a small `D` specifically because none of them
hold a connection under real contention or run a genuinely slow statement,
so 1002ms is expected to be ample in practice — but a sufficiently loaded
CI host (scheduler jitter, GC pauses, or contention from tests running
in the same suite) could in principle push an ordinary call past that
window and produce a flake distinct from anything this increment set out to
fix; watch this specific 1002ms/6100ms combination first if any of these
eight tests (or the consumer package's `run_effect_crash_uncertainty_test`,
same treatment) ever becomes intermittent. One of the eight
(`postgres_automatic_ack_retry_bounded_eventually_uncertain_test`) also
needed its own bounded retry-loop iteration cap raised (40 → 800) to match:
the coordinator's own retry _budget_ (~3 ticks) is unaffected by the lease's
absolute size, but each tick now fires roughly 4× further apart in
wall-clock terms (`lease / 3`, 6100/3 versus the old 300/3), so the same
number of _ticks_ now needs a proportionally larger _iteration_ budget to
observe them all. The consumer package's own
`run_effect_crash_uncertainty_test` needed the same treatment (its own pool
also set to `D = 1002`/`unique_lock_wait = 1`, lease bumped 500 → 6100) and
a correspondingly larger `await_state` budget (250 → 400 checks).

**Known limit, unchanged by this rule**: the derivation above assumes at
most one stalled acknowledgement ahead of one sibling's renewal. At
`maximum_concurrency > 2`, more than one sibling's renewal can queue up
behind the same stall, each also waiting out however many other siblings'
own `D`-bounded renewals are queued ahead of it — `3 × D + (N − 1) × D` for
the `N`-th sibling in that queue, not the `3 × D + D` this rule accounts
for. The real fix is moving lease renewals off the coordinator's own
message loop entirely — tracked in `docs/RELEASE-READINESS.md`, "Decide on
per-attempt storage calls" (not attempted here).

### The flaky `postgres_submit_unique_aborted_commit_is_commit_unknown_test`

Its own `QueryTimeout` flake was never really about the 5000ms deadline
directly — it is the same "pool just had a connection deliberately
terminated" transient recovery window every other terminate-then-retry test
in this file already defends against with `retry_transient_query`
(`run_ack_commit_connection_loss_test`'s own pattern, documented above in
"Isolation-level pinning"/Increment 2): `pgo_connection`'s own supervised
restart of the just-killed connection is not instantaneous, and this one
test's post-recovery retry (`submit_keep_existing`) and read
(`postgres.arguments`) were the only ones in the file calling straight
through without that tolerance. Wrapped both in the existing
`retry_transient_query` helper (20 attempts, 50ms apart) — the same
mechanism, not a bespoke one. The _same_ class of gap was independently
found (not asked for by name, but the same root cause) in
`postgres_resolved_observation_absent_on_commit_unknown_test`'s own
post-termination `postgres.state` read and sentinel `resolve_uncertain`
call, immediately after its own `resolve_uncertain`-`COMMIT` aborted-commit
scenario; fixed the same way. Confirmed deterministic across multiple
fresh-cluster reruns after the fix (previously intermittent depending on how
long `pgo_connection`'s own restart took relative to the very next call).

A third, unrelated `Undef` failure was found and fixed along the way:
`postgres_call_safely_wrapper_reports_closed_pool_test` declared its own
local `@external(erlang, "grind_postgres_ffi", "call_safely")` probe binding
at the old arity-1 (zero-argument closure) signature, which became
undefined the moment `call_safely`'s real Erlang arity changed to 2 for
Decision 1 above — updated to the new `fn(connection, fn(connection) ->
result)` shape.

### Limits

- **Loopback only.** The fault proxy runs on `127.0.0.1`; no real network
  partition, packet loss, or asymmetric latency is exercised, only
  in-process byte manipulation on an otherwise-healthy local TCP stream.
- **TLS untested.** Every test database connects with `sslmode=disable`;
  the proxy relays raw bytes and has no TLS termination or passthrough
  logic, so an `sslmode=require` deployment is not covered by any of this
  evidence.
- **A connect-time hang is not covered.** Every fault here is injected
  _after_ a real connection is already established; `gen_tcp:connect`
  itself blocking indefinitely against an unresponsive (not
  connection-refusing) host is a distinct, unaddressed gap — `pog`'s own
  connection-establishment path has no deadline of its own either, and
  Grind's new checkout deadline only bounds _checkout_ (acquiring an
  already-connected pooled connection), not the pool's own initial
  connect.
- **DEFECT 2's exact crash shape remains unproven, not disproven.** See
  above — the fix is applied on source-level confirmation of the missing
  `convert_error` clause, not empirical reproduction.
- **No poolers.** A connection pooler (PgBouncer or similar) between Grind
  and PostgreSQL is not exercised; `idle_in_transaction_session_timeout` as
  a startup parameter, in particular, could be silently dropped by one
  (the existing `default_transaction_isolation` pin's own doc comment
  already flags this same class of risk, which is why the admission
  transaction additionally pins isolation level in-transaction as defense
  in depth — no equivalent in-transaction fallback exists for an
  idle-session timeout, since by definition nothing is running to set it
  when the session goes idle).
- **Concurrent pending acknowledgements**, not just one at a time, can
  still starve sibling lease renewals even once the new lease rule passes
  — see "Lease rule" above.
- **"Bounds every Grind storage call" has two carve-outs.** The checkout
  deadline bounds a call only once a pooled connection is checked out; it
  does not bound the pool's own initial connect (see the connect-time-hang
  limit above), and a checkout that has to queue behind other contended
  callers is bounded by pgo's own overload-shedding heuristic, not by `D`
  directly (see the DEFECT 1 margin caveat above and the DEFECT 2 probe,
  where queued contention resolved in ~2.7s regardless of `D`).
- **`postgres.close` stops the pool before clearing its deadline**, not the
  reverse — reordered from the first version of this change specifically to
  close this gap: clearing the deadline first would leave a window where an
  in-flight checkout reads `persistent_term`'s fallback default (5000ms)
  instead of the configured one, between the erase and the pool actually
  stopping. Stopping first means any such checkout fails via the same
  `pgo_pool` exit `with_deadline_ms` already catches, regardless of which
  deadline it would otherwise have used.

### Full-gate confirmation

`nix develop --command bash scripts/test-postgres.sh` against a fresh
disposable PostgreSQL cluster (own `initdb`, own port, torn down on exit):
grind's own `gleam test` **180 passed, 0 failures** (170 pre-existing +
`fault_proxy_pass_through_test`, T1–T5, the DEFECT 2 probe, the new
DEFECT-1 default-settings contention test, and `pog_connection_pool_shape_test`),
the pinned Oban oracle harness, and the external-consumer package's own
`gleam test` **10 passed, 0 failures**, run together end to end — confirmed
across the increment's own fixes and again, once more, after the
independent-review round above (the narrowed/re-mapped/`pgo:break`-ed
post-checkout catch, the corrected lease-rule and lease-bumped-test
derivation text, the tightened fault-proxy elapsed-bound assertions and
their own mutation evidence, the `pgo:checkout/2`/tightened dependency
pins, the `pog_connection_pool_shape_test` regression guard, and the
`postgres.close` ordering fix).

## Increment 16 — Migration mechanism: versioned steps, advisory lock, upgrade harness

Replaces the single-shot, fresh-install-only `postgres.migrate` (one
`pog.transaction` wrapping the whole v11 DDL, no lock, no step concept) with
`migrate_with(database, steps)` — a real forward-only runner over
`grind/internal/migrations.migrations()`, one transaction per version, an
advisory lock, and a re-read-and-skip check per step (`postgres.migrate` is
`migrate_with(database, migrations())`). `priv/migrations/*.sql` mirrors
`migrations()` in cigogne's own file format, proven in lockstep by a
no-database conformance test using cigogne's own parser
(`grind_migrations_conformance_test`).

### Red evidence: concurrent migrators (pre-fix code, commit `aa54b60`)

The pre-fix `migrate` has no locking at all: `read_schema_generation` decides
`FreshSchema`, then one `pog.transaction` runs the entire v11 DDL — two
concurrent callers against the same empty schema race each other's
unlocked `CREATE TABLE`/marker `INSERT` statements with nothing serialising
them.

Proven empirically, not just reasoned about: a disposable `git worktree` was
checked out at `aa54b60` (this repository's `HEAD` immediately before this
increment), given a minimal standalone test that spawns two concurrent
`postgres.migrate(database)` calls against the same fresh, empty schema and
waits for both, and run against a fresh throwaway PostgreSQL cluster. Result:

```
#(Ok(Error(MigrationQueryFailed(ConstraintViolated(
    "duplicate key value violates unique constraint \"pg_type_typname_nsp_index\"",
    "pg_type_typname_nsp_index",
    "Key (typname, typnamespace)=(grind_schema_migrations, 2200) already exists.")))),
  Ok(Ok(Nil)))
should equal
#(Ok(Ok(Nil)), Ok(Ok(Nil)))
```

One of the two concurrent full-install calls loses the race on PostgreSQL's
own `pg_type` catalog uniqueness constraint (both attempted `CREATE TABLE
grind_schema_migrations` at once) and returns `MigrationQueryFailed`, exactly
as expected — the gap the advisory lock in `migrate_with`'s runner closes.
The equivalent test in the fixed code
(`postgres_migrate_concurrent_migrators_both_apply_once_test`,
`test/grind_test.gleam`) holds a real observer on the exact advisory-lock key
`migrate` uses, spawns the same two concurrent callers, and proves both
`Ok(Nil)` and exactly one marker row once the lock is released — confirmed
green in the full gate below. The worktree, its throwaway probe test, and its
throwaway cluster were all removed after capturing this; nothing from that
worktree is part of this change.

### Partial-failure step resumption (new behaviour; no pre-fix equivalent)

`migrate_with`'s per-step, resumable transaction semantics have no analogue
in the pre-fix single-transaction `migrate`, so there is no meaningful "red
on old code" run for this one — the old code has no step or resumption
concept to exercise. Instead,
`postgres_migrate_with_partial_failure_preserves_earlier_steps_test` proves
the new behaviour directly against a real fresh cluster: `migrate_with(real
++ [synthetic v12 ok, synthetic v13 failing via a `ddl_command_end` event
trigger targeting v13's own object])` returns `MigrationStepFailed(13, _)`,
leaves markers at exactly `{11, 12}`, v12's own object present and v13's
absent (its own transaction rolled back cleanly); removing the trigger and
re-running the identical step list resumes to `{11, 12, 13}` — proving
earlier committed steps are untouched by a later step's failure and a
corrected re-run picks up exactly where it left off.

### Three genuine defects this increment's own gate runs caught

None of these were deliberately injected mutations — they were caught by
simply running the real, official gate (`scripts/test-postgres.sh`) against
a fresh cluster after writing the new tests, which is itself evidence the
new assertions are not vacuous:

1. `grind_catalog_digest`'s `pg_constraint` query concatenated
   `contype` (PostgreSQL's `"char"` pseudo-type) directly with `text` via
   `||`, which PostgreSQL rejects as `42725 ambiguous_function` ("operator
   is not unique: text || \"char\"") — the whole upgrade-harness test failed
   with that error before any of its actual assertions ran. Fixed by an
   explicit `contype::text` cast.
2. The upgrade-harness smoke test asserted
   `postgres.outcome(..) == Ok(job.SucceededWith("42"))`, copied from a
   different, incrementing test worker; the smoke worker here returns its
   input unchanged, so the real result was `SucceededWith("41")` — a
   `should.equal` panic caught it immediately. Corrected to `"41"`.
3. The same test's quarantine-smoke step expected
   `queue.process_one(consumer) |> should.equal(Ok(False))` (nothing
   legitimately claimed, only the deliberately-expired job quarantined), but
   got `Ok(True)`: an earlier `submit_unique` call in the same test had left
   its own row genuinely still queued and claimable, and `process_one`
   correctly claimed and ran _that_ job in the same poll it also quarantined
   the expired one. Fixed by draining the unique-submission job first, so
   the quarantine check's own poll has no other legitimately claimable work
   competing in it.

### Full-gate confirmation (Round 1)

`nix develop --command bash scripts/test-postgres.sh` against a fresh
disposable PostgreSQL cluster (own `initdb`, own port, torn down on exit),
after the three fixes above: grind's own `gleam test` **184 passed, 0
failures** (180 pre-existing + `grind_migrations_conformance_test`,
`postgres_migration_future_version_precedes_shape_check_test`,
`postgres_migrate_concurrent_migrators_both_apply_once_test`,
`postgres_migrate_with_partial_failure_preserves_earlier_steps_test`, and
`postgres_migrate_upgrade_from_frozen_v11_fixture_test`), the pinned Oban
oracle harness, and the external-consumer package's own `gleam test` **10
passed, 0 failures**, run together end to end, all required contract markers
present.

### Full-gate confirmation (Round 2, after the fixes above)

Same command, same fresh-cluster discipline: grind's own `gleam test`
**186 passed, 0 failures** (184 Round 1 + `postgres_migrate_detects_missing_relation_in_declared_shape_test`
and `postgres_migration_quotes_mixed_case_schema_name_test`), the pinned
Oban oracle harness, and the external-consumer package's own `gleam test`
**10 passed, 0 failures**, all required contract markers present (including
the two new ones, `migrate-missing-relation-shape-detected` and
`migrate-mixed-case-schema-no-op-passed`). `gleam check` clean in both the
root package and `consumer/`; `nix fmt`/`nix flake check`/`git diff --check`
all clean.

### Round 2 — coordinator review: exact-shape model, quoting, and a broader upgrade harness

A second review round ("ACCEPT WITH FIXES") required five changes plus a
list of smaller items. All applied, gated green; the two genuinely
consequential findings — one demanded by the review, one found empirically
while implementing it — are detailed below.

**1 (required). "Trusted from the marker alone" removed.** A step is now
`migrations.Migration(version, statements, shape)`, where `shape` is the
version's own _cumulative_ expected set of `grind_`-prefixed relations
(name + kind) plus any required key columns — generalising the old
`v11_shape_matches`/`read_unique_key_columns` pair into one mechanism
`read_schema_generation` applies to _every_ version, not just 11.
`migrate_with` validates its own `steps` argument is exactly the contiguous
range `{11..latest}` (`let assert`, a precondition on the caller — a
malformed list is a programming error, not a runtime condition). After a
step's own statements run, `run_migration_step_transaction` re-reads the
generation once more, in the same transaction, and requires it now reports
exactly `AtVersion(step.version)` before committing — catching a step whose
DDL ran without SQL-level error but produced the wrong shape or marker.

Red evidence, captured honestly (the fix was already drafted before this
review comment, so red evidence needed a deliberate temporary revert rather
than a fresh implementation): `validate_expected_shape` was edited in place
to reproduce the old "trusted from the marker alone" behaviour (`Ok(AtVersion(version))`
unconditionally, skipping the shape check), the new test
`postgres_migrate_detects_missing_relation_in_declared_shape_test` (install
`migrate_with(migrations() ++ [synthetic v12])`, drop the synthetic v12
table, migrate again) was run against it, and failed exactly as predicted:

```
test: grind_test.postgres_migrate_detects_missing_relation_in_declared_shape_test
Ok(Nil)
should equal
Error(IncompatibleSchema)
```

The revert was then undone (Edit, not git) and the test reran green.

**A genuine bug this same test then found in the real fix, before it ever
reached the coordinator**: the first version of `v11_shape()` listed only
the DDL's own explicit objects (5 tables, 1 sequence, 1 partial index) —
7 relations. Running the real v11 statements against a fresh database and
querying `pg_class` directly showed **14** `grind_`-prefixed relations:
PostgreSQL's own implicit objects — `grind_jobs_id_seq` (the `bigserial`
column's sequence) and one backing index per `PRIMARY KEY`/`UNIQUE`
constraint (`grind_schema_migrations_pkey`, `grind_jobs_pkey`,
`grind_job_resolutions_pkey`, `grind_job_acknowledgements_pkey`,
`grind_job_acknowledgements_attempt_key`, `grind_unique_submissions_pkey`)
— are also `grind_`-prefixed and are compared _exactly_, so the
under-declared shape rejected its own freshly-installed, entirely correct
schema as `IncompatibleSchema`. Confirmed by literally applying the DDL and
running `SELECT relname, relkind FROM pg_class WHERE relname LIKE
'grind\_%'` rather than reasoning from the DDL text; `v11_shape()` and the
two synthetic test migrations (whose own `id integer PRIMARY KEY` tables
each need their own implicit `_pkey` index declared too) were corrected
and reverified against a real cluster. AGENTS.md's "Adding a migration" now
says to confirm the real relation set empirically for exactly this reason.

**2. Schema-name quoting.** `schema_migrations_table_exists`'s
`to_regclass(current_schema() || '.grind_schema_migrations')` silently
folded an unquoted, mixed-case schema name to lower case, so `to_regclass`
looked up a schema that did not exist and wrongly reported the marker table
absent even when fully installed. Fixed with `quote_ident(current_schema())`.
Proven against a real database whose default `search_path` is a quoted
schema named `"MixedCase"` (`GRIND_TEST_SCHEMA_MIXED_CASE_URL`, configured
once via `ALTER DATABASE ... SET search_path`, exactly like the existing
`grind_repeatable_read_test` database's own isolation-level override, before
the pool ever connects): `postgres_migration_quotes_mixed_case_schema_name_test`
proves a second `migrate` call is a clean `Ok(Nil)` no-op.

**3. A discriminating future-version-ordering test.**
`postgres_migration_future_version_precedes_shape_check_test` now drops
`grind_unique_submissions` (breaking v11's own declared shape) _and_ adds a
`12` marker on top, still expecting `UnsupportedSchemaVersion(12)` — a
shape-first implementation would evaluate the now-broken v11 shape and
misreport `IncompatibleSchema` instead. The previous version of this test
(an unrelated foreign object, not a broken shape) could not have told the
two orderings apart.

**4. The advisory lock embedded in the migration files themselves.** Every
version's own `statements` list — both `migrations()` and
`priv/migrations/*.sql` — now starts with the identical
`pg_advisory_xact_lock` statement `migrate_with`'s own `acquire_migration_lock`
already runs; re-acquiring the same transaction-scoped lock twice in one
transaction is a no-op. This is what makes cigogne applying migrations
directly serialise against a concurrent `postgres.migrate` caller too. v11's
file content changed (the lock statement is new), so its pinned sha256 was
recomputed (`2B79E6CBD28A36850E31E1D69CC0C353D9CCFC4A4D8CEC688B0DC9C4ECEF17A0`)
and `test/fixtures/schema/v11.sql` regenerated from it — both unreleased, so
re-pinning in place is correct, not a released-file tamper. The conformance
test now also uses cigogne's own `config.get("grind")` (exercising the real
`priv/cigogne.toml`) instead of a hand-built config, requires a pinned
sha256 for every file except the newest, checks the marker statement
literally starts with `INSERT INTO grind_schema_migrations`, and asserts the
frozen fixture equals the pinned v11 migration's own `up` section.

**5. Doc wording.** `MigrationCommitUnknown`'s doc comment (and README's,
and `IMPLEMENTATION-SCOPE.md`'s) now names its three distinct
may-or-may-not-have-committed shapes (a checkout failure before `BEGIN` ever
ran, `BEGIN` itself failing, and a failed `ROLLBACK` after a statement
error) instead of only "`BEGIN`/`COMMIT` failed". `MigrationQueryFailed`'s
doc comment now also names lock acquisition and the `READ COMMITTED` pin.
`migration_deadline`'s own doc comment now says explicitly that it applies
_per step_, not to the whole `migrate`/`migrate_with` call. README now also
states the lock is transaction-scoped (not session-scoped, the wrong word
the first version of this section used), that an application must pick one
owner (Grind's own `migrate` or cigogne) for a given database's schema
rather than mixing them, and that `grind_v11`'s own `down` section is a
real, destructive drop of every Grind table's data.

**Also done, smaller items**: `READ COMMITTED` is now pinned as each
migration step's own literal first statement (defence in depth, reusing
`unique_admission`'s own `pin_read_committed` query); `read_schema_marker`
and the new `relation_has_columns`/`read_grind_relations` all map a
transient read failure to `MigrationQueryFailed`, never `IncompatibleSchema`;
`scripts/generate-sql.sh`/`test-postgres.sh` now strip `\r` before extracting
a migration file's `up` section and fail loudly if the extraction is empty;
the catalog-equality digest now also covers `column_default`,
`ordinal_position`, and `pg_sequences`; and the upgrade harness (below) was
substantially broadened.

### Upgrade harness, broadened

`postgres_migrate_upgrade_from_frozen_v11_fixture_test` now seeds queued,
scheduled, and retryable rows (previously only executing/uncertain/
succeeded), and seeds its uniqueness row through a real `submit_unique` call
against the pre-migration schema — never a hand-written `request_sha256` —
so its own `unique_key_contract`/`unique_key_sha256` columns and hash are
exactly what a genuine post-upgrade replay must still match. After
migrating, the test now exercises the _seeded legacy rows themselves_, not
only fresh new traffic: the seeded already-expired executing lease is
genuinely quarantined by a real legacy-queue `process_one` poll (proven via
a constructed `job.new_handle`, the `@internal` constructor the same package
already exposes for exactly this), the seeded uncertain row is resolved
(`resolve_uncertain`), the seeded acknowledgement receipt is reconciled by
its own real command ID (`reconcile_acknowledgement`), and the seeded
uniqueness submission is replayed (`submit_unique` again with the same
submission ID) and asserted to resolve to the same job ID.

Two real bugs surfaced getting this green, both fixed and reverified against
a real cluster:

- Every raw-seeded row and constructed handle originally used a literal
  `'upgrade-owner'` as `storage_owner` — but `storage_owner` is not a
  caller-chosen string, it is `host:port/database`, computed by
  `postgres.validate`. Every typed call against a constructed handle
  therefore failed with `StateStorageOwnerMismatch`. Fixed by reading the
  pool's own real value (`postgres.storage_owner(upgrade_database)`, already
  a public accessor other tests in this suite use) once and using it
  everywhere instead of the hand-written string.
- The quarantine-poll and unique-submission-replay checks each raced a
  _different_ legitimately-claimable job left `queued` by an earlier step in
  the same test (the seeded `queued` row for the first; the just-replayed
  uniqueness submission for the second) — `queue.process_one` correctly
  claimed that other due job in the same poll instead of the one the
  assertion cared about, exactly the same shape of bug Round 1 already found
  once. Fixed by draining each competing job first and, for the plain seeded
  `queued`/`scheduled`/`retryable` rows, seeding them in a separate queue
  from the ones the smoke checks actually poll.

`reconcile_unique` specifically remains a stated scope cut (see below) —
`reconcile_acknowledgement` is no longer one.

### Scope cuts, stated honestly

- The design's optional item ("cigogne applies the migrations end to end,
  then `postgres.migrate` is a no-op") was not attempted — skipped for time,
  as the design explicitly allowed.
- `reconcile_unique` was not exercised against the seeded unique submission:
  it takes a `unique.PendingSubmission`, a value only produced by a
  `submit_unique` call that itself returned a commit-unknown outcome after a
  genuinely lost reply — there is no "committed id" form of it to call
  against an already-decided receipt. Exercising it honestly needs its own
  lost-reply fault-injection rig (`test/grind_fault_proxy.erl` or
  equivalent), not attempted here. The uniqueness side of the upgrade
  harness is instead proven through a real `submit_unique` replay
  (idempotent re-admission by receipt), which is the mechanism
  `reconcile_unique` itself also reads.

## Increment 17 — pog public-API migration: dropping Grind's own checkout for `pog.default_timeout`/`pog.transaction_with_timeout`

Answers docs/RELEASE-READINESS.md, "2b. pog upstream": switch Grind off pog's
internal `Connection`/checkout shape onto the fork's new public API
(`lostbean/pog`, branch `grind/timeouts`, commit `e9089cd`), with no change
in observable behavior. `gleam.toml` now depends on the fork via `git`/`ref`
(squirrel's `>= 4.1.0 and < 5.0.0` and cigogne's `>= 4.0.0 and < 5.0.0`
constraints are both satisfied by the fork's declared `4.1.0`); the direct
`pgo` dependency is dropped (still present transitively through `pog`'s own
`rebar.config`, since `src/grind_postgres_ffi.erl` no longer calls `pgo:*`
functions directly).

### Mechanism change

`postgres.validate` now calls `pog.default_timeout(config,
settings.statement_deadline_ms)` on the pool's own `pog.Config`, instead of
the previous `persistent_term` deadline `postgres.start`/`close` set and
cleared by hand. Every Grind storage call now runs through pog's public
`pog.execute`/`pog.transaction` (bounded by that pool-wide default) or
`pog.transaction_with_timeout(pool, migration_deadline_ms, ..)` for
`migrate`'s own steps — no manual `pgo:checkout`/`pgo:checkin`, no pattern
match on `{pool, Name} | {single_connection, Conn}`, and no fixed 5 s
migration cap (a migration step is bounded by `migration_deadline_ms`, not
by pog's old hardcoded checkout timeout, exactly as before this increment —
see the new characterization/mutation test below).

`src/grind_postgres_ffi.erl` shrank from 260 to 97 lines. Removed
entirely: `set_deadline/2`, `clear_deadline/1`, `with_deadline/3`,
`with_deadline_ms/4`, `guarded_query/2`, `guarded_transaction/2`,
`execute_safely/2`, `call_safely/2`, `transaction_safely/2`,
`transaction_or_checkout_failure/2`, `migration_transaction_safely/3` (all
of it — Grind's own bounded `pgo_pool:checkout/2` and every pattern match on
pog's compiled `Connection` shape). Kept: a single generic `guarded/3`
(catches a checkout `exit` when the pool process is gone, and a
`function_clause` crash whose own top frame is genuinely
`pog_ffi:convert_error` — see its own module doc comment for exactly which
two shapes and why pog's own `Result` API still does not cover them) plus
the two unrelated supervisor-stop helpers
(`stop_consumer_supervisor/1`/`stop_supervisor/1`, untouched). The
equivalent wrapper functions (`execute_safely`/`call_safely`/
`transaction_safely`/`migration_transaction_safely`) now live as plain
Gleam functions in `src/grind/postgres.gleam` (and a `transaction_or_
checkout_failure` counterpart in `src/grind/internal/unique_admission.gleam`)
that call `pog.execute`/`pog.transaction`/`pog.transaction_with_timeout`
directly and delegate only the two still-uncovered failure shapes to
`guarded`. `unique_admission`'s checkout-vs-mid-transaction distinction
(needed so `submit`/`submit_plain` can tell "definitely did not run" from
"ran, outcome unknown") is reconstructed without any internals: pog's own
`TransactionQueryError(ConnectionUnavailable)` can now only ever arise from
a checkout failure (confirmed by reading the fork's `pog_ffi:convert_error/1`
— only pgo's `none_available` maps to it, and `none_available` is only ever
returned by a checkout attempt, never mid-query), so that one shape alone is
reclassified into the outer "definitely did not run" bucket; the pool being
entirely gone is caught the same way as everywhere else, through `guarded`.

`pog_connection_pool_shape_test` (`test/grind_test.gleam`) and its
`grind_test_env:pool_connection_atom/1` FFI probe are removed — they existed
solely to guard the compiled `{pool, Name}` shape `grind_postgres_ffi` no
longer inspects. `postgres_call_safely_wrapper_reports_closed_pool_test`
(which redeclared its own `@external` binding straight to the old,
now-removed `call_safely/2` export) is rewritten to prove the same "closed
pool" contract through the public `postgres.arguments`, one of `call_safely`'s
own callers — the same shape `postgres_closed_pool_renewal_recovers_without_
rerun_test` already proves through `postgres.state`/`execute_safely`.

**Accepted, documented trade-off.** Grind no longer holds the raw `pgo`
connection/reference after the `econnreset`/`etimedout` convert-error crash
shape, so it can no longer `pgo:break/1` it before check-in the way the old
manual-checkout FFI did. pog's own checkin (`exception.defer`/`exception.
on_crash` inside `pog.transaction`, or `pgo:query/3`'s own internal `after`
cleanup for a plain `pog.execute`) still always runs — Erlang's `after`
semantics guarantee this regardless of how many stack frames up the crash is
eventually caught — so the connection is never leaked, only no longer
pre-emptively invalidated after this one specific crash shape.

### T1–T5 / DEFECT 2, before vs. after this increment

Same fault proxy, same tests, unchanged in meaning — "before" is Increment
15's own post-fix numbers (Grind's own manual checkout against pog 4.1
stock); "after" is this increment (pog's public API, same fork):

| Test           | Before (Increment 15, manual checkout)                                   | After (this increment, pog public API)                                   |
| -------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------------ |
| T1             | ~4011ms, `Ok(True)`                                                      | ~4013ms, `Ok(True)`                                                      |
| T2             | ~4010ms, `Error(QueueAckUnknown(...))`                                   | ~4006ms, `Error(QueueAckUnknown(...))`                                   |
| T3             | B ~4021ms; A ~14009ms total                                              | B ~4025ms; A ~14012ms total                                              |
| T4             | ~4000ms, `Error(QueueClaimFailed(pog.QueryTimeout))`                     | ~4000ms, `Error(QueueClaimFailed(pog.QueryTimeout))`                     |
| T5             | ~4011ms, `Error(QueueAckUnknown(...))`                                   | ~4013ms, `Error(QueueAckUnknown(...))`                                   |
| DEFECT 2 probe | ~2675–2690ms, `Error(StateQueryFailed(ConnectionUnavailable))`, no crash | ~2671–2672ms, `Error(StateQueryFailed(ConnectionUnavailable))`, no crash |

All within the same bound relative to `D` (`postgres.statement_deadline_ms`,
4000ms default) as before — the public-API migration changes the mechanism,
not the observable timing or outcome shape.

### Migration deadline: characterization test, then a mutation

`postgres_migration_deadline_long_step_succeeds_test` (new,
`test/grind_test.gleam`, dedicated disposable database
`grind_migration_deadline`) runs `migrate_with` against a synthetic step
whose own statement legitimately blocks for 6s (`pg_sleep(6)`, wrapped in an
outer `SELECT true FROM (...)` — `pg_types` cannot decode a bare `void`
result, the same reason `grind/internal/unique_admission`'s own
advisory-lock query wraps `pg_advisory_xact_lock` the same way) under a
deliberately shortened `migration_deadline_ms` of 9000 (instead of the
30000ms default, purely so the test does not have to wait out the full
default). **Green today, characterizing existing behavior, not a new
capability**: `migrate` already bounded a step by `migration_deadline_ms`
before this increment too (via the old `migration_transaction_safely`'s own
explicit-deadline checkout) — this increment's job was to preserve that,
not add it. Confirmed green (`elapsed_ms` between 6000 and 9000) against a
real cluster.

**Mutation, for red evidence that this is not vacuous**: `migration_
transaction_safely` temporarily edited (`Edit`, not a config toggle) to call
plain `pog.transaction(connection, callback)` — the pool's own
`pog.default_timeout` (4000ms) — instead of `pog.transaction_with_timeout
(connection, deadline_ms, callback)`, discarding `deadline_ms` entirely.
Against a real cluster (fresh `grind_migration_deadline` schema), the same
test now fails: `Error(MigrationCommitUnknown(12))` — the step's own
transaction is force-closed at the pool's shared 4000ms default before the
6s sleep ever completes, exactly the regression this increment's own
`pog.transaction_with_timeout` call exists to prevent. Reverted via `Edit`;
re-confirmed green afterward against a fresh schema with the real
`pog.transaction_with_timeout` call restored.

### Full suite

`scripts/test-postgres.sh`: 186 passed, no failures — unchanged from the
186 baseline before this increment (`pog_connection_pool_shape_test`
removed, one new migration-deadline test added: 186 − 1 + 1 = 186). Consumer
package: 10 passed, no failures, unchanged.

## Post-release-tidy name mapping (pure moves, no behavior change)

A later pre-release API-tidy pass (see the release tracker) moved the
claim/renew/acknowledge/quarantine protocol this document describes out of
`grind/postgres` into two new internal modules, and renamed a few of its
entry points along the way. Every mechanism, SQL statement, and test
described above by its old name still applies unchanged; only the qualified
name changed:

| Old (`grind/postgres`)                           | New                                                                                                                                                                                         |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `postgres.claim_one`                             | `attempt.claim_one`                                                                                                                                                                         |
| `postgres.execute_claim`                         | `attempt.execute_claim`                                                                                                                                                                     |
| `postgres.claim_identity`                        | `attempt.claim_identity`                                                                                                                                                                    |
| `postgres.renew_claim`                           | `attempt.renew`                                                                                                                                                                             |
| `postgres.release_unstarted_claim`               | `attempt.release_unstarted`                                                                                                                                                                 |
| `postgres.acknowledge_claim`                     | `attempt.acknowledge`                                                                                                                                                                       |
| `postgres.acknowledgement_command_id`            | `attempt.acknowledgement_command_id`                                                                                                                                                        |
| `postgres.live_lease_predicate`                  | `lease.live_lease_predicate`                                                                                                                                                                |
| `postgres.expired_lease_predicate`               | `lease.expired_lease_predicate`                                                                                                                                                             |
| `postgres.quarantine_expired_in_queue` (private) | `lease.quarantine_expired_in_queue` (public, now takes a raw connection/storage_owner/forwarder rather than an opaque `Database`, and returns `pog.QueryError` rather than `QueueRunError`) |

`QueueRunError`, `AckRejection`, `postgres.quarantine_expired`, and
`postgres.storage_owner` stayed on `grind/postgres`, which also gained
`@internal` `connection`/`forwarder` accessors so the two new modules can
read an opaque `Database` without depending on each other cyclically.

A follow-up commit in the same tidy pass narrowed `attempt.renew`'s own
return type from `Result(Bool, QueueRunError)` to
`Result(attempt.Renewal, pog.QueryError)` (`Renewal { Renewed LeaseLost }`
in place of `True`/`False`), so evidence above quoting a bare
`Error(QueueClaimFailed(pog.QueryTimeout))` for a renewal fault (T4) now
reads as plain `Error(pog.QueryTimeout)` — same fault, same bound, only the
wrapping removed. The same pass renamed `QueueAckRejected` to
`QueueStorageInvariantViolated` everywhere else it appears above (an
unregistered worker at claim time, or a fenced update/select matching an
impossible row count) — `renew`'s own single such branch, now outside
`QueueRunError` entirely, became a `panic` instead, since a primary-key-
fenced update matching more than one row was already provably
unreachable, never a tested outcome.

A later commit in the same pass renamed `unique.SubmitError.AdmissionFailed`
to `NotCommitted` (evidence above quoting `AdmissionFailed(...)` reads as
`NotCommitted(...)` now, same meaning) and deleted `postgres.SubmitError`
entirely: plain `submit`/`submit_at` now return `unique.SubmitError`
too, with their own uncertain-outcome case (`SubmitQueryFailed`) renamed
`CommitUnknownWithoutId` to sit alongside `submit_unique`/`submit_with_id`'s
`CommitUnknown` in the same unified type — the two differ only in whether a
`PendingSubmission` exists to reconcile from. `postgres.UnexpectedInsertRows`
(an unconditional single-row `INSERT ... RETURNING` matching zero or more
than one row) was already unreachable and became a `panic`, the same
treatment `renew`'s analogous impossible case got above.

Three more renames from the same pass, also appearing above under their old
names: `queue.start_manual`/`queue.start_with_policy`/`queue
.start_manual_with_policy` are gone — every evidence mention of them above
now reads as `queue.start(database, workers, policy)`, keyed on
`policy.polling` (`PollEvery`/`Manual`) instead of a separate function per
combination. `ConsumerDrainTimedOut` is `StopOutcome.StoppedDrainUnconfirmed`
(moved out of `StopError` entirely — the supervisor teardown it describes
had already succeeded). `queue.fail_next_worker_start`/
`queue.kill_next_worker_before_monitor` (armed mid-flight, by message, on an
already-running `Consumer`) are gone; the same two fault shapes are now
supplied once at start time via `grind/internal/consumer_hooks.Hooks`'
`before_worker_start`/`after_worker_start`.

## Increment 18 — pog dependency: dropping the fork, restoring Grind's own checkout

**User decision, superseding Increment 17 entirely**: drop the
`lostbean/pog` git dependency and go back to Grind's own bounded checkout
against vanilla `pog`/`pgo` from Hex — not a fork, and not pog's public
`pog.default_timeout`/`pog.transaction_with_timeout` API. `gleam.toml`
(root) now depends on `pog = ">= 4.1.0 and < 4.2.0"` and, newly, a direct
`pgo = ">= 0.20.0 and < 0.21.0"` — pinned this tightly (not the usual
`>= x.y.0 and < (x+1).0.0`) because `src/grind_postgres_ffi.erl` matches
pog's private `Connection` shape and calls `pgo:checkout/2`/`pgo:checkin/2`/
`pgo:break/1` directly, none of which either package's public contract
promises to keep stable across a minor version. `consumer/gleam.toml` has no
direct `pog`/`pgo` dependency (it never imports either module directly) and
needed no change beyond its `manifest.toml` re-resolving pog from Hex
transitively through `grind`.

### Mechanism: exactly Increment 17's "before" column, restored verbatim

`src/grind_postgres_ffi.erl` and `src/grind/internal/store.gleam` are back
to Increment 17's pre-migration shape (confirmed by diffing against commit
`49e6996`, the last commit before that migration, byte-for-byte identical
apart from doc-comment wording): `set_deadline/2`/`clear_deadline/1`
(`persistent_term`, keyed by the pool's atom name, set in `postgres.start`
and cleared in `postgres.close`), `with_deadline/3`/`with_deadline_ms/4`
(a `pgo:checkout/2` with an explicit `timeout` option, arming `pgo_pool`'s
own absolute deadline timer), `guarded_query/2`/`guarded_transaction/2`
(the same two failure shapes as always — a checkout `exit`, and a
`function_clause` crash whose own top frame is genuinely
`pog_ffi:convert_error`), and `execute_safely/2`/`call_safely/2`/
`transaction_safely/2`/`transaction_or_checkout_failure/2`/
`migration_transaction_safely/3`. The one structural difference from the
pre-Increment-17 layout: all five wrapper functions are now declared as
`@external` bindings in `grind/internal/store.gleam` (the shared module
Increment 13's tidy pass introduced) rather than as private functions
inside `grind/postgres.gleam` directly — `grind/postgres` and
`grind/internal/unique_admission` both call `store.execute_safely`/etc.,
unchanged from how they already called the (temporarily fork-based) wrappers
Increment 17 put there. `postgres.gleam`'s `Settings`/`ValidatedSettings`/
`Database` types, `statement_deadline`/`migration_deadline` setters,
`statement_deadline_ms` accessor, and `validate`'s checks are all unchanged
from before Increment 17 — Increment 17 removed `pog.default_timeout`'s
call site and nothing else in that surface, so reverting it back out
required no further changes there beyond removing that one call.
`pog_connection_pool_shape_test` (`test/grind_test.gleam`) and its
`grind_test_env:pool_connection_atom/1` FFI probe, removed by Increment 17,
are restored too.

### T1–T5 / DEFECT 2, before vs. after this increment

"Before" is Increment 17's own numbers (pog's public API, the now-dropped
fork); "after" is this increment (Grind's own checkout again, vanilla pog):

| Test           | Before (Increment 17, pog public API, fork)                              | After (this increment, Grind's own checkout, vanilla pog)                                     |
| -------------- | ------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------- |
| T1             | ~4013ms, `Ok(True)`                                                      | ~4010ms, `Ok(True)`                                                                           |
| T2             | ~4006ms, `Error(QueueAckUnknown(...))`                                   | ~4007ms, `Error(QueueAckUnknown(...))`                                                        |
| T3             | B ~4025ms; A ~14012ms total                                              | B ~4013ms; A ~14019ms total                                                                   |
| T4             | ~4000ms, `Error(pog.QueryTimeout)`                                       | ~4000ms, `Error(pog.QueryTimeout)`                                                            |
| T5             | ~4013ms, `Error(QueueAckUnknown(...))`                                   | ~4009ms, `Error(QueueAckUnknown(...))`                                                        |
| DEFECT 2 probe | ~2671–2672ms, `Error(StateQueryFailed(ConnectionUnavailable))`, no crash | ~2671ms (all 8 contended callers), `Error(StateQueryFailed(ConnectionUnavailable))`, no crash |

Within noise of both Increment 17's numbers and Increment 15's original
manual-checkout numbers (the mechanism this increment restores) — reverting
the mechanism changes nothing observable, exactly as Increment 17 itself
did not when it went the other direction.

### Full suite

`scripts/test-postgres.sh`: 187 passed, no failures (186 Increment-17
baseline + 1: `pog_connection_pool_shape_test` restored). Consumer package:
10 passed, no failures, unchanged. `gleam check` clean on both packages;
`nix fmt`/`nix flake check` clean.

### Remaining risk, accepted (see docs/RELEASE-READINESS.md, "2b")

This choice re-couples Grind to pog's private `Connection` shape and to
`pgo`'s own checkout/checkin/break API, which neither package's public
contract promises to keep stable. Guarded by the tight version pins above
and by `pog_connection_pool_shape_test` failing loudly the moment a pog/pgo
upgrade changes either shape, rather than this module silently
mismatching it. The trade-off Increment 17 accepted the other way — losing
`pgo:break/1` on a post-checkout crash, since pog's public API never hands
back the raw connection reference — no longer applies: this module holds
the raw `pgo` connection directly again, so a `function_clause` crash whose
top frame is `pog_ffi:convert_error` is once again `pgo:break/1`'d before
check-in, exactly as it was before Increment 17.

## Increment 19 — `close()` erasing a live pool's checkout deadline (independent review at `f42e6c0`)

**Claim**: closing a stale `Database` handle (its own supervisor already
stopped, e.g. by an earlier `close`) must never erase the checkout deadline
of a different, currently live pool that has since reused the same
registered name — `validate` gives every `start`/`close` cycle of the same
`ValidatedSettings` the identical pool name.

**Mechanism**: `postgres_close_stale_handle_does_not_erase_live_pool_deadline_test`
(`test/grind_test.gleam`) starts a pool under a distinctive
`with_statement_deadline(3200)`, closes it (a genuine, live stop), starts a
second pool under the _same_ `ValidatedSettings` (reusing the same pool
name), then closes the _first_, already-dead handle again — a no-op for a
correct implementation. It then runs a raw `pg_sleep(4.2)` (4200ms) through
`grind/internal/store.call_safely` against the second pool's own
connection: 4200ms clears the configured 3200ms deadline but is
comfortably under the FFI's own hardcoded 5000ms fallback, so the two
outcomes are cleanly distinguishable (an error vs. a clean success),
not just a timing window.

**Red-then-green**: temporarily reverted `close` to its pre-fix shape
(`stop_supervisor` called for its side effect only, `store.clear_deadline`
called unconditionally on every `close` regardless of whether that call
actually stopped a live process) and reran the test — it failed exactly as
expected:

```
panic test/grind_test.gleam:3379
test: grind_test.postgres_close_stale_handle_does_not_erase_live_pool_deadline_test
info: expected the connection to be force-closed around the configured
      3200ms statement deadline, not the FFI's own 5000ms fallback
```

i.e. the stale handle's `close` erased the live pool's deadline entry, the
`pg_sleep(4.2)` query ran under the FFI's own 5000ms fallback instead of
3200ms, and it completed successfully instead of erroring. Restoring the
fix (`close` only calls `store.clear_deadline` when `stop_supervisor`
reports it actually stopped a still-live process, via
`grind_postgres_ffi:stop_supervisor/1` now returning `{ok, boolean()}`
instead of a bare `nil`) makes the test pass: the `pg_sleep(4.2)` query
errors, confirming the 3200ms deadline was still in effect. `set_deadline`
was also moved to run before the pool's own supervisor starts (previously
after), closing an unrelated, narrower window where an in-flight checkout
against a brand-new pool could in principle run before any deadline was
attached at all.

**Also covered in the same commit**: `stop_supervisor` did not catch
`exit:timeout` from `gen_server:stop/3`, so a slow shutdown would crash the
caller and leave the pool's name registered; it now catches that case and
`close` surfaces it as a typed `postgres.CloseError(StopTimedOut)` instead
of crashing. No dedicated fault-injection test forces a genuine
`gen_server:stop` timeout here (the FFI's own 6000ms bound is itself hard
to force deterministically without a synthetic slow-stop hook); fixed on
source-level review of the missing `catch` clause, mirroring the existing
`stop_consumer_supervisor/1`'s own handling of the same case.

## Increment 20 — migration steps' own lock waits are now bounded

**Claim**: a migration step's own DDL/DML (e.g. an `ALTER TABLE` against a
large, actively used table) should fail fast and typed under lock
contention rather than blocking for up to the full `migration_deadline_ms`.

**Mechanism**: `run_migration_step_transaction` now sets a constant,
transaction-local `lock_timeout` (`migration_lock_timeout_ms`, 2000ms, via
`sql.set_lock_timeout` — the same Squirrel-generated query
`grind/internal/unique_admission` already uses for `unique_lock_wait_ms`)
right after acquiring the migration's own advisory lock; a step statement
that hits PostgreSQL's own `55P03 lock_not_available` is now mapped to the
typed, retry-safe `MigrationLockUnavailable(version)` instead of
`MigrationStepFailed`.
`postgres_migration_step_lock_timeout_returns_lock_unavailable_test`
(`test/grind_test.gleam`) proves this against a real conflicting lock: a
fresh v11-only database, an observer holding
`LOCK TABLE grind_jobs IN ACCESS SHARE MODE` in an open transaction
(`spawn_lock_holder`), then `migrate_with([v11, synthetic-v12-ALTER])`
against a _second_, independent pool. `main_database`'s own
`migration_deadline_ms` is shortened to 3500ms for this test only — not
because the default matters here, but because `spawn_lock_holder`'s
observer uses a plain, unwrapped `pog.transaction` (not one of Grind's own
checkout-bounded calls), so it is itself subject to pog's own hardcoded
~5000ms checkout hold time (`docs/RECOVERY-EVIDENCE.md`, "Acknowledgement
deadline") — the fix's own 2000ms lock timeout clears comfortably before
that, but a 30000ms default `migration_deadline_ms` would not, since the
observer's lock would be involuntarily released by pog's own hardcoded
timeout before the test could observe genuine deadline-driven blocking.

**Red-first / mutation** (this is also this change's own red-first
evidence, since it is exactly the code shape before this fix): temporarily
removed the `set_migration_lock_timeout` call and reran the test against a
fresh database:

```
panic src/gleeunit/should.gleam:10
test: grind_test.postgres_migration_step_lock_timeout_returns_lock_unavailable_test
info:
Error(MigrationCommitUnknown(12))
should equal
Error(MigrationLockUnavailable(12))
```

i.e. without the constant `lock_timeout`, the `ALTER TABLE` blocked on the
observer's table lock until `main_database`'s own 3500ms
`migration_deadline_ms` force-closed the connection (via Grind's existing
checkout-deadline mechanism), reporting the pre-existing
`MigrationCommitUnknown(12)` instead of the new, more precise
`MigrationLockUnavailable(12)` — exactly the "waits until the migration
deadline" behavior this change replaces. Restoring the call makes the
`ALTER` fail fast (~2000ms) with the typed error instead. The test also
confirms: the schema generation stays at 11 and the probe column is absent
while the lock is held; releasing the lock and rerunning the identical
steps succeeds; and `SHOW lock_timeout` on the pooled connection reports
PostgreSQL's ordinary default (`"0"`, no limit) afterward, not the
migration step's own constant 2000ms — `SET LOCAL` never leaks past the
transaction that set it.

**Not covered**: `priv/migrations/*.sql` applied directly through cigogne
does not get this `lock_timeout` automatically (it is set only inside
`postgres.migrate`'s own transaction, not in the released `.sql` files,
deliberately — see README, "Migrations"); an application relying on cigogne
directly should set its own `lock_timeout` if it wants the same fast-fail
behavior.

## Increment 21 — schema v12: `grind_jobs.finished_at` records when a job finished

**Claim**: every `grind_jobs` row that reaches one of the six terminal
states (`succeeded`, `business_failed`, `runtime_failed`,
`contract_mismatch`, `discarded`, `cancelled`) has a non-null `finished_at`,
and every row in a non-terminal state has a null one — enforced by a real
PostgreSQL `CHECK` constraint (`grind_jobs_finished_at_check`), not merely
by convention across the several independent write sites that set it
(`attempt.acknowledge_transaction`'s eight per-outcome branches,
`attempt.mark_contract_mismatch`, `sql.cancel_before_run`, and
`postgres.write_resolution`).

**Mechanism**: `grind_v12` (`priv/migrations/20260926000000-grind_v12.sql`,
`Migration(12, ...)` in `src/grind/internal/migrations.gleam`) adds the
column with a `DEFAULT now()` (backfilling every existing row, including
non-terminal ones), drops the default (so a future ordinary insert of a
non-terminal job — which never mentions this column — gets `NULL`, not
`now()`), nulls it back out on every row whose `state` is not terminal, and
only then adds the `CHECK` constraint validated against every row. The six
terminal states are spliced from one shared fragment
(`grind/internal/terminal.states_sql()`) into the constraint text, the
backfill's own `WHERE`, and `postgres.write_resolution`'s `finished_at`
`CASE`, so there is exactly one place that lists them. Every acknowledgement
branch that can _only_ ever land on a terminal state either way (a
concurrent cancellation overrides `succeeded`/`business_failed`/
`discarded`/`runtime_failed` with `cancelled`, and the plain `cancelled`
branch has no other outcome) sets `finished_at = clock_timestamp()`
unconditionally; the three branches that can also land on a genuinely
non-terminal state (`retryable`, `scheduled` via a worker snooze, and
`uncertain`) use a bare `CASE WHEN cancel_requested_at IS NOT NULL THEN
clock_timestamp() END` (implicit `NULL` otherwise), matching the state
`CASE` right next to it exactly.

`postgres_finished_at_check_constraint_rejects_mismatch_test`
(`test/grind_test.gleam`) probes the constraint directly with two raw
`INSERT`s (a non-terminal state with `finished_at` set, and a terminal one
with it left null), asserting `pog.ConstraintViolated(constraint:
"grind_jobs_finished_at_check", ..)` — PostgreSQL's own `23514
check_violation`, which `pog_ffi:convert_error` reports through
`ConstraintViolated` (it carries a `constraint` field, exactly like a
unique violation) rather than the generic `PostgresqlError(code, ..)` shape.
`postgres_finished_at_written_at_every_terminal_path_test` is the
table-driven proof over every write site: nine terminal jobs (one per
acknowledgement outcome that ends terminal, one contract-mismatch, one
`cancel_before_run`, and one `resolve_uncertain` for each of
`ConfirmSuccess`/`ConfirmBusinessFailure`) assert `finished_at IS NOT NULL`,
and five non-terminal jobs (fresh `queued`, an acknowledged `scheduled`
snooze, an acknowledged `retryable`, a forced `uncertain`, and that same job
after `AuthorizeReplay` — the one non-terminal outcome a resolution can
itself produce) assert it stays null. The upgrade harness
(`postgres_migrate_upgrade_from_frozen_v11_fixture_test`) now migrates its
frozen v11 fixture through the real `v11`+`v12` (plus a renumbered synthetic
`v13` step, since `v12` is no longer available for that role) rather than a
synthetic stand-in for `v12` itself, and separately asserts the backfill
result: the fixture's pre-migration `succeeded` row (written before
`finished_at` existed at all) picks up a `finished_at` dated from the
migration itself, while every non-terminal seeded row's stays null.

**Red-first / mutation** (two independent mutations, both reverted after
capturing this evidence):

1. Removed the `ALTER TABLE grind_jobs ADD CONSTRAINT
grind_jobs_finished_at_check ...` statement from `v12_statements()` only
   (deliberately _not_ from the mirrored `.sql` file, to isolate this from
   the conformance test) and reran `scripts/test-postgres.sh`:
   `grind_migrations_conformance_test` failed as expected (the two sources
   no longer match — proving that test still catches a source drift even
   here), and, isolating the constraint's own claim,
   `postgres_finished_at_check_constraint_rejects_mismatch_test` failed:
   ```
   let assert Error(queued_error) =
     pog.query(...) |> pog.execute(on: connection)
   value: Ok(Returned(1, []))
   info: Pattern match failed, no pattern matched the value.
   ```
   i.e. without the constraint, PostgreSQL silently accepted a `queued` row
   with `finished_at` already set. Restoring the statement made both tests
   pass again (192 passed, no failures).
2. Removed `finished_at = clock_timestamp()` from
   `acknowledge_transaction`'s `"discarded"` branch only (leaving the
   constraint itself, and every other branch, untouched) and reran the
   suite: the constraint's own defense-in-depth caught it immediately, as a
   _commit-time_ failure rather than a silently wrong value —
   `postgres_finished_at_written_at_every_terminal_path_test` failed with
   `Error(QueueProcessFailed(QueueAckFailed(ConstraintViolated(.., "grind_jobs_finished_at_check", ..))))`
   instead of `Ok(True)` for its discard scenario, and, independently, the
   pre-existing `postgres_worker_discard_has_distinct_committed_outcome_test`
   failed the exact same way — proving the constraint is a real safety net
   for a write site this change did not itself add a dedicated test for,
   not only for the new table-driven one. Exactly 2 failures (190 passed).
   Restoring the assignment made both pass again (192 passed, no failures).

**Also covered in the same commit**: the claim/quarantine hot-path partial
indexes `grind_jobs_claim_idx`
(`(storage_owner, queue, available_at, id) WHERE state IN ('queued',
'scheduled', 'retryable')`) and `grind_jobs_quarantine_idx`
(`(storage_owner, queue, lease_expires_at, id) WHERE state = 'executing'`),
and the retention-scan indexes `grind_jobs_finished_idx`,
`grind_unique_submissions_job_idx`, and `grind_job_resolutions_job_idx`
(the latter two ahead of a future `prune_finished`, not yet implemented).
No dedicated test proves these indexes are actually used by the planner
(`EXPLAIN`-based query-plan assertions are not otherwise used in this
suite); their shapes are derived directly from the exact predicates
`attempt.claim_registered_job` and `grind/internal/lease`'s quarantine scans
already use, and `read_schema_generation`'s existing exact-shape check
(`v12_shape`) proves each index exists with the right name after a real
`migrate`, empirically confirmed against `pg_class`/`pg_indexes` the same
way `v11_shape`'s own doc comment describes.

## Increment 22 — quarantine index shape corrected; claim eligibility is now a real index condition

**Increment 21's own `grind_jobs_quarantine_idx`** —
`(storage_owner, queue, lease_expires_at, id) WHERE state = 'executing'` —
never actually served `postgres.quarantine_expired` (the public,
cross-queue sweep): that query has no `queue` predicate at all and never
sorts by `lease_expires_at` (only `id`), so the planner fell back to a
primary-key walk across every `executing` row regardless of queue,
measured 384ms at 2,000,000 rows. `v12` is still unreleased and unpinned
(`AGENTS.md`), so this is a same-version edit rather than a new migration:
`(storage_owner, id) WHERE state = 'executing'` — no `queue`, no
`lease_expires_at`. Correct for both callers: `grind/internal
.lease.quarantine_expired_in_queue` (the per-queue scan every consumer poll
runs) still filters by `queue` and `lease_expires_at` as an ordinary
post-scan `Filter`, which is cheap once `(storage_owner, id) WHERE state =
'executing'` has already narrowed the scan to that owner's live attempts;
`postgres.quarantine_expired` (the cross-queue sweep) needed exactly this
shape and nothing more, since it never filters by `queue` at all. Measured
at 2,000,000 rows: 2.6ms cross-queue, 1.3ms per-queue, no sort either way
(`id` is already the index order). Confirmed via `pg_indexes` against a
real `migrate`d schema (`SELECT indexdef FROM pg_indexes WHERE indexname =
'grind_jobs_quarantine_idx'` reports exactly
`... USING btree (storage_owner, id) WHERE (state = 'executing'::text)`).

**`attempt.claim_registered_job`'s own eligibility check** — `available_at
<= clock_timestamp()` — never let the planner use `grind_jobs_claim_idx`'s
own `available_at` column as a real index bound, because `clock_timestamp()`
is `VOLATILE` (PostgreSQL must assume it can return a different value on
every row it is evaluated against, even within one statement) — index
scans require a `STABLE` or better comparison value to use as a scan
boundary, so this stayed a post-scan `Filter` applied after the index had
already been walked using only `storage_owner`/`queue`. `statement_timestamp()`
is `STABLE` for the lifetime of one statement — the exact same value
`clock_timestamp()` would have returned for a single, non-blocking
`UPDATE ... FROM (candidate CTE) ...` like this one (it never waits on a
lock — `FOR UPDATE SKIP LOCKED` never blocks) — so swapping it in lets the
planner fold `available_at <= statement_timestamp()` directly into the
index condition. Reproduced with `EXPLAIN` against a 200,000-row
`grind_jobs` (a realistic multi-queue distribution, `ANALYZE`d):

```
-- available_at <= clock_timestamp()
Index Scan using grind_jobs_claim_idx on grind_jobs
  Index Cond: ((storage_owner = 'owner') AND (queue = 'q5'))
  Filter: (... AND (available_at <= clock_timestamp()))

-- available_at <= statement_timestamp()
Index Scan using grind_jobs_claim_idx on grind_jobs
  Index Cond: ((storage_owner = 'owner') AND (queue = 'q5')
               AND (available_at <= statement_timestamp()))
  Filter: (... state check only)
```

i.e. `available_at` moves from the `Filter` line into the `Index Cond` line
— exactly the mechanism behind the reviewer's own measured 0.92ms → 0.009ms
at a larger scale. This is a read-eligibility check, never a lease/fencing
comparison: `lease.live_lease_predicate` and every `lease_expires_at`
comparison stay on `clock_timestamp()` unchanged, since a lease's liveness
must reflect the actual instant a fenced write commits, not merely when its
enclosing statement began.

**Also from the same review**: `postgres_finished_at_written_at_every_terminal_path_test`
now also drives the three cancel-overridable `acknowledge_transaction`
branches (`retryable`, `scheduled`, `uncertain`) through a concurrent
`postgres.cancel` while genuinely `executing` (reusing the
started/release handshake `run_cancel_running_ack_test` already
established), proving `finished_at` ends up set on their overridden
(`cancelled`) path too, not only their ordinary one; and a new
`terminal_states_sql_matches_every_job_state_test` ties
`terminal.states_sql()` to `job.state_to_stored` directly, over an
exhaustive `case` on every `job.State` variant, so a future new variant
fails this test file to compile rather than silently drifting from
`grind_jobs_finished_at_check`.

## Increment 23 — retention: `prune_finished`, the supervised pruner, and the `FOR KEY SHARE` admission race it exposed

**Claim**: `postgres.prune_finished` deletes only finished-and-old-enough
rows (scoped to the caller's own storage owner, oldest `finished_at` first,
`FOR UPDATE SKIP LOCKED` against a concurrent claim or another prune call),
along with each deleted job's own acknowledgement/uniqueness/resolution
receipts in the same autocommitted statement; `grind/pruner` is a supervised
background process that calls it on a timer with Oban-shaped defaults
(`interval_ms` 30000, `limit` 10000, `max_age_ms` 60000); and a concurrent
`submit_unique` admission deciding `KeepExisting` against a candidate a
prune call is also considering can never end up with a receipt that
outlives its own job.

**Mechanism**: `sql/prune_finished.sql` (squirrel-generated — its shape
never varies by call, only its three bound values do) is one `WITH doomed AS
(SELECT id FROM grind_jobs WHERE storage_owner = $1 AND finished_at IS NOT
NULL AND state IN (<six terminal>) AND finished_at < statement_timestamp() -
($2::bigint::double precision * interval '1 millisecond') ORDER BY
finished_at, id LIMIT $3 FOR UPDATE SKIP LOCKED), ...` statement cascading
into three `DELETE ... USING doomed` receipt deletes and a final `DELETE
FROM grind_jobs`, each `RETURNING 1` counted by the closing `SELECT`. The
cutoff uses `statement_timestamp()`, not `clock_timestamp()`, for the same
reason Increment 22 changed the claim query: it lets `finished_at < ...`
fold into `grind_jobs_finished_idx`'s own index condition instead of a
post-scan filter. There is no minimum retention floor beyond `older_than_ms

> 0`(matching Oban's own`max_age`, which is likewise only required to be
positive) — a deliberate decision (superseding an earlier draft of this plan
that proposed a 300000ms floor), documented in README, "Retention", as a
real trade-off: a retention window shorter than a live lease can turn a
late commit-unknown acknowledgement retry into `QueueAckStale
> (AckRecordMissing)` instead of a clean commit.

`grind/pruner` reuses `prune_finished`'s own validation (`validate_policy`
enforces the identical `older_than_ms`/`limit` bounds under different names,
plus a positive `interval_ms`) and, unlike Oban's own pruner plugin, has no
leader election at all: `prune_finished`'s `FOR UPDATE SKIP LOCKED` already
makes concurrent callers (several nodes' own supervised pruners, or a
pruner racing an operator's manual call) safe to run at once — each simply
skips whatever another one already holds — so there is no cluster-wide
single-writer invariant to elect a leader for in the first place. Each tick
loops immediately (never waiting out the rest of `interval_ms`) while its
own batch came back exactly `limit` rows, draining a backlog within one
scheduled tick rather than one batch per interval.

**`candidate_sql`'s missing lock** (`grind/internal/unique_admission`): a
`KeepExisting` uniqueness decision (the common case — most policies never
reschedule) read its candidate row with a bare `SELECT`, no lock at all.
Once `prune_finished` existed, this became a real race: `submit_unique`
reads an old, terminal candidate, decides `KeepExisting`, and is about to
commit its own `grind_unique_submissions` receipt naming that row's `id` —
if a concurrent `prune_finished` call deletes that exact row and commits
first, admission's own transaction still commits its receipt on schedule,
now naming a job that no longer exists. Fixed by locking every
non-reschedule candidate with `FOR KEY SHARE` (a reschedule already used
`FOR UPDATE`) — the weakest lock mode that still conflicts with a
`DELETE`, so `prune_finished`'s own `FOR UPDATE SKIP LOCKED` either skips
the row outright (if admission's lock is already held) or blocks until
admission's transaction ends, after which its own `SELECT` correctly finds
nothing there anymore.

**Red-first / mutation**, `postgres_prune_finished_admission_race_keeps_candidate_and_receipt_test`
(`test/grind_test.gleam`): a real barrier forces the exact overlap needed —
`install_admission_receipt_barrier` blocks `INSERT INTO
grind_unique_submissions` (scoped to one exact `submission_id`) behind
`pg_advisory_xact_lock`, so a background `submit_unique` call is provably
paused strictly _after_ its own candidate lock is taken and _before_ its
own receipt commits (`await_admission_blocked_on_receipt_insert` polls
`pg_stat_activity` for a backend genuinely waiting on that exact lock from
inside that exact query, rather than inferring the pause from timing).
While paused there, the test calls the real `prune_finished` against the
same row. With the fix: `race_prune.jobs` is `0` (skipped), the row and its
own eventual receipt both survive. Temporarily reverted the fix (`FOR KEY
SHARE` → `""`) and reran:

```
panic src/gleeunit/should.gleam:10
test: grind_test.postgres_prune_finished_admission_race_keeps_candidate_and_receipt_test
info:
1
should equal
0
```

i.e. without the lock, `prune_finished`'s own `SKIP LOCKED` no longer
skipped the contested row — it deleted it (`race_prune.jobs` came back `1`)
while the barrier-paused admission transaction was still open, mid-way
through committing a `KeepExisting` decision that names it. Restoring `FOR
KEY SHARE` made the test pass again (198 passed, no failures).

**Also covered in the same commit**: the state/receipt-cascade matrix
(`postgres_prune_finished_deletes_old_terminal_rows_test` — all six terminal
states old enough are pruned with their acknowledgement/uniqueness/
resolution receipts counted exactly; all six young enough and all five
non-terminal states survive; a second storage owner's own old row is
untouched), batch size and oldest-first ordering (five rows, `limit: 2`
four times → `2, 2, 1, 0`), `FOR UPDATE SKIP LOCKED` against an externally
held row lock (skipped while locked, pruned once released), pure validation
against a closed pool (`postgres_prune_finished_validates_arguments_test`),
and the two documented post-prune behaviors that follow directly from the
row being gone rather than needing their own fault injection:
`state`/`outcome`/`bind_handle` report `JobNotFound`,
`reconcile_acknowledgement` reports `ReceiptNotFound`, a `submit_with_id`
replay of the same `SubmissionId` inserts a genuinely new row (the
idempotency window is the retention window), and an `AllRetained`/
`while_retained()` uniqueness key admits a fresh submission once its old
occupant is pruned, rather than staying occupied forever.

**Not covered**: `reconcile_unique` against a submission whose own
`CommitUnknown` was never resolved before its underlying job was pruned
(the plan's own "reconcile_unique on old pending → CommitUnknown forever")
— reproducing a genuine lost-reply `CommitUnknown` needs the fault-proxy
machinery `docs/RECOVERY-EVIDENCE.md`'s earlier increments already use for
the acknowledgement deadline; layering `prune_finished` underneath that
setup as a fourth moving part was judged not worth the added flakiness risk
for this round. The behavior itself is a straightforward corollary of
"the receipt row is gone" (the same shape `ReceiptNotFound`/`AckRecordMissing`
already prove elsewhere), not a new code path.

## Increment 24 — independent review of Increment 23: `ON DELETE CASCADE`, lock-mode contention, and a real timer leak

**Claim 1 (correctness)**: even with Increment 23's own `FOR KEY SHARE` fix,
a `KeepExisting` admission's receipt could still end up orphaned by a
narrower, snapshot-timing race a per-row lock alone cannot close;
`grind_v12`'s `ON DELETE CASCADE` foreign keys (`grind_job_acknowledgements`/
`grind_unique_submissions`/`grind_job_resolutions`, all on `job_id`
referencing `grind_jobs(id)`) close it, since the database itself now
guarantees the receipt is gone whenever its job is — immune to which
statement's own snapshot did or did not see the receipt commit.
`postgres.prune_finished` no longer issues a second, explicit receipt
`DELETE` of its own at all; `PruneReport` is now `jobs`-only.

**Mechanism**: under `READ COMMITTED`, every CTE inside one SQL statement
shares that one statement's own start-of-statement snapshot. A receipt
committed by some other writer _after_ `prune_finished`'s own snapshot was
taken, but _before_ its scan actually reaches and locks the row that
receipt names, is invisible to that snapshot — an explicit,
snapshot-scoped `DELETE ... WHERE job_id = d.id` against the receipt table
(Increment 23's own shape) can never see or delete it, leaving it an orphan
once the job itself is deleted a moment later in the very same statement.
`ON DELETE CASCADE` fires its own fresh sub-query when the row is actually
deleted, immune to the deleting statement's own snapshot, so it still finds
and removes a receipt committed in exactly that window.

**Red-first / mutation**, all three reverted together (removing the three
`ADD CONSTRAINT ... FOREIGN KEY ... ON DELETE CASCADE` statements from
`v12_statements()` only, deliberately not from the mirrored `.sql` file) and
reran `scripts/test-postgres.sh`:

```
test: grind_test.postgres_prune_finished_deletes_old_terminal_rows_test
info:
[#(1, 1, 1)]
should equal
[#(0, 0, 0)]

test: grind_test.postgres_prune_finished_cascade_survives_late_committed_receipt_test
info:
[False]
should equal
[True]
```

i.e. without the foreign keys, deleting a job left its acknowledgement,
uniqueness-submission, and resolution receipts behind _even in the
ordinary case_ (no snapshot race needed — `prune_finished` genuinely never
deletes them itself anymore), and the dedicated snapshot-race test
(`postgres_prune_finished_cascade_survives_late_committed_receipt_test`,
below) reproduced the exact orphan the fix targets. `grind_migrations_conformance_test`
also failed, as expected (the two sources no longer matched — confirming
that test still catches this kind of drift even here). Restoring all three
statements made every test pass again (200 passed, no failures, reproduced
across three consecutive full runs both before and after this mutation
cycle).

`postgres_prune_finished_cascade_survives_late_committed_receipt_test`
(`test/grind_test.gleam`) forces the exact interleaving deterministically:
`install_snapshot_barrier` installs a PL/pgSQL function, called from inside
a prune-shaped query's own `WHERE` clause once per candidate row, that
blocks on `pg_advisory_xact_lock` only when evaluating one exact target
row id. With that lock held externally first, a background connection runs
`snapshot_barrier_prune_sql` (textually identical to `sql/prune_finished
.sql`, plus that one extra barrier call — the real, squirrel-generated
query has no injection point of its own to pause mid-scan from a test) —
proven genuinely paused there via `pg_stat_activity`
(`await_prune_blocked_on_snapshot_barrier`), not inferred from timing.
While paused, a receipt is inserted and committed directly, naming the
still-locked target row. Releasing the barrier lets the prune-shaped
statement's own scan reach and delete that row; the fix's own `ON DELETE
CASCADE` then removes the just-committed receipt too, even though it was
invisible to the deleting statement's own snapshot the whole time.

**Claim 2 (contention)**: `FOR KEY SHARE` (Increment 23) unlocked a new
problem: `attempt.claim_registered_job`'s own candidate lock, `postgres
.cancel_lock`, and `lease`'s own quarantine-scan candidate lock all used a
plain `FOR UPDATE`, which conflicts with _everything_, including a
read-only `FOR KEY SHARE` — so an ordinary claim, cancellation, or
quarantine sweep racing a `KeepExisting` admission's read of the identical
row could spuriously report `AdmissionContended` for a reason that was
never actually a write conflict (none of those three `UPDATE`s ever touch
`grind_jobs.id`, the only key column any unique index on that table
covers). Fixed by switching all three to `FOR NO KEY UPDATE`, which does
not conflict with `FOR KEY SHARE`; `prune_finished`'s own candidate lock
stays `FOR UPDATE`, since a `DELETE` conflicts with `FOR KEY SHARE`
regardless of the weaker mode.

**Evidence**: reproduced directly at the SQL level against a disposable
database (two `psql` sessions, no application code involved, to isolate
the lock-mode mechanism itself from any of Grind's own machinery):

```
-- session A: FOR NO KEY UPDATE held (uncommitted)
-- session B: FOR KEY SHARE, 500ms lock_timeout
BEGIN
 id | v
----+---
  1 | a
(1 row)
COMMIT                                  -- no wait at all

-- session A: FOR UPDATE held (uncommitted), 2s hold
-- session B: FOR KEY SHARE, 500ms lock_timeout
ERROR:  canceling statement due to lock timeout
CONTEXT:  while locking tuple (0,1) in relation "t"
```

i.e. `FOR NO KEY UPDATE` and `FOR KEY SHARE` never contend at all, while
`FOR UPDATE` and `FOR KEY SHARE` do — exactly the mechanism behind both the
bug (claim/cancel/quarantine using `FOR UPDATE`) and the fix (switching
them to `FOR NO KEY UPDATE`). The full gate's own existing concurrency
tests (overlapping claims, cancel-while-running, quarantine races) stayed
green throughout, confirming the weaker lock mode is still exactly as safe
for what each of those three `UPDATE`s actually needs.

**Claim 3 (`grind/pruner`, a real timer leak)**: `process.send_after`
targeting a _named_ subject resolves to whichever process currently holds
that name at _delivery_ time, not at scheduling time. Increment 23's own
`handle_message` scheduled its next `Tick` against `Pruner`'s own named
subject — so a timer an old, killed incarnation scheduled for itself did
not die with it: it still fired later and delivered to whatever new
incarnation the supervisor had since restarted, landing as an extra,
unaccounted-for tick on top of that new incarnation's own freshly
scheduled one. Fixed by scheduling `Tick` against a fresh, per-incarnation
subject created in the initialiser instead (exactly `grind/queue`'s own
`ConsumerState.incarnation_subject` pattern) — a subject with no live
owner left to deliver to once its own incarnation is gone.

**Red-first / mutation**,
`postgres_supervised_pruner_restart_ticks_exactly_once_test`
(`test/grind_test.gleam`): starts a pruner, waits for its first real tick
(confirming a fresh timer was just scheduled), kills the actor via its own
`@internal actor_pid` (`process.subject_owner` on the named subject,
resolved fresh, the same lookup `grind/queue.coordinator_pid` uses),
confirms a genuinely different pid took over, then counts `[grind, prune,
completed]` events in a bounded window sized to catch exactly one
legitimate post-restart tick. Temporarily reintroduced the exact original
shape (the named subject stored in `PrunerState` and used for both the
initial and every recurring `send_after`, rather than a fresh incarnation
subject) and reran:

```
panic src/gleeunit/should.gleam:10
test: grind_test.postgres_supervised_pruner_restart_ticks_exactly_once_test
info:
2
should equal
1
```

i.e. exactly the predicted leak — two completed events instead of one, the
killed incarnation's own leaked timer plus the new incarnation's own
legitimate one, arriving close enough together to both land inside the
same bounded window. Restoring the per-incarnation subject made the test
pass again (200 passed, no failures).

**Also covered in the same commit**: `grind/pruner` now calls
`prune_finished` exactly once per tick (Increment 23's own internal
drain-while-`jobs == limit` loop is gone, matching Oban's own pruner,
which never drains within one tick either — tune `limit`/`interval_ms`
down together instead of relying on an internal loop, documented in
`grind/pruner`'s own module doc comment alongside the `statement_deadline_ms`
trade-off of too large a `limit`); a failed tick emits `[grind, prune,
failed]` with a coarse `observation.PruneFailureKind` classifying the
underlying `pog.QueryError` (`PruneReplyLost`/`PruneResultUndecodable`/
`PruneRejected`/`PruneNotAttempted` — see that type's own doc comment for
which ones leave "did this actually delete anything" genuinely unknown);
`pruner.supervised` gives an application its own child specification to
embed directly into its own supervision tree instead of tracking the
separate, dedicated one `pruner.start` creates; and `pruner.validate_policy`
now calls `postgres.validate_retention_ms`/`postgres.validate_prune_limit`
(the same `@internal` functions `prune_finished` itself calls) instead of
maintaining an independent copy of the same two threshold checks.

## Increment 25 — second independent review of Increment 24: orphan-safe `ADD CONSTRAINT`, a measured cascade, corrected contention wording, a fourth `FOR NO KEY UPDATE`, and two cleanups

**Claim 1 (correctness, red-first)**: `grind_v12`'s own `ADD CONSTRAINT ...
FOREIGN KEY` statements (Increment 24) validate every existing row by
default, so a database that had already accumulated an orphaned receipt
under `v11` — no foreign key was enforcing anything yet, so this needed no
exotic scenario, just an old bug, a hand rollback, or direct SQL against
the database at any point before this upgrade — would fail this migration
outright with `23503 foreign_key_violation`, never reaching `v12` at all.
Fixed by adding one `DELETE FROM <receipt table> r WHERE NOT EXISTS (SELECT
1 FROM grind_jobs j WHERE j.id = r.job_id)` immediately before each
receipt table's own `ADD CONSTRAINT`, in the still-unreleased `v12`
migration (`priv/migrations/20260926000000-grind_v12.sql` and
`migrations.v12_statements`, kept byte-identical as always).

Red first, at the SQL level, against a real disposable cluster: applied a
real `v11` install, seeded one orphaned `grind_job_acknowledgements` row
(`job_id = 999999999`, which was never a real `grind_jobs.id`), then applied
`v12`'s own `up` section with the three `DELETE`s removed (reproducing the
pre-fix shape):

```
ERROR:  insert or update on table "grind_job_acknowledgements" violates foreign key constraint "grind_job_acknowledgements_job_id_fkey"
DETAIL:  Key (job_id)=(999999999) is not present in table "grind_jobs".
```

Green, same scenario, current (fixed) `v12`: the migration's own `up`
section completes with no error, and `SELECT count(*) FROM
grind_job_acknowledgements WHERE job_id = 999999999` reads back `0`.
`postgres_migrate_upgrade_from_frozen_v11_fixture_test`
(`test/grind_test.gleam`) now seeds this same shape (one orphan per receipt
table) as part of its own real, full upgrade harness run — through
`postgres.migrate_with`, not raw SQL, so it also proves the Gleam-side
`v12_statements` mirrors the priv file exactly, and asserts all three
orphans are gone once `migrate_with` returns `Ok(Nil)`. `mark_database_test_executed("migrate-upgrade-harness-passed")`.

**Claim 2 (measured, not assumed)**: whether `prune_finished`'s own cascade
(deleting up to `limit` jobs, each cascading into up to three receipt rows)
stays comfortably inside the pool's own default 4000ms
`statement_deadline_ms` at Oban's own default `limit` (10,000) was an
assumption before this round, not a measurement. Measured directly against
a real disposable cluster: a fresh `v12` schema seeded with 10,000 finished
(and old enough) jobs, each carrying one acknowledgement, one uniqueness
submission, and one resolution receipt (30,000 receipt rows total, the
worst case every one of them cascades) — the exact `prune_finished.sql`
statement (`limit: 10000`, `older_than_ms: 60000`) ran in:

```
Time: 65.870 ms
Time: 66.802 ms
Time: 66.967 ms
```

— three runs, ~66ms each, about **1.7% of the 4000ms default
`statement_deadline_ms`**, comfortably under the 50% caution threshold this
review set. **No change to `pruner.default_policy`'s `limit` (10,000) or to
`postgres.prune_limit_maximum` (10,000) was needed** — both stay exactly as
Oban's own pruner defaults them. `grind_job_acknowledgements_job_idx`/
`grind_unique_submissions_job_idx`/`grind_job_resolutions_job_idx`
(Increment 23) are exactly why: each cascade is an index lookup on `job_id`,
not a sequential scan, against however many receipt rows exist. This
number is recorded here (and in `grind/pruner`'s own module doc comment) as
the actual evidence behind "large enough `limit` risks timing out" already
being phrased as a risk, not a certainty, for the shipped defaults — a
much larger fan-out per job, a much slower disk, or a `limit` raised well
past `prune_limit_maximum` by a caller who forked this bound could still
change that conclusion; this measurement is not a guarantee for every
deployment, only for Oban's own shipped defaults on ordinary hardware.

**Claim 3 (contention wording correction)**: Increment 24's own new prose
(`unique_admission.candidate_sql`'s doc comment,
`docs/UNIQUENESS-CONTRACT.md` step 6) got the blocking direction backwards:
`prune_finished` itself never blocks (its own scan is `FOR UPDATE SKIP
LOCKED`, so a row already locked elsewhere is simply skipped); a
`KeepExisting` admission's own `FOR KEY SHARE` read is the side that can
block, when `prune_finished`'s `FOR UPDATE` already holds the row for the
duration of its own `DELETE` statement — and, if that wait exceeds the
admission's own `lock_timeout`, `AdmissionContended` is the correct result,
not a bug. Both doc comments are corrected to state the direction
correctly and to restate the window `FOR KEY SHARE` actually closes:
admission committing after `prune_finished`'s statement already took its
snapshot but before that statement's own scan reaches and locks this exact
row. No code changed for this claim — wording only.

**Claim 4 (a fourth `FOR NO KEY UPDATE`)**: `postgres.apply_uncertain_resolution`'s
own row lock (the audited-resolution path) still used a plain `FOR UPDATE`,
missed by Increment 24's own sweep of claim/cancel/quarantine. Its own later
`UPDATE grind_jobs` (in `write_resolution`) never touches a key column
(`id`, `storage_owner`, `worker_id`, `worker_version`,
`unique_key_contract`, `unique_key_sha256` — all in that `UPDATE`'s `WHERE`,
never its `SET`), so it moves to `FOR NO KEY UPDATE` for the same reason
the other three did: it does not conflict with `unique_admission`'s own
`FOR KEY SHARE`, so an audited resolution racing a `KeepExisting` read of
the same row no longer spuriously reports `AdmissionContended`. Proven by
the same concurrency tests Increment 24 already relied on
(`unique-contended-lock-wait-passed` and friends) staying green with this
fourth lock also downgraded — no new test was needed since the existing
suite already exercises this row under real concurrent contention.

**Claim 5 (cleanups)**:

- `unique_admission.classify_query_error`'s dedicated `job_id` foreign-key-
  violation branch was byte-identical in behavior to the general fallback
  it sat above (`submission.NotCommitted(violated)` where `violated` is the
  same value the fallback's own `submission.NotCommitted(error)` already
  wraps) — dead code, not a distinct outcome. Removed; the doc comment
  above `classify_query_error` now explains why the general case already
  covers it correctly.
- `sql/prune_finished.sql` now counts server-side (`WITH doomed AS (...),
deleted AS (DELETE ... RETURNING 1) SELECT count(*) FROM deleted`)
  instead of `RETURNING x.id` and `list.length`-ing the rows in
  `postgres.run_prune` — saves transferring up to `limit` row ids over the
  wire on a large batch just to discard them for a count. Regenerated via
  Squirrel; `sql.PruneFinishedRow` is now `PruneFinishedRow(count: Int)`.
- The schema compatibility check (`postgres.read_schema_generation` →
  `validate_expected_shape`) did not check `grind_v12`'s own three `ON
DELETE CASCADE` foreign keys at all: a plain foreign key backs no
  `pg_class` relation of its own, so `v12_shape`'s existing relation check
  could never have caught one being dropped by hand. `migrations.Migration`
  gained a `foreign_keys: List(String)` field (checked against
  `pg_constraint`, independently of `shape`); `v12_foreign_keys` lists the
  three constraint names. Proven by a new test
  (`postgres_migration_missing_foreign_key_shape_detected_test`,
  `missing-foreign-key-not-repaired`): drops
  `grind_job_acknowledgements_job_id_fkey` from an otherwise-genuine `v12`
  install, and confirms `postgres.migrate` now reports `IncompatibleSchema`
  rather than silently trusting the marker.

Gate: 201 passed (root, one new test — `postgres_migrate_upgrade_from_frozen_v11_fixture_test`'s
own orphan-cleanup assertions ride the existing test, adding no new count;
`postgres_migration_missing_foreign_key_shape_detected_test` is the one new
test function), 11 passed (consumer), `nix flake check` green, reproduced
across multiple full runs.

## Increment 26 — automatic polling fills every free slot (docs/RISKS.md risk 6)

**Claim (correctness, red-first)**: automatic-polling throughput used to be
bounded by `maximum_jobs_per_poll / poll_interval` regardless of
`maximum_concurrency` — `Poll` reset a per-round claim budget to
`maximum_jobs_per_poll` (default 1), `fill_automatic_slots` spent it one
claim at a time, and once it hit zero `continue_if_idle` waited a full
`poll_interval` even with a backlog still queued. Fixed by removing the
per-round budget (`ConsumerState.poll_remaining_jobs`) entirely:
`fill_automatic_slots` now keeps claiming into every free slot until either
`maximum_concurrency` is reached or a claim finds nothing, and only that
"found nothing" (or failed, or an unexpected worker exit) outcome routes to
`continue_if_idle`, which backs off to the next `Poll` timer at the
configured interval.

Red first, against the pre-fix code at this branch's parent commit
(`c6ee5cf`, `src/grind/queue.gleam` unmodified): added
`postgres_automatic_consumer_drains_backlog_without_per_interval_ceiling_test`
(`test/grind_test.gleam`) — `maximum_concurrency: 10`, a backlog of 50
near-instant jobs, `poll_interval: 1000`ms — and ran it standalone (a
disposable single-database Postgres with only
`GRIND_TEST_QUEUE_DATABASE_URL`/`GRIND_TEST_MARKER` set, not the full
`scripts/test-postgres.sh` matrix) against the unmodified queue actor:

```
panic src/gleeunit/should.gleam:10
test: grind_test.postgres_automatic_consumer_drains_backlog_without_per_interval_ceiling_test
info:
5
should equal
50
```

5 of 50 jobs succeeded within the bounded wait (4 poll intervals), matching
the ceiling's own prediction exactly (one claim at `t = 0, 1000, 2000,
3000, 4000`ms — five claims, not fifty). All 202 other tests this
standalone run could reach passed, including the new idle-consumer
companion test below, confirming the failure is specific to the backlog
ceiling and not a setup defect.

Green, same test, after the fix: the bounded wait (`4 * poll_interval_ms =
4000ms`) reaches all 50 `succeeded` rows, and a second, independent
assertion reads `finished_at` for the whole backlog back from the
database's own clock (`max(finished_at) - t0`, both timestamps read from
the same connection, never the test process's local wall clock) and
requires it stay under `job_count / 2 * poll_interval_ms` (25000ms) — a
loose bound chosen only to rule out the old ceiling (which would need
~50000ms for this backlog), not to assert a specific throughput number.

**Companion (behavior-preserving, not red-first)**:
`postgres_automatic_consumer_waits_full_interval_when_idle_test` proves the
fix does not turn an idle consumer into a busy loop: after letting an
automatic consumer with `poll_interval: 700`ms go idle, a job submitted
mid-interval is still `Queued` at `t_submit + 350`ms (half the interval —
a busy loop would have claimed it almost immediately) and reaches
`Succeeded` with `finished_at - t_submit` between 350ms and 1400ms (close
to, not many multiples of, one interval), both timestamps again read from
the database's own clock. This test already passed against the pre-fix
code (the old design never busy-looped; only its per-round _budget_
throttled unrelated to idleness), so it is committed purely as a
regression guard for the fix above, not as red-first evidence of its own.

`maximum_jobs_per_poll` was narrowed to `maximum_batch_jobs`
(`with_maximum_jobs_per_poll` -> `with_maximum_batch_jobs`,
`PolicyError.MaximumJobsPerPollMustBePositive` ->
`MaximumBatchJobsMustBePositive`): the field's one remaining, distinct
meaning is bounding how many jobs one manual `process_available` call
processes before returning (`run_batch_from`), unrelated to automatic
polling now that automatic filling depends only on `maximum_concurrency`.
This is a breaking, pre-release rename — existing automatic-mode tests
that set the old field purely to work around the per-poll ceiling
(auto-drain, auto-capacity, free-capacity, fault-proxy T3, and the
consumer package's `run_public_consumer_test`) simply drop the setting;
manual-batch tests (`batch-partial`, `batch-policy`) move to
`with_maximum_batch_jobs`.

Gate: `scripts/test-postgres.sh` green (203 passed root — the 201 from
Increment 25 plus the two new tests above — 11 passed consumer, pinned
Oban oracle harness green, Squirrel check green), `gleam check` green for
both `grind` and `consumer/`, `nix fmt` (one file reformatted, re-verified
green), `nix flake check` green, `git diff --check` clean. Committed at
`ac1b20d`.

## Increment 27 — storage owner identifies the database, not the URL (docs/RISKS.md risk 7)

**Privilege check (done before writing any code, per the design question this
increment had to answer)**: whether `pg_control_system()` needs superuser or
`pg_read_all_settings`, which would have forced a Grind-owned identity row
(and a v12 migration change) instead. Verified empirically against a real
disposable PostgreSQL 16.15 cluster: created an ordinary role granted only
`CONNECT` on the database (no other grant), and `SELECT
(pg_control_system()).system_identifier` succeeded identically for that role
and for the bootstrap superuser. No migration is needed; `storage_owner_identity.sql`
(new, Squirrel-generated) reads `pg_control_system()`'s `system_identifier`
plus `current_database()`/`current_schema()` with no `FROM` clause at all.

**Claim (correctness, red-first)**: `storage_owner` was `"<host>:<port>/
<database>"`, parsed from the connection URL by `validate` before any
connection existed — two pools reaching the same physical database through
different endpoints got different, mutually invisible owners. Fixed by
moving resolution into `start`, after connecting: `resolve_storage_owner`
reads the database's own identity (or uses an explicit
`with_storage_owner` override, with no query) instead.

Red first, against the pre-fix code at this branch (the parent commit,
`ac1b20d`, `src/grind/postgres.gleam` unmodified — the new
`with_storage_owner`/`StorageOwnerMustNotBeEmpty` surface does not exist yet
in that code, so only the convergence test, which uses exclusively
pre-existing public API, could run against it; the override test was
temporarily removed from the file for this one run and restored
immediately after). Ran standalone (a disposable single-database Postgres
with only `GRIND_TEST_DATABASE_URL`/`GRIND_TEST_MARKER` set):

```
panic src/gleeunit/should.gleam:10
test: grind_test.postgres_storage_owner_converges_across_different_endpoints_test
info:
"127.0.0.1:28347/grind_test"
should equal
"localhost:28347/grind_test"
```

Exactly the predicted shape: two pools reaching the identical database via
`127.0.0.1` and `localhost` (both resolving to the same disposable cluster)
got two different, URL-derived owner strings. All 203 other reachable tests
in that standalone run passed, confirming the failure is specific to this
claim.

Green, same test, after the fix: `postgres.storage_owner(database_a) ==
postgres.storage_owner(database_b)`, and a job submitted through
`database_a` is read back through `database_b` via `postgres.state` with no
`StorageOwnerMismatch` — the identity call now returns the same
`system_identifier`/`current_database()`/`current_schema()` triple
regardless of which host string reached it.

**Companion (new surface, not red-first)**:
`postgres_storage_owner_override_is_explicit_and_validated_test` covers
`with_storage_owner` itself — `validate` rejects an empty override with
`StorageOwnerMustNotBeEmpty`, and a non-empty override is used verbatim as
the resolved `storage_owner`, with no database query at all. This is new
public API with no pre-fix equivalent, so it is committed purely as
coverage for the surface, not as red-first evidence.

**Checked, not changed**: every `grind_fault_proxy_test.gleam` scenario that
starts a second `Database` against the same underlying cluster (T2, T3, both
naming their second value `observer`) only ever reads that second value's
raw `pog.Connection` (`postgres.connection(observer)`) for direct SQL —
never a typed, owner-checked call (`postgres.state`, `bind_handle`, ...)
against it. None of them depended on the old URL-derived owner divergence
between the direct endpoint and the proxy's own port, so none needed any
change for this fix.

Gate: `scripts/test-postgres.sh` green (205 passed root — the 203 from
Increment 26 plus the two new tests above — 11 passed consumer, pinned Oban
oracle harness green, Squirrel check green, including the new
`storage_owner_identity` query), `gleam check` green for both `grind` and
`consumer/`, `nix fmt` (one file reformatted, re-verified green), `nix
flake check` green, `git diff --check` clean. Committed at `fc9454e`.

## Increment 28 — automatic filling yields to the mailbox one claim at a time (docs/RISKS.md risks 4 and 5)

**Claim (correctness, red-first)**: `fill_automatic_slots` used to recurse
directly through `start_attempt`/`continue_after_start` from inside one
`Poll` (or `AttemptReturned`) message handler, so a burst that filled every
free slot up to `maximum_concurrency` did so without ever returning to the
coordinator's own mailbox in between — each claim, while in flight, still
blocks the single coordinator process the same way a stalled acknowledgement
does (`docs/RISKS.md` risk 4), so a burst of slow claims could stall a
pending `Renew`/`AttemptReturned`/`BeginShutdown` for up to
`maximum_concurrency × D` instead of one claim's worth, and a `stop` issued
mid-burst could still let up to `maximum_concurrency` jobs be claimed before
shutdown took effect. Fixed by replacing the direct recursion with a
`FillSlots` message: `continue_after_start`'s `Automatic` branch now asks
`request_fill` to send this coordinator's own incarnation a `FillSlots`
message (guarded by a new `fill_pending` flag, the same single-outstanding-
message shape `poll_scheduled` already uses for `Poll`) instead of calling
`fill_automatic_slots` again directly; `FillSlots` is handled in
`handle_message` exactly like `Poll` (clears `fill_pending`, then filling
only if not `shutting_down`). `fill_automatic_slots` itself now performs at
most one claim per call — filling more than one slot always goes back through
the mailbox first, so any `Renew`/`AttemptReturned`/`BeginShutdown` message
already queued ahead of a `FillSlots` is handled first.

Red first, against the pre-fix code at this branch's parent commit (`c3b1e61`,
`src/grind/queue.gleam` unmodified): added both new tests below to a worktree
checked out at that commit (test additions only, `queue.gleam` untouched) and
ran them standalone (a disposable single-database Postgres with only
`GRIND_TEST_QUEUE_DATABASE_URL`/`GRIND_TEST_MARKER` set, not the full
`scripts/test-postgres.sh` matrix — configured with
`synchronous_standby_names=grind_never_standby -c synchronous_commit=local`
to match the script's own cluster):

```
panic src/gleeunit/should.gleam:10
test: grind_test.postgres_automatic_fill_yields_to_shutdown_between_claims_test
info:
False
should equal
True
```

`should.be_true(claimed < job_count)` (`claimed < 10`) evaluated `False`: the
pre-fix coordinator claimed the _entire_ ten-job backlog before `BeginShutdown`
(sent, and confirmed queued behind the coordinator's still-blocked first
claim, while the very first claim's own `UPDATE` was held on a test-forced
`pg_advisory_xact_lock` barrier) ever took effect — exactly the "a stop
mid-burst still claims up to C jobs" bug this fix closes. All 206 other
reachable tests in that standalone run passed, including the companion
hot-loop test below, confirming the failure is specific to this claim.

Green, same tests, after the fix (`nix develop --command gleam test`, twice
in a row against a freshly recreated database each time, to rule out
timing flakiness): `postgres_automatic_fill_yields_to_shutdown_between_claims_test`
now observes `claimed == 1` — only the one claim already genuinely in flight
when `BeginShutdown` landed completes; `BeginShutdown` is handled (via FIFO
mailbox order) before the `FillSlots` message that same claim's own
completion enqueues, so no further claim is ever attempted.

**Companion (behavior-preserving, not red-first)**:
`postgres_automatic_fill_does_not_hot_loop_on_claim_error_test` proves the
fix does not turn a failing claim into a hot retry loop: `start_attempt`'s
`Error` branch already went (and still goes) through
`finish_without_claim`/`continue_if_idle`, never through `request_fill`, so a
persistently broken claim (forced by a `BEFORE UPDATE` trigger that always
raises, after bumping a plain sequence — `nextval` is not rolled back by the
trigger's own forced abort, unlike an ordinary table write, so it survives to
count real attempts) is retried at most once per `Poll` interval, never
faster. This test already passed against the pre-fix code (the old direct
recursion only ever _added_ claims after a _successful_ one; a failing claim
already stopped at `continue_if_idle` either way), so it is committed purely
as a regression guard for the fix, not as red-first evidence of its own.

Also updated: `docs/RISKS.md` risks 4 and 5 now note explicitly that a slow
or stalled _claim_ is the same kind of coordinator stall a stalled
acknowledgement is (risk 4), and that the `FillSlots` fix is what makes
their shared "one stall" derivation actually hold for automatic polling
(risk 5) — neither risk is fully closed (the multi-sibling
`maximum_concurrency > 2` gap in risk 5, and the general single-coordinator
design in risk 4, both remain open); risk 6's evidence paragraph is reworded
to stop overclaiming its regression tests are purely "database-clock-bound"
(each still waits out a local, bounded polling loop before reading its actual
pass/fail evidence from the database's own clock) and drops a stale,
session-local scratchpad path that was never a real repository artifact.
`maximum_batch_jobs`'s own doc comment now states plainly that its default of
1 makes `process_available` handle exactly one job per call, and why the
default is kept at 1 rather than raised (kept consistent with
`default_policy`'s own `maximum_concurrency: 1`, both a deliberate,
conservative, explicit-opt-in default).

Gate: `scripts/test-postgres.sh` green (207 passed root — the 205 from
Increment 27 plus the two new tests above — 11 passed consumer, pinned Oban
oracle harness green, Squirrel check green), `gleam check` green for both
`grind` and `consumer/`, `nix fmt` (no files reformatted), `nix flake check`
green, `git diff --check` clean.

## Increment 29 — `storage_owner` removed entirely; isolation is the PostgreSQL schema (docs/RISKS.md risk 7, superseding Increment 27)

**Claim (design change, USER DECISION)**: Increment 27 fixed _how_ the
owner value was derived (from the database's own identity instead of the
connection URL) but kept the underlying concept — a `storage_owner` column
on every table, scoped by an application-level value. This increment drops
the concept itself: isolation between logically distinct Grind
installations is now simply the PostgreSQL schema a pool's `search_path`
resolves to (see README, "Isolation"). `storage_owner` is removed from
`grind_jobs` and every receipt/resolution/submission table, from every
query (inline SQL and Squirrel-generated), from the unique advisory-lock
key (which keeps `current_schema()`, spliced literally, so two schemas
still never share a lock), from `Database`/`JobHandle`/`Conflict`
metadata, and from every error variant that existed only to report an
owner mismatch (`StorageOwnerMismatch`, `CancellationStorageOwnerMismatch`,
`StorageOwnerIdentityUnavailable`, `StorageOwnerMustNotBeEmpty`).
`postgres.with_storage_owner`, `postgres.storage_owner`, and
`storage_owner_identity.sql` are deleted outright. `grind_v12` — still
unreleased, edited in place per `AGENTS.md` — both adds `finished_at`
(Increment 21) and drops `storage_owner` everywhere, since both belong to
the same not-yet-shipped version; `grind_v11` (already released, frozen)
is untouched.

**Schema, before (v11, frozen) → after (v12, this change)**, confirmed
against a real disposable cluster via `pg_class`/`pg_constraint`, not
inferred from the DDL text alone:

- `grind_jobs_unique_candidate_idx`: `(storage_owner, worker_id,
worker_version, unique_key_contract, unique_key_sha256)` →
  `(worker_id, worker_version, unique_key_contract, unique_key_sha256)`
  (a non-unique performance index — no collision risk from dropping its
  leading column).
- `grind_job_acknowledgements_pkey`: `(storage_owner, command_id)` →
  `(command_id)`. `_attempt_key`: `(storage_owner, job_id, attempt_id,
attempt_epoch)` → `(job_id, attempt_id, attempt_epoch)`. Both provably
  collision-free: `command_id` and `job_id` are derived from `grind_jobs.id`
  (one `bigserial` sequence) and `grind_attempts_id_seq` (one shared
  sequence) — never from anything a caller could independently reuse
  across what used to be different owners.
- `grind_job_resolutions_pkey`: `(storage_owner, resolution_id)` →
  `(resolution_id)`. `resolution_id` is an operator-chosen audit label —
  genuinely collision-risked (see below).
- `grind_unique_submissions_pkey`: `(storage_owner, submission_id)` →
  `(submission_id)`. `submission_id` is a caller-chosen idempotency key —
  genuinely collision-risked (see below).
- `grind_jobs_finished_idx`/`grind_jobs_claim_idx`/`grind_jobs_quarantine_idx`
  (Increment 21/22, created fresh by `v12` itself, so edited in place
  rather than dropped and rebuilt): each loses its leading `storage_owner`
  column — a single schema's own `grind_jobs` never held more than one
  distinct value there to begin with, so this is a pure simplification, not
  a behavior change to what these indexes serve.
- `pg_class` object count for a fresh `v12` install: still exactly the 20
  relations `v12_shape()` declares (`grind_schema_migrations` ×2,
  `grind_jobs` ×6, `grind_job_resolutions` ×3, `grind_job_acknowledgements`
  ×4, `grind_attempts_id_seq` ×1, `grind_unique_submissions` ×3) — object
  _names_ are unchanged throughout, only column composition, so
  `read_schema_generation`'s own shape check needed no changes at all.

**The real collision risk, and why it is narrower than it first looks.**
Of the four dropped composite keys, only two can actually collide once
`storage_owner` is gone: `resolution_id` and `submission_id` are
caller/operator-chosen strings with no structural uniqueness guarantee, so
two different `storage_owner` values that deliberately shared one physical
schema on `v11` (via the now-removed `with_storage_owner` override) could
have coincidentally reused the identical string for unrelated purposes.
`command_id` and the acknowledgement attempt key are safe by construction
(derived from globally-unique sequences, never caller input) and needed no
check. Each of the two at-risk tables gets its own `DO $$ ... RAISE
EXCEPTION $$` block, immediately before that table's `DROP COLUMN`,
checking `count(DISTINCT storage_owner) > 1` per key value — **fail loudly
with a clear, named error, never silently merge** — before either
`ALTER TABLE ... DROP CONSTRAINT`/`DROP COLUMN` ever runs.

**Red-first evidence for the collision check**, against a real disposable
cluster seeded with the frozen `v11` fixture (`test/fixtures/schema/v11.sql`)
plus two rows under different `storage_owner` values sharing one
`submission_id` (`'shared-sub-id'`) and, separately, one `resolution_id`:
running `grind_v12`'s own `up` section against each seed produces exactly
the named, actionable error and touches nothing else:

```
psql:v12-up.sql:60: ERROR:  grind_v12: two distinct storage owners share a submission_id; dropping storage_owner would silently merge their grind_unique_submissions rows. Resolve this collision manually (rename or remove one side) before migrating.
```

```
psql:v12-up.sql:42: ERROR:  grind_v12: two distinct storage owners share a resolution_id; dropping storage_owner would silently merge their grind_job_resolutions rows. Resolve this collision manually (rename or remove one side) before migrating.
```

(Against the real `postgres.migrate_with` path rather than raw `psql`, the
whole step's transaction rolls back cleanly on either error — the same
`MigrationStepFailed(12, error)` shape every other failing step already
reports — never a partial commit.)

**Mutation evidence**: removing the `submission_id` `DO` block from the
migration's own statement list and re-running against the identical seeded
collision still fails closed — never silently merges — but with
PostgreSQL's own generic constraint error instead of the named one:

```
psql:v12-up-mutated.sql:65: ERROR:  could not create unique index "grind_unique_submissions_pkey"
DETAIL:  Key (submission_id)=(shared-sub-id) is duplicated.
```

This is exactly the distinction the `DO` block exists to make: both paths
refuse to merge the collision, but only the un-mutated one names the real
cause (two distinct owners sharing a key) and the remedy, rather than
leaving an operator to work out from a bare `23505` on a mid-migration
`ADD CONSTRAINT` what actually went wrong.

**Green (ordinary case, no collision)**: the identical `v12` `up` section
applied to a `v11` fixture with no cross-owner key collisions (every
existing installation under the default, non-override owner resolution —
one schema always had exactly one implicit owner value to begin with)
completes with no error, confirmed against the frozen upgrade-harness
fixture's own full seed (six `grind_jobs` states, a real `submit_unique`
receipt, an acknowledgement, a resolution, and three deliberately orphaned
receipts from Increment 25 — see `postgres_migrate_upgrade_from_frozen_v11_fixture_test`).

**Red found and fixed during this change, not merely anticipated**: the
upgrade-harness test's own `submit_unique` seed call (used specifically
because a hand-written fingerprint could never honestly match what a real
caller's replay after the upgrade must match) genuinely failed the first
time this whole change was run against it, since today's application code
never writes `storage_owner` at all any more, and the frozen `v11` fixture
still declares it `NOT NULL` with no default:

```
let assert test/grind_test.gleam:3224
 test: grind_test.postgres_migrate_upgrade_from_frozen_v11_fixture_test
value: Error(NotCommitted(PostgresqlError("23502", "not_null_violation", "null value in column \"storage_owner\" of relation \"grind_jobs\" violates not-null constraint")))
```

Fixed by adding a test-only `ALTER TABLE ... ALTER COLUMN storage_owner SET
DEFAULT` right after applying the frozen fixture (never touching the frozen
fixture file itself, which `grind_migrations_conformance_test` still checks
byte-for-byte) — the same shape a real `v11` database that happened to
already have a default would have, which the migration must handle
correctly regardless. Green after the fix, full gate below.

**Isolation tests, replacing the owner-specific ones removed
(`postgres_handles_are_bound_to_storage_owner_test`,
`postgres_storage_owner_converges_across_different_endpoints_test`,
`postgres_storage_owner_override_is_explicit_and_validated_test`,
`postgres_resolution_rebind_checks_storage_owner_test`)**:

- `postgres_two_schemas_share_a_database_but_stay_isolated_test` — two
  PostgreSQL roles, each with its own like-named schema (`CREATE SCHEMA
AUTHORIZATION <role>`, relying on nothing but PostgreSQL's own default
  `search_path` of `"$user", public` — the "recommended setup" README now
  documents), sharing one physical database: jobs (row counts, never a
  cross-schema handle read — two fresh schemas can legitimately mint the
  identical id, so a count is the correct proof, not a coincidence-prone
  lookup), uniqueness (the identical key/queue/`SubmissionId` independently
  admits `Inserted` in both schemas, never contending), quarantine, and
  retention are each proven isolated per schema.
- `postgres_two_urls_to_the_same_schema_share_it_test` — the Increment 27
  convergence idea kept, minus the owner-equality assertion: two URLs
  (`127.0.0.1` vs `localhost`) asserted genuinely different strings first,
  then proven to reach the identical schema (a job submitted through one is
  read back through the other).

**Checked, not changed**: `test/grind_fault_proxy_test.gleam` and the
`consumer/` package reference no `storage_owner`/`with_storage_owner`
surface at all (confirmed by search and by `gleam check` passing for both
with zero changes needed there).

Gate: `scripts/test-postgres.sh` green (205 passed root — replacing, not
adding to, Increment 28's 207: four owner-specific tests removed, two
schema-isolation tests added, net −2 plus the `resolution_route_a/b`
fixture's own now-dead plumbing removed — 11 passed consumer, pinned Oban
oracle harness green, Squirrel check green), `gleam check` green for both
`grind` and `consumer/` with zero warnings, `nix fmt` (5 files reformatted,
re-verified green), `nix flake check` green, `git diff --check` clean. Run
twice in full after the last formatting pass, both green.

---

## Increment 30 — explicit `Settings.schema`, handle-installation binding, an automated migration-collision test, and a forbidden-columns shape check (independent-review fixes to Increment 29, docs/RISKS.md risk 7)

**Context.** An independent review of Increment 29 (`5c88aa5`) accepted the
schema-based isolation design but flagged that it still left the schema
itself _inferred_ rather than configured, left no way to catch a
`JobHandle`/`PendingSubmission` used against the wrong installation, and had
only manual/psql evidence (not an automated test) for the v11→v12
collision-detection `DO` blocks. This increment closes all three, plus a
"forbidden columns" shape check and several smaller doc/test fixes flagged
in the same review.

**1. Explicit schema, not inferred.** `postgres.Settings` gains a `schema:
String` field (default `"public"`) and `postgres.with_schema`;
`postgres.validate` pins every pooled connection's own `search_path`
connection parameter to exactly that one configured schema (quoted via a
new `quote_ident`, doubling embedded `"` characters), unconditionally —
never left to whatever the connecting role or database would otherwise
default to. `migrate`/`migrate_with` create the configured schema if absent
(`ensure_schema_exists`, new `StorageError.SchemaCreationFailed`), checking
existence first via a plain `pg_namespace` lookup and only ever attempting
`CREATE SCHEMA IF NOT EXISTS` for a genuinely absent schema (see "Red found
and fixed" below for why the existence check is not optional). The
uniqueness admission advisory-lock key
(`unique_admission.lock_key_sql`/`lock_query`/`acquire_lock`) now binds this
configured schema as an ordinary SQL parameter instead of splicing
`current_schema()` into the query text — correct independently of
`search_path`'s own resolution rules, not merely by relying on the
connection-parameter pin holding for reasons outside the key's own control
(see docs/UNIQUENESS-CONTRACT.md and README, "Isolation", both rewritten).

**Red-first evidence — the `$user` `search_path`-fallback hazard is real,
not hypothetical.** `postgres_user_schema_fallback_shares_one_installation_test`
(`test/grind_test.gleam`) builds the exact scenario the coordinator named:
two PostgreSQL roles, each with its own personal, empty schema (`CREATE
SCHEMA AUTHORIZATION <role>`) ahead of `public` on PostgreSQL's own default
`search_path` (`"$user", public`), where Grind's real tables live only in
`public` — neither role ever calls `with_schema`, both simply take the
`"public"` default. Confirmed directly with `psql` against a throwaway
cluster, independently of any Grind code, that this is a genuine hazard
under the _pre-fix_ lock-key formula (`current_schema()` spliced into the
key, as Increment 29 left it): each role's own `SELECT current_schema()`
resolves to that role's own personal schema (`current_schema()` reports the
first schema in `search_path` that merely _exists_, regardless of whether
it holds any Grind object), while `to_regclass('grind_jobs')` in both
sessions resolves to the identical `public.grind_jobs` oid — two sessions
racing the same uniqueness key would therefore acquire _different_
advisory locks while contending on the same physical table, a genuine
duplicate-admission defect. After this increment's fix (schema bound
explicitly, `search_path` pinned to it), the test proves the fix directly
against real `postgres.submit_unique` calls: `postgres.installation`
(the new in-memory token, see point 2) is asserted identical for both
roles' `Database` values, and a call to `submit_unique` with an identical
key from each role in turn commits exactly one `Inserted` and one
`Existing` — never two independent rows.

**2. Handle-installation binding.** A new `grind/job.Installation` opaque
type (this physical database's `pg_database.oid` — read once, cheaply, at
`postgres.start`, new `StartError.InstallationQueryFailed` — plus the
configured schema) is stamped onto every `JobHandle` and `PendingSubmission`
at mint/bind time (`job.new_handle`, `submission.new_pending_submission`,
both gained an `installation` parameter). `state`, `cancel`, `arguments`,
`outcome`, `reconcile_acknowledgement`, `resolve_uncertain`, and
`reconcile_unique` each check the handle's/pending's own installation
against the `Database` they are called on _before_ any storage call, and
return a new typed mismatch error on disagreement
(`postgres.HandleFromAnotherInstallation`,
`postgres.CancellationFromAnotherInstallation`,
`postgres.ResolutionFromAnotherInstallation`,
`submission.HandleFromAnotherInstallation`). This is documented throughout
as a client-side sanity check only — the real isolation boundary stays the
PostgreSQL schema itself; nothing about this token is ever persisted or
compared against a stored value.

**Red-first evidence.**
`postgres_handle_from_another_installation_is_rejected_test` mints a handle
against schema A, then calls `state`/`arguments`/`cancel`/`outcome` against
`Database` B (a genuinely different schema on the same physical database,
confirmed via `postgres.installation(database_a) != postgres.installation(database_b)`)
where a same-numeric-id row also happens to exist (each schema mints its
own id 1 independently) — before this fix, every one of those calls would
have silently read or mutated schema B's own row (proven implicitly: the
`storage_fields`/`result_fields`/`reconciliation_fields` tuples this check
was added to carried no installation information at all prior to this
increment, so nothing before this change could have distinguished the two
handles). After the fix, all four calls return the new typed mismatch
error, and the matching same-schema call (`state(database_a, handle_a)`)
still succeeds normally, proving the rejection is genuinely about
installation identity and not a broken handle.

**3. Automated migration-collision test**, replacing Increment 29's
manual/psql-only evidence:
`postgres_migration_submission_collision_detected_test` and
`postgres_migration_resolution_collision_detected_test` each seed the
frozen v11 fixture (`test/fixtures/schema/v11.sql`) with two distinct
legacy `storage_owner` values sharing one `submission_id`/`resolution_id`
(legal under v11's own composite primary key) and assert
`postgres.migrate` returns `Error(postgres.MigrationStepFailed(12,
pog.PostgresqlError(_, _, message)))` with the exact named collision
message, and that the schema marker stays at `11` (the step's own
transaction rolled back cleanly, never partially applied).

**Red found and fixed while writing these two tests (not merely
anticipated).** The first draft of both tests seeded their two colliding
receipt rows referencing `job_id`s with no matching `grind_jobs` row at
all. `migrate` returned `Ok(Nil)` — a clean, unexpected success:

```
panic test/grind_test.gleam:3876
 test: grind_test.postgres_migration_submission_collision_detected_test
 info: expected MigrationStepFailed(12, _), got Ok(Nil)
```

Root-caused by inspecting the post-migration table directly (`psql`): both
seeded rows were gone — zero rows, not two, not one. `v12_statements`'s own
orphan cleanup (`DELETE FROM grind_unique_submissions r WHERE NOT EXISTS
(SELECT 1 FROM grind_jobs j WHERE j.id = r.job_id)`, Increment 25) runs
_before_ the collision `DO` block, and silently swept away both "orphaned"
receipts (naming a `job_id` that never existed) before the collision they
seeded was ever checked. Fixed by seeding a real `grind_jobs` row per side
first (new `seed_legacy_job` helper) and binding its real returned id into
each receipt insert — confirmed green afterward. This is exactly the kind
of test-authoring mistake red-first discipline is supposed to catch before
it ships as a false "the fix works" signal.

**4. Forbidden-columns shape check.** `migrations.Migration` gains a
`forbidden_columns: List(#(String, String))` field — the inverse of the
existing `key_columns` "must exist" check. `v12_forbidden_columns()` lists
`storage_owner` on all four tables it was dropped from.
`postgres.read_schema_generation`'s own shape validation
(`validate_expected_shape`, via a new `forbidden_columns_absent`, one
`information_schema.columns` query joined against an `unnest` of the
forbidden pairs) now fails closed (`IncompatibleSchema`) if any forbidden
column is still physically present despite the marker claiming that
version — catching a stale pre-edit dev database that ran an _older_ copy
of `v12_statements` (from before `storage_owner` was ever dropped from it)
whose marker already claims 12 but whose physical shape does not match.

**5. `RISKS.md` #10 — the 6-second figure is now flagged as a lower
bound**, not re-measured: `grind_v12` was edited in place (Increment 29,
after the original 6-second figure was measured in Increment 25) to add
index/primary-key rebuilds on three tables and two full-table `GROUP BY`
collision scans, none of which have themselves been measured at 2,000,000
rows. `README.md`'s own mention of the same figure is corrected the same
way.

**6. Smaller fixes from the same review:**

- A one-sentence lossiness note added to `v12_statements`'s own doc comment:
  the `down` direction re-adds `storage_owner` as `NOT NULL DEFAULT ''` on
  every row, never the original per-row value, which the `up` direction
  already discarded.
- `docs/UNIQUENESS-CONTRACT.md`'s "Schema v11" section corrected: its claim
  that `grind_unique_submissions` has "no foreign key ... matching the rest
  of the schema's convention of no cross-table foreign keys" was accurate
  for v11 alone but stale once `grind_v12` (documented a few sections later
  in the same file) deliberately breaks that convention — now says so
  explicitly and cross-references "Schema v12".
- `queue.gleam`'s `fill_pending` field doc corrected: it previously claimed
  `FillSlots` is "sent immediately after every successful automatic claim
  _that still has free capacity left_", but `request_fill` (via
  `continue_after_start`) sends it unconditionally after every automatic
  completion — the capacity check happens only once the message is handled,
  inside `fill_automatic_slots`. The code was already correct (an extra,
  harmless dispatch costs one mailbox round-trip, never a wasted claim); only
  the comment overclaimed a pre-check that was never there.
- `postgres_two_urls_to_the_same_schema_share_it_test` no longer varies the
  hostname (`127.0.0.1` vs `localhost`) to prove "two different-looking
  URLs, same schema" — that construction silently depended on `localhost`
  resolving to the same IPv4 loopback address this suite's cluster binds,
  not guaranteed on every machine (an IPv6-first resolver could send
  `localhost` to `::1`, where nothing listens). Now appends an inert,
  unrecognized query parameter `pog.url_config` never inspects (confirmed by
  reading that function: it looks up only `sslmode` by key and ignores
  every other query parameter), keeping the "genuinely different string,
  identical target" property without any DNS dependency.
- `postgres_two_schemas_share_a_database_but_stay_isolated_test` updated to
  call `with_schema` explicitly for each role, since the `$user`-fallback
  "recommended setup" it previously demonstrated (zero Grind-specific
  configuration) is superseded by explicit configuration — the schema
  itself is unchanged (`CREATE SCHEMA AUTHORIZATION <role>`), only how each
  pool is told to use it.

**Red found and fixed in the schema-creation path itself (not part of the
original plan, found while gating this increment).** The very first version
of `ensure_schema_exists` called `CREATE SCHEMA IF NOT EXISTS` unconditionally
on every `migrate` call. Against
`postgres_two_schemas_share_a_database_but_stay_isolated_test` and
`postgres_handle_from_another_installation_is_rejected_test` — both of
which use a role that owns its own already-_existing_ schema but holds no
broader database-level privilege, the least-privilege "recommended setup"
this same increment's README rewrite describes — this failed:

```
let assert test/grind_test.gleam:1939
 test: grind_test.postgres_two_schemas_share_a_database_but_stay_isolated_test
 code: let assert Ok(Nil) = postgres.migrate(database_a)
value: Error(SchemaCreationFailed(PostgresqlError("42501", "insufficient_privilege", "permission denied for database grind_owner_a")))
```

Root cause, confirmed against PostgreSQL's own documented behavior: `CREATE
SCHEMA`, even with `IF NOT EXISTS`, checks the connecting role's `CREATE`
privilege on the _database_ before it ever checks whether the schema
already exists — so a role that owns its own schema but was never granted
database-level `CREATE` gets `42501` on every single `migrate` call, even
though nothing would ever actually need creating. Fixed by checking
existence first via a plain, unprivileged `pg_namespace` lookup
(`schema_exists`) and only ever attempting `CREATE SCHEMA IF NOT EXISTS`
for a schema confirmed genuinely absent — preserving "migrate creates the
schema if absent" while never demanding a privilege an already-provisioned
least-privilege installation has no reason to hold. Both tests green after
the fix.

**Gate**: `scripts/test-postgres.sh` green — 209 passed root (four new
tests: the two collision tests, the `$user`-fallback test, the
cross-installation-handle test), 11 passed consumer, pinned Oban oracle
harness green, Squirrel check green. `gleam check` green for both `grind`
and `consumer/`. `nix fmt` applied and re-verified clean. `nix flake check`
green. `git diff --check` clean.

---

## Increment 31 — cluster-identifier disambiguation, a concurrent-schema-creation race fix, schema-name validation, and pooler documentation (second-round review fixes to Increment 30)

**Context.** A second independent review of Increment 30 (`c8b0fbb`)
accepted the design but found the `Installation` token could still
collide across two _different_ clusters built the same way (identical low
database OID, both defaulting to schema `"public"`), found
`ensure_schema_exists`'s own `CREATE SCHEMA IF NOT EXISTS` was not actually
concurrency-safe, found `Settings.schema` had no length/byte-content
validation, and asked for `docs/RISKS.md` #18 (connection poolers) and
`bind_handle`'s own doc comment to be updated. All four are addressed here.

**1. Cluster-identifier disambiguation.** `job.Installation` gains a third
field, `cluster_identifier: Option(Int)` — `pg_control_system()`'s own
`system_identifier`, a value generated once at `initdb` time and
effectively unique per cluster. Read once at `postgres.start`
(`read_cluster_identifier`), best-effort: `pg_control_system()` is a
restricted, superuser-adjacent function in stock PostgreSQL, so an
ordinary non-superuser connecting role (the common, least-privilege case)
typically cannot call it at all — any failure is swallowed into `None`
rather than ever failing `start` itself, since this disambiguation is a
client-side improvement, not something `start` depends on.
`job.same_installation` now requires the cluster identifier to also match
when both sides successfully read one; when either side could not, it
falls back to comparing only the database OID and schema, exactly as
before this field existed — a documented, residual gap (two different
clusters could still collide if `pg_control_system()` is unreadable on
either side), not a regression.

**Evidence.**
`postgres_installations_differ_across_databases_in_one_cluster_test` proves
the same-cluster case directly: two genuinely different physical databases
in the one disposable test cluster (both defaulting to schema `"public"`,
so the schema component alone cannot distinguish them) get different
`Installation` tokens — backstopped by the database OID either way,
whether or not the cluster identifier was itself readable for the
connecting role. The cross-_cluster_ collision this field exists to fix is
not independently reproducible from a single disposable test cluster (it
would need two separately-initialized clusters compared against each
other); documented instead, in `job.Installation`'s own doc comment and
`docs/RISKS.md` risk 7.

**2. Concurrent first-time schema creation.** `ensure_schema_exists`'s own
`CREATE SCHEMA IF NOT EXISTS` is not concurrency-safe on its own: two
sessions can both run the existence check first, both see the schema
absent, and both attempt the actual `CREATE` — PostgreSQL's own catalog
uniqueness check is what actually serialises them, surfacing to the loser
as a real error (`42P06 duplicate_schema` or `23505 unique_violation`
depending on timing) rather than the silent no-op `IF NOT EXISTS` might
suggest. Fixed: on any `CREATE SCHEMA` failure, `ensure_schema_exists`
re-checks existence and reports `Ok(Nil)` if the schema is now present,
regardless of which side actually created it.

**Evidence.**
`postgres_migrate_concurrent_first_time_schema_creation_both_succeed_test`
spawns two independent pools, both configured with the identical,
freshly-suffixed (never-before-used) schema name, and calls `migrate` on
both back-to-back from the same process with no artificial delay between
them — a real, if not perfectly deterministic, race between two genuine
PostgreSQL sessions (there is no lockable object to hold a hard barrier on
before the schema itself exists, unlike the advisory-lock-based barrier
`postgres_migrate_concurrent_migrators_both_apply_once_test` uses for the
_versioned-step_ race, which only ever engages after the schema already
exists). Both calls return `Ok(Nil)`, and the schema ends up at marker
version 12.

**3. Schema-name validation.** `postgres.validate` now rejects a
`Settings.schema` that is empty, exceeds PostgreSQL's own 63-byte
identifier limit (`NAMEDATALEN` 64, minus the terminator — a longer name
would otherwise be silently truncated by PostgreSQL itself to a different
schema than the one actually configured), or contains a NUL byte
(PostgreSQL text values cannot hold one at all; rejecting it outright is
safer than risking a driver- or C-level truncation silently changing which
schema is actually used) — all three fold into the existing
`ConfigError.InvalidSchema` variant, whose doc comment now describes all
three conditions.

**4. Documentation.** `docs/RISKS.md` risk 18 (no connection pooler
exercised) extended to name `search_path` as a third startup parameter a
pooler can silently drop or fail to apply per-checkout — and, specifically
for PgBouncer, why its transaction pooling mode is the sharpest version of
this risk: a client's own startup parameters are not necessarily applied
to whichever physical server connection it is handed per transaction
unless `search_path` is explicitly added to `track_extra_parameters` and
kept out of `ignore_startup_parameters`, so a pooler configured this way
would not error at all — it would simply run queries against whatever
schema that physical connection already happened to have, silently
breaking `postgres.with_schema`'s entire isolation guarantee. README's own
"Isolation" section gained a one-paragraph cross-reference to this same
risk. `postgres.bind_handle`'s own doc comment gained a note that a bare
`Int` id (as returned by `job.id_value` and typically what an application
persists across a restart) carries no `Installation` of its own to check
against — unlike a live `JobHandle` passed directly between calls, calling
`bind_handle` against the wrong `Database` for a stored id that happens to
also name a row there succeeds silently, binding to the wrong row, rather
than failing the way a live handle used against the wrong `Database` does.

**Gate**: `scripts/test-postgres.sh` green — 211 passed root (two new
tests: the cross-database-installation test, the concurrent-schema-creation
test), 11 passed consumer, pinned Oban oracle harness green, Squirrel check
green, run twice (once piped through `tail`, once with full output capture
to confirm exit 0 unambiguously). `gleam check` green for both `grind` and
`consumer/`. `nix fmt` applied and re-verified clean. `nix flake check`
green. `git diff --check` clean.

---

## Increment 32 — CI actually runs the PostgreSQL gate (docs/RELEASE-READINESS.md, "Packaging and documentation")

**Context.** `.github/workflows/ci.yml` ran only `gleam deps download`,
`gleam format --check`, `gleam build --warnings-as-errors`, and a plain
`gleam test` — no database configured at all. Every PostgreSQL-backed test
in `test/grind_test.gleam` short-circuits to a no-op with no
`GRIND_TEST_*_URL` set (its own `case database_url() { Error(Nil) -> Nil
... }` guard), so this CI was green regardless of whether any real
PostgreSQL behavior actually worked — the marker-checklist safety net that
makes `scripts/test-postgres.sh` fail closed on a silently-skipped test
lives entirely inside that shell script, never inside plain `gleam test`
itself, so CI never benefited from it.

**A second, independent problem found while fixing the first.** `gleam.toml`
depends on `sinal` as a local path dependency (`sinal = { path = "../sinal"
}`), not a Hex package. `actions/checkout@v4` with no `path:` checks a repo
out directly into `$GITHUB_WORKSPACE`, so `../sinal` would resolve to a
sibling of the _workspace_, not of the checked-out repo — a directory that
was never created by the old workflow at all. This means `gleam deps
download`/`gleam build` could not have resolved even the base compile step
on a clean checkout, independent of anything to do with PostgreSQL
coverage — either this workflow was already failing before this change, or
it had simply never been exercised end to end. Fixed by checking out
`gleam-dream/sinal` as an explicit sibling directory (`path: sinal` next to
`path: grind`) at a pinned commit (`f4622b67394965c18091ef95390cfd55ad6502db`,
matching what this session's own local dev environment already had
checked out) — pinned, not a floating branch, so a later Sinal change can
never silently break Grind's own CI without a deliberate bump.

**What changed.** `.github/workflows/ci.yml` now has two jobs:
`quick-check` (the original format/build/plain-test steps, relocated under
the sinal-sibling checkout, kept for fast feedback on a compile or format
error) and `postgres-gate` (new): `nix flake check`, Sinal's own test suite
(via Sinal's own `flake.nix`, matching `docs/RELEASE-READINESS.md`'s
long-standing "and Sinal's tests" goal), and
`nix develop --command bash scripts/test-postgres.sh` — the identical
command this whole gate has been run with, by hand, throughout this entire
review cycle, through the identical `flake.nix` dev shell rather than a
hand-rolled reconstruction of the toolchain. Since `scripts/test-postgres.sh`
already fails closed on any missing integration-contract marker, running it
verbatim in CI is what actually closes the original gap — a DB test that
silently skips now fails the job, not passes it. `actions/cache` added,
keyed on `manifest.toml`/`mix.lock` hashes, for `build/` (root, consumer,
and Sinal) and the Elixir oracle's `deps`/`_build`.

**Verification (GitHub Actions itself cannot be run from this environment):**

- `nix run nixpkgs#actionlint -- .github/workflows/ci.yml` — clean, no
  findings, after every edit to the workflow file.
- Every command the workflow runs was independently verified locally: `nix
develop --command gleam deps download`, `gleam format --check src test`,
  `gleam build --warnings-as-errors`, and a bare `gleam test` with no
  `GRIND_TEST_*_URL` set at all (confirming, concretely, the exact "211
  passed, no failures" false-confidence result this whole increment exists
  to stop CI from reporting) — all green. `cd sinal && nix develop --command
gleam test` — 80 passed, no failures. `nix develop --command bash
scripts/test-postgres.sh` (the `postgres-gate` job's own real command) —
  green, exit 0, confirmed twice.
- The `gleam-dream/sinal` pin is the exact commit this local development
  environment's own `../sinal` checkout is at (`git rev-parse HEAD`), so
  the workflow tests the same Sinal version this whole review cycle has
  actually been running against locally.

**Not independently verifiable from this environment, flagged rather than
assumed:** whether `gleam-dream/sinal` is reachable as a public GitHub
repository from a GitHub Actions runner (README already links it publicly;
not re-confirmed over the network here), and whether `cachix/install-nix-action@v27`
is still the current recommended major version of that action.

**Gate**: `scripts/test-postgres.sh` green (211 passed root, 11 passed
consumer, pinned Oban oracle harness green, Squirrel check green, exit 0
confirmed via full output capture). `gleam check` green for both `grind`
and `consumer/`. `nix fmt` applied and re-verified clean. `nix flake check`
green (required two rounds of restructuring a `docs/RELEASE-READINESS.md`
paragraph into shorter sub-bullets after the local interactive `nix fmt`
and the sandboxed `nix flake check` build disagreed on how to wrap a long
paragraph containing several inline code spans — an environment-level
formatter discrepancy, not a content defect; avoided rather than chased
further once the reformatted structure made both agree). `git diff --check`
clean. `nix run nixpkgs#actionlint` clean on the final workflow file.

## Increment 33 — CI/script robustness, and test-first coverage for Increment 31/32's own review findings

**Context.** A further review of `6842d39` (Increment 31) and `997d989`
(Increment 32) found: the oracle harness's `mix deps.get --check-locked`
has no Hex bootstrap of its own on a bare CI runner; the CI cache covers
`grind/build`/`grind/consumer/build` directories that
`scripts/test-postgres.sh` unconditionally `gleam clean`s anyway; several
CI hygiene gaps (`permissions`, a stale `install-nix-action`
major, a useless `nix_path`, a double-run trigger, `quick-check` never
building `consumer/`, no Nix-store cache); `job.same_installation` and
`postgres.validate`'s schema-name rejection had no direct unit tests; the
concurrent-schema-creation test raced two pools with no real barrier; and
`read_cluster_identifier` could raise (and log) a genuine PostgreSQL error
for a role denied the privilege. Every code/test item below was fixed
test-first: a new test proven red under a stated mutation, then green with
the fix. Two findings from this same review turned out to rest on premises
that did not hold empirically against a real PostgreSQL 16.15 cluster —
documented in place, not silently "fixed" against a fiction.

### 1. Hex/rebar bootstrap in `scripts/test-postgres.sh`

**Claim as stated**: a bare CI runner has no Hex archive and `mix
deps.get`'s own interactive "install Hex?" prompt reads EOF from a
non-interactive runner's stdin and aborts.

**What was actually verified.** Against this session's own Nix dev shell
(Elixir 1.18 / Mix, with network access to hex.pm and the OTP/rebar3 build
infrastructure), running `oracle`'s `mix deps.get --check-locked` with a
brand-new, empty `MIX_HOME`/`HEX_HOME` and stdin fully closed (`0<&-`, the
sharpest simulation of a non-interactive runner available here) did **not**
reproduce an abort: current Mix silently force-installs both the Hex
archive and its own rebar3 build with no prompt at all. The specific
"reads EOF and aborts" failure mode did not reproduce in this environment —
recorded here rather than claimed fixed on faith.

**What is genuinely true and fixed regardless.** That same unfixed run
built its own copy of rebar3 over the network (`* creating
.../mix/elixir/1-18-otp-28/rebar3`) — an avoidable network dependency the
dev shell's own `rebar3` (`flake.nix`) already provides — and used
whatever `~/.mix`/`~/.hex` the invoking user already had, never isolated
from the disposable gate run. `scripts/test-postgres.sh` now sets
`MIX_HOME`/`HEX_HOME` to a run-local directory under its own disposable
`$root` (already `rm -rf`'d by the existing `cleanup` trap) and
`MIX_REBAR3` to the nix store's own `rebar3`, scoped to the one subshell
that runs the oracle harness.

**Evidence.** Re-ran the fixed sequence (`mix local.hex --force
--if-missing` under the new `MIX_HOME`/`HEX_HOME`, then `mix deps.get
--check-locked`, stdin closed, brand-new empty home) and inspected the
resulting `MIX_HOME` afterward: `find "$MIX_HOME" -iname '*rebar3*'`
returned nothing — no network rebar3 build occurred, confirming
`MIX_REBAR3` was actually honored — where the unfixed sequence's identical
inspection found one. `mix deps.get --check-locked` itself still resolved
correctly (`All dependencies are up to date`) in both the isolated-fixed
and default-unfixed runs. The full gate (`scripts/test-postgres.sh`,
below) exercises the real fixed script end to end afterward.

### 2 & 3. CI workflow (`.github/workflows/ci.yml`)

`permissions: contents: read` added; `cachix/install-nix-action` bumped
`@v27` → `@v31` (confirmed current major via the action's own release
list); the useless `nix_path` input removed; the trigger changed from `on:
[push, pull_request]` (which double-runs every PR's own branch push) to
`push: branches: [main]` plus `pull_request`; `quick-check` now also runs
`gleam deps download`/`gleam build --warnings-as-errors` for `consumer/`
(previously exercised only by the heavier `postgres-gate` job, at the very
end of `scripts/test-postgres.sh`); a comment now states plainly that
`gleam-dream/sinal` must stay public for the sibling checkout to succeed
with no token. Cache: `grind/build` and `grind/consumer/build` dropped
(`scripts/test-postgres.sh` unconditionally `gleam clean`s both as its own
first step — see that script's own comment on the stale-fork incident this
guards against — so caching either tree only ever cached something this
same job immediately discards); `grind/oracle/deps`, `grind/oracle/_build`,
and `sinal/build` kept; the cache key now hashes only the manifests that
actually matter to those three trees (`oracle/mix.lock`,
`sinal/manifest.toml`) with **no** `restore-keys` fallback prefix, so a
same-OS/different-hash run can never silently restore a `sinal/build` or
oracle `_build` built against a different dependency version than the run
actually resolves. `DeterminateSystems/magic-nix-cache-action@v15` added,
caching the Nix store itself (every `nix develop`/`nix flake check`
derivation) independently of the Gleam/Mix artifact cache above.

**Evidence.** `nix run nixpkgs#actionlint -- .github/workflows/ci.yml` —
clean, no findings, on the final workflow file.

### 4. `job.same_installation` unit tests

Six pure, in-memory tests added (no `Database`, no PostgreSQL): identical
cluster identifiers match; different cluster identifiers differ even when
OID and schema agree; one side missing a cluster identifier and both sides
missing one both fall back to OID+schema; within that fallback, a
differing OID or a differing schema each still makes the installations
differ. `same_installation`'s own doc comment now states, in one sentence,
that the relation is not transitive (an installation whose cluster
identifier could not be read can compare equal, via the fallback, to two
installations that would themselves disagree once their own identifiers
are compared) and why that is safe — a handle only ever carries the token
of the single `Database` that minted it, so the function is only ever
called pairwise against that one minting `Database`, never chained.

**Red.** Mutation 1 (`_, _ -> True`, the fallback branch reporting a match
unconditionally): `job_same_installation_fallback_still_rejects_differing_oid_test`
and `..._differing_schema_test` both failed (`True should equal False`);
the other four tests, which already expect `True` in that branch, were
unaffected — confirming the mutation's blast radius matched exactly the
two tests meant to catch it. Mutation 2 (`_, _ -> schema_a == schema_b`,
dropping the OID comparison specifically): only
`..._differing_oid_test` failed. **Green**: reverted, `225 passed, no
failures`.

### 5 & 6. `postgres.validate` schema-name rejection unit tests

Seven pure tests added: empty name, a 64-byte ASCII name, a multibyte name
crossing 63 bytes at only 32 _characters_ (`"é"` × 32 — 2 bytes each), a
NUL byte, the literal `"$user"` token, and the reserved `pg_` prefix all
`Error(InvalidSchema)`; a name at exactly the 63-byte boundary is `Ok`.
`ConfigError.InvalidSchema`'s own doc comment now documents all five
rejected shapes, including _why_ `"$user"` and `pg_`-prefixed names are
rejected (a quoted, literal `"$user"` schema behaves differently from
`search_path`'s own unquoted `$user` substitution token and can leave the
migration advisory lock key `NULL`; PostgreSQL reserves the `pg_` prefix
for its own system/temporary schemas and would reject it anyway, only
later and less specifically, inside `migrate`).

**Red.** Mutation 1 (`schema_is_valid` always `True`): all six rejection
tests failed (`Ok(...) should equal Error(InvalidSchema)`); the
boundary-accepting test was unaffected. Mutation 2 (only the `$user`/`pg_`
checks removed, byte-length/NUL checks left intact): exactly
`postgres_schema_rejects_dollar_user_test` and
`postgres_schema_rejects_pg_prefix_test` failed, isolating item 6 from item 5. **Green**: reverted after each, `225 passed, no failures`.

### 7. Deterministic concurrent-schema-creation test

The original `postgres_migrate_concurrent_first_time_schema_creation_both_succeed_test`
spawned two `migrate` callers back-to-back with no real barrier ("there is
no lockable object to hold one on before the schema exists" — true only
until something is deliberately made into that object). Now: a third
session runs `BEGIN; CREATE SCHEMA "<name>";` against the exact,
freshly-suffixed schema name and never commits; both `migrate` callers are
spawned, then the test polls `pg_stat_activity` (`state = 'active' AND
wait_event_type = 'Lock' AND query = 'CREATE SCHEMA IF NOT EXISTS
"<name>"'`) until the count is exactly 2 — proof both are genuinely
blocked on the still-open blocking transaction, not inferred from a sleep
— before committing the blocker and asserting both `migrate` calls return
`Ok(Nil)`.

**Red.** Mutation: reverted `ensure_schema_exists`'s catch branch to the
pre-Increment-31 shape (`Error(create_error) ->
Error(SchemaCreationFailed(create_error))`, no recheck at all). Every run
now fails deterministically (not merely "usually"), since the barrier
forces the race every time:

```
test: grind_test.postgres_migrate_concurrent_first_time_schema_creation_both_succeed_test
info:
Ok(Error(SchemaCreationFailed(ConstraintViolated("duplicate key value violates unique constraint \"pg_namespace_nspname_index\"", "pg_namespace_nspname_index", "Key (nspname)=(grind_concurrent_schema_...) already exists."))))
should equal
Ok(Ok(Nil))
```

**Green**: reverted, `225 passed, no failures` (ran against a disposable
two-database harness dedicated to this and item 8's own DB-backed tests,
and again inside the full `scripts/test-postgres.sh` gate below).

### 8. `read_cluster_identifier` no longer risks a server-logged permission error

**A factual correction found while fixing this.** Stock, unmodified
PostgreSQL does **not** restrict `EXECUTE` on `pg_control_system()` at
all — confirmed empirically (an ordinary `CREATE ROLE ... LOGIN` role,
freshly created with no grants at all, ran `SELECT pg_control_system()`
successfully against this disposable cluster). This matches Increment 27's
own earlier privilege check (`docs/RECOVERY-EVIDENCE.md`, "Increment 27"),
which found the identical thing for `pg_control_system()`'s use in
`storage_owner`. `job.Installation`'s own doc comment and
`read_cluster_identifier`'s previously called it "a restricted,
superuser-adjacent function in stock PostgreSQL" — both corrected here.
The real hazard is a managed/hardened deployment that _deliberately_
revokes `EXECUTE` on it from `PUBLIC`, which is the scenario this fix and
its test actually exercise (via an explicit, test-scoped
`REVOKE`/`GRANT ... TO PUBLIC` pair).

**A second finding, about the review's own suggested fix shape.** The
review proposed guarding the call with a single query: `SELECT CASE WHEN
has_function_privilege('pg_control_system()', 'execute') THEN (SELECT
system_identifier FROM pg_control_system()) END`. This does **not**
actually work: PostgreSQL performs a function's own ACL check at
expression-initialization time for every function-call node the plan
contains, regardless of which `CASE` branch is reached at runtime.
Verified directly with `psql`: as a role with `EXECUTE` explicitly revoked,
the guarded query above still raised `ERROR:  permission denied for
function pg_control_system` and the server still logged it — identical to
the raw, unguarded call. The actual fix issues the privilege check as its
own separate query first (`has_pg_control_system_privilege`) and only ever
sends the real `pg_control_system()` query text at all when that check
reports `True` — verified the same way: the privilege-check-only query
returned `f` with zero log lines added, and the restricted call was never
sent.

**Test.** `postgres_start_with_non_superuser_role_never_logs_a_permission_error_test`
revokes `EXECUTE ... FROM PUBLIC` (restored via `exception.defer`
immediately after, regardless of outcome — scoped to `owner_a_url()`'s own
database, which several other tests share but none of which assert
anything about a cluster identifier specifically), starts a non-superuser
role's own pool, and checks both that `postgres.start` succeeds with
`Installation`'s cluster identifier falling back to `None`, and — the part
a return-value assertion alone cannot distinguish, since both the guarded
and unguarded query already resolve to `None` for this role either way —
that the disposable cluster's own server log (exposed to the test via a
new `GRIND_TEST_POSTGRES_LOG` environment variable, read with
`simplifile`) never gained the `permission denied for function
pg_control_system` line, polled for up to ~500ms to allow for log-write
buffering.

**Red.** Mutation: reverted `read_cluster_identifier` to the raw, unguarded
query (no privilege pre-check). Fails deterministically:

```
test: grind_test.postgres_start_with_non_superuser_role_never_logs_a_permission_error_test
info:
False
should equal
True
```

(`await_log_never_shows` returned `False` — the permission-denied line was
found in the log, as predicted.) **Green**: reverted, `225 passed, no
failures` against the same two-database harness.

### 9. `ensure_schema_exists`: keep the original `CREATE` error on a failed recheck

Nit-level fix, no dedicated new test requested by the review for this one
alone (already exercised indirectly by item 7's own test, which never
reaches this specific sub-branch since the recheck there always succeeds).
Previously, `Error(recheck_error) -> Error(recheck_error)` replaced the
original `CREATE SCHEMA` failure with whatever the secondary existence
probe's own error happened to be — less informative for a caller trying to
understand why the schema could not be created. Now `Ok(False) |
Error(_) -> Error(SchemaCreationFailed(create_error))` always surfaces the
original `create_error` regardless of whether the recheck itself
succeeded-but-reported-absent or failed outright. Verified by `gleam
check` (both branches now agree on the same constructor and error value)
and by item 7's own test continuing to pass (the recheck's success path is
unaffected).

**Gate.** `nix develop --command bash scripts/test-postgres.sh`: **225
passed root** (211 baseline + 14 new pure unit tests: 6 for item 4, 7 for
items 5/6, 1 pre-existing test's assertions widened for item 7's own
barrier — see above), **11 passed consumer**, pinned Oban oracle harness
green, Squirrel check green, exit `0`. `gleam check` green for `grind` and
`consumer/`. `nix fmt` applied and re-verified clean. `nix flake check` —
`all checks passed!`. `nix run nixpkgs#actionlint` clean on the final
`ci.yml`.

**Not fixed, and why.** Item 1's literal "interactive prompt reads EOF and
aborts" premise did not reproduce in this environment (see item 1 above);
the script-local `MIX_HOME`/`HEX_HOME`/`MIX_REBAR3` fix is applied anyway,
since it is strictly more correct and isolated regardless of which Mix
behavior a given CI runner exhibits.

## Increment 34 — end-to-end cigogne interop, and a genuine upgrade-boundary lost-reply `reconcile_unique` (docs/RELEASE-READINESS.md, "Migration gaps"; docs/RISKS.md risk 16)

**Context.** Two narrow interaction gaps remained between two
already-separately-tested mechanisms: (1) no test proved cigogne itself
applying `priv/migrations/*.sql` against a real database, with
`postgres.migrate` then a genuine no-op against that same database and the
two mechanisms' shared advisory lock genuinely serializing a concurrent
caller of the other one; (2) the upgrade harness had no test of a genuine
lost reply during `reconcile_unique` specifically spanning a schema
upgrade, as opposed to the general lost-reply evidence proven elsewhere
against a stable, already-latest schema. Commit 72fe573 closes both.

### 1. Cigogne applies Grind's files; `migrate` is then a no-op

`cigogne_applies_grind_files_then_migrate_is_noop_test`
(`test/grind_test.gleam`) drives cigogne directly as a Gleam library
(`cigogne.create_engine`/`apply_all`/`rollback`/`apply`), pointed at the
`Database`'s own shared pool via `config.ConnectionDbConfig` rather than
opening a second pool against `DATABASE_URL` — no new runtime dependency,
`cigogne` stays a dev-dependency exactly as before. Against a fresh schema:
cigogne applies both real files; `postgres.migrate` against the result
returns `Ok(Nil)` with the marker count unchanged at 2 (a genuine no-op,
not merely "no error"); a real worker submits, claims, and runs to
`Succeeded` against the cigogne-applied schema; and cigogne's own
`rollback` of `grind_v12` then `apply` of it again round-trips (marker
count 1 then 2 again), with the earlier job's own `outcome` still readable
afterward, unaffected by the column churn `ALTER TABLE ... DROP COLUMN`/
`ADD COLUMN` cycles the round trip causes (see "Not proven" below for why
this test does not compare a raw catalog digest across the round trip).

**Gate.** `nix develop --command bash scripts/test-postgres.sh`, full run,
exit `0`: `228 passed` root (`grind_cigogne_e2e`/`grind_cigogne_e2e_fresh`
databases), `11 passed` consumer, oracle green, Squirrel check green.

### 2. `postgres.migrate` serializes against a concurrent cigogne apply

`cigogne_apply_serializes_with_concurrent_migrate_test`
(`test/grind_test.gleam`, `grind_cigogne_concurrent` database) applies
`grind_v11` alone through cigogne first (synchronous, no race — the
realistic "an application has already been running a while" starting
point), then races cigogne applying `grind_v12` alone against a concurrent
`postgres.migrate` caller. Rather than hoping for a favorable scheduler
race — genuinely racing cigogne (no per-step skip check of its own) against
`migrate` (which does have one) for the _same_ unapplied step can otherwise
resolve either way, and the loser landing second on cigogne's own side
would hit a real duplicate-object error (`docs/RISKS.md` risk 11's own
documented mixing hazard, not a false alarm) — the test polls `pg_locks`
for the advisory lock actually being _granted_ to cigogne's own session
before ever starting `migrate`, making the ordering a proven fact rather
than a hopeful one: `migrate`, started only after cigogne provably already
holds the lock, is guaranteed to queue behind cigogne's still-open
transaction, and once cigogne commits, `migrate`'s own per-step re-read
sees `grind_v12` already applied and skips it. Both calls return `Ok(Nil)`;
exactly one marker row exists per version.

**Red (mutation).** Temporarily removed `grind_v12`'s own first `up`
statement (the `pg_advisory_xact_lock` line) from
`priv/migrations/20260926000000-grind_v12.sql` — chosen over mutating
`grind_v11`'s own file because `grind_v11` is applied synchronously, alone,
before the race even starts in this test, so only `grind_v12`'s own line is
actually exercised by the race; migrations already merged into one cigogne
transaction chunk would otherwise share `grind_v11`'s lock statement and
mask the mutation. `gleam test` (focused, `GRIND_TEST_CIGOGNE_CONCURRENT_URL`
only) against a disposable local cluster:
`await_advisory_lock_granted(connection, 250) |> should.equal(True)` failed
(`False should equal True`) — cigogne, applying the mutated file, never
takes the lock at all, so the poll never observes a grant. `grind_migrations_conformance_test` also
failed on the same mutation (an independent, expected side effect of the
same file edit — the byte-for-byte lockstep check).

**Green.** Reverted via `Edit` (exact prior text restored — `git diff
--stat` against HEAD showed no change to the file); re-ran the same focused
`gleam test`: `228 passed, no failures`.

### 3. A genuine lost-reply `reconcile_unique` across the v11→v12 boundary

`postgres_migrate_upgrade_reconcile_unique_lost_reply_test`
(`test/grind_test.gleam`, `grind_upgrade_lost_reply` database) seeds the
frozen v11 fixture, then two independent `submit_unique` calls whose
outcome is genuinely uncertain to the caller:

- **(a) Genuinely committed, reply lost** — the same deferred-constraint
  `synchronous_commit` trigger `run_unique_committed_reply_lost_store_unavailable_test`
  already uses (Increment 11), scoped to this submission id: the admission
  transaction's own `COMMIT` parks in `SyncRep` (no standby will ever
  connect — `synchronous_standby_names = grind_never_standby`), and closing
  the pool while it is parked there leaves it genuinely mid-flight.
  `submit_unique` returns `Error(CommitUnknown(pending))`. The orphaned
  backend is explicitly terminated and confirmed gone (PostgreSQL's own
  SyncRep wait does not notice a client disconnect on its own) _before_
  migrating, since the still-open transaction's own row lock would
  otherwise block the migration's `ACCESS EXCLUSIVE` DDL.
- **(b) Genuinely never reaches PostgreSQL** — the real TCP fault proxy
  (`test/grind_fault_proxy.erl`, the same mechanism T1-T5 in
  `test/grind_fault_proxy_test.gleam` use for the acknowledgement path),
  `OnCommit`/`DropRequest`: the triggering `commit` chunk is never
  forwarded, so PostgreSQL never attempts it at all.

`postgres.migrate_with(direct_database, migrations.migrations())` then
upgrades both `PendingSubmission`s' schema out from under them, from v11 to
v12 (narrowing `grind_unique_submissions`'s primary key from
`(storage_owner, submission_id)` to `(submission_id)`-only, among other
changes). `reconcile_unique` against the upgraded schema then resolves
each correctly: (a) to `Inserted`, with the real job id independently
confirmed via a raw `grind_jobs` query — not a fabricated one, and not
another `CommitUnknown`; (b) to `CommitUnknown` again, with zero rows on
either table — nothing was ever committed, so there is nothing to find on
either schema.

**A genuine empirical finding, not silently glossed over** (matching this
codebase's own practice — see Increment 33, "two findings ... rest on
premises that did not hold empirically"): scenario (a) was originally
attempted with the TCP fault proxy's `OnCommit`/`DropReply`, exactly the
mechanism T1 (`test/grind_fault_proxy_test.gleam`) uses for the
acknowledgement path. It does not work for `submit_unique`. Debugging with
a live `pg_stat_activity` poll (`state`, `wait_event_type`/`wait_event`,
`query`) showed the real backend parked on `active`/`Client`/`ClientRead`
with `query = commit`, continuously, for the full ten seconds polled both
before and after `submit_unique` itself returned `CommitUnknown` — zero
rows ever inserted on an independent, unproxied connection. PostgreSQL had
received the extended-protocol `Parse "commit"` message and was waiting to
read the client's next protocol message (`Bind`); pog's own connection
process never sends it once that `Parse` reply is swallowed, so the
transaction never reaches `Execute` at all and stays open until Grind's own
client-side deadline force-closes the socket, which cascades through the
proxy to closing its upstream connection too — PostgreSQL rolls the whole
transaction back on the disconnect, never a genuine commit. This is a
property of _this_ transaction shape (`pin_read_committed`, `set_lock_timeout`,
an advisory-lock acquisition, a candidate `SELECT`, and two `INSERT`s, all
before ever reaching `commit`), not a flaw in the proxy or in T1-T5, whose
own shorter, single-`UPDATE` acknowledgement transaction is timed
differently and which document their own non-determinism (sometimes a
transparent recovery, sometimes `QueueAckUnknown`) rather than a guaranteed
commit either. Recorded in the test's own doc comment; scenario (a) uses
the SyncRep-trigger technique instead, and (b) still uses the real TCP
proxy.

**Red (mutation).** Temporarily changed `sql.gleam`'s generated
`find_receipt` query from
`... FROM grind_unique_submissions WHERE submission_id = $1` to `... AND
false` (simulating "break the receipt lookup" — a squirrel-generated file
CLAUDE.md otherwise forbids hand-editing, mutated here only as a
temporary, immediately-reverted proof). `gleam test` (focused,
`GRIND_TEST_UPGRADE_LOST_REPLY_URL` only): `let assert Ok(submission.Inserted(committed_handle))
= postgres.reconcile_unique(direct_database, committed_pending)` failed —
`Error(CommitUnknown(...))` instead, `227 passed, 1 failures`. Confirms the
test genuinely depends on the receipt lookup finding the real,
already-committed row after the upgrade, not merely returning early on some
unrelated success.

**Green.** Reverted via `Edit` (exact prior text restored — `git diff
--stat` against HEAD showed no change to the file); re-ran the same focused
`gleam test`: `228 passed, no failures`.

**Gate.** `nix develop --command bash scripts/test-postgres.sh`, full run
against a fresh disposable cluster, exit `0`: `228 passed` root, `11
passed` consumer, pinned Oban oracle harness green, Squirrel check green —
every one of the four new completion markers
(`cigogne-e2e-migrate-noop-passed`, `cigogne-migrate-concurrent-serialize-passed`,
`upgrade-reconcile-unique-lost-reply-passed`, and the pre-existing
`migrate-upgrade-harness-passed`) present. `nix fmt` applied (one file
reformatted) and re-verified clean. `nix flake check` — `all checks
passed!`.

**Caveat for an application driving cigogne itself.** Cigogne keeps its own
migration-tracking bookkeeping (`priv/cigogne.toml`'s `[migration-table]`,
default `public._migrations`) entirely independent of
`grind_schema_migrations`; an application using `postgres.with_schema`
should point `migration-table` at the same (or another dedicated) schema
explicitly, or every `with_schema` install on one database shares the same
default tracking table — see README, "Migrations", for the full note and a
worked `config.ConnectionDbConfig` example.

## Cigogne migration serialization — test-controlled lock barrier

The independent refactor review exposed a synchronization weakness in
`cigogne_apply_serializes_with_concurrent_migrate_test`: one full gate run
failed while waiting to observe Cigogne's advisory lock, while the next run
passed. The old
poll checked for any granted advisory lock without keeping Cigogne's
transaction open. This section supersedes Increment 34's synchronization
claim for that test; production migration behavior is unchanged.

The test now controls a barrier with `LOCK TABLE grind_jobs IN ACCESS SHARE
MODE` inside its own `pog.transaction`. Real `cigogne.apply` takes v12's
advisory lock first, then blocks at v12's existing `ALTER TABLE`, which needs
an `AccessExclusiveLock`. The test checks the exact advisory key and database
in `pg_locks`, the table-lock wait on the same Cigogne backend, and the
blocker relationship through `pg_blocking_pids`.

Only after observing that held state does it start `postgres.migrate`. It
requires a waiter on the same advisory lock whose blocker is Cigogne, then
rechecks Cigogne's held state. Returning `Ok(Nil)` from the test transaction
explicitly commits and releases the table barrier. Both migration calls must
then succeed, with exactly two schema markers. If an assertion fails, Pog's
transaction callback rolls back before the deferred database cleanup.

The bounded polling observes a state the test keeps in place; it no longer
has to catch a transient lock. It uses live lock-manager views rather than
transaction-cached activity statistics. The existing polling interval and
completion timeouts remain unchanged, and neither migration call is retried.
Each observation allows 100 sleeps of 20 ms, plus query time, instead of the
old 250 sleeps. This leaves room for assertion rollback before the default
five-second connection checkout deadline. With the old bound, a negative
control instead failed during rollback with `QueryTimeout`.
The implementation is in
[test/grind/migrations/conformance_test.gleam](../test/grind/migrations/conformance_test.gleam).

Validation of the final observation bound included two temporary test-only
negative controls: omitting Cigogne's advisory statement failed the held-lock
assertion; replacing Grind migration with immediate success failed the
blocked-waiter assertion. Restoring the real calls passed on a fresh database,
including the required execution marker. Production code and migration SQL
were unchanged throughout these controls.

The full PostgreSQL gate passed: 228 root tests, 11 consumer tests, static SQL
regeneration checks, and the pinned Oban oracle. Independent review found no
blocking issue in the final test and documentation.

## Executor acknowledgements, reserved renewal and resource lifetime — 2026-09-28

The owner-approved runtime change moves automatic acknowledgement and
reconciliation to the supervised attempt process. The coordinator retains
capacity until the attempt settles. A separate renewer uses one reserved
connection per consumer and skips locked rows; ordinary claim and ACK
traffic cannot consume that connection. Attempt, epoch, owner, state and
live database-time lease fences remain in the SQL path. A lost reply
reconciles the same command and retained proposal; it never re-executes
the handler automatically.

The complete PostgreSQL gate passed 246 root tests, 11 consumer tests and
12 paired core scenarios, including the required database execution
markers and ledger checks. The log is retained at
`bench/results/repaired-gate-oy4M9w/validation/grind-full-gate-v4.log`.
These are local uncommitted results on 8431b61. The separate release
execution ledger records source snapshots and later benchmark evidence.

| Recovery claim                                                      | Fault and synchronization                                                                                                                                                                                                     | Observable result                                                                                                                                                                                                                 |
| ------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| First known ACK rollback retains the proposal                       | `test/grind/queue/executor_test.gleam:19` installs a real acknowledgement trigger. A sequence survives rollback and makes only the first terminal update raise SQLSTATE `40001`.                                              | Shutdown drains through the retry; the stored typed output is 42 for input 41, and the handler invocation subject has no second message.                                                                                          |
| Slow ACKs cannot serialize unrelated renewal behind the coordinator | `executor_test.gleam:92` holds handlers behind explicit subjects, releases the selected completions near renewal time, and observes the real trigger's activation. Healthy handlers remain active past their original leases. | Healthy siblings finish successfully and no second handler invocation appears. The repeated T2 matrix adds per-target activation and all-attempt headroom/state/receipt accounting.                                               |
| Saturating the ordinary pool does not consume renewal capacity      | `executor_test.gleam:194` occupies the only ordinary connection for eight seconds. A separate connection observes the active PostgreSQL sleep before checking the lease.                                                      | The job remains executing after 6.5 seconds and its lease has advanced by more than three seconds. It then finishes once with the expected output.                                                                                |
| An ambiguous committed ACK reconciles the original command          | `test/grind/queue/ack_failure_test.gleam:439` retains the existing committed-reply-loss regression; the independent-node M4 case cuts the COMMIT reply and observes a durable receipt while the old VM is stopped.            | A replacement VM reconciles that receipt and the effect count remains one. Quarantine without a receipt remains a different result.                                                                                               |
| Failed startup releases ownership before returning an error         | `test/grind/database/startup_test.gleam:24` drops the installation-query reply; other tests at `:70`, `:98` and `:117` force duplicate registration, pool-child failure and later-child failure.                              | The failed start returns no live owned resource tree. Identical settings can be retried. Unrelated registered processes and already-running pools retain their identity and deadline.                                             |
| Cleanup waits for real cache writers                                | Tests at `startup_test.gleam:214`, `:230` and `:234` hold the internal type writer or an admitted managed application call across teardown, then release it or kill the caller.                                               | Owned cache/deadline state disappears only after writers cannot repopulate it; a live sibling stays usable. The independent `validation/cache-probe/` retains the reproduced races and repaired result.                           |
| Stale pgo holders do not trap recovery after connection loss        | `test/grind/database/reconnect_test.gleam` covers a dead owner, a live owner with a closed socket, multiple stale holders and a pause that consumes the original checkout deadline.                                           | Stale holders are retired before SQL executes. The callback runs at most once; its error classification is preserved, and selecting another holder does not reset the deadline. Red and green logs are retained in `validation/`. |
| Consumer teardown removes only its own normal EXIT message          | `test/grind/queue/lifecycle_test.gleam:13` exercises stop in a caller trapping exits, alongside an unrelated child exit. `renewer_test.gleam:7` checks normal coordinator death.                                              | Stop leaves the unrelated EXIT available, and the renewer does not survive its coordinator.                                                                                                                                       |

The lifetime owner waits for the exact internal supervisor and admitted
managed calls before purging caches. A stalled application call may make
public close return `StopTimedOut` after six seconds; the owner continues
cleanup when that call finishes or dies. It never kills an application
caller to obtain a clean-stop result. Raw internal pog callers must arrange
their own drain, and forcibly killing the lifetime owner is outside this
cleanup contract.

Renewal remains bounded by ownership and failure semantics. A returned
proposal has a bounded renewal lifetime; reconciliation can still find a
committed receipt afterward, but an expired lease cannot authorize a fresh
write. The `L >= 4D` rule assumes an established, progressing reserved
connection. It does not bound initial connection, every pool checkout wait,
OS scheduling or a lock held on that attempt's own row. Loss of live
ownership still leads to quarantine and attributed `AuthorizeReplay`.

The independent-node rehearsal at
`resilience/results/repaired-300s-jBvw19` passed fourteen standalone cases
and eighteen mixed fault rounds. Its retained M2/M6 comparison classifies
Grind's explicit replay and concurrent pruning against Oban's automatic
Lifeline rescue and Peer-mediated pruning. It does not claim equal rescue
timing or exactly-once external effects. Its resource checks cover both primary
VMs after drain: fixed bounds on processes, memory, aggregate mailboxes,
owned deadline/type/query entries and database retention; atoms instead
have the documented allowance of three per fresh consumer start plus a
fixed warmup margin. The rehearsal observed 108 additional worker atoms
across 36 starts. It does not establish bounded atoms under indefinite
churn. Active timers and the forwarder mailbox/drop metrics are not directly
sampled. This was rehearsal evidence; the completed two-hour run is recorded
separately below.

## Host suspension during the first full soak — 2026-09-28

The first 86,400-second attempt in
`resilience/results/repaired-86400s-o23a23` failed. It completed fourteen
standalone cases and 162 complete mixed rounds. The nested fault in round
163 passed, but the primary healthy job did not reach `succeeded`.

The host power log records software sleep from 08:27:08 to 08:31:43 São Paulo
time: 275 seconds, spanning job 4878's lease expiry at 08:27:38.751176.
PostgreSQL quarantined that job at 08:31:43.364664. Its final row is
`uncertain`, with one effect, no acknowledgement receipt and no replay.
Those outcomes preserve the live-lease fence while the required healthy-job
success assertion correctly fails. Reserved renewal cannot progress during
whole-host suspension.

The actual process exit is 1. The frozen source and driver compare unchanged,
and the disposable processes and PostgreSQL cluster were removed. The retained
`orchestration/failure-diagnosis.md` links the power records, database snapshot,
effect records and cleanup evidence. `failure-evidence-sha256.json` identifies
the checked failure artifacts. The logged soak-case duration includes warmup;
this failed attempt provides no full-duration acceptance.

Controller, BEAM and database clock records diverged across sleep. Ordering
across that interval must use the retained protocol barriers and controller
event sequence rather than assume all wall timestamps remain aligned. The
evidence does not include every renewal or ACK return. It establishes the
host suspension and durable fencing outcome, without requiring a change to
the fencing rule.

The owner changed the prospective acceptance duration to 7,200 seconds on
2026-09-28. All fault, effect, receipt, fencing, audited replay and resource
assertions remain unchanged; the duration policy and its focused tests are
the only harness changes. The timed interval begins after warmup, and all
nine fault types still run at least twice. The fresh run has a separate
source snapshot and completed as recorded below. The failed first attempt
remains retained history and contributed no time toward the new acceptance
target. Day-long endurance remains unverified.

## Two-hour mixed soak and final audit — 2026-09-28

The approved fresh run in `resilience/results/repaired-7200s-8YvcJq`
completed with actual session 7230 exit 0. All 281 results passed: fourteen
standalone scenarios, 266 nested fault cases and the final soak result.
The mixed-workload clock began after warmup and reached 7,202.060718916939
seconds. The independent controller-monotonic bracket is
7,202.058878458 to 7,202.105857583 seconds; its lower bound independently
exceeds the approved 7,200-second target. The enclosing soak-case elapsed
value includes warmup and is not used as the mixed-workload duration.

| Nested fault                | Completed rounds |
| --------------------------- | ---------------: |
| VM kill                     |               30 |
| Worker-process kill         |               30 |
| Request-only partition      |               30 |
| Reply-only partition        |               30 |
| Bidirectional partition     |               30 |
| Lost COMMIT reply           |               29 |
| Slow ACK with network delay |               29 |
| Connection loss             |               29 |
| PostgreSQL restart          |               29 |

Each primary round retained 27 terminal jobs, 42 unique receipts and 26 effects.
Across 266 rounds, that is 7,182 jobs, 11,172 receipts and 6,916 effects; 77 warmup
effects bring the primary total to 6,993. These totals exclude the separately
audited nested cases. Every nested case retained its actual fault witnesses,
job/receipt/resolution rows and effects. Audited replay after a killed
attempt still permits one explicit additional effect; no exactly-once
external-effect guarantee is inferred.

The primary admin VM (PID 99993) and worker VM (PID 112) remained the same
independent VMs throughout. Every drained sample had 104 processes, one
checkout-deadline entry and 797 type-cache entries per VM. Query-cache
entries stayed at 21 for admin and 22 for worker; aggregate and owner
mailboxes and synthetic worker ETS entries were empty.

| Resource               |      Admin |     Worker |
| ---------------------- | ---------: | ---------: |
| Memory baseline, bytes | 57,415,191 | 58,616,640 |
| Maximum memory, bytes  | 57,836,519 | 59,186,600 |
| Final memory, bytes    | 57,517,791 | 58,649,784 |
| Atom baseline          |     14,863 |     15,086 |
| Final atoms            |     14,863 |     16,682 |

The worker's 1,596 additional atoms match 532 fresh consumer starts at three
atoms per start. All fixed resource bounds passed; this linear allocation
is not bounded atom use under indefinite churn. Samples are taken after
drain and consumer stop; active timers and the Sinal forwarder's own
mailbox/drop metrics are not directly measured. Database sessions peaked
at eight, below the audit ceiling of ten derived from the recorded main
pool sizes plus two; the original exact database-baseline sample was not
retained. Primary table/index/TOAST storage after pruning peaked at 245,760
bytes against the fixed 16 MiB bound.

The final audit checked 278 archived source files and 600 runtime files,
including exact file sets and hashes. The wrapper source digest is
`b15a8da75e7213928bcc31fc0ed00efd790107f56e3e5ca04cf8d3e739d6a6f7`;
the runtime digest is
`450e50494e11516e9ac5d75fc488f82c6a3347c07fb309013d4d42badaa24a5b`.
It fully parsed 520 JSONL files containing 43,417 records, with a check that
rejects partial trailing records. The dirty source remains labeled exploratory; completing
the approved duration does not relabel it as a clean release-candidate run.

**Auditor correction.** The original v4 audit exited 1 at
`round-233-healthy primary did not span fault`. Its predicate compared a
BEAM wall timestamp with a controller wall timestamp. Independent review
found the same mismatch in round 234 and a 128.905084 ms change between wall
and monotonic intervals in the controller around round 233. The callback's
recorded wall time even preceded the retained release-file timestamp,
although the frozen handler cannot return through that barrier before the
file exists. This invalid clock comparison did not demonstrate an early
handler return.

Reviewed v5 binds each primary job to its exact retained release file,
the matching effect/completion callback identity and file append order,
and controller-monotonic fsync→fault→schema-drain→clean-stop order.
The pinned runner creates that file only after the nested case and schema
drain return. Its replay check likewise uses the pre-replay effect count,
explicit request/resolution order, actual VM identities and old/new durable
attempt fences instead of comparing wall clocks across VMs. Three exact
harness source hashes bind these causal protocols to the archived source.

No workload, duration, fault, accounting, resource or source/runtime
requirement was relaxed. Both auditor versions, the original failed audit,
the independent diagnosis and the reviewed diff remain in `orchestration/`.
The final v5 audit ran as session 97027 and exited 0. Its report is
`soak-audit-v5.json`, SHA256
`de653b1fd4a7fa8054d9a6bf9113ac65697dfc79e1d509f0302b0ff94dc25c1a`.
Detailed fault reconstruction covers every nested case; the independent
standalone check covers passing results with matching start/completion
events. Nested schema cleanup is checked through each retained post-TRUNCATE
marker and its identity/order, not a second database query.

**Paired result and cleanup.** `paired-comparison.json` passes M2/M6 against
`oracle/results/20260928T015525Z-23535`. It classifies Grind's quarantine and
attributed replay against Oban's automatic Lifeline rescue, and Grind's
concurrent locked-row pruning against Oban's Peer failover. It does not claim
equal recovery timing or general feature equivalence.

Independent and parent cleanup reports found no exact retained BEAM node,
runtime, runner or PGDATA process identities. The disposable PGDATA and
cluster directory were absent. `run/postgres.log:920`–`:921` records the
final PostgreSQL process receiving an immediate shutdown request and
confirming shutdown. This establishes process absence and completed
immediate PostgreSQL shutdown, not graceful BEAM OS shutdown; the harness
can fall back to killing a VM after its public exit request. The retained
reports are `orchestration/independent-final-cleanup.json` and
`orchestration/parent-cleanup-verification.json`.

The approved two-hour requirement is satisfied within this recorded scope.
The failed 24-hour attempt remains failed, and day-long endurance remains
unverified.
