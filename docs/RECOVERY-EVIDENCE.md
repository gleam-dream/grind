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
