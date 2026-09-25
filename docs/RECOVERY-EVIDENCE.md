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
