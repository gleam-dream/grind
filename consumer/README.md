# Public Grind consumer

This separate Gleam package depends on Grind by local path and imports only
public Grind modules. It registers `Worker(PaymentRequest, String,
PaymentError)` alongside `Worker(Int, Int, Nil)`, admits both through their
definitions, runs them from a supervised automatic queue, and observes committed
typed success and a tagged application error.

The queue validates its polling interval and a four-jobs-per-poll limit before
the queue actor starts. The limit drains jobs serially and does not claim four
concurrent workers. Handler messages and a same-actor request form a bounded
synchronization barrier before the test reads committed outcomes.

The synthetic payment effect uses the caller's idempotency key. Nothing is
preseeded: two independently admitted jobs reuse the same idempotency key, so
the second job's own worker execution is what observes the key already taken
by the first job's own execution and receives the retained receipt instead of
applying the effect again. This demonstrates one way an application can
handle a crash window. Grind itself does not guarantee exactly-once effects,
and the test uses no paid service.

## Retry policy, running cancellation, and cooperative effects

`public_consumer_retry_and_running_cancellation_test` covers two more public
paths through a manually driven consumer:

- A worker's definition-bound retry policy (`worker.with_retry_policy`)
  retries once, after a short real delay, then succeeds; the job commits
  `Succeeded`. The handler has no direct access to Grind's own
  `RetryContext` (only a bound retry policy callback receives one); it
  tracks its own invocation count instead, which in this single-worker,
  no-concurrent-claims scenario advances in lockstep with Grind's persisted
  attempt count.
- A second job blocks on a barrier inside its handler. The test requests
  cancellation through `postgres.cancel` while the attempt is genuinely
  `Executing`, releases the barrier, and observes the committed outcome as
  `Cancelled` — regardless of what the handler itself returned. The handler's
  own synthetic effect record still shows the effect was applied and
  retained: Grind's cancellation commits a terminal state, it does not, and
  cannot, undo application-side work the handler already performed.

## Uncertainty and audited recovery through the public API

`public_consumer_effect_crash_uncertainty_audited_recovery_test` arms a
one-shot test fault (`consumer_effect:arm_crash_after_effect/1`) so a
worker's first attempt for a given key applies its synthetic effect and then
crashes before Grind can acknowledge anything. An automatic consumer with a
short lease is used so the crashed attempt's expiry is found by the
coordinator's own quarantine scan; both jobs are observed as `Uncertain`
through `postgres.state`/`postgres.outcome` — never as a business failure or
invalid-input outcome, confirming that a handler crash surfaces as worker
death and conservative recovery.

The application then inspects its own dedup table (not a Grind API) to
decide each resolution after rebinding a typed handle from the durable job
ID with `postgres.bind_handle`:

- One job is confirmed successful directly from the application's retained
  receipt (`postgres.resolve_uncertain` with `ConfirmSuccess`); the handler
  is never invoked again.
- The other job's replay is authorized (`AuthorizeReplay`); the automatic
  consumer picks the requeued row back up and reruns the handler, which
  calls the same synthetic effect with the same key and receives the
  original receipt — the effect count for that key never exceeds 1, even
  though the handler itself ran twice.

## Uniqueness admission through the public API

`public_consumer_unique_admission_existing_conflict_and_retry_test` and
`public_consumer_unique_reschedule_across_queues_test` exercise
`grind/unique`/`postgres.submit_unique` entirely through public imports:
`grind/unique`, `grind/postgres`, `grind/job`, `grind/worker`, `grind/queue`,
and `grind/registry` — no `@internal` function and no raw `pog` connection.

The first admits a job, observes a second, independently identified
submission against the same key as `unique.Existing`, rebinds that conflict
with the same `postgres.bind_handle` path used after a restart, and replays
the _original_ `SubmissionId` a third time to confirm it returns the
receipt's own recorded `Inserted` decision rather than a fresh conflict. A
manually driven consumer then actually runs the job and reads its typed
committed outcome.

The second seeds a job scheduled an hour out, then reschedules it from a
_different_ queue under an `AcrossQueues` policy and
`unique.RescheduleScheduledTo` — `conflict_queue` reports the row's actual
original queue, not the rescheduling submission's own, and the row is
claimable and runs to a typed outcome once its new time is due.

This exercise found no gap in the public surface: every step needed here —
building a selected or full-input key, a policy, a `SubmissionId`, admitting,
detecting a conflict, rebinding it, and reading a typed outcome — is already
reachable with public imports alone. See `docs/UNIQUENESS-CONTRACT.md` and
`docs/RECOVERY-EVIDENCE.md` (Increments 12 and 13) for the full contract and
mutation evidence.

Run the consumer as part of the disposable integration suite from the package
root with `nix develop --command bash scripts/test-postgres.sh`.
