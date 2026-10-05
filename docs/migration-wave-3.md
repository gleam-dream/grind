# Migrating Grind dependents after wave 3

Wave 3 replaced Grind's engine-shaped public modules with one facade. An
application now writes a worker, configures one runtime from its own
`pog.Config`, adds one child to its supervision tree, submits jobs with
`grind.submit`, and waits with `grind.await`. This guide lists every removed
or changed public item, with its replacement, grouped by the module it used
to live in. The last section indexes the symbols each dependent uses.

The modules `grind/postgres`, `grind/registry`, `grind/submission`,
`grind/pruner`, `grind/observation` and `grind/diagnostic` are gone from the
public surface. `grind/queue` keeps its name but now holds queue settings
only. Their implementations live under `grind/internal`, which has no
stability contract: do not import it.

## The whole program, before and after

```gleam
// Before
let assert Ok(input) = worker.codec("email-v1", worker.infallible(encode_email), email_decoder())
let assert Ok(output) = worker.codec("msg-id-v1", worker.infallible(json.string), decode.string)
let assert Ok(send_email) =
  worker.define("mailer.send", "v1", input, output, fn(email) { Ok("msg:" <> email.to) })
let assert Ok(settings) = postgres.settings(database_url) |> postgres.validate
let assert Ok(database) = postgres.start(settings)
let assert Ok(Nil) = postgres.migrate(database)
let assert Ok(workers) = registry.new("mailers")
let assert Ok(workers) = registry.register(workers, send_email)
let assert Ok(consumer) = queue.start(database, workers, queue.default_policy_validated())
let assert Ok(handle) = postgres.submit(database, "mailers", send_email, Email("a@b.c", "hi"))
process.sleep(500)
let assert Ok(job.SucceededWith(message_id)) = postgres.outcome(database, handle)
let assert Ok(queue.StoppedCleanly) = queue.stop(consumer)
let assert Ok(Nil) = postgres.close(database)

// After
let mailer =
  worker.new(
    "mailer.send",
    input: worker.codec(worker.infallible(encode_email), email_decoder()),
    output: worker.codec(worker.infallible(json.string), decode.string),
    perform: fn(email) { Ok("msg:" <> email.to) },
  )
  |> worker.with_queue("mailers")
let assert Ok(pool) = pog.url_config(pool_name, database_url)
// In the application's supervision tree:
let config = grind.new(pool) |> grind.with_worker(mailer) |> grind.with_startup_migration
supervisor.add(tree, grind.supervised(config, name))
// Anywhere:
let jobs = grind.named(name)
let assert Ok(admission) = grind.submit(jobs, job.new(mailer, Email("a@b.c", "hi")))
let assert Ok(grind.Succeeded(message_id)) =
  grind.await(jobs, grind.handle(admission), within: duration.seconds(5))
```

Scripts and tests that own their runtime use
`grind.start(config, process.new_name("jobs"))` and `grind.stop(jobs)`.

## Behavior changes to check

| Change            | Before                                                                                             | After                                                                                                                                                                                                                                         |
| ----------------- | -------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Handler timeout   | none; the lease was renewed forever                                                                | 15 min; the handler is stopped and the abandonment policy applies (`HoldUncertain`: the job becomes `uncertain`). `worker.with_timeout(worker.Infinity)` restores the old behavior.                                                           |
| Snoozes           | unbounded; a receiver that always answered 429 kept a job alive forever                            | 100 per job, then `Failed(BusinessUnrecorded, Some(job.SnoozeLimitReached), "snooze limit of 100 reached: <reason>")`. `worker.with_max_snoozes`.                                                                                             |
| Retention         | nothing pruned unless the application started a pruner                                             | the runtime's pruner deletes finished jobs older than 7 days. `grind.without_pruner` keeps everything; `grind.with_pruner(max_age:)` changes the age.                                                                                         |
| Payload size      | unbounded                                                                                          | 1 MiB per encoded input, output or error. A larger input fails submit with `PayloadTooLarge`; a larger output or error ends the job `Failed(RuntimeFailure, ..)`.                                                                             |
| Initial connect   | unbounded                                                                                          | `start` fails with `Unavailable` after 15 s.                                                                                                                                                                                                  |
| Retry delay       | 15 s doubling to 1 day                                                                             | the same, plus 0–10% random jitter.                                                                                                                                                                                                           |
| Queue concurrency | 1 per consumer                                                                                     | 10 per node.                                                                                                                                                                                                                                  |
| Shutdown grace    | 5 s                                                                                                | 15 s.                                                                                                                                                                                                                                         |
| Plain submit      | an `INSERT` with no receipt; a lost reply was `CommitUnknownWithoutId` and could duplicate the job | every submit records a receipt under a generated id; a lost reply is `CommitUnknown(pending)`, reconciled by `grind.reconcile_submission`. One more row per job.                                                                              |
| Versions          | every worker and codec named its version                                                           | default `"1"`. A job stored by a wave 2 worker under version `"v1"` is claimed only by a worker with `worker.with_version("v1")` and codecs with `worker.with_codec_version("...-v1")`; keep the old version strings for jobs already stored. |
| Correlation       | none                                                                                               | every job stores a correlation (yours, or a generated one); handlers read it and every job event carries it.                                                                                                                                  |
| Handler process   | the attempt process ran the handler                                                                | the handler runs in its own linked process. `process.self()` inside a handler is that process; a crash still abandons the attempt as before.                                                                                                  |
| Schema            | v12                                                                                                | v13: run `grind.migrate`, or apply `priv/migrations/20261002000000-grind_v13.sql` through cigogne. Jobs stored before v13 get an id-derived correlation.                                                                                      |

## `grind` (root)

| Removed           | Replacement                                       |
| ----------------- | ------------------------------------------------- |
| `grind.version()` | none; read the package version from `gleam.toml`. |

New: `Config`, `new`, `with_worker`, `with_queue`, `with_schema`,
`with_statement_deadline`, `with_unique_lock_wait`, `with_migration_deadline`,
`with_connect_timeout`, `with_max_payload_bytes`, `with_observation_capacity`,
`with_pruner`, `without_pruner`, `without_consumers`, `check`, `start`,
`supervised`, `named`, `stop`, `connection`, `migrate`, `submit`, `submit_in`,
`reconcile_submission`, `bind`, `arguments`, `state`, `outcome`, `await`,
`cancel`, and the error, outcome and admission types below.

## `grind/postgres` → `grind`, `grind/admin`

| Before                                                                                    | After                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `postgres.Settings`, `postgres.settings(url)`                                             | `grind.new(pool)` with a `pog.Config` you build: `pog.url_config(name, url)`. Grind keeps the config's pool name and size.                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `postgres.with_pool_size(s, n)`                                                           | `pog.pool_size(config, n)` on your `pog.Config`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `postgres.with_unique_lock_wait(s, ms)`                                                   | `grind.with_unique_lock_wait(config, duration.milliseconds(ms))`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `postgres.with_statement_deadline(s, ms)`                                                 | `grind.with_statement_deadline(config, duration.milliseconds(ms))`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `postgres.with_migration_deadline(s, ms)`                                                 | `grind.with_migration_deadline(config, duration.milliseconds(ms))`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `postgres.with_observation_capacity(s, n)`                                                | `grind.with_observation_capacity(config, n)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `postgres.with_schema(s, schema)`                                                         | `grind.with_schema(config, schema)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `postgres.validate`, `ValidatedSettings`, `ConfigError`                                   | checked by `grind.start`/`grind.supervised`; `grind.check(config)` returns `grind.ConfigError`, whose variants carry their numbers (`UniqueLockWaitTooCloseToDeadline(lock_wait_ms:, margin_ms:, deadline_ms:)`).                                                                                                                                                                                                                                                                                                                                      |
| `postgres.start(validated) -> Database`                                                   | `grind.start(config, name) -> Grind`, or `grind.supervised(config, name)` as a child.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `postgres.close(database)`                                                                | `grind.stop(grind)` (from any process; drains each queue first).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `postgres.Database`                                                                       | `grind.Grind`, found by name with `grind.named(name)`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `postgres.statement_deadline_ms(database)`                                                | removed; you configured it.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `postgres.StartError` (`PoolStartFailed`, `InstallationQueryFailed`)                      | `grind.StartError`: `InvalidConfig(ConfigError)`, `Unavailable(pog.QueryError)` (no connection within the connect timeout, or the first query failed), `StartFailed(description:)` (a process did not start; the description never carries an exit reason, which could hold the pool configuration).                                                                                                                                                                                                                                                   |
| `postgres.CloseError` (`StopTimedOut`)                                                    | `grind.StopError`: `NotStarted`, `OwnedBySupervisor`, `StopTimedOut`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `postgres.migrate(database)`                                                              | `grind.migrate(grind)`; errors are `grind.MigrateError`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `postgres.StorageError`                                                                   | `grind.MigrateError`: `MigrationUnavailable(error)` replaces `MigrationQueryFailed` and `SchemaCreationFailed`; `IncompatibleSchema`, `UnsupportedSchemaVersion(version:)`, `MigrationStepFailed(version:, reason:)`, `MigrationLockUnavailable(version:)`, `MigrationCommitUnknown(version:)` keep their meaning; `MigrateNotRunning` is new.                                                                                                                                                                                                         |
| `postgres.submit(db, queue, worker, input)`                                               | `grind.submit(grind, job.new(worker, input))`; the queue is the worker's (`worker.with_queue`), or `job.with_queue`. Returns `Admission`; match `Inserted(handle)`.                                                                                                                                                                                                                                                                                                                                                                                    |
| `postgres.submit_at(db, queue, worker, input, available_at)`                              | `grind.submit(grind, job.new(worker, input) \|> job.at(timestamp))`, or `job.after(duration)` for a delay measured on the database clock.                                                                                                                                                                                                                                                                                                                                                                                                              |
| `postgres.submit_with_id(db, queue, id, worker, input, availability)`                     | `grind.submit(grind, job.new(worker, input) \|> job.with_id(id))`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `postgres.submit_unique(db, queue, id, worker, input, availability, policy, on_conflict)` | `grind.submit(grind, job.new(worker, input) \|> job.with_id(id) \|> job.unique(policy))`; the conflict action is in the policy (`unique.reschedule_to`).                                                                                                                                                                                                                                                                                                                                                                                               |
| `postgres.reconcile_unique(db, pending)`                                                  | `grind.reconcile_submission(grind, pending)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| (new)                                                                                     | `grind.submit_in(grind, tx, job)` inside your `pog.transaction`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `postgres.bind_handle(db, worker, id)`                                                    | `grind.bind(grind, worker, id)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `postgres.arguments(db, handle)`                                                          | `grind.arguments(grind, handle)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `postgres.state(db, handle)`                                                              | `grind.state(grind, handle)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `postgres.outcome(db, handle)`                                                            | `grind.outcome(grind, handle)`, or `grind.await(grind, handle, within:)` instead of sleeping and polling.                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `postgres.JobReadError`                                                                   | `grind.ReadError`: `JobNotFound`, `QueueMismatch(expected:, actual:)` (was `QueueRouteMismatch`), `WorkerMismatch(..)` (was `WorkerContractMismatch`), `CodecMismatch(kind:, expected:, actual:)` (was `CodecContractMismatch`; `kind` is `job.CodecKind`), `UndecodableValue(reason:)` (was `CodecFailed`), `CorruptRecord(value:)` (was `InvalidStoredState` and `SucceededOutputMissing`), `HandleFromAnotherDatabase` (was `HandleFromAnotherInstallation`), `ReadUnavailable(pog.QueryError)` (was `JobReadQueryFailed`), `ReadNotRunning` (new). |
| `postgres.cancel(db, handle)`                                                             | `grind.cancel(grind, handle)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `postgres.CancellationResult`                                                             | `grind.CancelResult`, same variants; `AlreadyFinished` carries `job.State`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `postgres.CancellationError`                                                              | `grind.CancelError`: `CancelUnavailable(error)` (was `CancellationQueryFailed`), `CancelCommitUnknown` (was `CancellationCommitUnknown` and `CancellationWriteRejected`), `CancelMismatch` (was the queue, worker and stored-state mismatches), `CancelJobNotFound`, `CancelFromAnotherDatabase`, `CancelNotRunning`.                                                                                                                                                                                                                                  |
| `postgres.resolve_uncertain(db, handle, ResolutionRequest(id, by, details, decision))`    | `admin.resolve_uncertain(grind, handle, admin.resolution(decision, id:, by:, details:))`                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `postgres.Resolution` (`ConfirmSuccess`, `ConfirmBusinessFailure`, `AuthorizeReplay`)     | `admin.Decision`: `ConfirmSuccess`, `ConfirmFailure`, `AuthorizeReplay`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `postgres.ResolutionResult` (`ResolutionApplied`, `ResolutionAlreadyApplied`)             | `admin.Resolved`: `Applied(job.State)`, `AlreadyApplied(job.State)`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `postgres.ResolutionError`                                                                | `admin.Error`: `EmptyResolutionField(field:)`, `NotUncertain`, `ResolutionConflict`, `RecordMismatch`, `ResolutionNeedsErrorCodec`, `ResolutionValueRejected(reason:)`, `CancellationPending`, `ResolutionCommitUnknown(resolution_id:)`, `WrongDatabase`, `Unavailable`.                                                                                                                                                                                                                                                                              |
| `postgres.quarantine_expired(db, limit:)`, `QuarantineError`                              | `admin.quarantine_expired(grind, limit:)`, `admin.Error`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `postgres.prune_finished(db, older_than_ms:, limit:)`, `PruneReport`, `PruneError`        | `admin.prune_finished(grind, older_than: Duration, limit:) -> Result(Int, admin.Error)`.                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `postgres.prune_limit_maximum()`                                                          | 10,000, documented on `admin.prune_finished`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `postgres.reconcile_acknowledgement(db, handle, command_id)`, `AcknowledgementReceipt`    | `admin.reconcile_acknowledgement(grind, handle, command_id)`; the receipt's `committed_state` is `job.State`, `cause` is `Option(job.TerminalCause)` (was `business_failure_cause`), `committed_at` is a `Timestamp` (was `committed_at_unix_ms`).                                                                                                                                                                                                                                                                                                     |
| `postgres.QueueRunError`, `AckRejection`                                                  | internal; consumers report these through telemetry.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `postgres.connection(db)` (was `@internal`)                                               | `grind.connection(grind)`, public.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `postgres.validate_retention_ms`, `validate_prune_limit` (`@internal`)                    | removed.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |

## `grind/queue` (consumer) → `grind`, `grind/queue`, `grind/testing`

| Before                                                                                                                                | After                                                                                                                                                                                 |
| ------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `queue.start(db, registry, policy)`, `queue.Consumer`                                                                                 | the runtime starts one consumer per queue its workers use.                                                                                                                            |
| `queue.stop(consumer)`, `StopError`, `StopOutcome`                                                                                    | `grind.stop(grind)` from any process; `grind.StopOutcome` is `StoppedCleanly` or `StoppedWithActiveWork(active_attempts:)`. A supervised runtime drains when its supervisor stops it. |
| `queue.QueuePolicy`, `default_policy`, `validate_policy`, `ValidatedPolicy`, `default_policy_validated`, `PolicyError`                | `queue.new(name)` with setters, passed to `grind.with_queue`; checked by `start` as `grind.InvalidQueue(queue:, setting:, value:)`.                                                   |
| `queue.with_poll_interval(p, ms)`                                                                                                     | `queue.with_poll_interval(q, duration.milliseconds(ms))`                                                                                                                              |
| `queue.with_maximum_concurrency(p, n)`                                                                                                | `queue.with_concurrency(q, n)`                                                                                                                                                        |
| `queue.with_lease_duration(p, ms)`                                                                                                    | `queue.with_lease(q, duration.milliseconds(ms))`                                                                                                                                      |
| `queue.with_shutdown_grace(p, ms)`                                                                                                    | `queue.with_shutdown_grace(q, duration.milliseconds(ms))`                                                                                                                             |
| `queue.StartError.LeaseTooShortForDeadline(attempted_ms, minimum_ms)`                                                                 | `grind.LeaseTooShort(queue:, lease_ms:, minimum_ms:)` from `start`.                                                                                                                   |
| `queue.StartError.NoRegisteredWorkers`                                                                                                | a queue exists only through a worker; configuring one without a worker is `grind.QueueWithoutWorkers(queue)`.                                                                         |
| `queue.Polling`, `with_manual_polling`, `process_one`, `process_available`, `with_maximum_batch_jobs`, `BatchOutcome`, `ProcessError` | `testing.drain(grind, queue:, limit:, within:)` with the runtime `without_consumers`.                                                                                                 |

## `grind/registry` → `grind.with_worker`

| Before                                                                             | After                                                                                                                                                         |
| ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `registry.new(queue)`, `registry.register(r, worker)`, `Registry`, `RegisterError` | `grind.with_worker(config, worker)` per worker; the queue is the worker's. A duplicate id and version is `grind.DuplicateWorker(id:, version:)` from `start`. |
| `registry.queue(r)`                                                                | the queue you passed to `worker.with_queue` (`"default"` otherwise).                                                                                          |

## `grind/worker`

| Before                                                                                                                                              | After                                                                                                                                                                                                                                                                                                |
| --------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `worker.codec(version, encode, decoder) -> Result(Codec, CodecError)`                                                                               | `worker.codec(encode, decoder) -> Codec` with version `"1"`; `worker.with_codec_version(codec, version)` keeps an old version.                                                                                                                                                                       |
| `worker.CodecError` (`EmptyCodecVersion`)                                                                                                           | `with_codec_version` panics on an empty version.                                                                                                                                                                                                                                                     |
| `worker.define(id, version, input, output, perform) -> Result`                                                                                      | `worker.new(id, input:, output:, perform:)`, then `worker.with_version(v)` to keep an old version and `worker.with_queue(q)`.                                                                                                                                                                        |
| `worker.define_with_error_codec(id, version, input, output, error, perform)`                                                                        | `worker.new(..) \|> worker.with_error_codec(error)`                                                                                                                                                                                                                                                  |
| `worker.DefinitionError`                                                                                                                            | the constructors panic on an empty id, version or queue.                                                                                                                                                                                                                                             |
| `worker.with_queue_handler(w, handler: fn(input) -> WorkerResponse)`                                                                                | `worker.responding(id, input:, output:, handle: fn(context, input) -> Response)`; no dummy `perform`.                                                                                                                                                                                                |
| `worker.WorkerResponse`: `WorkerSucceeded`, `WorkerFailed`, `WorkerSnoozed(delay, reason)`, `WorkerDiscarded`, `WorkerCancelled`, `WorkerUncertain` | `worker.Response`: `Succeeded`, `Failed`, `Snoozed(after: Duration, reason:)`, `Discarded(reason:)`, `Cancelled(reason:)`, `Uncertain(evidence:)`.                                                                                                                                                   |
| `worker.from_result`                                                                                                                                | `worker.from_result` returns `Response`.                                                                                                                                                                                                                                                             |
| `worker.retry_delay(ms) -> Result(RetryDelay, RetryDelayError)`, `retry_delay_milliseconds`, `RetryDelay`, `retry_delay_maximum_milliseconds()`     | a `Duration`: `duration.milliseconds(ms)`; out-of-range delays are clamped.                                                                                                                                                                                                                          |
| `worker.with_max_attempts(w, n) -> Result`                                                                                                          | `worker.with_max_attempts(w, n) -> Worker`; panics below 1. `job.with_max_attempts` overrides one job.                                                                                                                                                                                               |
| `worker.retry_policy(fn(RetryFailure(e), RetryContext) -> RetryDecision)`, `with_retry_policy(w, policy)`                                           | `worker.with_retry_policy(w, fn(error, attempt) -> RetryDecision)`.                                                                                                                                                                                                                                  |
| `worker.RetryFailure`, `RetryContext`, `RetryPolicy`, `RetryPolicyError`                                                                            | the callback gets the typed error and the attempt number; a `responding` handler reads `worker.attempt(context)`, `max_attempts`, `snooze_count`.                                                                                                                                                    |
| `worker.RetryDecision`: `RetryAfter(RetryDelay)`, `DoNotRetry`                                                                                      | `worker.RetryAfter(Duration)`, `worker.DoNotRetry`.                                                                                                                                                                                                                                                  |
| `worker.default_retry_delay_milliseconds(attempt)`                                                                                                  | `worker.default_backoff(attempt) -> Duration` (before jitter).                                                                                                                                                                                                                                       |
| `worker.max_attempts_supported_maximum()`                                                                                                           | removed.                                                                                                                                                                                                                                                                                             |
| `worker.invoke`, `worker.respond`                                                                                                                   | `testing.perform(worker, input)`, `testing.perform_with(worker, context, input)`.                                                                                                                                                                                                                    |
| `worker.BusinessFailureCause` (`BudgetExhausted`, `RetryDeclined`)                                                                                  | `job.TerminalCause` (`BudgetExhausted`, `RetryDeclined`, `SnoozeLimitReached`).                                                                                                                                                                                                                      |
| `worker.CodecKind`                                                                                                                                  | `job.CodecKind`.                                                                                                                                                                                                                                                                                     |
| `worker.StoredCodecError`, `worker.Execution`                                                                                                       | internal.                                                                                                                                                                                                                                                                                            |
| (new)                                                                                                                                               | `worker.with_timeout(After(Duration) \| Infinity)`, `with_max_snoozes`, `with_abandonment(HoldUncertain \| ReplayAfterLeaseExpiry(max_replays:))`, `Context` accessors `job_id`, `attempt`, `max_attempts`, `snooze_count`, `queue`, `correlation`, `cancellation`, `deadline`, and `id`, `version`. |

A wave 2 worker kept its own version strings; keep them so stored jobs stay
claimable:

```gleam
// Before
let assert Ok(input) = worker.codec("payment-request-v1", worker.infallible(encode), decoder)
let assert Ok(output) = worker.codec("receipt-v1", worker.infallible(json.string), decode.string)
let assert Ok(charge) = worker.define("payments.charge", "v1", input, output, perform)

// After, compatible with jobs stored before
worker.new(
  "payments.charge",
  input: worker.codec(worker.infallible(encode), decoder) |> worker.with_codec_version("payment-request-v1"),
  output: worker.codec(worker.infallible(json.string), decode.string) |> worker.with_codec_version("receipt-v1"),
  perform:,
)
|> worker.with_version("v1")
|> worker.with_queue("payments")
```

A queue handler that read nothing from its job:

```gleam
// Before
let assert Ok(base) = worker.define_with_error_codec(id, "v1", input, output, error, fn(_) { Error(Nil) })
let delivery = worker.with_queue_handler(base, fn(input) { deliver(input) })

// After
worker.responding(id, input:, output:, handle: fn(context, input) { deliver(context, input) })
|> worker.with_error_codec(error)
```

## `grind/job`

| Before                                                                         | After                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| ------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `job.JobHandle`                                                                | `job.JobHandle`, unchanged in use.                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `job.id(handle) -> JobId`, `job.JobId`                                         | `job.id(handle) -> Int`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `job.id_value(handle)`                                                         | `job.id(handle)`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `job.queue(handle)`                                                            | unchanged.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `job.available_at(ms) -> Result(AvailableAt, AvailableAtError)`, `AvailableAt` | `job.at(job, timestamp.Timestamp)`; a time in the past runs now.                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `job.State`                                                                    | `job.State`, same variants. `job.state_name`, `state_from_name` and `is_finished` are new (`state_to_stored` was `@internal`).                                                                                                                                                                                                                                                                                                                                                                  |
| `job.Outcome`                                                                  | `grind.Outcome`: `Pending(state:)`, `Succeeded(output:)` (was `SucceededWith`), `Failed(failure:, cause:, description:)` (was `BusinessFailedWith`, `BusinessFailedWithCause`, `FailedOperationally`, `FailedOperationallyWithCause`), `Discarded(reason:)` (was `DiscardedWithReason`), `Cancelled(reason:)` (was `CancelledWithReason`), `Uncertain(evidence:)` (was `ReconciliationRequired`). `failure` is `Business(error)`, `BusinessUnrecorded`, `RuntimeFailure` or `ContractMismatch`. |
| `job.Installation`                                                             | internal.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| (new)                                                                          | `job.new(worker, input)`, `with_id`, `at`, `after`, `unique`, `with_correlation`, `with_queue`, `with_max_attempts`.                                                                                                                                                                                                                                                                                                                                                                            |

```gleam
// Before
case postgres.outcome(database, handle) {
  Ok(job.SucceededWith(receipt)) -> ...
  Ok(job.BusinessFailedWithCause(error, worker.BudgetExhausted)) -> ...
  Ok(job.FailedOperationally(description)) -> ...
  Ok(job.ReconciliationRequired(evidence)) -> ...
  Ok(job.Pending(_)) -> ...
  ...
}

// After
case grind.await(jobs, handle, within: duration.seconds(5)) {
  Ok(grind.Succeeded(receipt)) -> ...
  Ok(grind.Failed(grind.Business(error), Some(job.BudgetExhausted), _)) -> ...
  Ok(grind.Failed(_, _, description)) -> ...
  Ok(grind.Uncertain(evidence)) -> ...
  Ok(grind.Pending(_)) -> ...
  ...
}
```

## `grind/submission` → `grind`, `grind/job`

| Before                                                                            | After                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `submission.SubmissionId`, `submission_id(text) -> Result`, `submission_id_value` | `job.with_id(job, text)`; an empty id fails at submit with `grind.EmptyJobId`.                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `submission.Availability` (`Immediately`, `At(AvailableAt)`)                      | `job.at`, `job.after`; immediate by default.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `submission.Admission` (`Inserted`, `Existing`, `Rescheduled`)                    | `grind.Admission`, same variants.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `submission.Conflict`, `conflict_job_id`, `conflict_queue`, `conflict_state`      | `grind.Conflict(job_id:, queue:, state:)`, a record.                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `submission.PendingSubmission`, `pending_submission_id`                           | `grind.PendingSubmission`; pass it to `grind.reconcile_submission`.                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `submission.SubmitError`                                                          | `grind.SubmitError`: `InvalidInput(reason:)`, `PayloadTooLarge(bytes:, limit:)` (new), `EmptyJobId` (new), `IdConflict` (was `SubmissionConflict`), `UniquenessContended` (was `AdmissionContended`), `NotCommitted(reason:)`, `CommitUnknown(pending)`, `NotInTransaction` and `TransactionIsolationUnsupported(isolation:)` (new, `submit_in`), `WrongDatabase` (was `HandleFromAnotherInstallation`), `SubmitNotRunning` (new). `EmptyQueueName` and `CommitUnknownWithoutId` are gone. Branch on `grind.submit_error_kind`. |

## `grind/unique`

| Before                                                                            | After                                                                                                                                                 |
| --------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `unique.policy(key, scope, period, states)` and a separate `on_conflict` argument | `unique.policy(key, period)` (scope `WithinQueue`, states `Incomplete`, keep existing), then `with_scope`, `with_states`, `reschedule_to(timestamp)`. |
| `unique.ConflictAction` (`KeepExisting`, `RescheduleScheduledTo(AvailableAt)`)    | the default keeps the existing job; `unique.reschedule_to(policy, Timestamp)`.                                                                        |
| `unique.within_milliseconds(ms, from) -> Result(Period, PolicyError)`             | `unique.within(Duration, from:)`; panics on a non-positive period.                                                                                    |
| `unique.UniqueTimestamp` (`FromInsertion`, `FromSchedule`)                        | `unique.Origin`, same variants.                                                                                                                       |
| `unique.selected(name, select, codec) -> Result(Key, PolicyError)`                | `unique.selected(name, select, codec) -> Key`; panics on an empty name.                                                                               |
| `unique.PolicyError`                                                              | removed.                                                                                                                                              |
| `unique.full_input()`, `unique.while_retained()`, `QueueScope`, `States`          | unchanged.                                                                                                                                            |

```gleam
// Before
let assert Ok(period) = unique.within_milliseconds(3_600_000, unique.FromInsertion)
let policy = unique.policy(unique.full_input(), unique.WithinQueue, period, unique.Incomplete)
let assert Ok(id) = submission.submission_id("charge:" <> key)
postgres.submit_unique(db, "payments", id, charge, request, submission.Immediately, policy, unique.KeepExisting)

// After
let policy = unique.policy(unique.full_input(), unique.within(duration.hours(1), from: unique.FromInsertion))
grind.submit(jobs, job.new(charge, request) |> job.with_id("charge:" <> key) |> job.unique(policy))
```

## `grind/pruner` → `grind`, `grind/admin`

| Before                                                                                                                                                                              | After                                                                                                                                                                              |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pruner.start(db, policy)`, `pruner.supervised(db, policy)`, `Pruner`, `stop`, `StartError`, `StopError`, `supervisor_pid`, `Message`                                               | the runtime runs the pruner by default: every 30 s, up to 10,000 jobs, finished more than 7 days ago. `grind.with_pruner(config, max_age:)`, `grind.without_pruner(config)`.       |
| `pruner.PrunerPolicy`, `default_policy`, `with_interval`, `with_limit`, `with_max_age`, `validate_policy`, `ValidatedPrunerPolicy`, `default_policy_validated`, `PrunerPolicyError` | `grind.with_pruner(max_age:)`; the interval and batch are fixed. A non-positive age is `grind.NotPositive("pruner max age", _)`. Call `admin.prune_finished` for another schedule. |

## `grind/observation` and `grind/diagnostic` → `grind/telemetry`

| Before                                                                                                                                                                                                                               | After                                                                                                                                               |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `import grind/observation`, `import grind/diagnostic`                                                                                                                                                                                | `import grind/telemetry`; every descriptor and type keeps its name: `telemetry.acknowledged()`, `telemetry.capacity()`, `telemetry.AttemptContext`. |
| `AcknowledgedMeasurements`, `AdmittedMeasurements`, `ClaimedMeasurements`, `QuarantinedMeasurements`, `ResolvedMeasurements`, `CancellationMeasurements`, `ReleasedMeasurements`, `ContractMismatchMeasurements` (each `count: Int`) | one `telemetry.JobMeasurements(count:, monotonic_ms:)`.                                                                                             |
| `JobRef(job_id, queue, worker_id, worker_version)`                                                                                                                                                                                   | adds `correlation: Correlation`, emitted under the `correlation` key.                                                                               |
| `QuarantinedMetadata(ref, attempt, cancellation_was_requested)`                                                                                                                                                                      | adds `replayed: Bool`.                                                                                                                              |
| metadata `job.State`, `worker.BusinessFailureCause`, `worker.CodecKind`                                                                                                                                                              | `job.State`, `job.TerminalCause` (with `SnoozeLimitReached`), `job.CodecKind`.                                                                      |

The native event names and keys are unchanged except for the new
`correlation`, `monotonic_ms` and `replayed` keys, so native `:telemetry`
handlers keep working.

## Index by dependent

No sibling package depends on Grind. The oversight apps use these symbols;
each maps to the sections above.

### checkout

`postgres.settings`, `with_pool_size`, `with_schema`, `with_statement_deadline`,
`with_unique_lock_wait`, `validate`, `start`, `migrate`, `close`,
`Database`, `submit_with_id`, `bind_handle`, `state`, `outcome`,
`resolve_uncertain`, `ResolutionRequest`, `ResolutionApplied`,
`AuthorizeReplay`; `queue.default_policy`, `with_poll_interval`,
`with_maximum_concurrency`, `with_lease_duration`, `validate_policy`,
`start`, `stop`, `Consumer`; `registry.new`, `register`; `worker.codec`,
`infallible`, `Codec`, `define`, `with_queue_handler`, `retry_delay`,
`RetryDelay`, `WorkerResponse`, `WorkerSucceeded`, `WorkerSnoozed`,
`WorkerUncertain`, `WorkerCancelled`; `submission.submission_id`,
`Immediately`, `Admission`, `Inserted`, `Existing`, `Rescheduled`,
`conflict_job_id`; `job.id_value`, `state_to_stored`, `SucceededWith`,
`Succeeded`, `Uncertain`, `Queued`.

CHK-1 and CHK-2: build the runtime from the app's `pog.Config`, use
`grind.connection` for the app's queries and saga storage, and replace the
outbox with `grind.submit_in`. CHK-10: `worker.responding` needs no dummy
`perform`. CHK-6: `admin.list` finds uncertain jobs.

### webhooks

`postgres.settings`, `with_pool_size`, `with_schema`, `validate`, `start`,
`migrate`, `close`, `Database`, `submit_with_id`, `bind_handle`, `outcome`,
`JobReadError`; `queue.default_policy`, `with_poll_interval`,
`with_maximum_concurrency`, `with_manual_polling`, `validate_policy`,
`start`, `stop`, `process_one`, `process_available`, `BatchCompleted`,
`QueuePolicy`, `Consumer`; `registry.new`, `register`; `worker.codec`,
`infallible`, `Codec`, `define`, `define_with_error_codec`,
`with_queue_handler`, `with_max_attempts`, `with_retry_policy`,
`retry_policy`, `RetryPolicy`, `RetryAfter`, `DoNotRetry`, `BusinessFailure`,
`BudgetExhausted`, `retry_delay`, `invoke`, `WorkerResponse`,
`WorkerSucceeded`, `WorkerFailed`, `WorkerSnoozed`, `WorkerDiscarded`;
`submission.submission_id`, `Immediately`, `Inserted`, `Existing`,
`Rescheduled`, `conflict_job_id`; `job.id_value`, `Outcome`, `Pending`,
`Scheduled`, `SucceededWith`, `BusinessFailedWith`,
`BusinessFailedWithCause`, `DiscardedWithReason`; every
`observation.*` and `diagnostic.*` descriptor.

WHK-8: the snooze limit (100) ends a job whose receiver always answers 429;
`worker.snooze_count(context)` shows the count. WHK-5: the handler reads its
job id and correlation from the context, and every event carries the
correlation and `monotonic_ms`. Replace manual polling in tests with
`testing.drain`.

### extractor

`postgres.settings`, `with_pool_size`, `validate`, `start`, `migrate`,
`close`, `Database`, `submit`, `bind_handle`, `outcome`; `queue.default_policy`,
`QueuePolicy`, `PollEvery`, `validate_policy`, `start`, `stop`, `Consumer`;
`registry.new`, `register`; `worker.codec`, `Codec`, `Worker`,
`define_with_error_codec`, `with_queue_handler`, `with_max_attempts`,
`with_retry_policy`, `retry_policy`, `RetryAfter`, `BudgetExhausted`,
`retry_delay`, `retry_delay_milliseconds`, `WorkerResponse`,
`WorkerSucceeded`, `WorkerFailed`, `WorkerSnoozed`, `WorkerDiscarded`;
`job.id_value`, `JobHandle`, `State`, `Outcome`, `Pending`, `SucceededWith`,
`BusinessFailedWithCause`, `DiscardedWithReason`; `observation.admitted`,
`claimed`, `acknowledged`, `released`.

EXT-9: write the extracted invoice inside the job with `grind.connection`
(one pool), or enqueue follow-up work with `submit_in`. EXT-11:
`job.state_name`, `job.is_finished` and `grind.await` replace the app's
helpers and polling.

### secure_mcp

`postgres.settings`, `validate`, `start`, `migrate`, `close`, `Database`,
`submit`, `bind_handle`, `outcome`, `arguments`; `queue.default_policy`,
`with_poll_interval`, `with_manual_polling`, `validate_policy`, `start`,
`stop`, `process_one`, `Consumer`; `registry.new`, `register`;
`worker.codec`, `infallible`, `Codec`, `CodecError`,
`define_with_error_codec`, `with_retry_policy`, `retry_policy`, `DoNotRetry`;
`job.id_value`, `state_to_stored`, every `State` variant, `Pending`,
`SucceededWith`, `BusinessFailedWith`, `BusinessFailedWithCause`,
`FailedOperationally`, `FailedOperationallyWithCause`,
`DiscardedWithReason`, `CancelledWithReason`, `ReconciliationRequired`.

`worker.CodecError` and its `let assert` disappear (the codec is total);
`job.state_name` replaces the app's 15-line state mapping. The open
follow-up "no app test reaches `InvalidInput`": a refined codec now reaches
it from `grind.submit` (`consumer/test/grind_consumer/codec_test.gleam`
shows the shape).

### research_agent

`postgres.settings`, `with_pool_size`, `with_schema`,
`with_statement_deadline`, `with_unique_lock_wait`, `validate`, `start`,
`migrate`, `close`, `Database`, `submit`, `bind_handle`, `state`, `outcome`,
`cancel`, `CancellationResult`, `CancellationRequested`,
`resolve_uncertain`, `ResolutionRequest`, `AuthorizeReplay`,
`quarantine_expired`; `queue.default_policy`, `with_poll_interval`,
`with_maximum_concurrency`, `with_lease_duration`, `validate_policy`,
`start`, `stop`, `Consumer`; `registry.new`, `register`; `worker.codec`,
`Codec`, `define_with_error_codec`, `with_queue_handler`,
`with_max_attempts`, `with_retry_policy`, `retry_policy`, `RetryAfter`,
`DoNotRetry`, `BusinessFailure`, `BudgetExhausted`, `retry_delay`,
`WorkerResponse`, `WorkerSucceeded`, `WorkerFailed`, `WorkerUncertain`,
`WorkerDiscarded`, `WorkerCancelled`; `job.id_value`, `JobHandle`,
`Outcome`, `Pending`, `Uncertain`, `SucceededWith`,
`BusinessFailedWithCause`, `DiscardedWithReason`, `CancelledWithReason`;
the lifecycle `observation.*` descriptors.

RA-2: `worker.with_abandonment(worker.ReplayAfterLeaseExpiry(max_replays: n))`
replaces the 38-line replay sweep for an idempotent run driver, and
`admin.list` serves the operator sweep. RA-5: `worker.cancellation(context)`
fires when `grind.cancel` commits, so the cancellation fan-out can go. RA-11:
`grind.stop` works from any process, and one pool serves the app and Grind.

## Follow-up fixes

The apps' re-run against wave 3 found one defect and four hazards. These
changes fix them; the app-side edits are listed after the table.

| Finding                                           | Before                                                                                                                                                           | After                                                                                                                                                                                                                                                                                                                                              |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| RA-12: a replay emitted no `quarantined` event    | The scan's `RETURNING` read `attempt_id` after the replay had cleared it, so the event was skipped and `replayed: True` never reached a handler.                 | The event names the expired attempt from the row as it was before the scan (`attempt_id`, `epoch`, `attempt`), with `replayed: True`, from the claim-time scan and from `admin.quarantine_expired`.                                                                                                                                                |
| RA-12: a replay used a business attempt           | The redelivery was claimed as attempt 2, contradicting `with_max_attempts`'s doc.                                                                                | A replay rolls back the attempt number, as a snooze does: the redelivery is claimed as the same attempt under a new `attempt_id` and `epoch`, and only `max_replays` bounds replays. A handler that told a replay apart by `worker.attempt(context) > 1` must keep its own marker.                                                                 |
| Shared pool `search_path`                         | The pool built from the app's `pog.Config` pinned every session's `search_path` to Grind's schema, so an app kept Grind in `public` or qualified its own tables. | The pool keeps the app's `search_path`. Each Grind storage call sets Grind's schema when it checks out a connection and restores the session's value before the connection returns to the pool; a connection whose value cannot be restored is retired. `with_schema` works with unqualified app queries.                                          |
| The worker could not reach the pool               | A handler captured `pog.named_connection(pool_name)` or called `grind.connection(grind.named(name))` per attempt.                                                | `worker.connection(context)` returns the runtime's pool; `testing.with_connection(context, connection)` sets it in a test.                                                                                                                                                                                                                         |
| Consumers polled before the schema existed        | `grind.start` then `grind.migrate` left consumers polling missing tables.                                                                                        | A runtime that would start consumers checks the schema first. `start` fails with `SchemaNotMigrated(found:, required:)`, and `supervised` fails its child's start with the same text, until the schema is current. `grind.with_startup_migration` applies the migrations before any consumer starts. A runtime `without_consumers` is not checked. |
| `submit_in` emits no `admitted` event             | Undocumented recipe.                                                                                                                                             | Documented on `submit_in`: record the admission after your commit, keyed by `job.id(grind.handle(admission))` and the job's correlation.                                                                                                                                                                                                           |
| A plain submit matched three `Admission` variants | `Existing` and `Rescheduled` carried no handle, so a correct branch needed `grind.bind`.                                                                         | `grind.handle(admission)` returns the new or the occupying job. `Conflict` gains `handle` and the type parameters `Conflict(input, output, error)`.                                                                                                                                                                                                |
| `queue.with_poll_interval` takes a `Duration`     | Two apps added `gleam_time` for it.                                                                                                                              | Kept: every bound in Grind is a `Duration`, so the unit is in the call and a lease in seconds cannot be read as milliseconds. `grind/queue`'s module doc says so.                                                                                                                                                                                  |

| Before                                                                                      | After                                                                                                        |
| ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `grind.start(config, name)` then `grind.migrate(jobs)`                                      | `grind.start(config \|> grind.with_startup_migration, name)`; or migrate from a `without_consumers` runtime. |
| `grind.supervised(config, name)` then `grind.migrate(grind.named(name))`                    | `grind.supervised(config \|> grind.with_startup_migration, name)`.                                           |
| `pog.named_connection(pool_name)` or `grind.connection(grind.named(name))` inside a handler | `worker.connection(context)`.                                                                                |
| `public.`-qualified app tables, or `with_schema` dropped, on Grind's pool                   | unqualified app tables, with `grind.with_schema` back if the app wants Grind apart.                          |
| `case admission { Inserted(handle) -> .. Existing(c) \| Rescheduled(c) -> c.job_id }`       | `grind.handle(admission)` (and `job.id` of it).                                                              |
| `worker.attempt(context)` to detect a lease replay                                          | an app-owned marker; `[grind, job, quarantined]` with `replayed: True` observes it.                          |
| `grind.Conflict` in a type annotation                                                       | `grind.Conflict(input, output, error)`.                                                                      |

The apps' remaining findings for Grind are unchanged here: the handler's
`Cancelled(reason)` is replaced by `"cancelled by caller"` when a
cancellation was requested (research_agent), `worker.deadline` stays an
absolute `Timestamp` (checkout, research_agent), and a result write inside
the acknowledgement transaction (extractor, EXT-9) is not offered.

## Round 9: validation maintenance

Keep the test and benchmark PostgreSQL Unix sockets inside their temporary cluster directories (`pg_ctl -k "$root"`) rather than relying on a system socket directory. This is test portability only; job, workflow, lease, storage and application APIs are unchanged. No dependent source migration is required.
