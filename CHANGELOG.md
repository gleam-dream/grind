# Changelog

All notable changes to this package are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the package
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

The first release. Wave 3 of the release plan replaced the engine-shaped
public modules with one `grind` facade; see
[docs/migration-wave-3.md](docs/migration-wave-3.md) for every changed item.
Earlier changes are in [docs/migration-wave-2.md](docs/migration-wave-2.md).

### Follow-up fixes

Found by the apps' re-run against wave 3; see
[docs/migration-wave-3.md](docs/migration-wave-3.md), "Follow-up fixes".

- Fixed: a replayed attempt (`ReplayAfterLeaseExpiry`) now emits
  `[grind, job, quarantined]` with `replayed: True` and the expired
  attempt's id, epoch and number. The scan read the attempt after the
  replay had cleared it, so the event was never sent (RA-12).
- Changed: a replay rolls back the attempt number, as a snooze does, so it
  no longer uses one of `with_max_attempts`'s business attempts; the
  redelivery reads the same `worker.attempt`.
- Changed: a pool built from the application's `pog.Config` keeps the
  application's `search_path`. Grind sets its schema for each of its own
  storage calls and restores the session's value before the connection
  returns to the pool, so `grind.with_schema` and unqualified application
  tables work on one pool.
- Added: `worker.connection(context)`, the runtime's pool inside a handler,
  and `testing.with_connection`.
- Changed: a runtime that would start consumers refuses a schema behind its
  migrations: `start` returns `SchemaNotMigrated(found:, required:)` and
  `supervised` fails its child's start. Added `grind.with_startup_migration`,
  which migrates before any consumer starts, and
  `StartupMigrationFailed(MigrateError)`.
- Added: `grind.handle(admission)`, the admitted or occupying job.
  `Conflict` is now `Conflict(input, output, error)` with a `handle` field.
- Documented: the post-commit recipe for observing a `submit_in` admission,
  and why every bound, including `queue.with_poll_interval`, takes a
  `Duration`.

### Public surface

- Eight public modules: `grind`, `grind/worker`, `grind/job`, `grind/queue`,
  `grind/unique`, `grind/admin`, `grind/telemetry` and `grind/testing`.
  `grind/postgres`, the consumer formerly in `grind/queue`, `grind/registry`,
  `grind/submission`, `grind/pruner`, `grind/observation` and
  `grind/diagnostic` became machinery under `grind/internal`. No public
  module exports an `@internal` function, and every public module has a
  module doc; tests check both.
- `grind`: an opaque `Config` built with `new(pool: pog.Config)` and
  `with_*` setters; `start(config, name)`, `supervised(config, name)`,
  `named(name)`, `stop`, `check`, `connection`, `migrate`; `submit`,
  `submit_in`, `reconcile_submission`; `bind`, `arguments`, `state`,
  `outcome`, `await(within:)`, `cancel`; typed `ConfigError`, `StartError`,
  `SubmitError`, `ReadError`, `CancelError`, `StopError` and `MigrateError`,
  each with `describe_*`, and the stable `ErrorKind` classification.
- `grind/worker`: `new` and `responding` constructors that do not return
  `Result`; `codec` with version `"1"`; `with_queue`, `with_version`,
  `with_error_codec`, `with_max_attempts`, `with_retry_policy`,
  `with_timeout`, `with_max_snoozes`, `with_abandonment`,
  `with_codec_version`; the `Response`, `RetryDecision`, `Timeout` and
  `Abandonment` types; the opaque handler `Context`.
- `grind/job`: the `Job` builder (`new`, `with_id`, `at`, `after`, `unique`,
  `with_correlation`, `with_queue`, `with_max_attempts`), `State`,
  `TerminalCause`, `CodecKind`, `id`, `queue`, `state_name`,
  `state_from_name`, `is_finished`.
- `grind/queue`: an opaque `Queue` with `new`, `with_concurrency`,
  `with_poll_interval`, `with_lease` and `with_shutdown_grace`.
- `grind/unique`: `policy(key, period)` with `with_scope`, `with_states` and
  `reschedule_to`; the conflict action lives in the policy.
- `grind/admin`: `list` (with `query`, `in_queue`, `in_state`, `after`),
  `resolve_uncertain` (with `resolution`), `quarantine_expired`,
  `prune_finished`, `reconcile_acknowledgement`.
- `grind/telemetry`: the lifecycle and diagnostic Sinal descriptors in one
  module, typed with the public `grind/job` vocabulary.
- `grind/testing`: `perform`, `perform_with`, `context` and its setters,
  `cancelled`, and a bounded `drain`.

### Added

- The application's `pog.Config` builds Grind's pool, which keeps its name
  and size; `grind.connection` returns it, so one pool serves the
  application, Grind and other libraries.
- `grind.submit_in(grind, tx, job)` admits a job inside the application's
  open transaction, with its receipt and uniqueness decision, and restores
  the caller's `search_path` and `lock_timeout`.
- One supervised runtime: pool, forwarder, runtime registry, one consumer
  per queue the workers use, and the pruner, under a `RestForOne`
  supervisor. A name-based handle survives restarts; `stop` works from any
  process; on supervisor shutdown each queue drains up to its grace.
  `without_consumers` makes a submit-only node.
- A handler `Context` with job id, attempt, maximum attempts, snooze count,
  queue, correlation, deadline and a cancellation selector. A committed
  `cancel` reaches a running handler at its attempt's next lease renewal.
- Correlations: `job.with_correlation`, or a generated one, is stored with
  the job (schema v13), passed to the handler and carried by every
  `[grind, job, *]` event's `JobRef`.
- Every job event measures `JobMeasurements(count:, monotonic_ms:)`, so
  events from one node order causally across forwarders.
- `worker.with_abandonment(ReplayAfterLeaseExpiry(max_replays:))`: lease
  expiry requeues an abandoned attempt instead of holding it `uncertain`;
  `[grind, job, quarantined]` reports `replayed`. The default stays
  `HoldUncertain`.
- `admin.list` for operator sweeps, served by a partial index on uncertain
  jobs.
- `grind.await(within:)`, so applications no longer poll.
- Schema migration v13: `grind_jobs.correlation`, `max_replays` and
  `replay_count`, the `snooze_limit_reached` receipt cause, and
  `grind_jobs_uncertain_idx`.

### Changed

- Every submit records a receipt, under the job's id or a generated one, so
  a lost reply is always `CommitUnknown(pending)` and reconcilable;
  `CommitUnknownWithoutId` is gone.
- Bounded defaults: handler timeout 15 minutes (`Infinity` lifts it); 100
  snoozes per job, then `Failed(BusinessUnrecorded, Some(SnoozeLimitReached),
..)`; the pruner on by default with a 7-day maximum age; a 1 MiB payload
  limit with `PayloadTooLarge`; a 15 s initial connect at `start`; the
  default retry backoff adds up to 10% jitter; queue concurrency 10 and
  shutdown grace 15 s.
- Worker and codec versions default to `"1"`. Worker and job definitions
  are total and panic with the worker's id on a definition bug.
  Configuration and queue settings are checked at `start` with typed errors.
- Durations and times are `gleam_time` `Duration` and `Timestamp`.
- A handler runs in its own process, linked to its attempt, so its timeout
  and cancellation can be enforced.
- The stored outcome distinguishes `runtime_failed` and `contract_mismatch`
  from a business failure without an error codec.
- The bench harness's bookkeeping queries run on separate pools with a
  60 s timeout and retry a pgo pool restart (`noproc`); see
  `bench/src/grind_bench/harness_db.gleam`.

### Removed

- `grind.version()`.
- The manual-polling consumer API (`process_one`, `process_available`,
  `with_manual_polling`, `with_maximum_batch_jobs`); use `testing.drain`.
- The `Validated*` types and the `default_policy_validated` helpers.
