# Migrating from Grind 0.2 to 0.3

Grind 0.3.0 is a breaking release. Change the dependency to `grind >= 0.3.0 and < 0.4.0` in `gleam.toml`, then work through the sections below in order. The first one affects every installation.

| Change                                                                                            | Who must act                                                                    |
| ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| [Schema version 15 builds an index concurrently](#schema-version-15-builds-an-index-concurrently) | Every installation: whoever runs migrations, with `grind.migrate` or cigogne    |
| [Grind depends on Sinal 0.2](#grind-depends-on-sinal-02)                                          | Applications that attach Sinal handlers to Grind events, or depend on Sinal 0.1 |
| [A handler crash has its own cause](#a-handler-crash-has-its-own-cause)                           | Workers with a declared settlement, and operators who read uncertain jobs       |
| [A lost lease stops the handler](#a-lost-lease-stops-the-handler)                                 | Operators who read uncertain jobs, and handlers with external effects           |
| [`SubmitError` has a `BatchCommitUnknown` variant](#submiterror-has-a-batchcommitunknown-variant) | Code that matches `grind.SubmitError` exhaustively                              |

New in 0.3.0 and not breaking: `grind.submit_many`, which admits a list of jobs in one transaction. Two behaviour changes need no action and are listed under [Other changes](#other-changes).

## Schema version 15 builds an index concurrently

**What changed.** Version 15 adds the index `grind_jobs_settle_due_idx` on `grind_jobs (queue, settle_at, id)` for uncertain jobs with a due time. PostgreSQL builds it with `CREATE INDEX CONCURRENTLY`, which cannot run inside a transaction. The version's marker row is then written in its own transaction, as every earlier version's is. A Grind 0.3 runtime that runs consumers refuses a schema below version 15 with `SchemaNotMigrated`.

**Why.** Every consumer whose workers declare a settlement polls for due uncertain jobs. Without the index each poll read every uncertain job of the installation: about 20 ms per poll at 200,000 uncertain jobs, against about 0.1 ms with it. A plain `CREATE INDEX` would block every write to the jobs table for the whole build. ADR-0023 records the decision.

**What to do.** Apply version 15 before you start 0.3 nodes that run consumers, the way you already apply migrations:

- **With `grind.migrate` or `grind.with_startup_migration`.** Nothing changes in your code. `grind.migrate` builds the index on one connection before the version's transaction and holds Grind's migration lock as a session lock while it builds. That connection must keep its PostgreSQL session for the whole build: connect `grind.migrate` directly to PostgreSQL or through a pooler in session mode, never through a pooler in transaction mode.
- **With the cigogne mirror files.** Copy both new files from `priv/migrations`: `20261125000000-grind_v15_settle_due_idx.sql`, which builds the index and is marked `disable_transaction`, and `20261125000001-grind_v15.sql`, which writes the marker. Apply them in that order. Do not wrap the run in a transaction, for example with `psql --single-transaction` or a migration runner that puts every file in one `BEGIN`: PostgreSQL refuses `CREATE INDEX CONCURRENTLY` there. Cigogne takes no lock for the index file, so run one migrator at a time on a schema, and never mix cigogne and `grind.migrate` on one schema.

**What to expect while it runs.**

- Job reads and writes continue. The build takes a `SHARE UPDATE EXCLUSIVE` lock on `grind_jobs`, which does not block `SELECT`, `INSERT`, `UPDATE` or `DELETE`, so submission, claims and acknowledgements keep working. The lock does block `VACUUM` (autovacuum included), `ANALYZE`, other index builds and other schema changes on `grind_jobs` until the build ends.
- The build waits for transactions that were already open when it started. A long application transaction, such as one that holds a `grind.submit_in` admission open, delays the build until it ends.
- The build reads the whole jobs table twice, so its time grows with the table's size. `grind.migrate` gives the version its migration deadline, 30 seconds by default, which a large table can exceed. Raise it for that run, for example `grind.with_migration_deadline(config, duration.minutes(30))`, and run `grind.migrate` as a deployment step rather than at node boot.
- A build that fails or is cut off by the deadline leaves an invalid index and no version 15 marker. Run `grind.migrate` again: it drops the invalid index and builds it again. A cigogne user drops it by hand (`DROP INDEX CONCURRENTLY IF EXISTS grind_jobs_settle_due_idx`) and runs cigogne again.

## Grind depends on Sinal 0.2

**What changed.** Grind's dependency moved from `sinal >= 0.1.0 and < 0.2.0` to `sinal >= 0.2.0 and < 0.3.0`. Grind's own events and their names are unchanged.

**Why.** Sinal 0.2 binds every handler to the process that attaches it, so a handler no longer outlives that process.

**What to do.** If your application depends on Sinal directly, change its range to `sinal >= 0.2.0 and < 0.3.0` and run `gleam deps update sinal`. Attach the handlers that observe Grind events from a long-lived supervised process, as the [Sinal migration guide](https://github.com/gleam-dream/sinal/blob/main/docs/MIGRATION.md) describes: a handler attached in a function that returns is detached when its process exits.

## A handler crash has its own cause

**What changed.** `grind/abandoned.Cause` has a third variant, `HandlerCrashed`, and `grind/testing.with_cause` accepts it. A handler crash is a panic, a failed `let assert`, or any other exit of the handler's process without a return. Under `HoldUncertain`, the default, Grind now holds the crashed job `uncertain` at once, with the cause `HandlerCrashed` and a description of at most 500 characters and 2000 bytes: the kind of failure, its module, function and line, and the message the developer wrote for a `panic`, `let assert ... as`, `assert ... as` or `todo as`. The description never holds a value the failure carried. In 0.2 the job stayed `executing` until its lease expired (30 seconds by default) and was then held `uncertain` with the cause `LeaseExpired` and the text `expired attempt requires outcome reconciliation`, as if its node had died. Under `ReplayAfterLeaseExpiry` a crashed job is still left to lease-expiry recovery, which replays it while replays remain and no cancellation is pending, and otherwise holds it `uncertain` with the cause `LeaseExpired`.

**Why.** A settlement and an operator could not tell a bug in the handler from a lost node, and the job waited a whole lease before anything could decide it.

**What to do.** Add a branch to every settlement that matches `abandoned.cause` exhaustively. A crash usually means a bug in the handler, so keeping the job for an operator is a safe default:

```gleam
// 0.2
case abandoned.cause(job) {
  abandoned.TimedOut -> Ok(worker.ConfirmFailure("timed out"))
  abandoned.LeaseExpired -> Ok(worker.LeaveUncertain("needs a person"))
}

// 0.3
case abandoned.cause(job) {
  abandoned.TimedOut -> Ok(worker.ConfirmFailure("timed out"))
  abandoned.HandlerCrashed -> Ok(worker.LeaveUncertain("the handler crashed"))
  abandoned.LeaseExpired -> Ok(worker.LeaveUncertain("needs a person"))
}
```

Code that waited for lease expiry after a crash now sees the job decided earlier. Jobs that a crash held `uncertain` before the upgrade keep the cause `LeaseExpired`. A message written with `panic as`, `let assert ... as`, `assert ... as` or `todo as` is stored as written, so do not interpolate job data into it.

## A lost lease stops the handler

**What changed.** When a lease renewal finds that a running attempt has lost its lease (the lease expired, or lease-expiry recovery already moved the job), Grind now kills the handler at once and frees its slot. The attempt sends no acknowledgement. Lease-expiry recovery decides the job: once the lease has expired, the next claim's scan holds it `uncertain` with the cause `LeaseExpired`, or replays it under `ReplayAfterLeaseExpiry`. In 0.2 the handler kept running until it returned or reached its timeout (15 minutes by default), and only then was its acknowledgement rejected as stale.

A lost lease and a crash are therefore reported differently. A crash is acknowledged by the attempt itself, at once, with the cause `HandlerCrashed` and its description. A lost lease is never acknowledged by the attempt, and recovery records the cause `LeaseExpired` with the text `expired attempt requires outcome reconciliation`, as for a node that died.

**Why.** After a lease is lost no write of that attempt can commit, and under `ReplayAfterLeaseExpiry` another node may already run the same job, so the old handler only duplicated work and held a slot.

**What to do.** Nothing in your code. `grind/testing.drain` now counts an attempt that lost its lease as processed, as it counts a replay-safe timeout, instead of failing after the handler returned; read the job's durable state for its outcome. A handler can be stopped in the middle of its work after a lost lease, so give its external effects an idempotency key, as for any abandoned attempt. Grind notices a lost lease at the next renewal, up to a third of the lease after the loss.

## `SubmitError` has a `BatchCommitUnknown` variant

**What changed.** `grind.SubmitError` has a new variant, `BatchCommitUnknown(pending: List(PendingSubmission))`. Only `grind.submit_many` returns it, when the batch may or may not have committed. `grind.submit_error_kind` classifies it `Unknown`.

**What to do.** Add a branch to every exhaustive match on `SubmitError`. Treat it like `CommitUnknown`: pass each pending submission, one per job in list order, to `grind.reconcile_submission`, or retry the same batch when every job has an id.

```gleam
case grind.submit_many(jobs, batch) {
  Ok(admissions) -> handle(admissions)
  Error(grind.CommitUnknown(pending)) -> reconcile([pending])
  Error(grind.BatchCommitUnknown(pending:)) -> reconcile(pending)
  Error(other) -> report(other)
}
```

## Other changes

- A consumer now claims into all its free slots with one statement, instead of one job per statement. A shutdown request that arrives while a claim statement runs is handled after that statement's jobs start, so up to one batch (at most the free slots) can start after the request.
- After Grind's root supervisor is killed or crashes, the next start under the same name waits until every process of the previous runtime has exited, at most the longest queue grace plus the connect timeout and five seconds. A start that waits longer fails with `the previous runtime under this name did not stop in time`. In 0.2 the restart failed with `already_started` until the application's supervisor gave up.
