# Using Grind

Start with the [README](../README.md) for installation and a complete example.
This guide describes the public API, runtime ownership and advanced configuration.
The [operations guide](OPERATIONS.md) covers deployment and recovery.

## Defaults

Handlers, payloads, retry counts and snoozes have finite defaults.
Queued checkout can exceed the storage deadline; see "Deadlines and capacity".
Lift supported bounds explicitly when the application requires it.

| Operation                    | Default                                                        | Change it with                                        |
| ---------------------------- | -------------------------------------------------------------- | ----------------------------------------------------- |
| Handler execution            | 15 min, then the abandonment policy applies                    | `worker.with_timeout(worker.Infinity)` to lift        |
| Snoozes per job              | 100, then `Failed(.., SnoozeLimitReached, ..)`                 | `worker.with_max_snoozes`                             |
| Business attempts            | 20                                                             | `worker.with_max_attempts`, `job.with_max_attempts`   |
| Retry delay                  | 15 s doubling to 1 day, plus 0–10% jitter                      | `worker.with_retry_policy`                            |
| Abandoned attempt            | held `uncertain` (`HoldUncertain`)                             | `worker.with_abandonment(ReplayAfterLeaseExpiry(n))`  |
| Job retention                | pruner on: finished jobs deleted after 7 days                  | `grind.with_pruner(max_age:)`, `grind.without_pruner` |
| Encoded input, output, error | 1 MiB, then `PayloadTooLarge`                                  | `grind.with_max_payload_bytes`                        |
| Initial connect at `start`   | 15 s, then `Unavailable`                                       | `grind.with_connect_timeout`                          |
| Storage call                 | 4 s absolute deadline; queued checkout may exceed it           | `grind.with_statement_deadline`                       |
| Uniqueness lock wait         | 2 s, then `UniquenessContended`                                | `grind.with_unique_lock_wait`                         |
| Migration step               | 30 s                                                           | `grind.with_migration_deadline`                       |
| Waiting for a result         | `await(within:)` takes the bound                               |                                                       |
| `testing.drain`              | `within:` and `limit:` take the bounds                         |                                                       |
| Queue concurrency, per node  | 10                                                             | `queue.with_concurrency`                              |
| Poll interval                | 250 ms                                                         | `queue.with_poll_interval`                            |
| Attempt lease                | 30 s, renewed every 10 s (at least 4 × the statement deadline) | `queue.with_lease`                                    |
| Shutdown grace               | 15 s                                                           | `queue.with_shutdown_grace`                           |
| Observation forwarder        | 1,024 events in flight, then dropped and counted               | `grind.with_observation_capacity`                     |
| Worker and codec versions    | `"1"`                                                          | `worker.with_version`, `worker.with_codec_version`    |
| Queue                        | `"default"`                                                    | `worker.with_queue`, `job.with_queue`                 |

## Modules

| Module            | Holds                                                                                                                                                             |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `grind`           | `Config`, the runtime (`start`, `supervised`, `named`, `stop`, `connection`, `migrate`), `submit`, `submit_in`, reads, `await`, `cancel`, errors and their kinds. |
| `grind/worker`    | Workers, codecs, `Response`, retry, timeout, snooze and abandonment policies, and the handler `Context`.                                                          |
| `grind/job`       | The job builder (`new`, `with_id`, `at`, `after`, `unique`, `with_correlation`), `JobHandle`, `State`, `TerminalCause`.                                           |
| `grind/queue`     | Per-queue tuning: concurrency, polling, lease, shutdown grace.                                                                                                    |
| `grind/unique`    | Uniqueness policies.                                                                                                                                              |
| `grind/admin`     | Operator work: `list`, `resolve_uncertain`, `quarantine_expired`, `prune_finished`, `reconcile_acknowledgement`.                                                  |
| `grind/telemetry` | The Sinal events Grind emits.                                                                                                                                     |
| `grind/testing`   | `perform` a handler without a database; `drain` a queue on demand.                                                                                                |

Everything under `grind/internal` is machinery with no stability contract.
The [external consumer package](../consumer/README.md) exercises the public API
from outside the package.

## Workers and codecs

A worker is a definition written in source code, so invalid arguments to its
constructors and setters panic with the worker's id. This includes an empty
id, version or queue, or an out-of-range limit. `worker.new` takes a handler
`fn(input) -> Result(output, error)`; `worker.responding` takes
`fn(Context, input) -> Response(output, error)`, which may also snooze,
discard, cancel or report an uncertain effect.

A codec's encoder returns `Result(json.Json, String)`, so a validating codec
can reject a value; wrap a plain encoder with `worker.infallible`. A
json_blueprint codec maps its encode error to the reason:

```gleam
worker.codec(
  fn(value) {
    codec.to_json(invoice_codec, value)
    |> result.map_error(codec.describe_encode_error)
  },
  codec.decoder(invoice_codec),
)
```

| Value rejected                                   | Result                                                                                               |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------------------- |
| Input, or a `unique.selected` key, at submit     | `grind.InvalidInput(reason)` before any connection is used. Nothing is written.                      |
| Handler output or error, after the handler ran   | `Failed(RuntimeFailure, None, "output codec rejected the handler's output: <reason>")`; not retried. |
| A value confirmed with `admin.resolve_uncertain` | `admin.ResolutionValueRejected(reason)` before any write.                                            |

The worker id and version and each codec version are stored with every
job, so a consumer runs a job only with the exact definition it was
submitted under. Versions default to `"1"`; change one when a stored shape
changes.

## Jobs, receipts and outcomes

`job.new(worker, input)` runs now, in the worker's queue. `with_id` makes
the submit idempotent: resubmitting the same job under the same id returns
the first admission, and a different job under it returns `IdConflict`.
Every submit records a receipt, under the job's id or one Grind generates,
so a submit whose reply was lost returns `CommitUnknown(pending)`, which
`grind.reconcile_submission` settles. `at` and `after` schedule the job;
`unique` admits it only when no job occupies its key (see "Uniqueness");
`with_correlation` carries a `sinal/correlation` value into the handler's
context and every event about the job. Without one, Grind generates one.

`grind.outcome` and `grind.await` return `Pending(state)`, `Succeeded`,
`Failed(failure, cause, description)`, `Discarded`, `Cancelled` or
`Uncertain`. The error and outcome types may gain variants: branch on
`grind.submit_error_kind`, `read_error_kind` and `cancel_error_kind` where
a new variant should not break your code. `describe_*` functions give a log
line.

## One pool, and enqueueing inside your transaction

Grind builds its pool from the application's `pog.Config` and keeps its
pool name. `grind.connection(jobs)` returns that pool, so the application,
Grind and other libraries (a saga store, for example) share one pool; a
handler reaches it with `worker.connection(context)`, since a worker is
defined before the runtime exists. The pool's isolation is
`READ COMMITTED`. Its `search_path` stays the application's: Grind sets
its own schema (`grind.with_schema`, `public` by default) for each of its
storage calls and restores the session's value before the connection
returns to the pool, so an application can keep Grind in its own schema
and still query its tables unqualified.

`grind.submit_in(jobs, tx, job)` admits a job inside the application's
open transaction: the job, its receipt and any uniqueness decision commit or
roll back with the application's own writes. Grind sends no `BEGIN` or
`COMMIT` and emits no `admitted` event, because it cannot see your commit;
record the admission yourself after `pog.transaction` returns `Ok`, keyed
by `job.id(grind.handle(admission))` and the job's correlation. The
transaction must be
`READ COMMITTED` on Grind's database; Grind sets `search_path` and
`lock_timeout` for its own statements and restores yours.

```gleam
pog.transaction(grind.connection(jobs), fn(tx) {
  use _ <- result.try(orders.confirm(tx, order))
  grind.submit_in(jobs, tx, job.new(receipt(), order.id) |> job.with_id(order.id))
  |> result.map_error(ReceiptNotQueued)
})
```

## The runtime

`grind.supervised(config, name)` is one child of the application's tree;
`grind.named(name)` returns a handle that is valid before the runtime starts
and across its restarts, so a web handler reaches Grind without threading a
value. The tree restarts later children when an earlier one restarts, so
consumers always run against the current pool. On shutdown each queue stops
claiming and waits up to its grace for running jobs. `grind.start(config,
name)` starts an unsupervised runtime for scripts and tests, and
`grind.stop` stops it from any process. `grind.without_consumers` makes a
submit-only node. `start` and `supervised` validate the configuration;
`grind.check` returns the same typed `ConfigError` up front.

## Handler context, cancellation and abandoned attempts

A `responding` handler receives a `worker.Context`: `job_id`, `attempt`,
`max_attempts`, `snooze_count`, `queue`, `correlation`, `deadline`,
`cancellation` and `connection` (the runtime's pool). `grind.cancel` of a running job commits a cancellation
request; the attempt's next lease renewal (within a third of the lease)
delivers it to the handler's `cancellation` selector, and a handler that
stops early returns `Cancelled`.

A handler that exceeds its timeout is stopped. An attempt can also be
abandoned by a node that died. The worker's abandonment policy decides what
follows: `HoldUncertain`, the default, holds the job `uncertain` until an
operator resolves it with `admin.resolve_uncertain`; it never runs twice
without a decision. `ReplayAfterLeaseExpiry(max_replays:)` requeues it after
its lease expires, at most that many times; choose it for handlers that are
idempotent by construction. A replay redelivers the same business attempt,
so it does not count against `max_attempts`, and `[grind, job, quarantined]`
reports it with `replayed: True`. `admin.list(jobs, admin.query(limit: 100) |>
admin.in_state(job.Uncertain))` finds the jobs that wait for an operator.

A snooze reschedules the job without using an attempt. After
`max_snoozes` (100) snoozes, the next one ends the job as
`Failed(BusinessUnrecorded, Some(SnoozeLimitReached), ..)`, so a receiver
that always answers 429 cannot keep a job alive forever.

## Testing

`testing.perform(worker, input)` runs a handler in the test's process,
round-tripping the input, output and error through the worker's codecs.
`testing.perform_with(worker, testing.context(job_id: 7, attempt: 3), input)`
chooses the context. `testing.drain(jobs, queue:, limit:, within:)` claims
and runs a queue's due jobs on demand; configure the runtime
`without_consumers` so nothing else claims them.

## Isolation: one installation per schema

Every job, quarantine scan, uniqueness domain, and retention sweep is simply
whatever `grind_jobs` and its sibling tables hold in one PostgreSQL schema —
there is no separate owner column scoping rows within one shared schema.
Which schema is **explicit configuration, not inferred**: `grind.new`
defaults the schema to `"public"`; `grind.with_schema(config,
"myschema")` overrides it. Every Grind storage call runs with `search_path`
set to exactly that one configured schema (quoted safely, set when the call
checks out its connection and restored before the connection returns to the
pool), so it is never left to whatever the connecting role or database
would otherwise default to, and the application's own queries on the shared
pool keep the application's `search_path`. Two
pools configured with the same schema (through any connection string that
reaches the same physical database) share the identical installation and
see each other's jobs; two pools configured with different schemas — in the
same physical database or not — are fully isolated from each other, because
Grind installs its own complete set of tables (`grind_jobs` and siblings)
independently into each schema. The uniqueness domain's own advisory lock
binds this same configured schema as an ordinary query parameter (never
`current_schema()` resolved server-side), so it agrees with the schema every
other query in that pool actually reads and writes by construction, not by
coincidence.

**Why explicit.** PostgreSQL's default search_path (`"$user", public`)
can make `current_schema()` identify a user's empty personal schema while
Grind tables are read from public. Deriving installation identity from that
value would give two roles different identities for the same tables.
Grind sets the configured schema explicitly; the
[isolation tests](../test/grind/database/isolation_test.gleam) exercise this case.

**Creating the schema.** `grind.migrate` creates the
configured schema (`CREATE SCHEMA IF NOT EXISTS`, safely quoted) if it does
not already exist — but only after confirming it is genuinely absent, never
unconditionally: `CREATE SCHEMA IF NOT EXISTS` itself demands database-level
`CREATE` privilege from the connecting role even when the schema already
exists, which the recommended least-privilege setup below deliberately does
not grant. `grind.start` and every other call never create a schema —
against a schema whose tables do not exist yet, they fail with an ordinary
typed storage error instead (missing-relation errors from the same query
that would otherwise have run).

**Recommended setup.** Have an administrator create the schema once, owned
by the role Grind connects as (`CREATE SCHEMA AUTHORIZATION myrole`), and
pass that same name to `with_schema` — this is the least-privilege shape:
the connecting role never needs database-level `CREATE`, only ownership of
its own schema, exactly like
`postgres_two_schemas_share_a_database_but_stay_isolated_test`
(`test/grind/database/isolation_test.gleam`) sets itself up. `with_schema`'s own default
(`"public"`) is fine for a single-installation deployment with no need to
share a database with another Grind installation. Two installations sharing
one physical database must pass genuinely distinct schema names to
`with_schema` — nothing enforces that they were meant to be distinct; see
[operations](OPERATIONS.md) for this remaining configuration requirement. **A connection pooler in front of
PostgreSQL must be configured to preserve `search_path`** — PgBouncer's
transaction pooling mode in particular can hand a physical server
connection to a client without applying that client's own startup
parameters, silently pointing an installation at the wrong schema with no
error at all; see [operations](OPERATIONS.md) for pooler requirements. On a pool built
from the application's `pog.Config`, Grind sets `search_path` per storage
call with a session-level `set_config` and restores it afterwards, so the
pooler must also keep one server session for the whole checkout (session
pooling).

## Observations

`grind/telemetry` exposes Grind's own [Sinal](https://github.com/gleam-dream/sinal)
event descriptors — Grind does not own a telemetry event sum type or a
subscription API; attach with plain `sinal.observe`/`sinal.attach` exactly as
you would to any other Sinal event:

```gleam
import grind/telemetry
import sinal

let _attachment =
  sinal.observe(telemetry.acknowledged(), fn(measurements, metadata) {
    // metadata.committed_state, metadata.proposed, metadata.confirmation, ...
    io.println("job " <> int.to_string(metadata.ref.job_id) <> " acknowledged")
  })
```

The events currently published, one per durable job-lifecycle transition:

- `[grind, job, admitted]` — a `grind.submit` with or without an id, or a
  uniqueness decision (`Inserted`, `Existing`,
  `Rescheduled`). `submission_id` is the job's id, or the one Grind generated. Public `grind.reconcile_submission` never emits (see below).
- `[grind, job, claimed]` — one row atomically claimed for execution.
- `[grind, job, quarantined]` — one abandoned attempt (an expired lease found
  by a claim-time scan, or the public `quarantine_expired` sweep) moved to
  `uncertain`. A consumer's own claim-time scan covers every worker
  id/version in the queue it polls, not only the ones it currently
  registers.
- `[grind, job, resolved]` — an audited operator decision committed against
  an `uncertain` job (`resolve_uncertain`).
- `[grind, job, cancellation_decided]` — a cancellation request that changed
  something durable (`CancellationDecidedBeforeRun` or `CancellationDecidedWhileRunning`); the
  read-only outcomes (`AlreadyCancelled`, `AlreadyUncertain`,
  `AlreadyFinished`) never emit. `CancellationDecidedWhileRunning` can be delivered
  again for an idempotent re-request against an already-executing job.
- `[grind, job, released]` — a claimed attempt refunded before its worker
  ever ran (the temporary worker child failed to start).
- `[grind, job, contract_mismatch_recorded]` — a claimed attempt parked in
  the terminal, nonclaimable `contract_mismatch` state because a registered
  worker's codec contract no longer matches what was persisted at admission;
  unlike `released` above, this job does not go back to `queued`.
- `[grind, job, acknowledged]` — one committed disposition for one claimed
  attempt (the original descriptor; see its own doc comment in
  `grind/telemetry` for the full detail this section summarizes below).

Every Grind observation is emitted through a `Database`'s own
`sinal/forwarder.Forwarder` (sized by `grind.with_observation_capacity`, default
1024, shared across lifecycle, pruning and diagnostic events — one `Forwarder` per
`Database`, not one per event kind), never through a plain `sinal.emit`, so a
slow or raising attached handler stalls only the forwarder process — never
the coordinator proving a commit or the worker that produced it. The
forwarder is nested under its own supervisor, added to `Database`'s root
supervisor as a `Temporary` child: a handler that itself exits or is killed
(not just raises) can take the forwarder process down, and a persistently
crashing handler can exhaust that nested supervisor's own restart budget —
but a `Temporary` child's termination is never restarted and never counts
against its parent's own budget, so this can never affect the PostgreSQL
pool. Once the nested supervisor's own restart budget is exhausted, the
forwarder is never restarted again for that `Database`'s lifetime — a
permanent degraded state, not a transient gap, recoverable only by
restarting the `Database` itself. In that degraded state, observations are
simply unavailable: `forwarder.emit` reports `ForwarderUnavailable`, which
Grind already discards, so jobs keep being admitted, claimed, and
acknowledged normally with no observations at all.

**Lifecycle delivery semantics:**

- **Best-effort.** An event can be delivered more than once (a `Reconciled`
  observation after an earlier `Replied` one for the exact same dedupe key,
  such as `command_id` on `acknowledged`) and can be lost entirely (forwarder
  capacity exceeded, reported via `[sinal, forwarder, dropped]`; the
  forwarder down between a crash and its next supervised restart; or a call
  that returns a commit-unknown outcome — `QueueAckUnknown`, `CommitUnknown`,
  `CancellationCommitUnknown`, `ResolutionCommitUnknown` — whose own reply
  was lost before this process could prove anything either way. That last
  case is not "delivered later": nothing was ever emitted for it, and
  because the recovery APIs below also never emit, a commit reached this way
  can go entirely unobserved even after a caller successfully reconciles it).
- **The durable truth is Grind's own tables and receipts, never an
  observation.** Do not build a system of record on an attached handler; read
  `grind.state`/`grind.outcome`/`admin.reconcile_acknowledgement`
  for anything that must not be lost or double-counted.
- **Handlers run in the forwarder process, not the coordinator or the
  worker.** `self()` inside a handler is the forwarder; process-dictionary
  context from the emitting call is not carried across the hop, and a
  handler that raises is isolated by native `:telemetry` (the forwarder keeps
  running) but a handler that itself exits or is killed can still take the
  forwarder down.
- **Emitted only once a commit is proven.** Never from inside a database
  transaction callback, never for a rolled-back, aborted, stale, or
  commit-unknown outcome. `confirmation` on the metadata is `Replied` (this
  call's own commit reply) or `Reconciled` (proven by reading a durable
  receipt back after a lost reply) — both are genuine proof of a commit,
  never "assumed". A committed value (such as `committed_state`) always comes
  from what was actually committed, never from a worker's proposal — a
  concurrent cancellation can override any proposal with a committed
  `Cancelled`. Retry-budget exhaustion is decided by the worker's own
  business retry policy before the acknowledgement ever runs (an exhausted
  retry is proposed as a business failure, not as a retry the acknowledgement
  later reinterprets); the acknowledgement's `attempt_count < max_attempts`
  check is a defensive consistency guard that rejects a stale ack outright
  rather than silently committing a different outcome.
- **A pure receipt-read recovery API never emits, period — not "to avoid
  double-reporting", but as its own real gap.** `reconcile_acknowledgement`
  and `grind.reconcile_submission` are offered for a caller to recover its own return
  value after a lost reply, independently of whatever call originally
  produced that commit. When the originating call (`acknowledge`/
  `submit`/`cancel`/`resolve_uncertain`) itself already emitted —
  because its own transaction reply came back normally — a later
  reconciliation call correctly does not re-emit that same commit. But when
  the originating call returned a commit-unknown outcome, it never emitted
  anything (see "best-effort" above), and the reconciliation call that later
  recovers the outcome does not emit either: that committed transition can
  end up with no observation at all, ever, even though the durable row and
  receipt are both fully correct. This is a real, accepted gap, not a
  double-reporting safeguard — read `grind.state`/`grind.outcome`/the
  reconciliation APIs themselves for anything that must account for a
  commit-unknown recovery.

The [observation tests](../test/grind/observations) exercise event metadata,
producer delivery and runtime lifecycle. [ADR-0009](adr/0009-separate-observations-from-qualification-evidence.md)
records the distinction between observations and qualification evidence.

## Operational diagnostics

`grind/telemetry` provides six typed Sinal descriptors under
`[grind, diagnostic, …]`. Attach with `sinal.observe`, as above. They share the
Database's bounded forwarder with lifecycle events, including renewal through
the reserved pool. Subscriber delay, overflow and unavailability do not control
job execution. Delivery is best-effort and ordered only within one producer.

| Descriptor              | What it reports                                                                                                                                                  |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `renewal`               | Renewed, skipped lock, unavailable live fence, storage failure, or completion-renewal budget exhaustion. Optional signed lease headroom uses the database clock. |
| `acknowledgement`       | Returned ACK transaction/reconciliation outcome: replied, reconciled, rolled back, unknown, rejected fence or command conflict.                                  |
| `acknowledgement_retry` | Scheduled ACK retry number, delay and time pending. The handler is not rerun.                                                                                    |
| `checkout`              | Actual pool wait, checkout candidates and full storage-call duration, labelled by operation and main/reserved pool.                                              |
| `claim_failed`          | Quarantine-scan or candidate-claim failure with a sanitized error kind.                                                                                          |
| `capacity`              | Local maximum, active, handler-running, ACK-pending and available slots, plus draining status.                                                                   |

Timing fields use microseconds; lease headroom and retry delay use milliseconds.
Renewal duration describes the batch call and is repeated for each attempt it
observed. Use checkout events to count storage calls. Capacity describes the
coordinator's local ledger, including pending ACKs, and is not global queue depth.
Metadata contains node, consumer, queue and attempt identities where applicable;
it excludes job payloads, error messages, SQL and connection settings.

A missing live fence does not establish expiry. A skipped lock does not identify
its owner. Completion-renewal budget exhaustion is a local limit, not proof of
quarantine. An unknown ACK does not prove commit or rollback. The existing
`telemetry.acknowledged()` event with `Reconciled` confirmation remains proof
of a matching durable receipt.

Checkout coverage includes queue quarantine scans, claims, batch renewal, ACK
transactions and their reconciliation reads. It excludes other public storage
operations, proposal validation before storage and contract-mismatch parking.
Nested calls on a checked-out connection do not emit another checkout event.
Wait measures actual checkout calls, including stale candidates; total duration
also includes admission, SQL and cleanup. Pool waiting can exceed the configured
storage deadline. A process killed before return may emit no completed sample.

See [the diagnostics contract and acceptance evidence](../docs/design/design-layer.pdf).

## Uniqueness

`grind/unique` gives `job.unique` a typed
policy: a full-input or selected key, a queue scope (`WithinQueue`/
`AcrossQueues`), an occupancy period, and an eligible-states group. Admission
runs inside one PostgreSQL transaction, serialized by a domain-wide advisory
lock, and returns a typed handle (`Inserted`), an existing conflict
(`Existing`), or a rescheduled conflict (`Rescheduled`). `job.with_id`
gives a plain admission — no uniqueness policy — the same retry safety by
reusing the identical admission receipt, request fingerprint, and
reconciliation machinery: a caller-supplied job id retry converges on
the original `Inserted` outcome instead of risking a duplicate row.
`grind.reconcile_submission` recovers a caller's own return value after a lost reply,
independently of whichever call produced the commit.

Key equality is exact (PostgreSQL `jsonb::text` SHA-256), not containment;
the uniqueness identity always includes the worker id and version, so
cross-worker uniqueness is not implemented; only `available_at` on a `scheduled`
row can be moved on conflict (`RescheduleScheduledTo`) — no other field
replacement. `while_retained()` means "until pruned", not "forever" — see
"Retention" below. See
[docs/adr/0003-separate-command-receipts-from-uniqueness.md](../docs/adr/0003-separate-command-receipts-from-uniqueness.md) for the full
contract and its failure modes. Cross-worker uniqueness, general field
replacement and unique bulk insertion remain unimplemented; the
different-key/same-job id receipt race remains unverified.

## Authority and failure boundaries

Admission never executes the handler. Every submission has a caller-supplied
or generated receipt key. Retain PendingSubmission after CommitUnknown and
reconcile the original request. Rebuilding job.new without its original key
creates another command. job.with_id is a submission key, separate from the
integer durable job ID.

Uniqueness selects equivalent retained jobs; it does not serialize their
external effects. Claims and acknowledgement writes require the current
attempt identity, epoch, owner and live database lease. A handler return is
not a committed queue outcome. Automatic ACK recovery retains its proposal
and does not invoke the handler again.

Cancellation is cooperative and cannot retract an effect. Current
cancellation-first acknowledgement changes even an explicit Uncertain response
to Cancelled and clears uncertainty evidence. Investigate application effect
records as well as the uncertain listing. The unresolved policy is recorded in
[ADR-0006](../docs/adr/0006-keep-cancellation-and-effect-uncertainty-distinct.md).

## Deadlines and capacity

Storage deadline D defaults to four seconds; migration step deadline defaults
to thirty seconds; uniqueness lock wait defaults to two seconds and must leave
at least one second before D. The adapter calculates an absolute deadline
before checkout and runs its callback at most once. The pinned pgo pool does
not hard-bound a queued checkout by D; its deadline timer arms after connection
transfer. A borrowed transaction inherits its caller's timeout. await within
checks after reads, so its final read can extend beyond the wait budget.

Lease L must be at least 4D. A separate renewer and reserved one-connection pool
per used queue protect renewal from ordinary main-pool saturation. Pending ACKs
retain local slots and renew for a finite completion window. The timing bound
assumes a progressing established connection and actor scheduling; outage,
reconnect and row contention still require fencing and quarantine. Local
concurrency does not impose a cluster-wide limit.

See the [design layer](../docs/design/design-layer.pdf),
[coverage](../docs/COVERAGE.md) and [operations guide](../docs/OPERATIONS.md) for the
complete model, retained feature scope, resource ownership and recovery limits.

## Migrations

`grind.migrate` applies `grind/internal/migrations.migrations()` —
Grind's own hand-maintained, forward-only list of versioned schema steps —
in ascending order, one PostgreSQL transaction per step. Each step's
transaction pins `READ COMMITTED` and opens by taking a
**transaction-scoped** advisory lock (`pg_advisory_xact_lock`, keyed by a
fixed Grind string plus the current schema, held until that step's own
transaction commits or rolls back — not a session-scoped lock spanning
multiple statements), so two concurrent `migrate` callers — in this process
or another, including one driven through cigogne (see below) — serialise
instead of racing the same step; it then re-reads the schema's current
version and skips the step if it is already applied, otherwise runs that
version's own statements (its own trailing `grind_schema_migrations` marker
`INSERT` included), re-reads the generation once more to confirm it now
reports exactly that step's version, and commits. A step whose statements
fail rolls back cleanly (`MigrationStepFailed(version, error)`) without
disturbing any earlier, already-committed step; re-running `migrate` picks
up where it left off. `MigrationCommitUnknown(version)` covers three
distinct "may or may not have committed" shapes — a checkout failure before
`BEGIN` ever ran (definitely not committed, but reported the same way since
retrying is equally safe either way), `BEGIN` itself failing or losing its
reply, and a failed `ROLLBACK` after a statement error (the deadline
force-closing the connection) — re-running `migrate` is always safe
regardless of which one occurred, since the next attempt's own re-read
tells it whether that step actually committed. `migrate` fails closed
rather than repairing a foreign, partial, tampered, or pre-baseline
(legacy, never-migrated) schema — `IncompatibleSchema` or
`UnsupportedSchemaVersion`, never a silent best-effort fix.

Every migration is also published under `priv/migrations/` as a plain `.sql`
file in [cigogne](https://hexdocs.pm/cigogne)'s own format
(`<14-digit UTC timestamp>-grind_v<N>.sql`, `--- migration:up` /
`--- migration:down` / `--- migration:end`), alongside a `priv/cigogne.toml`
— including, as each version's own first `up` statement, the identical
advisory-lock statement `migrate` itself runs, so an application applying
Grind's migrations directly through cigogne
(`cigogne.include_lib("grind", ..)`; see cigogne's own docs for the exact
call) serialises against a concurrent `grind.migrate` caller the same
way. Grind itself never reads these files at runtime — `migrations()` is
the only thing `grind.migrate` executes. **Pick one owner for Grind's
schema per database — either `grind.migrate` or cigogne, never both**;
mixing them against the same schema can fail on a duplicate-object error the
first time the second mechanism tries to (re-)apply a step the other one
already committed. **`grind_v11`'s own `down` section drops every Grind
table — all job, receipt, and resolution data with them** — cigogne's
rollback is a real, destructive operation here, not a reversible preview. A
no-database test (`grind_migrations_conformance_test`) reads
`priv/migrations/` through cigogne's own public parser (via
`config.get("grind")`, exercising the real `priv/cigogne.toml`) and proves
every file's statements, marker version, and filename version stay
byte-for-byte in lockstep with `migrations()`, and that every released file
(every one but the newest, which may still be under active development)
has a pinned sha256 that matches — so the two can never silently drift
apart. A separate, real-database test proves the two mechanisms actually
_interoperate_, not merely that the files match: cigogne itself applies
every file to a fresh schema, `grind.migrate` against the result is a
genuine no-op (`read_schema_generation` accepts it as a fully up-to-date
install, never `IncompatibleSchema`/`UnsupportedSchemaVersion`), the
cigogne-applied schema is fully functional for ordinary submit/claim/ack
traffic, cigogne's own down-then-up of `grind_v12` round-trips, and a
concurrent `grind.migrate` caller genuinely queues behind cigogne's own
held advisory lock and then no-ops once cigogne commits — see
the [migration concurrency tests](../test/grind/migrations/concurrency_test.gleam).

Driving cigogne from Gleam (rather than its CLI) needs no extra runtime
dependency — `cigogne` is already a dev-dependency (used by
`grind_migrations_conformance_test` above) — and can share a `Database`'s
own connection instead of opening a second pool against `DATABASE_URL`/
`PGHOST` separately:

```gleam
import cigogne
import cigogne/config

let assert Ok(base_config) = config.get("grind")
let cigogne_config =
  config.Config(
    ..base_config,
    database: config.ConnectionDbConfig(grind.connection(jobs)),
  )
let assert Ok(engine) = cigogne.create_engine(cigogne_config)
let assert Ok(Nil) = cigogne.apply_all(engine)
```

`config.get("grind")` reads the real, published `priv/cigogne.toml`
(`migrations.migration_folder`, `"migrations"`), so this differs from the
default config only in _how_ it connects — `grind.connection`'s own
`pog.Connection` is a pool handle just like the one every raw-SQL helper in
this codebase's own test suite already passes around, so cigogne and the
rest of the application genuinely share one pool.

Cigogne keeps its own migration-tracking table (`priv/cigogne.toml`'s
`[migration-table]` section — `schema`/`table`, defaulting to
`public`/`_migrations`), entirely independent bookkeeping from
`grind_schema_migrations`: cigogne's own `applied`/`unapplied` computation
never reads Grind's marker table, only its own. An application that also
uses `grind.with_schema` to put Grind's own tables in a non-`public`
schema must point cigogne's connection at that schema itself: every
unqualified `CREATE TABLE`/`ALTER TABLE` in `priv/migrations/*.sql`
resolves against the connection's own `search_path`, and the pool
`grind.connection` returns keeps the application's (Grind sets its schema
only for its own storage calls). Give cigogne a connection whose
`search_path` is that schema (a `pog.connection_parameter` on a separate
migration pool), or migrate with `grind.migrate`/`with_startup_migration`,
which always target the configured schema. Cigogne's own _tracking table_ location is a separate decision the
application must make explicitly (`priv/cigogne.toml`'s
`[migration-table] schema = "..."`, or the equivalent
`config.MigrationTableConfig` override): left at the default, every Grind
schema on one database would share the same `public._migrations` table,
which is fine for a single schema but ambiguous once more than one
`grind.with_schema` install shares a database — point `migration-table`
at the same schema `with_schema` names, or a schema dedicated to migration
bookkeeping, to keep it unambiguous.

Each step's transaction also sets a constant, transaction-local
`lock_timeout` (2000ms) right after acquiring its own advisory lock — a
DDL/DML statement that cannot acquire whatever lock it needs (typically an
`ALTER`/`CREATE INDEX` against a large, actively used table under
concurrent access) within that bound fails the step with
`grind.MigrationLockUnavailable(version)` instead of blocking for up to
the full `migration_deadline_ms`; safe to retry `migrate` once the
conflicting lock clears, exactly like `MigrationStepFailed`. Run
`grind.migrate` as an explicit deploy step, not at
application/node boot, once a table this large is in play, so a slow or
contended migration does not block every node's own startup. This
`lock_timeout` is set only inside `grind.migrate`'s own transaction, not
inside `priv/migrations/*.sql` itself — an application applying those files
directly through cigogne does not get it automatically and should set its
own `lock_timeout` first if it wants the same fast-fail behavior instead of
waiting out cigogne's own default.

`grind_v12` (`finished_at`, "Retention" below) is the first version whose own
statements do real, size-proportional work against `grind_jobs` rather than
only creating new, empty objects: its `ADD COLUMN ... DEFAULT now()` and its
backfill `UPDATE` each rewrite every existing row once, and its `ADD
CONSTRAINT` (`grind_jobs_finished_at_check`) validates every row again — all
under the one `ACCESS EXCLUSIVE` lock `ALTER TABLE` already takes for the
whole step, not merely while acquiring it. Measured at 2,000,000 rows, this
`finished_at` portion alone takes roughly 6 seconds — a **lower bound**, not
`grind_v12`'s full current cost: `grind_v12` was later edited in place,
before its own release, to also drop `storage_owner` from every table (see
`docs/OPERATIONS.md` #10 and `src/grind/internal/migrations.gleam`, "Dropping
`storage_owner`"), adding index/primary-key rebuilds and two full-table
collision scans that have not themselves been measured at this scale. That
time is bounded by `Settings.migration_deadline_ms` (default 30000ms) for
the _entire_ step,
not by the 2000ms `lock_timeout` above (`lock_timeout` only bounds _waiting_
to acquire a lock another session already holds; it does nothing once this
step's own `ALTER`/`UPDATE` has acquired its lock and is doing its own
work), and grows roughly linearly with `grind_jobs`'s row count — a
large-enough table can exceed the default and report
`MigrationCommitUnknown(12)` with nothing committed; re-running `migrate`
against the same table fails the exact same way until either
`migration_deadline_ms` is raised past the table's own measured cost, or the
file is applied directly via cigogne/`psql` as its own deploy step outside
Grind's deadline-bounded execution entirely. Run `grind.migrate` (or
apply this file) as an explicit deploy step, well before node boot, once
`grind_jobs` is this large.

`grind_v12` also requires a stop-the-world deploy, not a rolling one: it is
not safe for old and new application code to run against the schema on
either side of this migration. Old, pre-`finished_at` code acknowledging a
job to a terminal state writes no `finished_at` at all, which
`grind_jobs_finished_at_check` rejects with `23514` the moment `grind_v12`
has committed; new, `finished_at`-aware code (this release) writing that
same column against the still-`v11` schema hits an undefined-column error
the moment it runs _before_ `grind_v12` has committed. Migrate first, with
every writer stopped, then deploy the new code — never both versions
writing concurrently across the migration.

`grind_v12`'s own `ADD CONSTRAINT ... FOREIGN KEY` statements (adding
`ON DELETE CASCADE` from each receipt table to `grind_jobs`, "Retention"
below) validate every existing row in that receipt table by default, which
would fail the whole step with `23503 foreign_key_violation` on a database
that had already accumulated an orphaned receipt row under `v11` (no
foreign key was enforcing anything there yet) for any reason — an old bug,
a hand rollback, direct SQL. Each `ADD CONSTRAINT` is preceded by its own
`DELETE FROM <receipt table> WHERE NOT EXISTS (SELECT 1 FROM grind_jobs
...)`, removing any such orphan first rather than letting the migration
itself fail closed on one. The
[upgrade tests](../test/grind/migrations/upgrade_test.gleam) exercise a seeded orphan.

See [qualification](evidence/qualification.md) for the migration evidence
requirements, [ADR-0007](adr/0007-validate-forward-migrations-and-bound-recovery-retention.md)
for the upgrade and retention decisions, and [AGENTS.md](../AGENTS.md)
("Adding a migration") for the steps to add a version.

## Retention

A job is prunable once it has finished (reached one of the six terminal
states — `succeeded`, `business_failed`, `runtime_failed`,
`contract_mismatch`, `discarded`, `cancelled`) and stayed that way for at
least a configured age; `admin.prune_finished` deletes it, scoped to the
caller's own schema (never by queue — retention is a property of the
whole schema):

```gleam
admin.prune_finished(jobs, older_than: duration.minutes(1), limit: 1000)
// -> Ok(842)
```

Deleting a job's own row also removes its acknowledgement, uniqueness-
submission, and resolution receipts — via `grind_v12`'s own `ON DELETE
CASCADE` foreign keys on `job_id`, not a second delete `prune_finished`
issues itself, so only `jobs` is counted.

One call is one bounded batch, never an unbounded sweep: the count can be
fewer than `limit` but never more. A caller draining everything currently
prunable loops while the count equals `limit`.

The runtime's pruner, on by default, runs one `prune_finished` batch every
30 seconds with `limit` 10,000 and deletes jobs that finished more than
7 days ago (`grind.with_pruner(max_age:)` changes the age;
`grind.without_pruner` keeps everything). It never drains a backlog within
one tick, matching Oban's pruner; a batch of exactly `limit` rows is picked
up again on the next tick. Its first tick fires 30 seconds after it starts.
Unlike Oban's own plugin, there is **no leader election**: it is safe
to run a supervised pruner (or call `prune_finished` directly) on every
node in a cluster at once, since candidates are selected `FOR UPDATE SKIP
LOCKED` — a concurrent pruner (another node's, or a concurrent manual call)
simply skips whatever this one already holds, rather than either blocking
or double-deleting. Every `prune_finished` call — whether from a supervised
pruner's own tick or called directly — emits `[grind, prune, completed]` on
success (the count deleted) or `[grind, prune, failed]` on error (see
`grind/telemetry`), with a coarse classification of the underlying error
for the failed case, since a supervised pruner has no direct caller to
return a `PruneError` to.

There is no minimum retention floor beyond `older_than_ms`/`max_age_ms`
being positive — matching Oban's own `max_age`, which is likewise only
required to be positive. This is a real trade-off, not a free lunch:

- **A retention window shorter than a live lease risks turning a late,
  otherwise-recoverable commit-unknown acknowledgement retry into a stale
  one.** If a job's own row is pruned while an old, abandoned attempt's
  automatic ack retry is still in flight against it (see "Authority and failure boundaries"), that retry
  finds no row at all and reports `QueueAckStale(AckRecordMissing)` — the
  same shape an ordinary lost/reassigned row already produces, not a new
  failure mode, but one you can cause yourself by pruning too aggressively
  relative to the queue's lease (`queue.with_lease`).
- **`reconcile_acknowledgement` reports `ReceiptNotFound`** once a job's own
  acknowledgement receipt is pruned — read it before the retention window
  closes if you need to recover a return value after a lost reply.
- **A retry with `job.with_id` or `job.unique` using the same request identity
  after its original receipt is pruned is indistinguishable from a genuinely
  new request**: it inserts a fresh row with a new job id instead of
  returning the original one. The idempotency window job id gives
  you is exactly the retention window, not forever.
- **A pending `grind.reconcile_submission` call for a submission whose underlying job
  was pruned before its own `CommitUnknown` was ever resolved can never
  recover that decision** — the receipt it would have read back is gone.
- **An `AllRetained`/`while_retained()` uniqueness key reopens once its
  occupying row is pruned**, not "forever": `while_retained()` means "until
  pruned", never "permanently". A finite period (`unique.within_milliseconds`)
  already has its own expiry independent of pruning; choose a retention
  window at least as long as the period if you need the occupancy guarantee
  itself to hold for the period's full duration regardless of when pruning
  runs — see `docs/adr/0003-separate-command-receipts-from-uniqueness.md`.
- **`grind_jobs_finished_idx`/`grind_unique_submissions_job_idx`/
  `grind_job_resolutions_job_idx`** (`grind_v12`) back this: the candidate
  scan orders by `(finished_at, id)`, and each receipt delete
  seeks its own table by `job_id` rather than scanning it.

[ADR-0003](adr/0003-separate-command-receipts-from-uniqueness.md) records the
interaction with uniqueness policies. The
[deletion tests](../test/grind/retention/deletion_test.gleam) exercise the
admission/pruning race protected by the unique admission candidate lock.
