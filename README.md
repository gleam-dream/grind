# Grind

Grind runs typed background jobs on PostgreSQL and Erlang/OTP: a
strongly-typed Oban. A worker declares its input, output and error types,
their JSON codecs and its queue; one supervised child runs the pool,
consumers and pruner; `grind.submit` admits a job; `grind.await` waits for
its typed outcome. Oban OSS `v2.24.1` is the behavioral reference; Grind
does not wrap or embed Oban. The package is not yet published to Hex.

## Quick start

```gleam
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleam/otp/static_supervisor as supervisor
import gleam/time/duration
import grind
import grind/job
import grind/worker
import pog

pub type Email {
  Email(to: String, subject: String)
}

pub fn mailer() -> worker.Worker(Email, String, Nil) {
  worker.new(
    "mailer.send",
    input: worker.codec(worker.infallible(encode_email), email_decoder()),
    output: worker.codec(worker.infallible(json.string), decode.string),
    perform: fn(email) { Ok("msg:" <> email.to) },
  )
  |> worker.with_queue("mailers")
}

/// One child in the application's supervision tree migrates the schema,
/// then runs the pool, one consumer per queue and the pruner.
pub fn children(pool: pog.Config, name: process.Name(grind.Message)) {
  let config =
    grind.new(pool)
    |> grind.with_worker(mailer())
    |> grind.with_startup_migration
  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(grind.supervised(config, name))
}

/// Anywhere in the application: a handle found by name.
pub fn send(name: process.Name(grind.Message), email: Email) {
  let jobs = grind.named(name)
  let assert Ok(admission) = grind.submit(jobs, job.new(mailer(), email))
  grind.await(jobs, grind.handle(admission), within: duration.seconds(5))
  // Ok(grind.Succeeded("msg:a@b.c"))
}
```

A runtime that runs consumers starts only on a current schema, so no
consumer polls before Grind's tables exist. `grind.with_startup_migration`
applies the missing migrations before the consumers start. Without it,
`start` fails with `SchemaNotMigrated(found:, required:)` (and `supervised`
fails its child's start) until the schema is migrated at deploy time:
`grind.migrate(jobs)` from a runtime started `without_consumers`, or
[`priv/migrations`](priv/migrations) through cigogne (see "Migrations").
Leave startup migration off when the runtime's role may not run DDL.
`test/grind/facade/readme_test.gleam` compiles and runs this program.

## Defaults

Every wait, payload, retry and snooze is bounded by default. An unbounded
value must be asked for explicitly.

| Operation                        | Default                                                        | Change it with                                        |
| -------------------------------- | -------------------------------------------------------------- | ----------------------------------------------------- |
| Handler execution                | 15 min, then the abandonment policy applies                    | `worker.with_timeout(worker.Infinity)` to lift        |
| Snoozes per job                  | 100, then `Failed(.., SnoozeLimitReached, ..)`                 | `worker.with_max_snoozes`                             |
| Business attempts                | 20                                                             | `worker.with_max_attempts`, `job.with_max_attempts`   |
| Retry delay                      | 15 s doubling to 1 day, plus 0–10% jitter                      | `worker.with_retry_policy`                            |
| Abandoned attempt                | held `uncertain` (`HoldUncertain`)                             | `worker.with_abandonment(ReplayAfterLeaseExpiry(n))`  |
| Job retention                    | pruner on: finished jobs deleted after 7 days                  | `grind.with_pruner(max_age:)`, `grind.without_pruner` |
| Encoded input, output, error     | 1 MiB, then `PayloadTooLarge`                                  | `grind.with_max_payload_bytes`                        |
| Initial connect at `start`       | 15 s, then `Unavailable`                                       | `grind.with_connect_timeout`                          |
| Storage call, including checkout | 4 s; a connection obtained later sends nothing                 | `grind.with_statement_deadline`                       |
| Uniqueness lock wait             | 2 s, then `UniquenessContended`                                | `grind.with_unique_lock_wait`                         |
| Migration step                   | 30 s                                                           | `grind.with_migration_deadline`                       |
| Waiting for a result             | `await(within:)` takes the bound                               |                                                       |
| `testing.drain`                  | `within:` and `limit:` take the bounds                         |                                                       |
| Queue concurrency, per node      | 10                                                             | `queue.with_concurrency`                              |
| Poll interval                    | 250 ms                                                         | `queue.with_poll_interval`                            |
| Attempt lease                    | 30 s, renewed every 10 s (at least 4 × the statement deadline) | `queue.with_lease`                                    |
| Shutdown grace                   | 15 s                                                           | `queue.with_shutdown_grace`                           |
| Observation forwarder            | 1,024 events in flight, then dropped and counted               | `grind.with_observation_capacity`                     |
| Worker and codec versions        | `"1"`                                                          | `worker.with_version`, `worker.with_codec_version`    |
| Queue                            | `"default"`                                                    | `worker.with_queue`, `job.with_queue`                 |

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
The [external consumer package](consumer/README.md) exercises the public API
from outside the package.

## Workers and codecs

A worker is a definition written in source code, so its constructors and
setters are total: an empty id, version or queue, or an out-of-range limit,
panics with the worker's id. `worker.new` takes a handler
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

**Why explicit, not inferred from `search_path`'s own default.** An earlier
design left `search_path` to whatever the connecting role or database
defaulted to and merely documented the recommended setup. That was a real,
fixed defect, not a hypothetical one: PostgreSQL's own default `search_path`
(`"$user", public`) makes `current_schema()` report the _first_ schema in
`search_path` that merely _exists_ — not the first one that actually holds
any Grind table — so a role with its own personal, empty `"$user"` schema
ahead of `public` (where Grind's real tables actually live) would silently
compute a different schema identity than another such role, even though
both operate on the exact same physical table; see
[docs/RISKS.md](docs/RISKS.md) risk 7 and
`postgres_user_schema_fallback_shares_one_installation_test`
(`test/grind/database/isolation_test.gleam`) for the concrete duplicate-admission hazard this
caused and the proof it is now closed.

**Creating the schema.** `grind.migrate` create the
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
[docs/RISKS.md](docs/RISKS.md) risk 7 for the residual configuration-
discipline hazard this leaves open. **A connection pooler in front of
PostgreSQL must be configured to preserve `search_path`** — PgBouncer's
transaction pooling mode in particular can hand a physical server
connection to a client without applying that client's own startup
parameters, silently pointing an installation at the wrong schema with no
error at all; see [docs/RISKS.md](docs/RISKS.md) risk 18. On a pool built
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

See [docs/RECOVERY-EVIDENCE.md](docs/RECOVERY-EVIDENCE.md), "Acknowledged
observation" and "Round 2 observations", for the full mutation-proven
evidence.

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

See [the diagnostics contract and acceptance evidence](docs/OPERATIONAL-DIAGNOSTICS.md).

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
cross-worker uniqueness is out of scope; only `available_at` on a `scheduled`
row can be moved on conflict (`RescheduleScheduledTo`) — no other field
replacement. `while_retained()` means "until pruned", not "forever" — see
"Retention" below. See
[docs/UNIQUENESS-CONTRACT.md](docs/UNIQUENESS-CONTRACT.md) for the full
contract, its failure modes, and everything still out of scope (cross-worker
uniqueness, general field replacement, unique bulk insertion, and the
untested different-key/same-job id receipt race).

## Guarantees and non-guarantees

Standing facts worth reading before depending on Grind for anything with a
real external effect:

- **An absent receipt does not prove the external effect did not happen.** A
  worker can perform its effect (charge a card, send an email, call an API)
  and the process, connection, or host can die before that outcome is ever
  durably recorded. Grind's tables and receipts prove what committed; they
  never prove the negative case. This extends to a pruned receipt exactly
  the same way: `admin.prune_finished` deletes a job's own
  acknowledgement, uniqueness-submission, and resolution receipts alongside
  it (see "Retention" below), so an absent receipt for an old job can also
  simply mean it aged out of the retention window — never proof the effect
  it recorded did not happen.
- **Reconciliation and job id replay only work while the job is
  retained.** `reconcile_acknowledgement`, `grind.reconcile_submission`, and a
  a submit with `job.with_id` or `job.unique` retry of the same request identity all
  depend on reading back a receipt row that `prune_finished` deletes once
  its own job is old enough — see "Retention" below for exactly what each
  one does once that row is gone.
- **Database ownership fencing does not make external effects exactly-once.**
  Attempt IDs, epochs, and lease expiry prevent two live claims from both
  believing they own a row, and prevent a stale claim's acknowledgement from
  overwriting a newer one — but none of that constrains what a worker did to
  the outside world before or after that fencing decision.
- **The crash window between an effect and its acknowledgement is
  unavoidable.** No amount of database fencing closes the window between a
  worker performing its effect and that outcome being durably committed. Only
  the caller's own idempotency key, or a two-phase external protocol, closes
  it.
- **Application-level deduplication is required for any effect that must not
  repeat**, and is exercised this way in the consumer tests (see
  `consumer/test/grind_consumer/recovery_test.gleam`'s dedup-key job): the worker looks
  up its own application-owned dedup record before performing its effect,
  never relying on Grind's attempt/delivery counts alone.
- **Storage calls use a Grind-owned checkout deadline.** Claims run in
  the coordinator, acknowledgements in their attempt processes, and lease
  renewal in a separate actor with a reserved one-connection pool per consumer.
  `grind.with_statement_deadline` (default 4 s, `D`, validated positive) is
  enforced by Grind's own `grind_postgres_ffi.erl`, which checks out a
  connection from pog's own pool itself (`pgo:checkout/2`, with `timeout`
  set to `infinity` and one absolute `deadline` shared across candidate
  checkouts). It retires unusable candidates before invoking the storage
  operation at most once against one checked-out connection. This applies
  to every Grind storage call: inline
  SQL, every Squirrel-generated call, and a whole `pog.transaction` including
  its own `BEGIN`/`COMMIT`. A real TCP fault-proxy test proves a dropped
  `COMMIT` reply, a dropped `BEGIN` reply, a request that never reaches
  PostgreSQL at all, and a stalled lease-renewal `UPDATE` all resolve within
  roughly this bound instead of hanging indefinitely
  (`docs/RECOVERY-EVIDENCE.md`, "Acknowledgement deadline"). Grind depends on
  vanilla `pog` from Hex, pinned to a tight range (`gleam.toml`) because this
  couples Grind directly to pog's private `Connection` shape, pgo's private
  connection record and checkout/return APIs, and its pool topology and cache
  layouts. The version ranges limit upgrade scope but cannot guarantee those
  contracts remain stable. `pog_connection_pool_shape_test`
  (`test/grind_test.gleam`) checks the pog pool tuple; startup/cache and
  reconnect regressions cover the additional private contracts. This
  deadline does not bound the pool's own _initial_
  connect (a `gen_tcp:connect` to an unresponsive, not
  connection-refusing, host has no deadline of its own either way), and a
  checkout that has to _queue_ behind other contended callers is bounded by
  pgo's own overload-shedding heuristic (`pgo_pool`'s CoDel-style queue
  target), not by `D` alone. A request that never reaches PostgreSQL (the
  request itself lost, not just its reply) can still leave a real server
  session idle in transaction holding row locks until
  `idle_in_transaction_session_timeout` (roughly `2 ×` the checkout
  deadline, set automatically for every session in the pool, including a
  caller's own `pog.transaction` against the same named pool) clears it — a
  separate, independent bound from the checkout deadline, covering the case
  the checkout deadline alone does not (the client-side socket was never
  actually stuck).
  The reserved renewal pool uses the same database and session configuration,
  but claims, admissions and acknowledgements cannot check out its connection.
  Renewal updates the consumer's live attempts in one statement and skips
  rows locked by acknowledgements. A slow ACK therefore cannot hold up a
  healthy sibling's renewal. Each consumer adds one PostgreSQL connection;
  include it when sizing the database's connection budget.
  a queue requires `L ≥ 4 × D`, independently of concurrency, for a
  renewal cadence of `L / 3`. The next timer is armed before the current
  database call, so query duration does not extend every interval. This is
  a bound for progressing storage calls, not a guarantee through arbitrary
  scheduler stalls, a database outage, or contention on the same job's row.
  Expired ownership still fails closed and requires audited recovery.
- **Automatic acknowledgements retain the completed proposal and concurrency
  slot through retry.** The attempt process retries both a known rollback
  (`QueueAckFailed`) and an ambiguous result (`QueueAckUnknown`) with the same
  command identity. It never invokes the handler again. The independent
  renewer continues for at most one lease duration after receiving the
  completion notice. After that, retries may reconcile a committed receipt,
  but cannot write through an expired fence. A persistently unresolved job
  eventually becomes `uncertain` when an expiry sweep reaches it.
- **Observations are best-effort, never a system of record.** See
  "Observations" above for the full delivery semantics; do not build
  anything that must not be lost or double-counted on an attached handler.
- **Plain `submit`/`submit_at` may have committed even when they return an
  error.** Neither has a request identity to deduplicate against, so a
  `CommitUnknownWithoutId` reply does not mean the row was never inserted — the
  connection can be lost after PostgreSQL already committed it. Do not
  blindly retry either one; use `job.with_id` (a caller-supplied
  job id, no uniqueness policy) or `job.unique` (a uniqueness
  policy) for a job that might need to be resubmitted safely.

## Deadlines

Three validated, positive settings on `grind.Config` bound how long a
storage call, a migration step, and a uniqueness lock wait may take:

- **`statement_deadline`** (`with_statement_deadline`, default 4000ms) bounds
  every storage call — inline SQL, a Squirrel-generated call, and a whole
  transaction including its own `BEGIN`/`COMMIT` — against a half-open or
  otherwise unresponsive connection. Enforced by Grind's own bounded checkout
  in `grind_postgres_ffi.erl`, not by pog/pgo's own unconfigurable default;
  see "Storage calls use a Grind-owned checkout deadline"
  under "Guarantees" above for the full mechanism and its limits (it does not
  bound the pool's initial connect, and a queued checkout is bounded by
  pgo's own overload shedding instead).
- **`migration_deadline`** (`with_migration_deadline`, default 30000ms)
  bounds one `migrate` step's whole transaction, separately from
  `statement_deadline` — see "Migrations" below for why a large table can
  still exceed it.
- **`unique_lock_wait`** (`with_unique_lock_wait`, default 2000ms) bounds
  every lock wait inside the uniqueness admission transaction; it must clear
  `statement_deadline` by at least 1000ms
  (`UniqueLockWaitTooCloseToDeadline` otherwise), so contention surfaces as
  `UniquenessContended` rather than a raw timeout.

`grind.start` rejects a lease shorter than `4 × statement_deadline` before
starting any process (`grind.LeaseTooShort`). This minimum is
independent of `maximum_concurrency`: renewal has its own actor, reserved
connection, and batch statement. The old coordinator timing rule and its T2
reproduction remain in [PERFORMANCE-EVIDENCE.md](docs/PERFORMANCE-EVIDENCE.md)
as historical evidence. Current implementation and validation progress are
tracked in [RELEASE-EXECUTION.md](docs/RELEASE-EXECUTION.md).

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
`docs/RECOVERY-EVIDENCE.md`, Increment 34.

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
`docs/RISKS.md` #10 and `src/grind/internal/migrations.gleam`, "Dropping
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
itself fail closed on one — see `docs/RECOVERY-EVIDENCE.md`, Increment 25,
for the red-first proof against a seeded orphan.

See `docs/RELEASE-READINESS.md` ("Migration mechanism") and
`docs/RECOVERY-EVIDENCE.md` for the mutation- and concurrency-proven
evidence, and `AGENTS.md` ("Adding a migration") for the steps to add a new
version.

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
  automatic ack retry is still in flight against it (see "Guarantees",
  "an acknowledgement that comes back `QueueAckUnknown`..."), that retry
  finds no row at all and reports `QueueAckStale(AckRecordMissing)` — the
  same shape an ordinary lost/reassigned row already produces, not a new
  failure mode, but one you can cause yourself by pruning too aggressively
  relative to the queue's lease (`queue.with_lease`).
- **`reconcile_acknowledgement` reports `ReceiptNotFound`** once a job's own
  acknowledgement receipt is pruned — read it before the retention window
  closes if you need to recover a return value after a lost reply.
- **A a submit with `job.with_id` or `job.unique` retry of the same request identity
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
  runs — see `docs/UNIQUENESS-CONTRACT.md`.
- **`grind_jobs_finished_idx`/`grind_unique_submissions_job_idx`/
  `grind_job_resolutions_job_idx`** (`grind_v12`) back this: the candidate
  scan orders by `(finished_at, id)`, and each receipt delete
  seeks its own table by `job_id` rather than scanning it.

See `docs/UNIQUENESS-CONTRACT.md` for the full interaction with uniqueness
policies, and `docs/RECOVERY-EVIDENCE.md` ("Increment 23") for the
mutation-proven admission-race fix `prune_finished` required
(`grind/internal/unique_admission`'s candidate lock).

## Development and integration checks

See the [module map](docs/MODULE-MAP.md) for implementation and test locations,
and the [recovery evidence index](docs/RECOVERY-EVIDENCE.md#topic-index) for
fault and recovery evidence.

Run all checks in a fresh local PostgreSQL cluster with separate databases for
Grind, the pinned Oban harness, and the public-import consumer:

```sh
nix develop --command bash scripts/test-postgres.sh
```

The script removes its disposable cluster on exit and refuses to use an
occupied test port. Plain `gleam test` runs pure tests and skips database tests
when their explicit test URL is absent; the script requires database markers so
those skips cannot count as integration passes.

**CI** (`.github/workflows/ci.yml`) runs this exact script, through the
identical `nix develop` shell `flake.nix` defines, in a `postgres-gate` job —
not a hand-rolled reconstruction of the toolchain, and not merely a plain
`gleam test` with no database configured (which would pass regardless of
whether any PostgreSQL-backed behavior actually works, since every such test
short-circuits to a no-op with no test URL set — exactly what a separate,
faster `quick-check` job's own plain `gleam test` step does _not_ prove on
its own). `postgres-gate` also runs [Sinal](https://github.com/gleam-dream/sinal)'s
own test suite and `nix flake check`. Sinal is a local path dependency
(`../sinal`, not a Hex package), so CI checks it out as a sibling directory
at a pinned commit (`SINAL_REF` in the workflow file) — bump that
deliberately when Sinal changes.

The pinned oracle source, commit, licenses, normalized observations, deliberate
differences, and per-behavior evidence categories are recorded in
[oracle/ORACLE-LEDGER.md](oracle/ORACLE-LEDGER.md). The separate
[consumer package](consumer/README.md) imports only public Grind modules.

The PostgreSQL gate also checks the oracle ledger and compares a shared catalog
of paired Grind/Oban scenarios. It retains results and source/catalog hashes under
`oracle/results/`. Independent-node fault scenarios and the long mixed soak live
in the [resilience harness](resilience/README.md); load and durable-completion
measurements live in the [benchmark harness](bench/README.md). Current acceptance
status is recorded in [release readiness](docs/RELEASE-READINESS.md).
Generated results are ignored working files: review them, record the checked
summary, then remove them. Benchmark methods and historical measurements live
in the benchmark README; raw run archives are not part of the repository.
