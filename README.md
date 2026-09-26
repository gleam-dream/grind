# Grind

Grind is a typed background-job library for Gleam on PostgreSQL and Erlang/OTP.
Oban OSS `v2.24.1` is used as a behavioral reference; Grind does not wrap or
embed Oban. The implementation is experimental and has not been published to
Hex.

The current runnable slice includes typed, versioned worker definitions,
heterogeneous registration, PostgreSQL admission and typed result reads,
absolute one-time scheduling, and a supervised queue consumer with bounded
per-consumer concurrency. Its validated policy separates local worker capacity
from the maximum jobs claimed per poll. Attempts use database-time leases,
fenced acknowledgement receipts, and conservative uncertainty recovery.
Business failures support a persisted attempt limit, deterministic default or
definition-bound retry policy, and typed terminal causes; queue handlers can
also snooze with a checked delay. Explicit discard, worker uncertainty, and
cooperative cancellation are implemented. Uniqueness admission
(`submit_unique`/`reconcile_unique`) checks a typed full-input or selected
key against a policy's queue scope, occupancy period, and eligible states
inside one locked transaction, returning a typed handle, an existing
conflict, or a rescheduled conflict; this milestone is complete, including
concurrent admission under a forced barrier, lock contention, period-boundary
timing, live rescheduling, uncertain-commit reconciliation, selected keys, and
public-API consumer coverage — see
[docs/UNIQUENESS-CONTRACT.md](docs/UNIQUENESS-CONTRACT.md) for the full
contract and its remaining, explicitly listed gaps (cross-worker uniqueness,
general field replacement, unique bulk insertion, and a few other named
items). `submit_with_id` gives a plain admission the same retry safety
without a uniqueness policy — a caller-supplied `SubmissionId` reuses the
identical admission receipt, request fingerprint, and reconciliation
machinery, converging a same-id retry on the original `Inserted` outcome
instead of risking a duplicate row; see "Admission receipts" in
[docs/UNIQUENESS-CONTRACT.md](docs/UNIQUENESS-CONTRACT.md). A consumer's own
per-poll quarantine scan covers every worker id/version in the queue it
polls, not only the ones it currently registers, so an executing row a
retired worker version left behind is still quarantined once its lease
expires; a separate public `postgres.quarantine_expired(database, limit:)`
sweeps expired executing rows across every queue in a storage owner, for a
queue no consumer polls at all. The experimental
v11 schema installs only into an empty schema; earlier experimental markers
(including v10) and partial Grind schemas fail closed without repair.
Acknowledgement receipts retain committed attribution and a proposal fingerprint,
not typed historical proposals. Typed outcome reads return the job's current
result.
See [implementation scope](docs/IMPLEMENTATION-SCOPE.md) for the delivered
boundary and complete retained backlog.

## Observations

`grind/observation` exposes Grind's own [Sinal](https://github.com/gleam-dream/sinal)
event descriptors — Grind does not own a telemetry event sum type or a
subscription API; attach with plain `sinal.observe`/`sinal.attach` exactly as
you would to any other Sinal event:

```gleam
import grind/observation
import sinal

let assert Ok(id) = sinal.handler_id("my-app-acknowledged-log")
let assert Ok(_attachment) =
  sinal.observe(id, observation.acknowledged(), fn(measurements, metadata) {
    // metadata.committed_state, metadata.proposed, metadata.confirmation, ...
    io.println("job " <> int.to_string(metadata.ref.job_id) <> " acknowledged")
  })
```

The events currently published, one per durable job-lifecycle transition:

- `[grind, job, admitted]` — a plain `submit`/`submit_at`, a `submit_with_id`
  commit, or a `submit_unique` decision (`Inserted`, `Existing`,
  `Rescheduled`). `submission_id` is `Some` for both the `submit_with_id` and
  `submit_unique` cases. Public `reconcile_unique` never emits (see below).
- `[grind, job, claimed]` — one row atomically claimed for execution.
- `[grind, job, quarantined]` — one abandoned attempt (an expired lease found
  by a claim-time scan, or the public `quarantine_expired` sweep) moved to
  `uncertain`. A consumer's own claim-time scan covers every worker
  id/version in the queue it polls, not only the ones it currently
  registers.
- `[grind, job, resolved]` — an audited operator decision committed against
  an `uncertain` job (`resolve_uncertain`).
- `[grind, job, cancellation]` — a cancellation request that changed
  something durable (`CancelledBeforeRun` or `CancellationRequested`); the
  read-only outcomes (`AlreadyCancelled`, `AlreadyUncertain`,
  `AlreadyFinished`) never emit. `CancellationRequested` can be delivered
  again for an idempotent re-request against an already-executing job.
- `[grind, job, released]` — a claimed attempt refunded before its worker
  ever ran (the temporary worker child failed to start).
- `[grind, job, contract_mismatch]` — a claimed attempt parked in the
  terminal, nonclaimable `contract_mismatch` state because a registered
  worker's codec contract no longer matches what was persisted at admission;
  unlike `released` above, this job does not go back to `queued`.
- `[grind, job, acknowledged]` — one committed disposition for one claimed
  attempt (the original descriptor; see its own doc comment in
  `grind/observation` for the full detail this section summarizes below).

Every Grind observation is emitted through a `Database`'s own
`sinal/forwarder.Forwarder` (sized by `postgres.observation_capacity`, default
1024, shared across every `[grind, job, *]` event above — one `Forwarder` per
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

**Delivery semantics — read before depending on this for anything but
diagnostics:**

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
  `postgres.state`/`postgres.outcome`/`postgres.reconcile_acknowledgement`
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
  and `reconcile_unique` are offered for a caller to recover its own return
  value after a lost reply, independently of whatever call originally
  produced that commit. When the originating call (`acknowledge`/
  `submit_unique`/`cancel`/`resolve_uncertain`) itself already emitted —
  because its own transaction reply came back normally — a later
  reconciliation call correctly does not re-emit that same commit. But when
  the originating call returned a commit-unknown outcome, it never emitted
  anything (see "best-effort" above), and the reconciliation call that later
  recovers the outcome does not emit either: that committed transition can
  end up with no observation at all, ever, even though the durable row and
  receipt are both fully correct. This is a real, accepted gap, not a
  double-reporting safeguard — read `postgres.state`/`postgres.outcome`/the
  reconciliation APIs themselves for anything that must account for a
  commit-unknown recovery.

See [docs/RECOVERY-EVIDENCE.md](docs/RECOVERY-EVIDENCE.md), "Acknowledged
observation" and "Round 2 observations", for the full mutation-proven
evidence.

## Guarantees and non-guarantees

Standing facts worth reading before depending on Grind for anything with a
real external effect:

- **An absent receipt does not prove the external effect did not happen.** A
  worker can perform its effect (charge a card, send an email, call an API)
  and the process, connection, or host can die before that outcome is ever
  durably recorded. Grind's tables and receipts prove what committed; they
  never prove the negative case.
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
  `consumer/test/grind_consumer_test.gleam`'s dedup-key job): the worker looks
  up its own application-owned dedup record before performing its effect,
  never relying on Grind's attempt/delivery counts alone.
- **The coordinator runs claim and acknowledgement SQL synchronously, bounded
  by a Grind-owned checkout deadline — for a connection already in hand.**
  `postgres.statement_deadline` (default 4000ms, `D`, validated positive)
  bounds every Grind storage call _once a pooled connection is checked out_ —
  inline SQL, every Squirrel-generated call, and a whole transaction
  including its own `BEGIN`/`COMMIT` — against a stalled or half-open
  connection: a real TCP fault-proxy test proves a dropped `COMMIT` reply, a
  dropped `BEGIN` reply, a request that never reaches PostgreSQL at all, and
  a stalled lease-renewal `UPDATE` all resolve within roughly this bound
  instead of hanging indefinitely (`docs/RECOVERY-EVIDENCE.md`,
  "Acknowledgement deadline"). It does not bound the pool's own _initial_
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
  actually stuck). While any of this is in flight, it still blocks that
  coordinator's single message loop until it returns or errors — including
  every other active attempt's own lease-renewal tick under the same
  `maximum_concurrency > 1` consumer, since one coordinator process serves
  all of them.
  `queue.start`/`queue.start_with_policy` reject a lease that does not
  clear a multiple of this deadline before starting any process
  (`queue.LeaseTooShortForDeadline`: `6 × D` at `maximum_concurrency > 1`,
  `1.5 × D` at `maximum_concurrency` of exactly 1 — both derived with zero
  algebraic margin at the minimum itself, from a renewal timer firing every
  `L / 3` and a stalled pending acknowledgement occupying the coordinator
  for up to `3 × D`; see `queue.LeaseTooShortForDeadline`'s own doc comment
  for the full derivation). Even a lease that clears this rule assumes at
  most one stalled acknowledgement ahead of one sibling's renewal: at
  `maximum_concurrency > 2`, more than one sibling's renewal can queue up
  behind the same stall, each also waiting out however many other siblings'
  own `D`-bounded renewals are ahead of it in that same queue — not fully
  covered by this rule, since the real fix (moving renewals off the
  coordinator's own loop) is not yet done
  (`docs/RELEASE-READINESS.md`, "Decide on per-attempt storage calls").
- **In automatic mode, an acknowledgement that comes back `QueueAckUnknown`
  keeps holding its concurrency slot while it retries.** The coordinator
  retries the exact same acknowledgement on its own renewal timer, renewing
  the lease first for roughly one lease duration's worth of ticks, until a
  known outcome commits it or that bound is spent — a persistently failing
  commit then lets the lease lapse and ends up `uncertain`, the same
  recovery path an unattended crash already relies on. Either way the slot
  stays occupied (and counted against `maximum_concurrency`) until it
  resolves; a manual `process_one` caller is unaffected and still gets
  `QueueAckUnknown` back synchronously, as always.
- **Observations are best-effort, never a system of record.** See
  "Observations" above for the full delivery semantics; do not build
  anything that must not be lost or double-counted on an attached handler.
- **Plain `submit`/`submit_at` may have committed even when they return an
  error.** Neither has a request identity to deduplicate against, so a
  `SubmitQueryFailed` reply does not mean the row was never inserted — the
  connection can be lost after PostgreSQL already committed it. Do not
  blindly retry either one; use `submit_with_id` (a caller-supplied
  `SubmissionId`, no uniqueness policy) or `submit_unique` (a uniqueness
  policy) for a job that might need to be resubmitted safely.

## Migrations

`postgres.migrate` applies `grind/internal/migrations.migrations()` —
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
call) serialises against a concurrent `postgres.migrate` caller the same
way. Grind itself never reads these files at runtime — `migrations()` is
the only thing `postgres.migrate` executes. **Pick one owner for Grind's
schema per database — either `postgres.migrate` or cigogne, never both**;
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
apart.

See `docs/RELEASE-READINESS.md` ("Migration mechanism") and
`docs/RECOVERY-EVIDENCE.md` for the mutation- and concurrency-proven
evidence, and `AGENTS.md` ("Adding a migration") for the steps to add a new
version.

## Development and integration checks

Run all checks in a fresh local PostgreSQL cluster with separate databases for
Grind, the pinned Oban harness, and the public-import consumer:

```sh
nix develop --command bash scripts/test-postgres.sh
```

The script removes its disposable cluster on exit and refuses to use an
occupied test port. Plain `gleam test` runs pure tests and skips database tests
when their explicit test URL is absent; the script requires database markers so
those skips cannot count as integration passes.

The pinned oracle source, commit, licenses, normalized observations, deliberate
differences, and per-behavior evidence categories are recorded in
[oracle/ORACLE-LEDGER.md](oracle/ORACLE-LEDGER.md). The separate
[consumer package](consumer/README.md) imports only public Grind modules.
