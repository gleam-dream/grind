# Risk register

A standing inventory of Grind's known correctness, durability, and operational
risks. Each entry is a real, currently-accepted trade-off or an open gap — not
a hypothetical. Entries come from `README.md` ("Guarantees and
non-guarantees"), `docs/RECOVERY-EVIDENCE.md` ("Limits" sections),
`docs/UNIQUENESS-CONTRACT.md` ("Failure modes" and "Out of scope"),
`docs/RELEASE-READINESS.md`, `oracle/ORACLE-LEDGER.md`, and code comments.

Status meanings: **accepted** (a deliberate trade-off, not expected to
change without a design decision), **open** (a real gap, no fix scheduled),
**mitigated** (a defense exists but does not close the gap completely).

## Contents

1. [Coupling to pog's private `Connection` shape](#1-coupling-to-pogs-private-connection-shape)
2. [pog's rollback crashes on a failed rollback](#2-pogs-rollback-crashes-on-a-failed-rollback)
3. [Network-fault coverage stops at a killed backend](#3-network-fault-coverage-stops-at-a-killed-backend)
4. [Coordinator renewal starvation under several pending acknowledgements](#4-coordinator-renewal-starvation-under-several-pending-acknowledgements)
5. [Lease-vs-deadline rule has zero margin and does not cover more than two siblings](#5-lease-vs-deadline-rule-has-zero-margin-and-does-not-cover-more-than-two-siblings)
6. [Default throughput ceiling is low and each claim costs two statements](#6-default-throughput-ceiling-is-low-and-each-claim-costs-two-statements)
7. [Storage owner is derived from the connection URL, not the database itself](#7-storage-owner-is-derived-from-the-connection-url-not-the-database-itself)
8. [External effects are not exactly-once](#8-external-effects-are-not-exactly-once)
9. [Retention ends reconciliation and replay guarantees](#9-retention-ends-reconciliation-and-replay-guarantees)
10. [Large-table migration cost and the `grind_v12` stop-the-world deploy](#10-large-table-migration-cost-and-the-grind_v12-stop-the-world-deploy)
11. [Mixing cigogne and `postgres.migrate` on one database](#11-mixing-cigogne-and-postgresmigrate-on-one-database)
12. [Atom growth from repeated validate/start calls](#12-atom-growth-from-repeated-validatestart-calls)
13. [Sinal forwarder delivery is best-effort](#13-sinal-forwarder-delivery-is-best-effort)
14. [Plain `submit`/`submit_at` are not retry-safe](#14-plain-submitsubmit_at-are-not-retry-safe)
15. [Untested different-key/same-`SubmissionId` race](#15-untested-different-keysame-submissionid-race)
16. [Migration-path gaps: no end-to-end cigogne test, no genuine lost-reply upgrade test](#16-migration-path-gaps-no-end-to-end-cigogne-test-no-genuine-lost-reply-upgrade-test)
17. [No load, multi-node, or soak evidence yet](#17-no-load-multi-node-or-soak-evidence-yet)
18. [No connection pooler exercised](#18-no-connection-pooler-exercised)

---

### 1. Coupling to pog's private `Connection` shape

**What can happen.** Grind's own bounded checkout
(`grind_postgres_ffi.erl`) calls `pgo:checkout/2`/`checkin`/`break` directly
and pattern-matches pog's private `Connection` representation
(`{pool, Name} | {single_connection, Conn}`) to reach the underlying `pgo`
pool. Neither shape is part of pog's public contract. A pog/pgo upgrade that
changes either one silently breaks Grind's deadline enforcement instead of
failing to compile.

**Likelihood / impact.** Low likelihood (both dependencies are pinned to
narrow ranges), high impact if it happens silently (deadlines stop applying
without any visible error).

**Current mitigation.** `gleam.toml` pins `pog` to `>= 4.1.0 and < 4.2.0` and
`pgo` to `>= 0.20.0 and < 0.21.0` — a routine minor/patch bump inside the
pinned range cannot change either shape without also bumping past the pin.
`pog_connection_pool_shape_test` independently asserts the exact shape at
test time.

**Evidence.** `test/grind_test.gleam`'s `pog_connection_pool_shape_test`
fails loudly the moment either shape changes.

**Status.** Accepted — a deliberate decision (no fork of pog) documented in
`docs/RELEASE-READINESS.md` ("2b. pog dependency") and `README.md`
("Guarantees").

---

### 2. pog's rollback crashes on a failed rollback

**What can happen.** pog's own transaction crash-cleanup path uses
`let assert` on its `ROLLBACK`. If a crash happens during `COMMIT` and the
subsequent rollback attempt itself fails (rather than just the original
statement), pog crashes the caller process instead of returning an error
Grind could catch and classify.

**Likelihood / impact.** Low likelihood (a rollback failing after an
already-failing commit is a narrow, unusual window), high impact where it
happens (the caller process crashes rather than getting a typed error, so
Grind's own conservative "unknown" classification never runs).

**Current mitigation.** None inside Grind — this is upstream pog behavior,
outside Grind's own transaction-callback code. Grind's coordinator is a
supervised process, so a crash here is contained by OTP supervision and
surfaces as worker/coordinator death, which existing recovery paths (lease
expiry, quarantine) already handle — but the specific `CommitUnknown`
classification for this exact case is bypassed by the crash.

**Evidence.** Untested — noted as a known, pre-existing upstream behavior in
`docs/RELEASE-READINESS.md` ("2b. pog dependency", "Known, pre-existing
behaviors unrelated to the above").

**Status.** Accepted (upstream behavior, not a Grind defect) — open in the
sense that no red/green test exercises this exact path.

---

### 3. Network-fault coverage stops at a killed backend

**What can happen.** All of Grind's fault-injection evidence for the
checkout deadline uses a real TCP fault proxy on `127.0.0.1` (dropped
`COMMIT`/`BEGIN` replies, a dropped request, a stalled renewal `UPDATE`) or a
direct `pg_terminate_backend`. Three distinct classes of real-world network
failure are not exercised by any of it:

- A genuine network partition, packet loss, or asymmetric latency (the proxy
  only does in-process byte manipulation on an otherwise-healthy loopback
  stream).
- A TLS connection (every test database connects with `sslmode=disable`; the
  proxy relays raw bytes with no TLS termination or passthrough).
- A connect-time hang: every injected fault assumes a connection is already
  established. `gen_tcp:connect` itself blocking against an unresponsive
  (not connection-refusing) host during the pool's initial connect has no
  deadline at all — Grind's checkout deadline only bounds acquiring an
  already-connected pooled connection.

**Likelihood / impact.** Moderate likelihood in a real deployment (TLS is
common in production Postgres, and true network partitions happen), high
impact if encountered (an unbounded hang instead of the documented
deadline-bounded behavior).

**Current mitigation.** None beyond the loopback-only proxy evidence for the
cases it does cover. `idle_in_transaction_session_timeout` (set to
`2 × statement_deadline_ms`) is an independent, connection-parameter-level
backstop for the case where a request never reaches PostgreSQL at all, but
it does not help the connect-time-hang case (no session exists yet) or a
TLS-specific failure mode.

**Evidence.** `test/grind_fault_proxy_test.gleam`, T1–T5, cover the
loopback/established-connection cases. `docs/RECOVERY-EVIDENCE.md`,
"Acknowledgement deadline", "Limits", names all three gaps explicitly as
untested.

**Status.** Open.

---

### 4. Coordinator renewal starvation under several pending acknowledgements

**What can happen.** One queue coordinator process serves every attempt
under a `maximum_concurrency > 1` consumer: claim, acknowledgement, and
lease-renewal SQL all run synchronously on that one process's message loop.
A stalled acknowledgement (up to `3 × statement_deadline_ms`) blocks every
other active attempt's own renewal tick for as long as it is in flight. With
more than one stalled acknowledgement queued at once, later renewals wait
out however many `D`-bounded stalls are ahead of them in the same loop —
this is not bounded by `queue.LeaseTooShortForDeadline` at all (see risk 5).

**Likelihood / impact.** Low-to-moderate likelihood (needs concurrent
stalled acknowledgements, which itself needs a database/network fault),
moderate-to-high impact (a lease can lapse and quarantine a still-live
attempt purely from coordinator contention, not from the attempt actually
failing).

**Current mitigation.** `queue.LeaseTooShortForDeadline` bounds the
single-stall case. The real fix — moving lease renewal and acknowledgement
off the shared coordinator loop and onto each attempt's own worker process —
is scoped but not implemented.

**Evidence.** Documented, not tested: `README.md` ("Guarantees"),
`docs/RECOVERY-EVIDENCE.md` ("Acknowledgement deadline", "Limits",
"Concurrent pending acknowledgements"), `docs/RELEASE-READINESS.md"`
("Decide on per-attempt storage calls").

**Status.** Open — tracked as a post-release design decision pending
load-test evidence.

---

### 5. Lease-vs-deadline rule has zero margin and does not cover more than two siblings

**What can happen.** `queue.LeaseTooShortForDeadline` requires a lease of at
least `6 × D` (`maximum_concurrency > 1`) or `1.5 × D` (`maximum_concurrency`
exactly 1), derived from a renewal timer firing every `L / 3` against a
stalled acknowledgement occupying the coordinator for up to `3 × D`. The
derivation has **zero algebraic margin at the minimum itself** — a lease set
exactly at the boundary has no slack for anything beyond the one stall the
formula assumes. The rule also assumes at most one stalled acknowledgement
ahead of one sibling's renewal: at `maximum_concurrency > 2`, more than one
sibling's renewal can queue up behind the same stall, and the rule does not
account for that (see risk 4).

**Likelihood / impact.** Low likelihood of hitting the exact boundary in
practice (most deployments set a lease comfortably above the minimum), high
impact if it is hit (a passing validation gives a false sense of safety at
`maximum_concurrency > 2`).

**Current mitigation.** The rule is enforced at `queue.start` (fails closed
before any process starts). Its own doc comment states the derivation and
the N>2 gap explicitly rather than silently understating it.

**Evidence.** `queue.LeaseTooShortForDeadline`'s own doc comment
(`src/grind/queue.gleam`); `README.md` ("Guarantees"); untested beyond the
single-stall case.

**Status.** Accepted for `maximum_concurrency <= 2`; open for `> 2` — same
underlying fix as risk 4.

---

### 6. Default throughput ceiling is low and each claim costs two statements

**What can happen.** Default automatic-polling throughput is bounded by
`maximum_jobs_per_poll / poll_interval`. With the shipped defaults
(`maximum_jobs_per_poll = 1`, `poll_interval = 250ms`), one consumer claims
at most about 4 jobs/second regardless of available worker concurrency —
raising `maximum_concurrency` alone does not raise throughput unless
`maximum_jobs_per_poll` and/or `poll_interval` are also tuned. Separately,
every claim attempt issues two statements against `grind_jobs`: a `LIMIT 1`
quarantine scan runs before every claim query, not only when a claim
actually finds an expired row.

**Likelihood / impact.** High likelihood of surprising a new deployment
(default settings look conservative until measured), low-to-moderate impact
(a tuning problem, not a correctness one — raising the two settings resolves
it).

**Current mitigation.** Both settings are exposed and validated
(`queue.with_poll_interval`, `queue.with_jobs_per_poll`); the ceiling and the
per-claim quarantine-scan cost are a direct, documented consequence of the
polling design, not a hidden inefficiency.

**Evidence.** Measured in a throwaway benchmark, not in the committed test
suite — see the bench findings captured during this documentation pass
(`/private/tmp/claude-501/-code-gleam-dream-grind/437b6ab5-1d50-4d7a-9ecf-bd7c556979f8/scratchpad/bench-plan.md`,
"Findings"). No committed load-test evidence yet (see risk 17).

**Status.** Accepted (a tuning trade-off inherent to poll-based claiming) —
the underlying throughput/latency measurement work itself is open, tracked
in `docs/RELEASE-READINESS.md` ("Evidence still missing", "Load").

---

### 7. Storage owner is derived from the connection URL, not the database itself

**What can happen.** `storage_owner` is computed as
`"<host>:<port>/<database>"` from the parsed connection URL
(`postgres.validate`), not from any property of the database itself. Two
consequences, in opposite directions:

- Two pools that reach the exact same physical database through **different**
  host:port endpoints (a different DNS name, a load balancer, a direct IP
  versus a hostname, or a connection pooler in front of Postgres) are treated
  as two different, mutually invisible storage owners — no shared quarantine,
  no shared uniqueness domain, no shared retention scope, even though they
  are writing the same rows.
- Two pools that reach the **same** host:port/database but a different
  PostgreSQL schema (`search_path`) are treated as the _same_ storage
  owner — `storage_owner` excludes the schema entirely, so schema-level
  isolation an application might expect is not provided.

**Likelihood / impact.** Low-to-moderate likelihood (most deployments use
one stable endpoint per database), high impact if it occurs (silent loss of
quarantine/uniqueness coordination between what the operator believes is one
logical database).

**Current mitigation.** None beyond documentation. The same-schema case is
called out in `docs/UNIQUENESS-CONTRACT.md` ("Failure modes"); the
different-endpoint case is not documented anywhere else prior to this
register.

**Evidence.** Confirmed by source inspection (`postgres.validate`,
`src/grind/postgres.gleam`); untested (no test exercises two differently
named endpoints against one physical database).

**Status.** Accepted as a known limitation of the current owner model.

---

### 8. External effects are not exactly-once

**What can happen.** Grind's database fencing (attempt IDs, epochs, lease
expiry) prevents two live claims from believing they own the same row and
prevents a stale acknowledgement from overwriting a newer one — but none of
that constrains what a worker already did to the outside world (charged a
card, sent an email, called an API) before or after that fencing decision.
An absent receipt never proves the effect did not happen: the worker can
have performed it and then the process, connection, or host died before the
outcome was durably recorded. The same is true for a _pruned_ receipt — an
old job's absent receipt can simply mean it aged out of retention, never
proof the effect did not happen.

**Likelihood / impact.** This is a structural property of at-least-once
delivery, not a bug — it will occur under real crash timing. Impact depends
entirely on the effect; unbounded for a non-idempotent external effect with
no application-level dedup.

**Current mitigation.** Application-level deduplication is the documented,
required mitigation — exercised in `consumer/test/grind_consumer_test.gleam`'s
dedup-key job, which looks up its own application-owned dedup record before
performing its effect rather than trusting Grind's attempt/delivery counts
alone.

**Evidence.** `README.md` ("Guarantees and non-guarantees"); the consumer
package's dedup-key test.

**Status.** Accepted — a structural limit of the delivery model, not
expected to change.

---

### 9. Retention ends reconciliation and replay guarantees

**What can happen.** `postgres.prune_finished` deletes a finished job's row
and, via `grind_v12`'s `ON DELETE CASCADE`, its acknowledgement,
uniqueness-submission, and resolution receipts together. Once pruned:

- A late, otherwise-recoverable `QueueAckUnknown` retry against that row
  reports `QueueAckStale(AckRecordMissing)` instead of recovering.
- `reconcile_acknowledgement` reports `ReceiptNotFound`.
- A `submit_with_id`/`submit_unique` retry of the same request identity
  inserts a brand-new row instead of returning the original one — the
  idempotency window is exactly the retention window, not forever.
- A pending `reconcile_unique` call for a submission whose `CommitUnknown`
  was never resolved can never recover that decision.
- An `AllRetained`/`while_retained()` uniqueness key reopens once its
  occupying row is pruned — `while_retained()` means "until pruned," never
  "permanently."

There is **no minimum retention floor** beyond `older_than_ms`/`max_age_ms`
being positive (matching Oban's own `max_age`) — an operator can configure a
retention window shorter than a live lease or shorter than a uniqueness
period, and nothing in Grind stops them.

**Likelihood / impact.** Low likelihood with reasonable defaults (60s
`max_age_ms`, well below typical lease durations), high impact if
misconfigured (silent loss of exactly the recovery/idempotency guarantees
the rest of the system provides).

**Current mitigation.** All five consequences above are explicitly
documented with the exact typed error each one produces. No enforced floor
exists; the mitigation is operator discipline (choose a retention window at
least as long as the longest lease and the longest uniqueness period in
use).

**Evidence.** `README.md` ("Retention"); `docs/UNIQUENESS-CONTRACT.md`.

**Status.** Accepted — matches Oban's own pruner contract by deliberate
choice.

---

### 10. Large-table migration cost and the `grind_v12` stop-the-world deploy

**What can happen.** `grind_v12` is the first migration whose statements do
real, size-proportional work: its `ADD COLUMN ... DEFAULT now()`, backfill
`UPDATE`, and `ADD CONSTRAINT` checks each touch every existing row under one
`ACCESS EXCLUSIVE` lock. Measured at 2,000,000 rows, `grind_v12` takes
roughly 6 seconds — a large-enough table can exceed
`migration_deadline_ms` (default 30000ms) and report `MigrationCommitUnknown(12)`
with nothing committed; re-running fails the same way until the deadline is
raised or the file is applied directly outside Grind's deadline-bounded path.
Separately, `grind_v12` requires a genuine stop-the-world deploy: old
pre-`finished_at` code writing a terminal state after `grind_v12` commits
hits `23514`; new `finished_at`-aware code writing against the still-`v11`
schema hits an undefined-column error. The two schema versions cannot
coexist with live writers on both sides.

**Likelihood / impact.** Low likelihood (affects only large, established
Grind deployments upgrading past v11), high impact where it applies (a
failed migration on a production-sized table, or a correctness break from a
rolling deploy across the migration).

**Current mitigation.** The cost and the stop-the-world requirement are both
explicitly measured and documented, with the exact commands to apply the
migration as its own deploy step outside normal node boot.

**Evidence.** `README.md` ("Migrations"); `docs/RECOVERY-EVIDENCE.md`,
Increment 25, for the measured cascade/backfill timing.

**Status.** Accepted — an inherent cost of the schema change, not a defect;
mitigated by documentation and by running the migration as its own deploy
step.

---

### 11. Mixing cigogne and `postgres.migrate` on one database

**What can happen.** Grind's schema can be applied either through
`postgres.migrate` or through cigogne against the published
`priv/migrations/*.sql` files — the two mechanisms are kept in lockstep by a
conformance test, but nothing prevents an operator from using both against
the same database. Doing so can fail with a duplicate-object error the first
time the second mechanism tries to (re-)apply a step the other one already
committed. Applying `priv/migrations/*.sql` directly through cigogne also
does not get Grind's own `lock_timeout` fast-fail behavior (that is set only
inside `postgres.migrate`'s own transaction) — an operator relying on
cigogne alone should set their own `lock_timeout` first.

**Likelihood / impact.** Low likelihood (requires an operational mistake:
picking two migration owners for one schema), moderate impact (a failed
migration step, recoverable by re-running the single chosen mechanism, not
data loss on its own — though see the next point).

**Current mitigation.** README explicitly states "pick one owner ... never
both." The shared advisory lock (identical statement in both paths)
serializes concurrent migrators from either mechanism against the same step,
so the failure mode is a clean duplicate-object error, not silent
corruption. Separately — not this specific risk, but a sharp edge in the
same area — `grind_v11`'s own cigogne `down` section drops every Grind
table (all job, receipt, and resolution data with it): a cigogne rollback
here is a real, destructive operation, not a reversible preview.

**Evidence.** `grind_migrations_conformance_test`
(`test/grind_test.gleam`) proves the two sources stay byte-for-byte in
lockstep; the dual-ownership failure mode itself is documented, not tested
(no test runs both mechanisms against one database simultaneously).

**Status.** Accepted — an operational discipline requirement, documented but
not mechanically enforced.

---

### 12. Atom growth from repeated validate/start calls

**What can happen.** Erlang atoms are never garbage-collected. Several
Grind code paths create at least one atom on each call: `postgres.validate`
creates the pool's `process.Name` once per distinct `ValidatedSettings`
value (though it is reused, not recreated, on repeated `start`/`close`
cycles against the _same_ validated settings — see `docs/RELEASE-READINESS.md`,
"2b"); `grind/observation`'s descriptor constructors build native atom
lists per call (`job_event_name`, `atom.create` for each event's name
components); each `queue.start`/`pruner.start` also creates process names for
its own supervision tree. A caller that repeatedly builds _fresh_
`ValidatedSettings` values (rather than reusing one) — or that calls a
descriptor-constructing function in a hot loop rather than once at setup —
grows the atom table without bound over a long-running node's lifetime,
eventually crashing the VM (the atom table has a fixed maximum size).

**Likelihood / impact.** Low likelihood under the documented usage pattern
(construct settings/descriptors once at application startup, reuse the
resulting value) — high impact if violated on a long-lived node (an
unrecoverable VM crash, not a graceful failure).

**Current mitigation.** Pool-name reuse across repeated `start` calls for
the same `ValidatedSettings` (fixed in the same change that dropped the pog
fork — see `docs/RELEASE-READINESS.md`, "2b", "Pool name lifetime").
Everything else relies on the documented calling convention (validate
once, register workers once, build event descriptors once) rather than a
mechanical guard.

**Evidence.** Not measured — `docs/RELEASE-READINESS.md` ("Evidence still
missing", "Soak") lists atom growth as untested over time.

**Status.** Open — no soak test exists to bound or disprove this.

---

### 13. Sinal forwarder delivery is best-effort

**What can happen.** Every `grind/observation` descriptor is delivered
through a `Database`'s own `sinal/forwarder.Forwarder`, which can drop an
event past its configured capacity (default 1024, shared across all
`[grind, job, *]` events on one `Database`) or lose it entirely if the
forwarder is down between a crash and its next supervised restart. A
persistently crashing or exiting handler can exhaust the forwarder's nested
supervisor's own restart budget, permanently degrading observations for that
`Database`'s remaining lifetime (recoverable only by restarting the
`Database` itself). Separately, a commit-unknown outcome whose reply was
lost is never observed at all, by either the originating call or a later
reconciliation call — a real, accepted gap, not a double-reporting
safeguard.

**Likelihood / impact.** Low likelihood of capacity overflow under normal
load (1024 is generous for typical handler latency), moderate impact if it
occurs (silent observability loss, never a correctness loss — durable state
is unaffected).

**Current mitigation.** `[sinal, forwarder, dropped]` reports overflow.
Admission, claiming, and acknowledgement all continue unaffected regardless
of forwarder health — this is a deliberate design property (the forwarder
can never affect the PostgreSQL pool), not an accident.

**Evidence.** `docs/RECOVERY-EVIDENCE.md` ("Acknowledged observation",
"Round 2 observations") proves overflow reporting, raising-handler
isolation, and forwarder-crash-loop pool survival by mutation.

**Status.** Accepted — observations are documented as best-effort,
never a system of record (`README.md`, "Observations").

---

### 14. Plain `submit`/`submit_at` are not retry-safe

**What can happen.** Neither `submit` nor `submit_at` carries a request
identity to deduplicate against. A `CommitUnknownWithoutId` reply does not
mean the row was never inserted — the connection can be lost after
PostgreSQL already committed the insert. Blindly retrying either call on
that error can insert a duplicate job.

**Likelihood / impact.** Low likelihood (needs a connection loss at the
exact moment between commit and reply), high impact if a caller retries
blindly (a duplicate job, with all the consequences of duplicate execution
for a non-idempotent worker).

**Current mitigation.** `submit_with_id` (a caller-supplied `SubmissionId`,
no uniqueness policy needed) and `submit_unique` (a uniqueness policy) both
give the same admission a retry-safe path by reusing the admission receipt,
fingerprint, and reconciliation machinery. The doc comment on
`submission.SubmitError` points callers needing retry safety at one of
these instead of plain `submit`/`submit_at`.

**Evidence.** `README.md` ("Guarantees"); `docs/UNIQUENESS-CONTRACT.md`
("Admission receipts").

**Status.** Accepted — a deliberate scope boundary (retry safety is
opt-in via `submit_with_id`/`submit_unique`), not a defect in the ID-less
path itself.

---

### 15. Untested different-key/same-`SubmissionId` race

**What can happen.** Two submitters can share the same caller-supplied
`SubmissionId` while using two _different_ uniqueness keys (or none). Since
the domain lock is keyed by the uniqueness key, not by `SubmissionId`, these
two submitters never contend the same advisory lock — they can race directly
on the `grind_unique_submissions` receipt table's own primary key
(`SubmissionId`), a `23505` conflict shape distinct from the ordinary
domain-lock-serialized path every other admission race in this codebase is
proven against.

**Likelihood / impact.** Low likelihood (requires an application bug: reusing
a `SubmissionId` across logically different requests), moderate impact if it
occurs (an unproven code path handling the concurrent insert — behavior is
plausible from reading the code but not verified by a forced-overlap test
the way every other admission race in this codebase is).

**Current mitigation.** None beyond the general `23505` handling already
present in the admission transaction for the ordinary same-key case; this
specific different-key shape has not been isolated and forced.

**Evidence.** Explicitly named as untested in
`docs/UNIQUENESS-CONTRACT.md` ("Failure modes", "Out of scope").

**Status.** Open.

---

### 16. Migration-path gaps: no end-to-end cigogne test, no genuine lost-reply upgrade test

**What can happen.** Two evidence gaps remain in migration testing:

- No test proves the full round trip of cigogne applying Grind's
  `priv/migrations/*.sql` files against a real database and then
  `postgres.migrate` being a genuine no-op against that same database (the
  conformance test proves the _files_ match `migrations()` byte-for-byte,
  not that cigogne's own apply mechanism interacting with Grind's own
  version-read agrees end to end).
- The upgrade harness has no test of a genuine lost reply during
  `reconcile_unique` specifically in the context of a schema upgrade (as
  opposed to the general lost-reply `reconcile_unique` evidence proven
  elsewhere against a stable schema).

**Likelihood / impact.** Low likelihood (both are narrow interaction
gaps between two already-separately-tested mechanisms), low-to-moderate
impact if a real gap exists (most likely surfaces as a confusing error
during an upgrade, not silent data loss, since both underlying mechanisms
are independently proven).

**Current mitigation.** None beyond the two mechanisms' own separate test
coverage (the conformance test for cigogne-file/`migrations()` lockstep; the
general `reconcile_unique` lost-reply tests against a stable schema).

**Evidence.** Named explicitly as open in `docs/RELEASE-READINESS.md`
("1. Contract decisions", "Migration gaps").

**Status.** Open.

---

### 17. No load, multi-node, or soak evidence yet

**What can happen.** Four categories of evidence that would matter for a
production deployment have not been produced:

- **Load**: throughput/latency at `maximum_concurrency > 1`, several
  consumers against one database, polling cost, and lock contention under
  realistic concurrency are not measured in the committed test suite (see
  risk 6 for the one informal benchmark taken during this documentation
  pass).
- **Multi-node**: two BEAM nodes claiming from the same queues, with one
  killed mid-job, is not exercised.
- **Soak**: atom growth (risk 12), timer accumulation, forwarder mailbox
  growth, and pool connection behavior over a long-running process are not
  measured over time.
- **TLS/pooler-fronted deployments**: see risks 3 and 18.

**Likelihood / impact.** Certain to matter eventually for any production
deployment beyond a single node/single consumer; impact is unknown precisely
because the evidence does not exist yet — that is the risk.

**Current mitigation.** None yet; this evidence is planned, not produced.

**Evidence.** `docs/RELEASE-READINESS.md` ("4. Evidence still missing")
lists all three categories explicitly as open.

**Status.** Open — planned.

---

### 18. No connection pooler exercised

**What can happen.** A connection pooler (PgBouncer or similar) placed
between Grind and PostgreSQL is not exercised by any test. Two of Grind's
connection-parameter-level defenses are startup parameters
(`default_transaction_isolation`, `idle_in_transaction_session_timeout`) that
some poolers can silently drop depending on pooling mode (transaction vs.
session pooling in particular). `default_transaction_isolation` already has
an in-transaction fallback pin as defense in depth against exactly this; no
equivalent in-transaction fallback exists for
`idle_in_transaction_session_timeout`, since by definition nothing is
running to set it once a session has already gone idle.

**Likelihood / impact.** Moderate likelihood (poolers are common in
production Postgres deployments), moderate impact if a pooler drops the
idle-session timeout (loses one specific, independent backstop — the
checkout deadline and other defenses still apply).

**Current mitigation.** `default_transaction_isolation`'s in-transaction
fallback pin. No equivalent exists for the idle-session timeout.

**Evidence.** Named explicitly in `docs/RECOVERY-EVIDENCE.md` ("Limits", "No
poolers").

**Status.** Open.
