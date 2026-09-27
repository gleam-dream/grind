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
6. [Each claim costs two statements, one per free slot per round](#6-each-claim-costs-two-statements-one-per-free-slot-per-round)
7. [`search_path` must point at the intended schema](#7-search_path-must-point-at-the-intended-schema)
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
19. [Single coordinator process caps per-consumer throughput at high concurrency](#19-single-coordinator-process-caps-per-consumer-throughput-at-high-concurrency)

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

**Status update (2026-09-27): confirmed real by reproduction, severity
raised to high.** This was previously an open-but-theoretical risk, derived
algebraically from the coordinator's own serialization but not reproduced
under load. An independent reviewer's benchmark reproduction
(`docs/PERFORMANCE-EVIDENCE.md`, "L6T2") found the effect directly: at
`C=10`, `L=6D`, `D=2000ms`, `K=5` concurrent slow (but committing)
acknowledgements quarantined all 5 non-stalled siblings; `K=6` quarantined
all 4. At real defaults (`D=4000ms`, `L=30000ms`), the reviewer found
smallest failing setups of: slow acks around 3.2s each that still commit,
`K≈7`; acks timing out at the `D` bound, `K=5` borderline and `K=6` failing
(retries make it worse from `K≥3`); and a slow disk adding ~2s to every
commit at `C=10`, which fails with **no** slow acks (`K`) needed at all. The
general limit this reproduction derives is `2L/3 ≥ C · D_eff`, i.e.
`L ≥ 1.5 · C · D` — at `C=10`, `D=4s`, this requires `L ≥ 60s`. **The
shipped validation rule, `L ≥ 6D`
(`queue.LeaseTooShortForDeadline`/`minimum_lease_for_deadline`,
`src/grind/queue.gleam` ~231-272), only covers `C ≤ 2`; it does not hold for
`C > 2`** — see risk 5, which already named this gap algebraically before
this reproduction confirmed it empirically.

**The outcome is silent.** A job whose handler already succeeded, but whose
sibling's renewal or acknowledgement was starved behind the coordinator's
own serialized queue, ends up `uncertain` — indistinguishable, from the
outside, from a job that genuinely stalled or crashed. Resolving it needs an
audited operator decision (`resolve_uncertain`), and replaying it (`AuthorizeReplay`)
carries the ordinary duplicate-effect risk any replay does (risk 8).

**Two related but distinct starvation modes**, both reaching the same
outcome: **renewal starvation** (the sibling is still executing when its own
renewal tick queues up behind the stall) and **ack starvation** (the sibling
has already finished; its own `AttemptReturned` acknowledgement is queued
behind the same slow acks that starved it, and its `Renew` messages sit in
the identical coordinator mailbox). The committed benchmark evidence's own
`K=8` result (`docs/PERFORMANCE-EVIDENCE.md`, "L6T2") is the second mode,
ack starvation — not the pending-ack-retry chain (`3D + (N-1)*D`) risk 5's
derivation describes, which is a distinct mechanism (an ack that itself
times out and enters the coordinator's own bounded retry loop) that this
benchmark run never exercised.

**Remedy: owner decision pending, not chosen in this pass.** Three options
are on the table, none implemented:

1. **A per-consumer renewer process** that also renews finished-but-unacked
   and pending-ack attempts, with acknowledgements staying serialized on the
   coordinator as today. This is the reviewer's minimum recommended
   pre-release fix, and was also the owner's earlier pre-chosen first remedy
   for this risk. Narrower in scope than option 2 below: it addresses
   renewal/ack starvation specifically without also addressing risk 19's
   throughput ceiling.
2. **Move the acknowledgement into the attempt process itself** — the
   already-deferred T3 follow-up ("ack from the attempt process",
   `docs/PERFORMANCE-EVIDENCE.md`, "T3 verdict"; risk 19) and the fuller
   fix. This addresses both this risk and risk 19's throughput ceiling at
   once, at the cost of a larger design change touching the proven recovery
   code in `grind/internal/attempt`.
3. **Document the limit and tighten validation** to the safe envelope
   `L ≥ 1.5 · C · D` derived above, without changing the coordinator's own
   architecture.

**The reviewer recommends against releasing with documentation alone
(option 3 by itself)** — recorded here as the reviewer's own position, not
a decision made. Which remedy ships, and when, is an open owner decision;
see `docs/RELEASE-READINESS.md` for the tracked item. See also risk 19 (the
T3 coordinator throughput ceiling), which shares the identical root cause
(one serialized coordinator message loop) and whose "ack from the attempt
process" remedy is the same as option 2 above.

**What can happen (original description).** One queue coordinator process serves every attempt
under a `maximum_concurrency > 1` consumer: claim, acknowledgement, and
lease-renewal SQL all run synchronously on that one process's message loop.
A stalled acknowledgement (up to `3 × statement_deadline_ms`) blocks every
other active attempt's own renewal tick for as long as it is in flight. With
more than one stalled acknowledgement queued at once, later renewals wait
out however many `D`-bounded stalls are ahead of them in the same loop —
this is not bounded by `queue.LeaseTooShortForDeadline` at all (see risk 5).
**A slow or stalled claim is the identical kind of stall, not a separate
concern**: `attempt.claim_one` also runs synchronously on the same message
loop, so a claim that takes up to `D` to fail (or to hang against an
unresponsive connection, up to the same bound) blocks every other active
attempt's renewal for exactly as long, the same as a stalled acknowledgement
does. Before the `FillSlots`-message fix (`fill_automatic_slots`/
`request_fill`, `grind/queue`), automatic polling could compound this: a
single `Poll` recursed directly through up to `maximum_concurrency` claims
inside one message handler before ever checking the mailbox again, so a
burst filling every free slot with slow claims could stall a pending
renewal (or a `BeginShutdown`) for up to `maximum_concurrency × D` instead of
one claim's worth. The fix bounds that to one claim between mailbox
checks — `FillSlots` is sent as a message, not called directly, so a
`Renew`/`AttemptReturned`/`BeginShutdown` already queued is handled before
the next claim starts — which is exactly the "one stall" case risk 5's own
derivation already assumes, but does not eliminate the underlying
one-claim-blocks-the-loop property this risk describes.

**Likelihood / impact — severity raised to high (2026-09-27).** Previously
rated low-to-moderate likelihood as a needs-a-fault scenario; the reviewer's
reproduction (above) shows it needs no fault at all, only ordinary
committing acknowledgements that are merely slow (a full disk, a busy
database, a loaded network) at a concurrency and lease/deadline ratio well
within realistic deployment range (`C=10`, defaults). Impact remains a
silent `uncertain` outcome requiring audited resolution, with the ordinary
duplicate-effect risk on replay (risk 8) — now understood to be reachable
in ordinary operation, not only under an active fault.

**Current mitigation.** `queue.LeaseTooShortForDeadline` bounds the
single-stall, `C ≤ 2` case (see risk 5) but is now confirmed insufficient
for `C > 2` — the reproduction above shows it failing at the shipped
default lease/deadline ratio once `C` and stall count both rise. No code
mitigation beyond this exists yet; the three remedy options above are all
unimplemented, pending the open owner decision.

**Evidence.** `docs/PERFORMANCE-EVIDENCE.md`, "L6T2: sibling starvation
under slow acks (T2)" (committed `K=8` result, reinterpreted as ack
starvation; reviewer reproduction at `K=5`/`K=6` cited as reviewer runs, not
committed to this repository). Documented before this reproduction:
`README.md` ("Guarantees"), `docs/RECOVERY-EVIDENCE.md` ("Acknowledgement
deadline", "Limits", "Concurrent pending acknowledgements"),
`docs/RELEASE-READINESS.md` ("Decide on per-attempt storage calls").

**Status.** Open, high severity — a post-release design decision (which
remedy, options 1/2/3 above) pending owner sign-off; the reviewer
recommends against shipping with option 3 (documentation) alone.

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
account for that (see risk 4). The same "one stall" assumption also covers a
slow claim, not only a stalled acknowledgement (see risk 4's own note on
this): the `FillSlots`-message fix in `grind/queue` (`fill_automatic_slots`/
`request_fill`) is what makes that assumption hold for automatic polling
specifically — before it, a burst filling every free slot with slow claims
could itself present up to `maximum_concurrency` stalls in a row, not one,
which this rule's derivation never accounted for at all. The fix closes that
gap for claims; it does not touch the still-open `maximum_concurrency > 2`
multi-sibling gap this paragraph already describes.

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

### 6. Each claim costs two statements, one per free slot per round

**What can happen.** Automatic polling used to be bounded by
`maximum_jobs_per_poll / poll_interval` regardless of `maximum_concurrency`
(with the shipped defaults, `maximum_jobs_per_poll = 1` and
`poll_interval = 250ms`, one consumer claimed at most about 4 jobs/second no
matter how high `maximum_concurrency` was set) — this per-poll ceiling is
fixed: a poll (or a slot freed by a completing job) now keeps claiming into
every free slot for as long as `maximum_concurrency` allows and jobs are
available, backing off to the full `poll_interval` only once a claim
actually finds nothing (Oban-like; see `grind/queue`'s automatic-polling doc
comments and `docs/RECOVERY-EVIDENCE.md`). `maximum_jobs_per_poll` was
narrowed to `maximum_batch_jobs` and no longer has any effect on automatic
polling — it now only bounds how many jobs one manual `process_available`
call processes before returning (its one remaining, distinct meaning; a
separate cap here would be redundant with `maximum_concurrency`, which
already bounds automatic claiming). What remains, unchanged: every claim
attempt still issues two statements against `grind_jobs` — a `LIMIT 1`
quarantine scan runs before every claim query, not only when a claim
actually finds an expired row — so a burst that fills many slots in one
round issues that many quarantine scans; this stays bounded by real claim
throughput (concurrency and completion rate), never a busy loop with no
forward progress, since a step that finds nothing to claim always stops and
waits for the next poll instead of retrying immediately (see
`fill_automatic_slots`'s doc comment). Recovering `K` quarantined
(lease-expired) rows still costs `K` separate claims either way.

**Likelihood / impact.** Low likelihood now that the per-poll ceiling is
gone; the residual per-claim quarantine-scan cost is a documented, accepted
characteristic of claim-time quarantine, not a hidden inefficiency or a
correctness gap.

**Current mitigation.** `maximum_concurrency` is the only setting that now
bounds automatic-polling throughput; `poll_interval` only bounds how long an
idle consumer waits before trying again. `queue.with_maximum_batch_jobs`
remains for tuning manual batch size, independent of automatic polling.

**Evidence.** `postgres_automatic_consumer_drains_backlog_without_per_interval_ceiling_test`
and `postgres_automatic_consumer_waits_full_interval_when_idle_test`
(`test/grind_test.gleam`) are committed, deterministic regression coverage
for the fix and for the no-busy-loop guarantee, respectively — not purely
"database-clock-bound" (an earlier overclaim in this entry): each test still
waits out a local, bounded polling loop for the system to reach the state it
then reads, and only the pass/fail evidence itself (`finished_at` timestamps,
row counts) is read from the database's own clock rather than the test
process's. Real load/throughput numbers remain a throwaway benchmark, not
committed test evidence. No committed load-test evidence yet (see risk 17).

**Status.** The per-poll ceiling itself is resolved. The per-claim
quarantine-scan cost is accepted (an inherent cost of claim-time
quarantine, not a defect) — the underlying throughput/latency measurement
work itself is open, tracked in `docs/RELEASE-READINESS.md` ("Evidence still
missing", "Load").

---

### 7. `search_path` must point at the intended schema

**What can happen (resolved by removal, twice over).** Three successive
designs tried to derive an owner value scoping rows within one shared
schema: first from the connection URL (`"<host>:<port>/<database>"`, which
conflated different endpoints to the same database and ignored
`search_path` entirely), then from the database's own identity
(`pg_control_system()`'s `system_identifier` plus
`current_database()`/`current_schema()`, or an explicit
`postgres.with_storage_owner` override). All three added a whole concept —
a whole schema column, `storage_owner`, on every table — to solve a problem
PostgreSQL's own schema mechanism already solves on its own.
`storage_owner` and `with_storage_owner` are removed entirely (see
`docs/UNIQUENESS-CONTRACT.md`, and `grind_v12`'s own migration comments in
`src/grind/internal/migrations.gleam`, "Dropping `storage_owner`"):
isolation between logically distinct Grind installations is now simply the
PostgreSQL schema a pool's `search_path` resolves to — see README,
"Isolation". Two pools whose `search_path` resolves to the same schema
share one installation; two pools whose `search_path` resolves to different
schemas — in the same physical database or not — are fully isolated, since
each schema holds its own independent set of `grind_` tables.

**The `$user`-fallback hazard this risk originally named, and how it is now
closed.** An earlier revision of Grind left `search_path` entirely to
whatever the connecting role or database defaulted to, relying on an
operator to configure `search_path` explicitly per role. That was a real,
proven hazard, not merely a hypothetical one: PostgreSQL's own
`current_schema()` reports the _first_ schema in `search_path` that merely
_exists_ — not the first one that actually holds any object — so a role
with its own personal, empty `"$user"` schema (the ordinary default,
`"$user", public`) ahead of `public` in `search_path`, where Grind's real
tables actually live, would report a _different_ `current_schema()` value
per role even though both write to the exact same physical `grind_jobs`
table. Since the uniqueness admission lock key used to be keyed by
`current_schema()`, two such roles racing the identical uniqueness key
could each acquire a _different_ advisory lock and both insert — a genuine
duplicate-admission defect, not merely a theoretical one; see
`postgres_user_schema_fallback_shares_one_installation_test`
(`test/grind_test.gleam`) for the red-then-green proof. `Settings.schema`
(`postgres.with_schema`, default `"public"`) closes this at its root:
`postgres.validate` pins every pooled connection's own `search_path`
connection parameter to exactly this one configured schema, so
`current_schema()` can only ever resolve to it (or to nothing, before
`migrate` first creates it) regardless of what a role's own default
`search_path` would otherwise have been — no role-specific `"$user"`
schema, however it is configured, can shadow the schema Grind was actually
told to use. The uniqueness advisory lock key itself no longer even relies
on `current_schema()` resolving correctly: it binds the configured schema
as an ordinary SQL parameter (see
`grind/internal/unique_admission.lock_key_sql`'s own doc comment), so it is
correct by construction rather than by relying on the connection-parameter
pin as its only line of defense.

**The residual hazard.** Nothing in Grind can stop an operator from
deliberately configuring two pools that were _meant_ to be isolated with
the identical `Settings.schema` value — that is indistinguishable, from
Grind's own point of view, from a deliberately shared installation, and
there is no way for `postgres.start`/`postgres.migrate` to know an
operator's intent, only the schema they were actually told to use. This is
now a purely a configuration-discipline question (did the operator pass the
schema they meant to), not a mechanism defect: `postgres.with_schema`'s own
value is explicit, bound, and enforced identically on every pooled
connection, with no fallback path left for it to silently diverge from.

**Likelihood / impact.** Low likelihood now (an operator has to actively
misconfigure or omit `with_schema` for two installations that were meant to
be distinct; the previous `$user`-fallback trap that made this easy to hit
by accident is closed); still high impact if it occurs (silent, undetected
sharing of one schema by two applications that believed themselves isolated
— no error, no observation, just merged job/uniqueness/quarantine/retention
state).

**Current mitigation.** `postgres.with_schema` plus `postgres.validate`'s
own `search_path` connection-parameter pin, described above — no longer
documentation-only. `docs/UNIQUENESS-CONTRACT.md` and README, "Isolation"
both state the explicit-schema contract and name the residual
configuration-discipline hazard. `postgres.validate` additionally rejects
the literal schema name `"$user"` outright (a quoted `"$user"` behaves
differently from `search_path`'s own unquoted `$user`-substitution
convention this risk names, and can leave the migration advisory lock
key `NULL`) and any `pg_`-prefixed name (reserved by PostgreSQL for its
own system/temporary schemas) — closing off the two schema-name shapes
most likely to be an accidental copy-paste of that same `$user` convention,
rather than relying on documentation alone to warn against them.

**Evidence.**
`postgres_user_schema_fallback_shares_one_installation_test` proves the
`$user`-fallback hazard this risk originally named is closed (two roles,
each with its own empty personal schema, neither ever calling
`with_schema`, converge on one shared installation and one advisory lock).
`postgres_two_schemas_share_a_database_but_stay_isolated_test` and
`postgres_two_urls_to_the_same_schema_share_it_test` prove the two
remaining halves of the mechanism (explicitly distinct schemas isolate; the
same schema through different connection strings converges).
`postgres_handle_from_another_installation_is_rejected_test` proves the
client-side backstop (a handle minted against one schema, used against
another, is rejected before any storage call) — see `grind/job`'s
`Installation` type. None of these, nor anything else, proves an operator
passed `with_schema` the value they actually intended in a real deployment
— the residual hazard above.

**Status.** The old owner-derivation defect this risk originally named, and
the `$user`-fallback hazard the schema-based redesign initially left open,
are both resolved. The residual configuration-discipline hazard (did the
operator choose distinct schema values for installations meant to be
distinct) is open and accepted, like risk 11's migration-ownership
discipline.

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
`ACCESS EXCLUSIVE` lock. Measured at 2,000,000 rows, `grind_v12`'s original
`finished_at`-backfill statements (`docs/RECOVERY-EVIDENCE.md`, Increment 25)
took roughly 6 seconds — **treat this figure as a lower bound, not a current
measurement**: `grind_v12` was later edited in place, before its own release,
to also drop `storage_owner` (see `src/grind/internal/migrations.gleam`,
"Dropping `storage_owner`"), adding a second round of `ACCESS EXCLUSIVE`-held
work the original 6-second figure never included — `DROP INDEX`/`CREATE
INDEX` rebuilding `grind_jobs_unique_candidate_idx`, a `DROP CONSTRAINT`/`ADD
CONSTRAINT PRIMARY KEY` rebuild (and the index backing it) on each of the
three receipt tables, two `GROUP BY ... HAVING count(DISTINCT storage_owner)

> 1`collision scans (one over`grind_unique_submissions`, one over
`grind_job_resolutions`, each a full scan of that table), and three `DROP
> COLUMN storage_owner`statements. None of this has been separately measured
at 2,000,000 rows; the true current cost is`6s + `(this added work), not
`6s`outright. A large-enough table can exceed`migration_deadline_ms`(default 30000ms) and report`MigrationCommitUnknown(12)`with nothing
committed; re-running fails the same way until the deadline is raised or the
file is applied directly outside Grind's deadline-bounded path. Separately,`grind_v12` requires a genuine stop-the-world deploy: old pre-`finished_at`code writing a terminal state after`grind_v12`commits hits`23514`; new
`finished_at`-aware code writing against the still-`v11` schema hits an
> undefined-column error. The two schema versions cannot coexist with live
> writers on both sides.

**Likelihood / impact.** Low likelihood (affects only large, established
Grind deployments upgrading past v11), high impact where it applies (a
failed migration on a production-sized table, or a correctness break from a
rolling deploy across the migration).

**Current mitigation.** The stop-the-world requirement is explicitly
documented, with the exact commands to apply the migration as its own deploy
step outside normal node boot. The cost figure is explicitly flagged above
as a lower bound pending a fresh measurement against the current (post-
`storage_owner`-removal) `grind_v12` statement list.

**Evidence.** `README.md` ("Migrations"); `docs/RECOVERY-EVIDENCE.md`,
Increment 25, for the original measured cascade/backfill timing (now a lower
bound, not a current figure — see above).

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

**Current mitigation.** Both gaps closed (72fe573):
`cigogne_applies_grind_files_then_migrate_is_noop_test` and
`cigogne_apply_serializes_with_concurrent_migrate_test` prove the full
round trip (cigogne applies, `migrate` no-ops, both succeed when racing the
shared advisory lock); `postgres_migrate_upgrade_reconcile_unique_lost_reply_test`
proves a genuine lost-reply `reconcile_unique` specifically across the v11→v12
boundary. See `test/grind_test.gleam` and `docs/RECOVERY-EVIDENCE.md`,
Increment 34, including a documented empirical finding (the TCP fault
proxy's `OnCommit`/`DropReply` does not produce a genuine commit for
`submit_unique` specifically, unlike the acknowledgement path).

**Evidence.** `docs/RELEASE-READINESS.md` ("1. Contract decisions",
"Migration gaps"); `docs/RECOVERY-EVIDENCE.md`, Increment 34.

**Status.** Closed.

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
between Grind and PostgreSQL is not exercised by any test. Three of Grind's
connection-parameter-level defenses are startup parameters
(`default_transaction_isolation`, `idle_in_transaction_session_timeout`, and
— since `postgres.with_schema` — `search_path`) that some poolers can
silently drop or fail to apply per-checkout, depending on pooling mode.
`default_transaction_isolation` already has an in-transaction fallback pin
as defense in depth against exactly this; no equivalent in-transaction
fallback exists for `idle_in_transaction_session_timeout`, since by
definition nothing is running to set it once a session has already gone
idle, or for `search_path`, since nothing in Grind re-asserts it once a
transaction is already under way.

---

### 19. Single coordinator process caps per-consumer throughput at high concurrency

**What can happen.** Every claim, renewal, and acknowledgement for one
`queue.start` consumer is a synchronous round trip on that consumer's own
single coordinator process — the same serialization risks 4-6 describe for
the fault/stall case also caps ordinary, fault-free throughput: one
coordinator can only have one such round trip in flight at a time, so
raising `maximum_concurrency` on a single consumer buys idle worker slots,
not more concurrent database round trips. Measured on 25894a6
(`docs/PERFORMANCE-EVIDENCE.md`, "T3 verdict", L7): one consumer at
`maximum_concurrency = 50` reached about 897 jobs/s, while five consumers at
`maximum_concurrency = 10` each (same total concurrency, same pool) reached
about 2799 jobs/s — the single coordinator delivered roughly 32% of the
split-coordinator throughput at identical capacity, well under this
project's own 70% T3 threshold. PostgreSQL itself was not the constraint:
server-side CPU stayed under 8% of the whole 12-core benchmark machine even
in the single-coordinator shape (about 86% of _one_ core, which a single
serial connection can reach on its own regardless of how much spare
capacity the other 11 cores hold); a statistical profile of the coordinator
process itself spent about 76% of its own samples inside `prim_inet:recv0`
(waiting on the network for PostgreSQL's reply) and about 5% checking out a
pool connection, versus roughly 0.4ms of measured `pg_stat_statements` exec
time per job against a measured ~1.1ms wall-clock time per job at that
shape — the gap is the coordinator's own serial round-trip overhead, not
server-side work.

**Likelihood / impact.** Certain at high single-consumer concurrency (no
fault needed, unlike risk 4); impact is a throughput ceiling, not a
correctness gap — every job still completes correctly, just slower than the
database could otherwise sustain. Mitigated today by topology: splitting the
same total concurrency across more consumers or queues (each its own
coordinator) recovers the throughput this risk otherwise caps.

**Current mitigation.** Documented topology guidance: prefer more consumers
(or queues) at a moderate `maximum_concurrency` (around 10 per consumer, the
shape measured to scale close to linearly) over one consumer at very high
`maximum_concurrency`. No code change yet — the two remedies below are
deferred by user decision (2026-09-27), tracked as post-release performance
optimizations, not release blockers:

1. **Batch claim** (following Oban's `fetch_jobs`): one claim statement
   claims up to the number of free slots at once (`UPDATE ... WHERE id IN
(SELECT ... LIMIT $demand FOR UPDATE SKIP LOCKED)`) instead of one
   claim per free slot, and one expired-lease quarantine sweep per poll
   instead of one per claim. Expected effect: far fewer serial coordinator
   round trips per job.
2. **Ack from the attempt process** (following Oban's `executor`): the
   attempt's own worker process runs the acknowledgement transaction
   itself and reports only the outcome back to the coordinator, so acks run
   in parallel, bounded by the pool, instead of serially on the
   coordinator's own loop. Fencing (attempt id/epoch/owner) stays enforced
   in SQL, so guarantees are unchanged; the pending-ack retry moves into or
   beside the attempt process, which touches the proven recovery code in
   `grind/internal/attempt` — this needs its own design pass and every
   recovery test (`docs/RECOVERY-EVIDENCE.md`) must stay green.

A renewal-only process per consumer (the plan's first-considered remedy for
risk 4) would not address this risk: it moves lease renewal off the
coordinator, not claim or ack, so the single-coordinator claim/ack
serialization this risk describes would remain. Where lease renewal itself
should live is being revisited once `docs/PERFORMANCE-EVIDENCE.md`'s L6
(renewal starvation) T1/T2 results are in.

**Evidence.** `docs/PERFORMANCE-EVIDENCE.md`, "T3 verdict" and "L7:
coordinator bottleneck" (`bench/results/2026-09-26-25894a6/l7.csv`,
`profile.csv`, `statements.csv`). `docs/RELEASE-READINESS.md`, "4. Evidence
still missing" tracks the two deferred remedies above.

**Status.** Open — mitigation is topology guidance only; both remedies are
tracked, deferred post-release optimizations (user decision, 2026-09-27),
not scheduled.

**PgBouncer specifically, and why `search_path` is the sharpest edge of the
three.** PgBouncer applies a client's own startup parameters to the
_server_ connection it hands out only under certain configurations: by
default it can silently **ignore** startup parameters it does not
recognize or was not told to track (`ignore_startup_parameters`), and even
when told to track one (`track_extra_parameters`, which must explicitly
list `search_path` to preserve it at all under PgBouncer's own default
parameter handling), **transaction pooling mode** hands a client a
_different_ physical server connection per transaction — one that was
last configured for whatever startup parameters some _other_ client
session set on it, not necessarily this one's. A pooler configured this
way would not error; it would simply run Grind's queries against
whatever schema that physical connection's `search_path` happens to
already be set to, silently breaking the entire isolation model
`postgres.with_schema` depends on (README, "Isolation") — indistinguishable
from a correctly-isolated installation until two workloads' data
inexplicably starts mixing. This is a categorically worse failure mode
than the pre-existing `idle_in_transaction_session_timeout` risk below: a
dropped timeout loses one backstop; a dropped or stale `search_path` can
silently point an entire installation at the wrong schema.

**Likelihood / impact.** Moderate likelihood (poolers are common in
production Postgres deployments, and PgBouncer's transaction pooling mode
specifically is a popular, recommended default for high-connection-count
deployments — exactly where Grind is most likely to be introduced); high
impact for `search_path` if encountered (silent cross-installation data
mixing, not merely a lost backstop), moderate impact for the other two.

**Current mitigation.** `default_transaction_isolation`'s in-transaction
fallback pin. No equivalent exists for the idle-session timeout or for
`search_path`. An operator placing PgBouncer (or similar) between Grind and
PostgreSQL must run it in **session pooling mode**, not transaction pooling,
and must explicitly configure it to preserve `search_path` (PgBouncer:
add `search_path` to `track_extra_parameters`, and ensure
`ignore_startup_parameters` does not include it) — otherwise `postgres.with_schema`'s
own guarantee does not hold through the pooler. This is documentation-only
today; nothing in Grind detects or defends against a pooler silently
misapplying `search_path`.

**Evidence.** Named explicitly in `docs/RECOVERY-EVIDENCE.md` ("Limits", "No
poolers").

**Status.** Open.
