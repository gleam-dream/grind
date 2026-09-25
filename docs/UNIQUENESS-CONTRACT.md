# Uniqueness contract

This document records the approved contract for Grind's uniqueness admission
(`grind/unique`, `grind/internal/unique_admission`, and
`submit_unique`/`reconcile_unique` in `grind/postgres`), the decisions behind
it, the schema v11 change it required, its failure modes, and what remains
out of scope. It supersedes the sketch in
[`grind-design.md`](https://github.com/gleam-dream/oversight/blob/main/grind-design.md)
and the exploratory profile in
`oversight/research/grind-uniqueness-contract.md`, both of which predate
Grind's current `Worker(input, output, error)`/`JobHandle(input, output, error)`
shape.

## Status

Implemented: schema v11; the pure `grind/unique` policy module (which also
owns the four public admission-result types — see "Module placement"); the
admission transaction (lock, receipt lookup, time sampling, candidate
selection, decision, receipt, commit) in `grind/internal/unique_admission`,
reached through `submit_unique`/`reconcile_unique`; sequential admission and
identity tests, including PostgreSQL JSON-equality cases and worker identity
isolation; queue scope (`WithinQueue`/`AcrossQueues`); the full
state-eligibility matrix, forced by SQL across all 11 persisted states and
all four `States` groups, plus a live transition through a real manually-driven
consumer; period-boundary timing at the database clock for `FromInsertion`,
`FromSchedule` (past and future), `while_retained()`, and the shared
`period_predicate` fragment at an exact microsecond instant; and
`SubmissionId` receipt idempotency, including replay after a period has
elapsed, a changed-input conflict, replay returning the originally observed
state rather than the row's current state, and a changed output codec version
conflicting on replay; concurrent admission under a real, barrier-forced
overlap (a test-only `BEFORE INSERT` trigger blocking on a test-held advisory
lock forces three simultaneous `submit_unique` calls, same key, to actually
interleave), proven by `pg_stat_activity` polling for the exact expected
lock-contention shape (one submitter blocked inserting behind the barrier,
the other two blocked on the domain advisory lock) rather than by timing —
exactly one settles `Inserted`, the other two settle `Existing` against that
same job id, and exactly one row persists; the mixed-scope variant of the
same barrier (`WithinQueue` in one queue racing `AcrossQueues` in another,
same key) proving the domain lock's deliberate exclusion of queue from its
own key actually serializes cross-scope submissions; the real invariant
behind "Admission transaction" step 4's receipt lookup — it must run after
the domain lock and before this transaction's own write, not specifically
"before candidate selection" (that placement is a coincidence of the
unmutated code, proven unobservable on its own) — proven directly both by a
live scenario (a submitter waiting on the domain lock while an identical,
same-`SubmissionId` request commits ahead of it returns that committed
`Inserted` decision from the receipt, not a fresh `Existing` conflict) and
by two mutations that move the lookup to the two positions that _are_
observably wrong (before the lock; after the write); and lock contention
(`AdmissionContended`) proven against both a real held domain lock and a
real held row lock (the `RescheduleScheduledTo`
variant), including that the bounded `lock_timeout` a contended attempt sets
does not leak into a later statement on the same reused pooled connection
(proven by mutation against a _committed_, not merely contended, attempt on
that connection — a rolled-back transaction's `set_config` reverts
regardless of that setting). Also proven: the admission transaction pins
its own isolation to `READ COMMITTED` as its literal first statement,
rather than assuming the connecting role or database's own
`default_transaction_isolation` already is — against a real, dedicated
disposable database configured with `default_transaction_isolation =
'repeatable read'`, the same forced-overlap barrier genuinely duplicates a
row without this pin, and does not with it. See `docs/RECOVERY-EVIDENCE.md`,
Increments 8 and 9, and "Isolation-level pinning".

Also proven (Increment 10): `RescheduleScheduledTo` moving `available_at` on
a genuinely `scheduled` conflict, with the receipt recording both the
previous and new `available_at`; every other conflicting state (`queued`,
`retryable`, `executing`, `uncertain`) left completely unchanged under the
same action; a rescheduled row genuinely claimable by a real manually-driven
consumer once its new `available_at` is due; and the live race between a
real claim (which locks the scheduled row as part of its own claim `UPDATE`,
forced to block mid-transaction behind a test trigger) and a concurrent
reschedule submission for the same key — the reschedule's own candidate
`SELECT ... FOR UPDATE` genuinely waits on the row lock the claim holds, and
PostgreSQL's own `EvalPlanQual` re-check hands it the row's fresh post-claim
state once released, not the stale `scheduled` value it started waiting
behind: `Existing` with the observed `Executing` state under `Incomplete`,
`Inserted` (a fresh row) under `ScheduledOnly`. Proven by mutation: widening
the Gleam-level `state = "scheduled"` pattern match to fire unconditionally
produces a fabricated `Rescheduled` decision against a row nothing actually
changed (the reschedule `UPDATE`'s own redundant SQL-level guard alone
silently no-ops the write, but the wrongly-reported decision and receipt are
exactly the bug this proves); the SQL-level guard alone, in isolation, is
unobservable given the Gleam-level match and the row lock already protect
it — recorded honestly, the same discipline as Increment 8's own
unobservable-reordering mutation.

Also proven (Increment 11), with `install_syncrep_reply_trigger` generalized
to take a table and predicate (scoped by `submission_id` on
`grind_unique_submissions` rather than a server-generated `job_id`, which
is not known before the admission transaction that would create it even
starts): a genuinely committed admission whose reply is lost after
PostgreSQL has already committed locally still returns `Ok(Inserted(handle))`
directly from `submit_unique`, resolved by `run`'s own automatic
receipt-lookup fallback, not a `CommitUnknown` the caller must separately
reconcile; the identical mechanism applied to a reschedule commit returns
`Ok(Rescheduled(conflict))`, not `Existing` — the receipt's own recorded
_decision_, not the row's current state (which looks like an ordinary
scheduled conflict either way), is what is decoded; an aborted commit (a
deferred trigger's `pg_sleep` during the transaction's own `COMMIT`,
terminated before it can finish) reports `CommitUnknown(pending)`, with zero
jobs and zero receipts, genuinely indistinguishable from a lost reply after
a real commit — `reconcile_unique` alone can never resolve it (nothing was
ever recorded), so the only correct recovery is a plain retry of the same
`SubmissionId`. A pool closed before `submit_unique` ever sends anything
gives `AdmissionFailed(ConnectionUnavailable)` — knowably not committed, no
`PendingSubmission`, no receipt lookup attempted — via a dedicated FFI
wrapper (`transaction_or_checkout_failure`, `grind_postgres_ffi.erl`) that
distinguishes a checkout failure (nothing was ever attempted) from pog's own
transaction outcome. **This distinction was added as a bug fix, not a design
choice recorded after the fact**: `run`'s original code disguised a checkout
failure and a genuinely uncertain mid-transaction connection loss as the
same `pog.TransactionQueryError` shape, and a pool closed while a commit is
genuinely parked (and therefore possibly already committed) hit exactly
that ambiguity — the follow-up receipt lookup this second case needs also
failed to reach the (now fully closed) store, and the unfixed code returned
that lookup failure as `AdmissionFailed`, silently discarding the
`PendingSubmission` a caller would need to ever learn the zombie transaction
later committed. Fixed in two parts: `reconcile_from_receipt` now maps a
failed lookup to `CommitUnknown(pending)` (the same as finding no receipt
yet, mirroring `reconcile_unknown_ack`'s `Ok(None) | Error(_) ->
QueueAckUnknown`), and `run` uses the FFI wrapper above so a checkout
failure is never routed through that same fallback in the first place. With
the fix: a pool closed while a commit is genuinely parked reports
`CommitUnknown(pending)`; `reconcile_unique(pending)`, tried while the
zombie is still parked, is a pure receipt lookup with no lock of its own, so
it still reports `CommitUnknown` (the zombie's own receipt insert is not yet
visible to any other session); a second, independent recovery path — a
plain `submit_unique` retry of the same `SubmissionId`, tried while the
zombie is still parked — genuinely needs the domain lock the zombie holds
and reports `AdmissionContended` (no second row, either path); once the
zombie is terminated and confirmed gone (`wait_for_backend_gone`),
`reconcile_unique` resolves from the now-visible receipt — not candidate
selection reinterpreting the row as a fresh conflict — `Inserted` with the
original job id, one row, and the plain-retry path converges on that same
id. Proven red before the fix (the exact bug: `submit_unique`'s reply for
the store-unavailable fault was `AdmissionFailed(ConnectionUnavailable)`
instead of the expected `CommitUnknown(pending)`) and by mutation after it
(reverting either half of the fix independently reproduces the bug, or
turns every genuinely-committed-but-reply-lost case red at once, or makes
the store-unavailable retry surface `SubmissionConflict` from the receipt
table's own primary-key constraint instead of `Inserted`).

See `docs/RECOVERY-EVIDENCE.md`, Increments 10 and 11, for the full
mechanism, synchronization, root cause, and quoted red/mutation output.

Also proven (Increment 12): `unique.selected`'s key contract
(`"selected:" <> name <> ":" <> codec_version`) genuinely isolates a
projected key by name and by codec version independently of the projected
value itself — a non-selected input field never enters the key (a same-key
submission that only changes an unselected field still conflicts); a
different key name alone, or a different codec version alone, with the
identical projection and projected value, both admit a fresh row instead of
conflicting; a full-input key and a selected key never collide even over the
exact same input, because their contract prefixes ("full-input:" vs
"selected:") differ unconditionally; and a selected key's equality is exact,
not containment — a projected subset value does not conflict with a stored
projected superset, the identical departure from Oban's own semantics
already proven for full-input keys
(`postgres_submit_unique_json_equality_matches_postgres_jsonb_test`), now
also proven when the compared value is a caller-chosen projection rather
than the whole input. Proven by mutation: dropping the key name from the
contract string (`"selected:" <> codec_version`, no name) makes a
differently-named selected key falsely conflict with an already-admitted
one (`postgres_submit_unique_selected_key_scoping_test` turns red: an
expected `Inserted` observes `Existing` against the other name's row
instead) — reverted immediately, `gleam check` recompiled clean and `git
diff` showed no trace of the mutated line.

Also proven (Increment 13), entirely through `consumer/`, the separate
package that imports only public Grind modules: a plain admission, a second,
differently-identified admission against the same key observed as
`Existing`, rebinding that conflict's job id with the existing
`postgres.bind_handle` path, and reading a typed committed outcome after a
manually driven consumer actually runs the original job; replaying the
_original_ `SubmissionId` returns the receipt's own recorded `Inserted`
decision with the original job id rather than a fresh conflict against the
still-present row; and, as the advanced case, an `AcrossQueues` policy's
`RescheduleScheduledTo` moving a genuinely `scheduled` row's `available_at`
from a submission made through a _different_ queue than the row's own,
proven by `conflict_queue` reporting the row's actual original queue (not
the rescheduling submission's own queue) and by the row becoming claimable
and running to a typed `Succeeded` outcome once its new time is due.
Confirmed by mutation: reusing the same production mutation that proves
`AcrossQueues` at the root level (`candidate_sql`/`bind_candidate_params`
always adding `queue = $q`) turns
`public_consumer_unique_reschedule_across_queues_test` red too (the
rescheduling submission inserts a second row instead of finding the
cross-queue conflict) — reverted immediately, `gleam check` recompiled
clean and `git diff` showed no trace of the mutated lines. See
`docs/RECOVERY-EVIDENCE.md`, Increments 12 and 13.

## Module placement

The admission transaction lives in `grind/internal/unique_admission`, which
has no dependency on `grind/postgres` — `grind/postgres` depends on it
instead, one-directionally. This module owns the SQL and never appears in
Grind's public API. Unlike an earlier draft, it has **no parallel mirror
types**: it builds and returns `grind/unique`'s own public
`Admission`/`Conflict`/`SubmitError`/`PendingSubmission` values
directly (via a small set of `@internal` constructors and field accessors on
the opaque ones — `new_conflict`, `new_pending_submission`, and
`pending_submission_storage_owner`/`pending_submission_worker`/
`pending_submission_request_sha256` — rather than an intermediate fields
record), so
`submit_unique`/`reconcile_unique` in `grind/postgres` are thin entry points
that call straight through, not a second translating layer. `Availability`
and `ConflictAction` also live in `grind/unique` rather than `grind/postgres`,
specifically so both `grind/postgres` and `grind/internal/unique_admission`
can depend on them without a cycle — a deliberate placement adjustment from
an earlier draft of this contract, which had `Availability` in
`grind/postgres`.

## Public contract

`src/grind/unique.gleam` (pure policy values; the admission-result types
below are produced by `grind/internal/unique_admission`'s PostgreSQL
transaction, but the types themselves live here too, so `grind/postgres`
never needs its own mirrors — see "Module placement" above):

```gleam
pub type QueueScope { WithinQueue  AcrossQueues }
pub type UniqueTimestamp { FromInsertion  FromSchedule }
pub opaque type Period
pub fn within_milliseconds(ms: Int, from: UniqueTimestamp) -> Result(Period, PolicyError)
pub fn while_retained() -> Period
pub type States { Incomplete  ScheduledOnly  IncompleteOrSucceeded  AllRetained }
pub type Availability { Immediately  At(job.AvailableAt) }
pub type ConflictAction { KeepExisting  RescheduleScheduledTo(job.AvailableAt) }
pub opaque type Key(input)
pub fn full_input() -> Key(input)
pub fn selected(name: String, select: fn(input) -> key, codec: worker.Codec(key)) -> Result(Key(input), PolicyError)
pub opaque type Policy(input)
pub fn policy(key: Key(input), scope: QueueScope, period: Period, states: States) -> Policy(input)
pub opaque type SubmissionId
pub fn submission_id(value: String) -> Result(SubmissionId, PolicyError)
pub fn submission_id_value(id: SubmissionId) -> String
pub type PolicyError { NonPositivePeriod  PeriodAbovePrecisionBound  EmptyKeyName  EmptySubmissionId }

pub opaque type Conflict
pub fn conflict_job_id(c: Conflict) -> Int
pub fn conflict_queue(c: Conflict) -> String
pub fn conflict_state(c: Conflict) -> job.State
pub type Admission(input, output, error) {
  Inserted(job.JobHandle(input, output, error))
  Existing(Conflict)
  Rescheduled(Conflict)
}
pub opaque type PendingSubmission(input, output, error)
pub fn pending_submission_id(p: PendingSubmission(i, o, e)) -> SubmissionId
pub type SubmitError(input, output, error) {
  EmptyQueueName
  AdmissionContended
  SubmissionConflict
  AdmissionFailed(pog.QueryError)
  CommitUnknown(PendingSubmission(input, output, error))
}
```

`src/grind/postgres.gleam` additions (thin entry points over
`grind/internal/unique_admission`, returning `grind/unique`'s types
unchanged):

```gleam
pub fn submit_unique(database, queue, submission_id, worker, input, availability, policy, on_conflict)
  -> Result(unique.Admission(i, o, e), unique.SubmitError(i, o, e))
pub fn reconcile_unique(database, pending) -> Result(unique.Admission(i, o, e), unique.SubmitError(i, o, e))
pub fn unique_lock_wait(settings: Settings, milliseconds: Int) -> Settings
```

`ConflictAction`'s reschedule target lives on the action itself
(`RescheduleScheduledTo(job.AvailableAt)`), not as a separate combination of
`Availability` and a bare `RescheduleScheduled` marker. This removes an
entire invalid-combination case: there is no longer an "reschedule with no
target" state to reject before storage, because it is not constructible.
`Availability` then governs only a fresh insertion.

`Conflict` is not a `JobHandle`: it carries the persisted job id, storage
owner, actual queue, worker id/version, and the state observed at decision
time (not necessarily the row's current state — it may have progressed
since), but no codecs. Callers rebind it with the existing
`bind_handle(database, worker, unique.conflict_job_id(conflict))` before
reading typed state — the same function used to rebind a durable id after a
restart. There is no separate `bind_conflict`; one binding function already
exists and does the same job.

`UniqueReceiptContradicted` (an earlier draft's separate variant for a
`reconcile_unique` mismatch) was merged into `SubmissionConflict`:
both `submit_unique`'s own in-transaction receipt check and a later
`reconcile_unique` call now report exactly the same conflict the same way,
since a caller reacts to "this `SubmissionId` does not mean what you think
it means" identically regardless of which call discovered it.

## Decisions

1. **Key equality is exact, computed by PostgreSQL, not "canonical JSON."**
   Two keys conflict exactly when SHA-256 of PostgreSQL's own `jsonb::text`
   rendering of each encoded key is identical (`unique_key_sha256`, computed
   server-side). This is a deliberate, documented departure from the earlier
   lab contract (`oversight/research/grind-uniqueness-contract.md`, "Compare
   full and selected JSON maps... Match the pinned Basic query rather than
   assuming language-level map equality") and from Oban's own containment
   semantics. Concretely: object key order is irrelevant (`jsonb` normalizes
   it); **duplicate object fields collapse last-wins**, because PostgreSQL's
   own `jsonb` parser already does this the moment the encoded key is cast to
   `jsonb` — Grind does not additionally canonicalize or reject duplicates,
   so a caller-encoded key with a repeated field silently keeps only the
   last occurrence, exactly as the rest of Grind's JSONB storage already
   behaves for admitted input; numeric exponent forms normalize (`1e2` →
   `100`); `1` and `1.0` are **distinct** (`numeric`'s stored scale is part
   of `jsonb`'s text output); array order is significant. No cross-runtime
   canonical-JSON FFI was written or is planned — PostgreSQL is the single
   source of truth for what "equal" means, and Grind never claims to
   reproduce that decision in Gleam. The _request_ fingerprint (below,
   Decision 9) is a separate, unrelated hash computed in Gleam.
2. **Equality, not containment.** Oban's Basic engine compares `args`/`meta`
   by containment in both directions (a subset does not conflict with the
   set that contains it, except when the selection is nonempty, per the
   pinned research notes). Grind's first profile uses exact equality only:
   `{"id":1}` never conflicts with `{"id":1,"extra":2}` (proven by
   `postgres_submit_unique_json_equality_matches_postgres_jsonb_test`).
   Broader containment semantics are out of scope (see below).
3. **The uniqueness identity always includes the worker id and version.**
   Cross-worker uniqueness is deferred (a conflict with a different worker
   cannot safely supply the submitting worker's output/error codecs). Two
   `Worker` values with the same id but a different version are always
   isolated from each other, proven by mutation (below).
4. **`Conflict` is not a handle**, and callers reuse `bind_handle` rather than
   a parallel `bind_conflict`. This keeps one contract-checked path from a
   durable job id to a typed handle, used identically after a process
   restart, after reading a durable id, or after a uniqueness conflict.
5. **Only `submit_unique`-admitted rows carry key material.** `unique_key_contract`
   and `unique_key_sha256` are `NULL` on every row inserted by plain
   `submit`/`submit_at`; the candidate query's `unique_key_contract = $n`
   equality can never match `NULL`, so plain-submitted rows are structurally
   invisible to uniqueness admission — not merely excluded by a states
   filter (proven by `postgres_submit_unique_ignores_plain_submitted_rows_test`,
   which deliberately uses the widest `AllRetained` states group to rule out
   the states filter as the reason). Converging plain `submit`/`submit_at`
   on the same `Availability` type `submit_unique` now uses is retained
   backlog, not done in this slice (see "Out of scope").
6. **A blocking, bounded lock wait**, not Oban's advisory-lock-miss-returns-
   a-conflict-with-`nil`-id. `submit_unique` sets `lock_timeout` (from
   `unique_lock_wait`, default 5000ms, validated positive by `validate`
   before any pool starts) then acquires `pg_advisory_xact_lock`. Every
   query in the admission transaction — not only the lock acquisition — is
   classified through the same function, so a PostgreSQL `55P03`
   (`lock_not_available`) on _any_ of them, not just the advisory lock
   itself, becomes `AdmissionContended`; a contended result never
   implies a persisted conflict exists.
7. **Schema v11 is fresh-install-only**, exactly like v10 was. There is no
   migration path and none is planned; see below.
8. **No implicit default period and no `UniqueKeyCouldNotEncode`.** Oban's
   list-valued policy defaults to 60 seconds; Grind's `Policy` always
   requires an explicit `Period` (`within_milliseconds` or `while_retained`).
   Key encoding in Grind is total (an ordinary `worker.Codec`'s `encode`
   cannot fail), so there is no encode-failure variant to report — a
   difference from the exploratory profile's `UniqueKeyCouldNotEncode`.
9. **The request fingerprint is computed in Gleam, not SQL**, unlike the key
   digest (Decision 1). `grind/internal/unique_admission` builds a fixed-order
   JSON array (`json.preprocessed_array`) from the request's fields, encodes
   it with `json.to_string`, and hashes it with SHA-256 (`crypto:hash/2` via
   a one-line Erlang FFI, `src/grind_unique_ffi.erl`) — a single `bytea`
   bound as `PendingSubmission`'s and `grind_unique_submissions.request_sha256`'s
   whole representation, compared with ordinary `BitArray` equality in
   Gleam. This is a deliberate difference from the key digest: retrying a
   `SubmissionId` with an identical request is a much stricter,
   single-caller check that does not need PostgreSQL's JSON normalization
   the way key _equality between different callers_ does — a
   deterministically-encoded Gleam value already reproduces byte-for-byte on
   a retry. The envelope is: queue, worker id/version, input codec version
   and JSON, key contract and JSON, scope, period (milliseconds and origin),
   states, action (including its reschedule target, if any), availability,
   the worker's output and error codec versions, and max attempts. Presence
   flags keep an absent optional field distinct from a legitimate value,
   matching the ack proposal fingerprint's existing pattern. **The output
   and error codec versions are part of the envelope specifically so a
   replayed `SubmissionId` can never return a handle bound to codecs that
   differ from those it was originally admitted under** — a worker
   redefinition that changes either codec version makes a retried
   `SubmissionId` a conflict, not a silently-returned stale-typed handle.

## Admission transaction

`submit_unique` first rejects, before any resource is touched: an empty
queue name (`EmptyQueueName`). This is proven with a _closed_
database pool (`postgres_submit_unique_rejects_before_touching_storage_test`)
— the pool is started then immediately closed, so any query attempt beyond
this check would surface as a storage failure, not the exact pure error
asserted.

Inside one PostgreSQL transaction (`transaction_safely`, the same wrapper
`migrate`/`acknowledge`/`resolve_uncertain` already use):

1. **`SET TRANSACTION ISOLATION LEVEL READ COMMITTED`, the literal first
   statement of the transaction** — before `set_config`, before the lock,
   before anything else. PostgreSQL rejects `SET TRANSACTION` if it is not
   the first statement, so it must run before any other query, not merely
   "early". It is load-bearing, not defensive decoration: steps 4 and 6
   below are plain reads whose correctness depends on seeing whatever
   another submitter committed _while this transaction was waiting_ at step
   3 — and only `READ COMMITTED` takes a fresh snapshot per statement.
   `REPEATABLE READ`/`SERIALIZABLE` freeze the snapshot at the
   transaction's _first_ statement, which — without any pin — would be
   step 2 (`set_config`, itself an ordinary `SELECT`), i.e. _before_ the
   lock wait even starts. A role or database configured with
   `default_transaction_isolation = 'repeatable read'` would then make a
   waiting submitter's plain reads miss the row/receipt the transaction it
   waited behind just committed, silently duplicating the row — with no
   error and no other code-visible signal. Proven by
   `postgres_submit_unique_admission_safe_under_repeatable_read_test`
   against a real, dedicated disposable database configured with
   `default_transaction_isolation = 'repeatable read'`
   (`GRIND_TEST_REPEATABLE_READ_URL`, `scripts/test-postgres.sh`) — genuine
   red without this pin (two rows, both `Inserted`), green with it. This
   in-transaction pin is now the second of **two** layers: `postgres.validate`
   also pins every pooled connection's own `default_transaction_isolation`
   to `read committed` as a startup connection parameter
   (`pog.connection_parameter`), which alone is already sufficient for
   this specific transaction and additionally covers every other
   transaction in `src/grind` (the acknowledgement and audited-resolution
   paths depend on the same `READ COMMITTED` assumption for a different
   reason — see `docs/RECOVERY-EVIDENCE.md`, "Isolation-level pinning").
   This transaction's own `SET TRANSACTION` is kept anyway as defense in
   depth: a connection pooler between Grind and PostgreSQL could drop or
   ignore a startup parameter, where an in-transaction `SET TRANSACTION`
   cannot be silently dropped the same way.
2. `SELECT set_config('lock_timeout', $1, true)` to the validated
   `unique_lock_wait_ms` (a bound parameter, not spliced text). The third
   argument (`is_local`) matters on its own: `true` scopes the setting to
   this transaction, reverting at both `COMMIT` and `ROLLBACK`; `false`
   would behave like a plain session-level `SET`, which survives a
   `COMMIT` and would leave every later statement on the same pooled
   physical connection bound to this call's `unique_lock_wait` — proven by
   mutation (`postgres_unique_lock_timeout_does_not_leak_to_later_statements_test`;
   see `docs/RECOVERY-EVIDENCE.md`, Increment 9). A _rolled-back_ attempt
   cannot distinguish `is_local: true` from `false` on its own — PostgreSQL
   reverts a GUC change made inside an aborted transaction either way — so
   that test's evidence comes from a _committed_ attempt on the same
   connection, not the contended one.
3. `pg_advisory_xact_lock` on a domain-wide key: `hashtextextended` of a
   fixed-order array (`'grind-unique-v1'`, `current_schema()`, storage
   owner, worker id, worker version, key contract, hex-encoded key digest).
   The lock key deliberately excludes queue, period, states, and action, so
   every policy submitted against the same key serializes against every
   other — a `WithinQueue` and an `AcrossQueues` policy on the same key
   never run their candidate selection concurrently (proven under a real,
   barrier-forced overlap — `docs/RECOVERY-EVIDENCE.md`, Increment 8).
   Exposed as one query-building function, `@internal
unique_admission.lock_query(storage_owner, worker_id, worker_version,
key_contract, encoded_key) -> pog.Query(Bool)`, used by both this step
   and any test that needs to hold this exact same lock (built from
   `@internal unique_admission.lock_key_sql`, in turn), so neither
   `acquire_lock` nor a test re-encodes the SQL and parameter binding by
   hand. `pg_advisory_xact_lock` itself returns `void`, which `pg_types`
   cannot decode (see the PostgreSQL driver note below), so the query is
   wrapped: `SELECT true FROM (SELECT pg_advisory_xact_lock(...)) AS
grind_unique_lock`.
4. **Receipt lookup by `(storage_owner, submission_id)`, immediately after
   the lock and before any write this transaction might make.** The
   necessary condition is "before any write," not "before candidate
   selection" specifically — those happen to coincide in the unmutated code
   because candidate selection is the very next step, but the two are not
   the same claim, and the difference is exactly what separates an
   unobservable reordering from an observably broken one (see below).
   Given step 1's `READ COMMITTED` pin, a concurrent submitter that already
   committed its receipt and its row while this transaction waited at step
   3 is fully visible the moment this transaction's own statements start
   running — this is true for every statement in this transaction, not
   specifically the receipt lookup, which is why moving the lookup to
   immediately _after_ candidate selection (still before any write) proved
   observably identical in testing (a genuine mutation was run and produced
   no distinguishing failure — see `docs/RECOVERY-EVIDENCE.md`, Increment
   8). What _is_ observably load-bearing is running the lookup before this
   transaction performs its _own_ write: moving it earlier than the lock
   (step 3) races this call's own admission decision against the very
   commit it should instead observe and return, surfacing as a `23505` on
   the receipt table's own primary key (`SubmissionConflict`) instead of
   the replayed decision; moving it later than the insert (inside step 7,
   after the `INSERT` but before recording the receipt) still returns the
   correct _value_ for a same-key concurrent submitter (since candidate
   selection already found the row), but for a _sequential_ replay after
   the original row's occupancy period has elapsed, it silently commits a
   second, orphaned job row alongside the correct return value — caught not
   by the returned value but by a plain row count. Both are proven by
   mutation; see `docs/RECOVERY-EVIDENCE.md`, Increment 8. If the receipt's
   request fingerprint matches the current call, its recorded decision
   (`Inserted`/`Existing`/`Rescheduled`) is reconstructed and returned
   immediately, with no further writes. A mismatch — or a stored
   `decision`/`observed_state` this code does not recognize, which fails
   closed the same way rather than being silently trusted — is
   `SubmissionConflict`. The unrecognized-value branch is unreachable
   in practice: `grind_unique_submissions.decision` and `.observed_state`
   both carry a `CHECK` constraint against the exact closed vocabulary this
   code decodes (schema v11, below), so only a directly tampered or
   corrupted row could reach it — this code does not special-case that
   possibility with its own failure, and folding it into the same
   `SubmissionConflict` a fingerprint mismatch produces was judged
   simpler than adding a variant no schema-respecting caller can trigger.
5. `SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint` — the
   one "now" for this transaction, sampled after the lock as Unix
   _microseconds_ and reused (via `to_timestamp($n::double precision / 1000000.0)`)
   for `inserted_at`, `available_at` (when `Immediately`), and the period
   predicate. See "PostgreSQL driver note" below for why this round-trips
   through a bound integer instead of a decoded `timestamptz`, and why
   microseconds rather than milliseconds.
6. Candidate selection: same storage owner, worker id, worker version, key
   contract, and key digest; `state = ANY($n::text[])` bound from the
   policy's eligible states (a `pog.array` parameter — Grind's own closed
   vocabulary, never caller-supplied text, but bound rather than spliced);
   `AND queue = $q` only under `WithinQueue`; the period predicate (below)
   unless `WhileRetained`; `ORDER BY id LIMIT 1`; `FOR UPDATE` only when the
   action is `RescheduleScheduledTo` (the row lock is only needed to
   re-check `state = 'scheduled'` immediately before updating it; contended
   by a concurrently held row lock, this reports `AdmissionContended` the
   same as step 3 — proven under a real held row lock, `docs/RECOVERY-
EVIDENCE.md`, Increment 9).
7. Decide: no candidate → `INSERT` a new job with its key columns set,
   `inserted_at = now`; a `scheduled` candidate under `RescheduleScheduledTo(at)` →
   `UPDATE ... SET available_at = to_timestamp(...) WHERE id = $id AND state = 'scheduled'`
   (the `state = 'scheduled'` guard is re-checked under the row lock from
   step 6, not assumed from the candidate read); any other candidate →
   unchanged `Existing`.
8. Record the receipt (`grind_unique_submissions`): the request fingerprint
   (Decision 9), decision, job id, the job's actual queue (which can differ
   from the submitted queue under `AcrossQueues`), observed state, and — for
   a reschedule — the previous and new `available_at`. Then commit.

**Error classification mirrors the ack path** (`resolve_ack_transaction_result`/
`reconcile_unknown_ack`), and is centralized in one function
(`classify_query_error`) used at every call site: a `pog.TransactionRolledBack`
error is already one of `submit_unique`'s own typed errors (raised directly
by the callback — `AdmissionContended` on any `55P03` in the
transaction, or `SubmissionConflict` from a `23505` on the receipt
table's primary key — a concurrent submitter winning the same
`(storage_owner, submission_id)` row after this call's own receipt lookup
found nothing. This is a _different_ race from the same-key barrier
Increment 8 forces (there, the domain lock itself serializes the receipt
lookup against a same-key concurrent submitter): a `23505` on the receipt
table can only happen between two submitters sharing the same
`SubmissionId` but a _different_ key (so they never contend the same
domain lock and both reach the receipt insert), and that specific
different-key race remains untested — see "Out of scope"). `run` reaches
pog's own transaction outcome through a dedicated FFI wrapper,
`transaction_or_checkout_failure` (`grind_postgres_ffi.erl`), which
distinguishes a checkout failure — the pool could not hand out a connection
at all, so `BEGIN` never ran; knowably not committed — from pog's own
`Result(a, pog.TransactionError(b))`. A checkout failure is
`AdmissionFailed(pog.ConnectionUnavailable)` directly, with no
`PendingSubmission` and no receipt lookup attempted. A `pog.TransactionQueryError`
(the connection was lost mid-transaction — checked out fine, so its outcome
is genuinely unknown, not knowably absent) re-reads the receipt once,
outside the transaction, on the same `Database` value's pool: a match still
resolves the call; no receipt yet, _or the lookup itself failing to reach
the store_, both return `CommitUnknown(pending)` — mirroring the
acknowledgement path's `reconcile_unknown_ack`'s `Ok(None) | Error(_) ->
QueueAckUnknown` — carrying everything needed (`storage_owner`,
`submission_id`, the worker, and the request fingerprint) to retry the same
lookup later via `reconcile_unique`. `reconcile_unique` shares that exact
same receipt-lookup function, so it has the identical fail-safe behavior: a
lookup that cannot reach the store answers "still unknown," never a
definite (and possibly wrong) answer inferred from its own failure to
check.

### PostgreSQL driver note

`pg_types` (via `pgo`, which Grind's `pog` dependency wraps) is configured to
decode `timestamp` columns as plain Unix-microsecond integers
(`application:set_env(pg_types, timestamp_config, integer_system_time_microseconds)`,
set once by `pog`'s own pool startup), but its `timestamptz` decoder
(`pg_timestampz:decode/2`) unconditionally discards that configuration and
always returns a date/time tuple instead — confirmed by reproducing the
exact `FunctionClause` crash this caused when the admission code first tried
to decode a `pog.timestamp_decoder()` result, then reading
`pg_timestampz.erl`'s source to find the discarded `TypeInfo`. `clock_timestamp()`
and `grind_jobs.available_at`/`inserted_at` are all `timestamptz`. Rather
than decode that tuple shape (undocumented as a stable `pog` contract),
every value this admission code reads back from PostgreSQL is read as a
Unix-integer `bigint` via `extract(epoch FROM ...)`, and every value it
writes converts an integer parameter back with `to_timestamp(...)`. "Now" is
sampled and round-tripped in **microseconds** (`* 1000000`), not
milliseconds, specifically so the period predicate (below) compares in the
`timestamptz` domain at full precision — a boundary test at an exact
microsecond (increment 6, not exercised by this increment) is expressible
without further precision loss, unlike an epoch-millisecond `bigint`
comparison would allow. Writing an `AvailableAt` value (only ever
millisecond-precision by construction) still uses a millisecond round-trip.
This is a pinned `pog`/`pgo`/`pg_types` version quirk, not a Grind design
choice, and is recorded here so a future upgrade attempt to decode
timestamps directly knows why the code does not.

Separately, binding a `List(String)` as a query parameter via
`pog.array(pog.text, ...)` for `state = ANY($n::text[])` **was independently
verified to work** against this same pinned `pog`/`pgo` version (confirmed
with a standalone probe query, `SELECT 'queued' = ANY($1::text[])`, returning
`Ok(Returned(1, [True]))`); an earlier draft of this contract incorrectly
attributed a crash to array binding when the actual cause was the `void`-typed
lock-acquisition decode described above.

## Schema v11

Fresh-install-only, exactly like v10. `grind_jobs` gains two nullable
columns, `unique_key_contract text` and `unique_key_sha256 bytea`, with a
check constraint requiring both null or both set (digest exactly 32 bytes),
and a non-unique partial index
`grind_jobs_unique_candidate_idx (storage_owner, worker_id, worker_version,
unique_key_contract, unique_key_sha256) WHERE unique_key_sha256 IS NOT NULL`
(a performance aid for candidate selection; PostgreSQL does not use it to
enforce anything — the advisory lock does that). A new table,
`grind_unique_submissions`, keyed `(storage_owner, submission_id)`, has no
foreign key to `grind_jobs` (matching the rest of the schema's convention of
no cross-table foreign keys); its `observed_state` column carries the same
CHECK constraint (the eleven `grind_jobs.state` values) as `grind_jobs.state`
itself. The schema marker moves from `10` to `11`.

Recognizing an existing v11 install also cheaply verifies the two
`unique_key_*` columns exist on `grind_jobs` (`read_unique_key_columns`), not
only the marker and object counts — a fail-closed check against a
partially-applied or tampered install that happens to have the right table
and marker but not the columns admission actually depends on.

**User impact: a v10 database must be reinstalled, not migrated.** `migrate`
on a genuine, never-touched v10 install (the exact object shape: four
tables, the pre-v11 attempt sequence, no `grind_unique_submissions`) now
returns `UnsupportedSchemaVersion(10)` instead of accepting it. There is no
upgrade path, and none is planned — this experimental package has never
promised one. A v11 install that is missing only `grind_unique_submissions`
(the identical object-count shape a genuine v10 install has) is distinguished
from a real legacy v10 install by its marker: `IncompatibleSchema`, not
`UnsupportedSchemaVersion(10)`, because a marker of `11` at that reduced
shape means an install was tampered with or corrupted, not that it
genuinely predates v11.

## Failure modes

- **The sampled "now" is a snapshot**, not a live boundary: a row that
  becomes eligible (crosses its period) a microsecond after this
  transaction's `now` was sampled is not seen by this call.
- **Uniqueness holds only among `submit_unique` callers.** Nothing prevents
  a plain `submit`/`submit_at` call from adding a row that a
  uniqueness-aware caller never learns about (by design — see Decision 5).
- **`storage_owner` excludes the PostgreSQL schema** (a pre-existing
  limitation shared with every other Grind admission/read path, not
  introduced here): two pools pointed at the same host/port/database but a
  different `search_path` schema are not distinguished.
- **`grind_unique_submissions` is unbounded** — nothing in this slice prunes
  or archives old receipts. A pruner is retained backlog, exactly like the
  rest of Grind's plugin/recurring-schedule surface.
- **A long-held row lock can cause false contention on reschedule.** The
  `FOR UPDATE` row lock `RescheduleScheduledTo` takes on its candidate is
  held for the rest of the admission transaction; a long-running concurrent
  transaction on that same row (e.g. an in-progress acknowledgement) can
  make an unrelated `submit_unique` call wait, though never past its own
  `unique_lock_wait` bound (Decision 6 — every query in the transaction,
  including this row lock, is classified through the same `55P03` check).
- **A process crash between committing a receipt and returning to the
  caller** is the same unavoidable class of gap documented for the ack path
  in `docs/RECOVERY-EVIDENCE.md`: the commit is durable, but the caller
  never learns the outcome from that call. `reconcile_unique` (via a
  retained `PendingSubmission`) is the recovery path — proven end to end
  against a real forced fault (a lost commit reply, an aborted commit, and a
  lost reply with the store also unavailable) in
  `docs/RECOVERY-EVIDENCE.md`, Increment 11. Only a pool closed _before_
  `submit_unique` ever sends anything does not retain a `PendingSubmission`
  — that fault is knowably not-committed (`AdmissionFailed(ConnectionUnavailable)`),
  so a plain retry of the same `SubmissionId` is the only recovery needed.
  The different-key `23505` receipt-PK race (see above) also remains
  untested.

## Out of scope (this slice)

Cross-worker uniqueness; general field replacement on conflict (Oban's
`args`/`priority`/`tags`/etc. replacement); arbitrary caller-supplied state
lists (only the four named `States` groups); unique bulk insertion; a
backend-specific optimization surface; SQLite (Grind has no SQLite storage
backend at all); the different-key `23505` receipt-PK race described above
(two submitters sharing a `SubmissionId` but not a uniqueness key, so they
never contend the domain lock). Queue scope, the state-eligibility matrix,
period-boundary timing, receipt/`SubmissionId` idempotency, concurrent
admission under a forced barrier (including its mixed-scope variant and the
receipt-ordering evidence), lock contention (including the row-lock
reschedule variant and the `lock_timeout` non-leak check), live rescheduling
races, uncertain-commit reconciliation against a real forced fault, selected
keys (including the deliberate containment-vs-equality difference for a
nested projected value), and public-API consumer coverage of admission,
existing-conflict rebinding, `SubmissionId` replay, and an across-queue
reschedule — increments 4 through 13 of the same approved plan — are now
covered; see "Status". This milestone is otherwise complete. Converging
plain `submit`/`submit_at` onto `unique.Availability` instead of
`Option(job.AvailableAt)` remains retained backlog, noted in Decision 5.
