# Release readiness

Tracks the work between the current experimental slice and a first release of
Grind, published together with its sibling libraries. Tick an item only when
its evidence is committed (tests, gate markers, and an entry in
`docs/RECOVERY-EVIDENCE.md` where behavior changes).

Status legend: `[ ]` open, `[~]` in progress, `[x]` done (commit hash).

## 1. Contract decisions (decided 2026-09-25)

- [x] **Old-version executing rows** (a86e439). A consumer's scan
      quarantines expired executing rows for every worker and version in the
      queues it polls; public `postgres.quarantine_expired(database, limit:)`
      sweeps queues no consumer polls. Evidence: `docs/RECOVERY-EVIDENCE.md`,
      Increment 14.
- [x] **Retry-safe plain submit** (a86e439). `postgres.submit_with_id` shares
      the unique-admission receipt, fingerprint and reconciliation through one
      request with an optional uniqueness policy; ID-less `submit`/`submit_at`
      document that a failed call may have committed. Evidence:
      `docs/RECOVERY-EVIDENCE.md`, Increment 14; `docs/UNIQUENESS-CONTRACT.md`,
      "Admission receipts".
- [x] **Acknowledgement deadline** (a8feb68). A real TCP fault proxy
      (`test/grind_fault_proxy.erl`) proves the code as found _was_ bounded
      (~5000ms, pog's own hardcoded checkout deadline) for a dropped `COMMIT`
      reply, a dropped request, a dropped `BEGIN` reply, and a stalled
      lease-renewal `UPDATE` (T1–T5) — but only as a side effect of a
      dependency's internals, never configurable or validated against the
      lease. Replaced with a Grind-owned, validated `postgres.with_statement_deadline`
      (default 4000ms) every storage call now goes through
      (`src/grind_postgres_ffi.erl`), plus `queue.LeaseTooShortForDeadline`
      validating the lease against it. Mutation-proven (checkout timeout
      forced to `infinity`: T1/T4 genuinely unbounded; T2/T3/T5 still bounded
      via the independent DEFECT-3 fix below, not the checkout deadline).
      Evidence: `docs/RECOVERY-EVIDENCE.md`, Increment 15.
- [x] **Migration mechanism** (1fb3c5d). Versioned, advisory-locked,
      per-step migrations with exact declared shapes; cigogne-format files in
      `priv/migrations`; upgrade harness from a frozen v11 fixture. Evidence:
      `docs/RECOVERY-EVIDENCE.md`, Increment 16.
- [ ] Migration gaps: an end-to-end test that cigogne applies Grind's files
      and `migrate` is then a no-op; an upgrade-harness `reconcile_unique` on
      a genuine lost reply.

### Defects found while designing the deadline (fix with it)

- [x] (a8feb68) Default `unique_lock_wait` (5000 ms) equalled pgo's fixed 5000 ms
      transaction checkout deadline, so contention could surface as
      `CommitUnknown`/`AdmissionFailed(QueryTimeout)` instead of
      `AdmissionContended`. `postgres.validate` now rejects
      `unique_lock_wait_ms + 1000 >= statement_deadline_ms`
      (`UniqueLockWaitTooCloseToDeadline`); default lowered to 2000 ms.
      Red-then-green mutation evidence: `docs/RECOVERY-EVIDENCE.md`,
      Increment 15 (DEFECT 1).
- [x] (a8feb68) pog's `convert_error` raises `function_clause` on some pgo error shapes
      (e.g. "deadline reached while in queue", `econnreset`, `etimedout`).
      Fixed structurally for the checkout-time shape (Grind's own wrapper
      never lets pog run its own checkout any more) and defensively for the
      post-checkout query shape (`guarded_query`/`guarded_transaction` catch
      `error:function_clause`). Attempted but did not reproduce the exact
      crash empirically (pgo's own overload shedding returns an
      already-handled error first under ordinary contention) — fixed on
      source-level confirmation of the missing clause, not a red test.
      Evidence and the reproduction attempt: `docs/RECOVERY-EVIDENCE.md`,
      Increment 15 (DEFECT 2 probe).
- [x] (a8feb68) A lost `COMMIT` request leaves a server session idle in transaction
      holding row locks. `postgres.validate` now sets
      `idle_in_transaction_session_timeout` to `2 × statement_deadline_ms`
      (8000 ms by default) as a connection startup parameter. Mutation-proven
      as an independent backstop from the checkout deadline (T2/T3/T5 in
      Increment 15 still converge with the checkout deadline disabled).
      TCP keepalive is a real, complementary defense for a genuine network
      partition, not exercised (loopback only) — documented, not fixed here.
- [x] (a8feb68) Migrations share the 5000 ms deadline; a longer DDL step would fail.
      Fixed as a side effect of the Grind-owned deadline: `migrate` now uses
      its own `postgres.migration_deadline_ms` (default 30000 ms) instead of
      the shared per-pool deadline. The migration _mechanism_ itself
      (Grind-owned versioned `.sql` files) is still the separate, tracked
      item above — out of scope for this run.
- [x] Flaky under load: `postgres_submit_unique_aborted_commit_is_commit_unknown_test`
      (`QueryTimeout` from the same 5000 ms deadline). Root cause was never
      the deadline directly — a transient pool-recovery window right after
      the test's own deliberate `pg_terminate_backend`, already tolerated
      elsewhere in the suite via `retry_transient_query` but missing here;
      wrapped the two exposed calls in it. The same gap, independently
      found, was also fixed in
      `postgres_resolved_observation_absent_on_commit_unknown_test`.
      Confirmed deterministic across multiple fresh-cluster reruns. Evidence:
      `docs/RECOVERY-EVIDENCE.md`, Increment 15.
- [x] (a8feb68) Known limit, documented: several pending acknowledgements can still
      starve sibling renewals even once `queue.LeaseTooShortForDeadline`
      passes; the real fix is renewals off the coordinator loop (not
      attempted here). Documented in README ("Guarantees") and
      `docs/RECOVERY-EVIDENCE.md`, Increment 15.

## 2. Sinal release readiness

- [x] Bounded emitter-side forwarder merged to Sinal `master` (355617c).
- [ ] Sinal version, CHANGELOG, and publish metadata.

- [ ] Close the forwarder test gaps: single duplicate-drop guard unobservable;
      restart race only stress-tested.
- [ ] Grind depends on a published Sinal version range instead of `../sinal`.

## 2b. pog dependency (fork dropped; Grind-owned checkout restored)

- [x] **Decision (user, superseding the 5701ede/Increment 17 fork migration):**
      drop the `lostbean/pog` git dependency entirely. Grind depends on
      vanilla `pog` from Hex (`>= 4.1.0 and < 4.2.0`), with `pgo` also
      declared directly and pinned just as tightly (`>= 0.20.0 and < 0.21.0`)
      since `grind_postgres_ffi.erl` calls straight into it. No PRs upstream
      are needed for Grind's own release; #85/#86/#87 against lpil/pog are no
      longer blocking anything here (left open only if the pog maintainer
      still wants the upstream discussion).
- [x] **Release blocker resolved:** nothing but Hex dependencies remains in
      `gleam.toml`/`consumer/gleam.toml` — the git dependency that previously
      blocked Hex publication is gone.
- [x] Restored: Grind's own bounded checkout in `grind_postgres_ffi.erl`
      (`with_deadline`/`with_deadline_ms`, a `pgo:checkout/2` with an explicit
      `timeout`), `Settings.statement_deadline_ms`/`migration_deadline_ms`
      with their own setters and validation, and the per-pool
      `persistent_term` deadline set in `postgres.start` and cleared in
      `postgres.close`. `docs/RECOVERY-EVIDENCE.md` has the before/after
      fault-proxy numbers for this reversal.
- [ ] **Remaining risk, accepted:** this couples Grind directly to pog's
      private `Connection` shape (`{pool, Name} | {single_connection, Conn}`)
      and to `pgo`'s own `checkout`/`checkin`/`break` API, neither of which
      pog's public contract promises to keep stable. Guarded two ways: the
      exact version pins above (a routine minor/patch bump cannot silently
      change either shape without also bumping past the pin), and
      `pog_connection_pool_shape_test` (`test/grind_test.gleam`), which fails
      loudly the moment either shape does change instead of this module
      silently mismatching it.
- [x] Pool name lifetime (plan commit 15, see below): `postgres.validate`
      creates the pool's name once, and every `start` of that same
      `ValidatedSettings` value reuses it — not fresh per `start` — so a
      close/reopen cycle against the same validated settings reopens under
      the same name a still-running `Consumer` is already addressing. The
      `persistent_term` deadline entry `set_deadline` attaches to that name
      is erased by a matching `postgres.close`; a process that starts a
      `Database` and never closes it (or crashes before closing) still
      leaks one small entry per distinct `ValidatedSettings` value that was
      ever started — down from one per `start` call before this commit, and
      down from one per `start` call under the pre-fork design too, since a
      caller reusing the same `ValidatedSettings` for repeated
      close/reopen cycles now shares one name across all of them.
- [ ] Known, pre-existing behaviors unrelated to the above: (a) pog's
      transaction crash cleanup uses `let assert` on its rollback, so a
      crash during `COMMIT` followed by a failed rollback crashes the
      caller instead of returning an error. (b) a checkout that has to
      queue behind other contended callers is bounded by `pgo_pool`'s own
      CoDel-style overload shedding, not by `D` alone.

## 3. Public API tidy-up (breaking after release)

- [x] Collapse the four `queue.start*` variants (294e67b). One
      `queue.start(database, workers, policy: ValidatedPolicy)`, keyed on a
      `Polling` type (`PollEvery(interval_ms:) | Manual`) instead of a
      separate boolean argument.
- [x] `resolve_uncertain` takes a record instead of positional strings
      (47396c9).
      `ResolutionRequest(resolution_id:, resolved_by:, details:, decision:)`.
- [x] Replace stringly typed public fields (`AckRejection.state`,
      `QueueAckProposalCodecMismatch.kind`, `AcknowledgementReceipt.committed_at`)
      (5f95e08, a9a1c7a). `AckOwnershipChanged`/`AckRecordChanged` dropped
      their redundant `state` field entirely; `AckStateChanged.state` is
      `job.State`; `QueueAckProposalCodecMismatch`/`QueueCodecMismatch.kind`
      is the new `worker.CodecKind` (moved from `observation.CodecKind`,
      which described a worker concept, not an observation one);
      `AcknowledgementReceipt.committed_at: String` (an ISO-8601 `to_char`
      rendering) is now `committed_at_unix_ms: Int`.
- [x] One `SubmitError` shape across `postgres` and `unique` (a6b2f3e).
      `postgres.SubmitError` is gone; plain `submit`/`submit_at` return
      `unique.SubmitError` too, with their own uncertain-outcome case
      renamed `CommitUnknownWithoutId`.
- [x] Move the internal claim/ack/renew protocol out of `postgres.gleam`
      into `grind/internal/` (33ce119). `grind/internal/attempt`
      (claim/renew/acknowledge) and `grind/internal/lease` (lease-fencing
      predicates, quarantine scan).
- [x] Move test-only fault hooks out of the production coordinator
      (8efa554). `grind/internal/consumer_hooks.Hooks`, supplied once at
      start time (`@internal queue.start_with_hooks`), replaces the two
      mid-flight `InjectWorkerStartFailure`/`KillNextWorkerBeforeMonitor`
      messages.
- [x] Test hygiene: `postgres_resolved_observation_absent_on_commit_unknown_test`
      uses fixed queue/worker names, so it fails on a reused database (the
      gate always uses a fresh cluster) (ceeaa03). Suffixed the queue name
      and worker id with the test's own already-computed per-run suffix.
- [x] Trim doc-comment density in `grind/internal/unique_admission.gleam`
      (comments doubled in a86e439) to match the rest of the codebase
      (08ed52c).
- [x] Review naming (`ConsumerDrainTimedOut` after a successful stop,
      `attempt.renew` error names) (81f8fb3, 4d37cfe). `ConsumerDrainTimedOut`
      is `StopOutcome.StoppedDrainUnconfirmed`; `attempt.renew` returns
      `Renewal { Renewed LeaseLost }` instead of a bare `Bool`.
- [x] `grind/submission` module (user-approved optional item O1, plan commit
      13). `SubmissionId`/`submission_id`/`submission_id_value`,
      `Availability`, `Admission`/`Conflict` (+accessors), `PendingSubmission`
      (+accessors), and `SubmitError` move out of `grind/unique` into a new
      `grind/submission` module; `EmptySubmissionId` becomes its own
      `SubmissionIdError` there. `grind/unique` keeps only the uniqueness
      policy vocabulary (`Key`, `Policy`, `States`, `Period`, `QueueScope`,
      `ConflictAction`) — these types apply to plain `submit`/`submit_at`
      too, which never touch a uniqueness policy at all.
- [x] One `JobReadError` (user-approved optional item O4, plan commit 14)
      for `bind_handle`/`arguments`/`state`/`outcome`/
      `reconcile_acknowledgement`, replacing five separate, inconsistently
      shaped error types (`HandleBindError`, `ArgumentError`, `StateError`,
      `OutcomeError`, `AckReconciliationError`). Every queue/worker-contract
      mismatch across all five now carries the same `expected`/`actual`
      payload (several were previously bare); each function's own doc
      comment says exactly which variants it can and cannot return.
      `ExecutedBusinessFailure.cause` is now typed as
      `worker.BusinessFailureCause` instead of a raw `String` (O5); the
      identical `worker.FailureCause` merges into it — moved to
      `grind/worker` rather than `grind/job` as O5 originally named it,
      since `grind/job` already depends on `grind/worker` for `Codec`/
      `Worker` and the reverse would cycle. One canonical
      `worker.business_failure_cause_to_string`/`_from_string` pair replaces
      three separate ad hoc strings-to-enum mappings that had accumulated in
      `grind/postgres`, `grind/observation`, and `grind/internal/attempt`.
- [x] Observation names (user-approved optional item O6, plan commit 16). `[grind, job, cancellation]` → `[grind, job, cancellation_decided]`, `[grind, job, contract_mismatch]` → `[grind, job, contract_mismatch_recorded]` — both the telemetry event name and the `grind/observation` descriptor function (`cancellation`/`contract_mismatch` → `cancellation_decided`/`contract_mismatch_recorded`), matching the past-participle naming
      every other event already uses (`acknowledged`, `admitted`, `claimed`,
      `quarantined`, `resolved`, `released`). `CancellationOutcome`'s
      constructors drop their redundant `Outcome` suffix
      (`CancelledBeforeRunOutcome`/`CancellationRequestedOutcome` →
      `CancelledBeforeRun`/`CancellationRequested` — the type name already
      says "outcome"). The `Measurements`/`Metadata` type names themselves
      are unchanged, matching the plan's scope of "event names and
      descriptor functions" only.
- [x] Settings cleanup (user-approved optional items O2+O3, plan commit 15).
      `postgres`'s setters are renamed `with_*` to match `queue`'s own
      (`pool_size`/`unique_lock_wait`/`statement_deadline`/
      `migration_deadline`/`observation_capacity` →
      `with_pool_size`/`with_unique_lock_wait`/`with_statement_deadline`/
      `with_migration_deadline`/`with_observation_capacity`). `Settings` no
      longer carries `process.Name(pog.Message)` at all — `postgres.settings`
      takes only a database URL now; the pool's own name is created once
      inside `validate` (not accepted from the caller, and not fresh on
      every `start` — see "2b" above for why that distinction matters).
      Every test that reconstructed a raw connection via
      `pog.named_connection(pool_name)` now uses the existing `@internal`
      `postgres.connection(database)` instead, since the caller no longer
      holds a name to reconstruct one from.

## 4. Evidence still missing

- [ ] Network faults through a TCP proxy (half-open socket, partition), beyond
      backend termination.
- [ ] Load: throughput/latency at concurrency > 1, several consumers per
      database, polling cost and lock contention.
- [ ] Multi-node: two BEAM nodes on the same queues, killed mid-job.
- [ ] Soak: atoms, timers, forwarder mailbox, pool connections over time.
- [ ] Decide on per-attempt storage calls (post-release candidate, internal):
      move lease renewal and acknowledgement from the queue coordinator into
      each attempt's worker process so a hung call stalls one job, not its
      siblings. Trade-offs: more pool connections, new stop/drain and
      renewal-vs-quarantine races, per-attempt observation ordering. Decide
      with load-test evidence on renewal starvation and coordinator throughput.

## 5. Packaging and documentation

- [ ] Hex metadata; license and Apache-notice review for Oban-derived material.
- [ ] Getting-started guide built on the consumer package; generated API docs.
- [ ] CI workflow running `scripts/test-postgres.sh`, `nix flake check`, and
      Sinal's tests. Must fetch dependencies fresh: a cached `build/`
      directory carried over from before the pog fork was dropped once
      silently kept the old fork's compiled artifacts in play even after
      `gleam.toml`/`manifest.toml` moved to vanilla Hex `pog`. Do not reuse a
      persistent `build/` cache across CI runs unless it is keyed on
      `manifest.toml`'s own hash. `scripts/test-postgres.sh` now guards this
      directly: it refuses to run if either manifest resolves `pog` from
      anything but Hex, and unconditionally runs `gleam clean` in both the
      root and consumer projects before compiling — plain
      `gleam deps download` does not help here, since it trusts
      `packages.toml`'s own record of what is already downloaded, not the
      package directories on disk. This costs a full rebuild on every gate
      run; accepted as the simplest guard that cannot itself drift from how
      `gleam` tracks its own cache.
- [ ] Update Oversight design docs (`sinal-design.md` forwarding decision,
      `grind-design.md` §15 observations, `API-COVERAGE.md`) — needs the
      owner's go-ahead because Oversight is a separate repository.
- [ ] Organise branch history for merge into `main`.

## Follow-ups outside Grind

- Saga, Relay and LLM Wire: switch `sinal.emit` to `sinal/forwarder`; correct
  Saga's coordinator comment that overclaims handler isolation.
