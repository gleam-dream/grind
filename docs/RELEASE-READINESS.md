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
      lease. Replaced with a Grind-owned, validated `postgres.statement_deadline`
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

## 2b. pog upstream (fork lostbean/pog, branch feat/timeouts)

- [ ] Transaction deadline and pool-wide default query timeout in pog
      (local clone /code/gleam-dream/pog); propose upstream to lpil/pog.
- [ ] Switch Grind to the public pog API once available (git dependency on
      the fork until released), removing the FFI's reliance on pog's internal
      `Connection` shape and the exact pog pin; lift the 5 s migration cap.

## 3. Public API tidy-up (breaking after release)

- [ ] Collapse the four `queue.start*` variants.
- [ ] `resolve_uncertain` takes a record instead of positional strings.
- [ ] Replace stringly typed public fields (`AckRejection.state`,
      `QueueAckProposalCodecMismatch.kind`, `AcknowledgementReceipt.committed_at`).
- [ ] One `SubmitError` shape across `postgres` and `unique`.
- [ ] Move the internal claim/ack/renew protocol out of `postgres.gleam` into
      `grind/internal/`.
- [ ] Move test-only fault hooks out of the production coordinator.
- [ ] Test hygiene: `postgres_resolved_observation_absent_on_commit_unknown_test`
      uses fixed queue/worker names, so it fails on a reused database (the gate
      always uses a fresh cluster).
- [ ] Trim doc-comment density in `grind/internal/unique_admission.gleam`
      (comments doubled in a86e439) to match the rest of the codebase.
- [ ] Review naming (`ConsumerDrainTimedOut` after a successful stop,
      `renew_claim` error names).

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
      Sinal's tests.
- [ ] Update Oversight design docs (`sinal-design.md` forwarding decision,
      `grind-design.md` §15 observations, `API-COVERAGE.md`) — needs the
      owner's go-ahead because Oversight is a separate repository.
- [ ] Organise branch history for merge into `main`.

## Follow-ups outside Grind

- Saga, Relay and LLM Wire: switch `sinal.emit` to `sinal/forwarder`; correct
  Saga's coordinator comment that overclaims handler isolation.
