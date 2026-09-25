# Release readiness

Tracks the work between the current experimental slice and a first release of
Grind, published together with its sibling libraries. Tick an item only when
its evidence is committed (tests, gate markers, and an entry in
`docs/RECOVERY-EVIDENCE.md` where behavior changes).

Status legend: `[ ]` open, `[~]` in progress, `[x]` done (commit hash).

## 1. Contract decisions (decided 2026-09-25)

- [ ] **Old-version executing rows.** Consumers quarantine expired executing
      rows for every worker and version in the queues they poll; add a public
      global `quarantine_expired` operation for queues no consumer polls.
      Quarantine never runs code, so no codec is needed.
- [ ] **Retry-safe plain submit.** Optional caller-supplied `SubmissionId` on
      plain submit, reusing the unique-admission receipt, fingerprint and
      reconciliation (no uniqueness policy). The ID-less `submit` stays and is
      documented as "may have committed on error; do not blindly retry".
- [ ] **Acknowledgement deadline.** Verify with a TCP-proxy fault test whether
      pog/pgo bounds a `COMMIT` on a half-open socket. If bounded: document the
      bound and validate that the lease exceeds it. If not: add a client-side
      per-statement deadline that feeds the existing `QueueAckUnknown` retry.
- [ ] **Migration mechanism.** Grind-owned versioned `.sql` migrations applied
      by `postgres.migrate` (one transaction per step, advisory lock against
      concurrent migrators, fail-closed checks kept), written in cigogne's file
      format so applications may import them. Add an upgrade-test harness
      (install previous release schema, migrate, run the gate) before v12.

## 2. Sinal release readiness

- [x] Bounded emitter-side forwarder merged to Sinal `master` (355617c).
- [ ] Sinal version, CHANGELOG, and publish metadata.
- [ ] Close the forwarder test gaps: single duplicate-drop guard unobservable;
      restart race only stress-tested.
- [ ] Grind depends on a published Sinal version range instead of `../sinal`.

## 3. Public API tidy-up (breaking after release)

- [ ] Collapse the four `queue.start*` variants.
- [ ] `resolve_uncertain` takes a record instead of positional strings.
- [ ] Replace stringly typed public fields (`AckRejection.state`,
      `QueueAckProposalCodecMismatch.kind`, `AcknowledgementReceipt.committed_at`).
- [ ] One `SubmitError` shape across `postgres` and `unique`.
- [ ] Move the internal claim/ack/renew protocol out of `postgres.gleam` into
      `grind/internal/`.
- [ ] Move test-only fault hooks out of the production coordinator.
- [ ] Review naming (`ConsumerDrainTimedOut` after a successful stop,
      `renew_claim` error names).

## 4. Evidence still missing

- [ ] Network faults through a TCP proxy (half-open socket, partition), beyond
      backend termination.
- [ ] Load: throughput/latency at concurrency > 1, several consumers per
      database, polling cost and lock contention.
- [ ] Multi-node: two BEAM nodes on the same queues, killed mid-job.
- [ ] Soak: atoms, timers, forwarder mailbox, pool connections over time.

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
