# Isolate renewal and retain acknowledgement proposals

<a id="adr-0005"></a>

- **Decision.** Temporary attempt actors retain handler results and retry one stable acknowledgement command after known rollback or unknown reply. A separate renewer per consumer uses a reserved connection pool and skips locked rows. Pending ACKs retain local capacity and have a finite renewal lifetime.

- **Rationale.** The old coordinator serialized acknowledgements and renewal. Slow ACKs on the ordinary pool could starve healthy sibling leases. Independent renewal and attempt-owned retries protect unrelated work without allowing expired fences or reinvoking handlers.

- **Alternatives.** Multiplying lease by concurrency masks coordinator coupling and scales outage delay. Sharing the main pool leaves renewal vulnerable to saturation. Unlimited pending-result renewal would hold capacity forever; unlimited effect replay is unsafe.

- **Evidence and history.** 1e87d2c255e02b28848028a9fecdf0315bd4c129 landed ACK isolation, reserved renewal and lifetime cleanup. Prior T2 reproductions and B1–B10 harness corrections remain attributable in Git and ADR-0011 mapping. RELEASE-EXECUTION approved responsibility split is captured in the design. Current src/grind/internal/queue/{worker,renewer}.gleam and attempt.gleam govern timing; test/grind/queue/ack_failure_test.gleam and benchmark T2 regressions retain executable proof.

- **Source revisions.** [1e87d2c2](https://github.com/gleam-dream/grind/commit/1e87d2c255e02b28848028a9fecdf0315bd4c129).
