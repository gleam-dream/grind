# Separate command receipts from uniqueness selection

<a id="adr-0003"></a>

- **Decision.** All submissions receive a supplied or generated receipt key. Same key and exact prepared fingerprint returns the original decision; conflicting use is refused. Uniqueness separately selects only matching retained key contracts using PostgreSQL jsonb text hashes and explicit incoming state/time policies.

- **Rationale.** Command recovery must survive state changes and expired matching windows. Different callers can have equivalent keys while their commands differ. Keeping domain locking and bounded contention prevents reporting a conflict without a persisted candidate.

- **Alternatives.** Oban Basic containment and advisory-lock miss semantics are deliberate differences. Pure canonical JSON equality would conflict with the PostgreSQL parser and numeric scale. Replacing arbitrary payloads or cross-worker typed handles needs an additional contract.

- **Evidence and history.** docs/UNIQUENESS-CONTRACT.md Decisions 1–10 captured here, with stale total encoders/no plain receipts/bind-only conflicts superseded by current code; research/grind-uniqueness-contract.md and interface_lab/UNIQUE-JOBS.md explain original alternatives. Current src/grind/internal/unique_admission/request.gleam and query.gleam, src/grind/internal/submission.gleam, test/grind/unique provide executable evidence.
