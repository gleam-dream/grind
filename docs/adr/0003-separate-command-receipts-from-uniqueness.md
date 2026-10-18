# Separate command receipts from uniqueness selection

<a id="adr-0003"></a>

- **Decision.** All submissions receive a supplied or generated receipt key. Same key and exact prepared fingerprint returns the original decision; conflicting use is refused. Uniqueness separately selects only matching retained key contracts using PostgreSQL jsonb text hashes and explicit incoming state/time policies.

- **Rationale.** Command recovery must survive state changes and expired matching windows. Different callers can have equivalent keys while their commands differ. Keeping domain locking and bounded contention prevents reporting a conflict without a persisted candidate.

- **Alternatives.** Oban Basic containment and advisory-lock miss semantics are deliberate differences. Pure canonical JSON equality would conflict with the PostgreSQL parser and numeric scale. Replacing arbitrary payloads or cross-worker typed handles needs an additional contract.

- **Evidence and history.** The original uniqueness contract decisions are captured here, and the earlier total encoders, missing plain receipts and bind-only conflicts are superseded by current code. The original uniqueness research explains the alternatives. Current src/grind/internal/unique_admission/request.gleam and query.gleam, src/grind/internal/submission.gleam, test/grind/unique provide executable evidence.
