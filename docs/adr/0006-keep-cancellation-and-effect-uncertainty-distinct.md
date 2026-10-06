# Decide how cancellation and effect uncertainty coexist

<a id="adr-0006"></a>

- **Decision.** Proposed; unresolved. Cancellation intent and an unknown external effect answer independent questions. Do not claim current Cancelled means effects are known absent. A representation preserving both is a candidate, not an accepted runtime change.

- **Rationale.** Current ACK SQL gives committed cancellation priority even over explicit Uncertain. It clears uncertain_at and evidence, sets finished_at, and removes the row from admin uncertainty listing. Expired-lease quarantine with cancellation instead retains Uncertain and blocks replay. The inconsistent paths lose operational evidence.

- **Alternatives.** Keep current terminal precedence and require a separate application effect ledger; preserve Uncertain while retaining cancellation intent; or extend outcome state with independent disposition and effect knowledge. Each requires explicit operator, read, telemetry and retention semantics. Merely relabeling Cancelled does not recover lost stored evidence.

- **Evidence and history.** src/grind/internal/attempt/acknowledgement.gleam acknowledgement_transaction SQL and test/grind/queue/acknowledgements_test.gleam intentionally assert cancellation precedence. The preceding ecosystem review reported a disposable public PostgreSQL consumer probe at checkout 510ca006d1af7ee35018676ee6aab026cc151b45: cancel followed by worker.Uncertain produced Cancelled and empty uncertain listing. The probe used synthetic evidence, not a real external charge, and was not installed as a retained test. The source and existing acknowledgement test retain the precedence behavior. Original policy rationale beyond cancellation precedence is unknown.

- **Source revisions.** [510ca006](https://github.com/gleam-dream/grind/commit/510ca006d1af7ee35018676ee6aab026cc151b45).
