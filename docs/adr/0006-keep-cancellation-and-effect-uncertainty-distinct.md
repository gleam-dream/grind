# Keep cancellation intent and effect uncertainty as independent facts

<a id="adr-0006"></a>

- **Decision.** Treat cancellation intent and an unknown external effect as independent facts. A committed Cancelled never means that effects are known absent. The ordering between the two is decided in [ADR-0012](0012-preserve-uncertainty-through-cancellation.md).

- **Rationale.** The acknowledgement SQL gave committed cancellation priority over an explicit Uncertain. That path cleared the uncertainty timestamp and evidence, set the finished time and removed the row from the operator uncertainty listing. Expired-lease quarantine with cancellation instead retained Uncertain and blocked replay. The two paths disagreed, and the disagreeing path lost operational evidence.

- **Alternatives.** Keep terminal precedence and require a separate application effect ledger; preserve Uncertain while retaining cancellation intent; or extend outcome state with an independent disposition and effect knowledge. Each alternative needs explicit operator, read, observation and retention semantics. Relabeling Cancelled does not recover stored evidence that was already lost.

- **Evidence and history.** The acknowledgement transaction SQL in src/grind/internal/attempt/acknowledgement.gleam and test/grind/queue/acknowledgements_test.gleam asserted cancellation precedence at the time of this decision. A disposable public PostgreSQL consumer probe at checkout 510ca006d1af7ee35018676ee6aab026cc151b45 reported that cancel followed by worker.Uncertain produced Cancelled and an empty uncertain listing. The probe used synthetic evidence, not a real external charge, and was not installed as a retained test. The original policy rationale beyond cancellation precedence is unknown.

- **Source revisions.** [510ca006](https://github.com/gleam-dream/grind/commit/510ca006d1af7ee35018676ee6aab026cc151b45).
