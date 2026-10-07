# Preserve effect uncertainty independently of cancellation

<a id="adr-0012"></a>

- **Decision.** Accepted. Preserve Uncertain, exact effect evidence, attempt identity and cancellation intent when explicit uncertainty and cancellation occur in either order. Reuse the existing state, cancellation timestamp and resolution APIs. Pending cancellation forbids AuthorizeReplay. Attributed ConfirmSuccess or ConfirmFailure settles the job. No schema migration or public outcome variant is required.

- **Rationale.** Cancellation intent cannot establish the result of an external effect. Preserving Uncertain makes explicit acknowledgements consistent with expired-lease quarantine and protects evidence from ordinary retention. Keeping cancellation intent prevents the correction from silently permitting another attempt. The application effect ledger remains authoritative for financial intent and reconciliation.

- **Alternatives.** Keep current terminal precedence and require a separate application effect ledger; preserve Uncertain while retaining cancellation intent; or extend outcome state with independent disposition and effect knowledge. The existing state plus cancellation timestamp is sufficient for the demonstrated recovery decisions. Independent public disposition/effect types would add states without a demonstrated caller need. Retaining terminal precedence loses evidence and is rejected. Ordinary queued cancellation and cancellation precedence over non-uncertain acknowledgements are unchanged.

- **Supersedes.** Resolves the proposed ruling in [ADR-0006](0006-keep-cancellation-and-effect-uncertainty-distinct.md).

- **Evidence and history.** Before this correction, src/grind/internal/attempt/acknowledgement.gleam acknowledgement_transaction SQL and test/grind/queue/cancellation_test.gleam intentionally required cancellation precedence. The preceding ecosystem review reported a disposable public PostgreSQL consumer probe at checkout 510ca006d1af7ee35018676ee6aab026cc151b45: cancel followed by worker.Uncertain produced Cancelled and empty uncertain listing. The probe used synthetic evidence, not a real external charge, and was not installed as a retained test. The cited revision retained that precedence behavior. The retained public regression now checks both orderings, and the acknowledgement fault test checks committed reply loss without a handler rerun. Original policy rationale beyond cancellation precedence is unknown.

- **Source revisions.** [510ca006](https://github.com/gleam-dream/grind/commit/510ca006d1af7ee35018676ee6aab026cc151b45).

- **Compatibility.** Existing rows remain readable. Historically cleared uncertainty evidence cannot be reconstructed from acknowledgement fingerprints. Deployments must reconcile such cases from their application effect records. Cancellation on an already-Uncertain row now records intent while continuing to return AlreadyUncertain; operators must confirm its result instead of authorizing replay.
