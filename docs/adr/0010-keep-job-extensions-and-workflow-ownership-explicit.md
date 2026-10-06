# Keep job extensions and workflow ownership explicit

<a id="adr-0010"></a>

- **Decision.** Durable dependency jobs, batches, chunks and step-result recording stay in the retained Grind scope with native admission/fence/receipt contracts. Typed Saga progress, compensation and durable operations remain Saga-owned even when executed by a Grind worker or stored on a shared pool.

- **Rationale.** One typed boundary may reuse worker outputs without a shared erased error/retry/runtime abstraction. Running an entire local Saga in a job only makes the outer job durable; individual effects and undo progress require Saga storage and explicit recoverable operation identities.

- **Alternatives.** A generic workflow facade would merge dependency scheduling with compensation semantics and obscure replay safety. Restoring retired fabric_grind/saga_grind-style bridge packages without a concrete current port would introduce competing owners. A store engine interface or SQLite adapter without a real consumer would be speculative.

- **Evidence and history.** oversight/research/composition-contracts.md, child-lifecycle-contract.md, workflow-boundaries.md and shared-foundations.md; interface_lab WORKER-OUTCOMES, CLAIM-OWNERSHIP, WORKER-REGISTRY, QUEUE-CONFIGURATION, SCHEDULED-JOBS and UNIQUE-JOBS retain role boundaries. Their injected fakes/compile negatives are interface evidence, not durability evidence. Current oversight checkout and research_agent consumers use public Grind and Saga storage directly. Full original grind-design scope and release API DECISIONS/PLAN Round 9+ retain the independent package boundary.
