# Keep job extensions and workflow ownership explicit

<a id="adr-0010"></a>

- **Decision.** Durable dependency jobs, batches, chunks and step-result recording stay in the retained Grind scope with native admission/fence/receipt contracts. Typed Saga progress, compensation and durable operations remain Saga-owned even when executed by a Grind worker or stored on a shared pool.

- **Rationale.** One typed boundary may reuse worker outputs without a shared erased error/retry/runtime abstraction. Running an entire local Saga in a job only makes the outer job durable; individual effects and undo progress require Saga storage and explicit recoverable operation identities.

- **Alternatives.** A generic workflow facade would merge dependency scheduling with compensation semantics and obscure replay safety. Restoring retired fabric_grind/saga_grind-style bridge packages without a concrete current port would introduce competing owners. A store engine interface or SQLite adapter without a real consumer would be speculative.

- **Evidence and history.** The original composition, child-lifecycle, workflow-boundary and shared-foundation contracts, and the interface-lab laws for worker outcomes, claim ownership, worker registry, queue configuration, scheduled jobs and unique jobs, are maintained outside this repository and retain the role boundaries. Their injected fakes and compile negatives are interface evidence, not durability evidence. Application consumers use public Grind and Saga storage directly. The full original design scope retains the independent package boundary.
