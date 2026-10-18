# Pin released migration files by sha256

<a id="adr-0016"></a>

- **Decision.** Every released migration mirror file, which is every file except the newest, has its uppercase SHA-256 recorded in released_migration_sha256 in test/grind/migrations/conformance_test.gleam. A version is pinned once it is released, meaning merged and no longer under review. The newest version stays unpinned while it may still change.

- **Rationale.** Applications apply the mirror files directly through cigogne and depend on their bytes staying stable. A post-release edit would make two installations of the same version differ. A pinned digest turns an accidental edit into a failing test.

- **Alternatives.** Relying on version-control history detects an edit only after review. A lock file adds a second source of truth beside the test. Leaving files unpinned permits silent divergence between applied and source statements.

- **Evidence and history.** The conformance test reads the mirror files through the public cigogne parser and proves that statements, marker version and file name version stay in lockstep with the source migration list. A separate database test proves that cigogne and grind.migrate interoperate on one schema.
