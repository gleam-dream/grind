# Build indexes concurrently in a step outside the version transaction

<a id="adr-0023"></a>

## Decision

- A migration version may declare concurrent indexes: indexes built with `CREATE INDEX CONCURRENTLY IF NOT EXISTS`, which PostgreSQL refuses inside a transaction. `grind.migrate` builds them before the version's transaction, on one session that holds the schema-keyed migration lock as a session lock, then applies the version's statements and marker in the transaction as before. The shape of the version lists the index, and the shape check requires every Grind index to be valid.
- An index left invalid by a failed build is dropped concurrently and built again by the next `grind.migrate`. While the lock is held no other `grind.migrate` builds the same index, so an invalid index found under the lock is never one being built.
- The session lock is taken with `pg_try_advisory_lock`, retried until the migration deadline, never with the blocking form: a session waiting in a statement holds a snapshot, and the concurrent build waits for every older snapshot, which would deadlock with the builder.
- The schema check of the preceding version accepts the next version's concurrent indexes, because they exist before that version's marker commits.
- The cigogne mirror publishes each concurrent index as its own file with the `disable_transaction` option, named after its version, before the version's file; cigogne allows one statement in such a file.
- Version 15 builds `grind_jobs_settle_due_idx` on `(queue, settle_at, id)` for uncertain jobs with a due time. The settler's take compares the due time with `statement_timestamp()` instead of `clock_timestamp()`, because PostgreSQL can bound an index scan only by a stable expression.
- This amends ADR-0021, under which version 14 added no index and the settler selected due jobs through `grind_jobs_uncertain_idx`.

## Rationale

- Every settler polls its queue's due jobs once per poll interval. Without an index on the due time, each poll read every uncertain job of the installation: about 23 ms per poll at 200,000 uncertain jobs, on every node and queue that declares a settlement. Uncertain jobs are not pruned, so the cost grows until an operator resolves them.
- A plain `CREATE INDEX` holds a lock that blocks every write to the jobs table for the whole build, which a running installation cannot accept for a table of that size. The concurrent build does not block writes, but it cannot run in a transaction.
- Keeping the marker in the version's transaction keeps the existing rule that the marker and the shape check decide whether a version applied. The index alone is idempotent and can be rebuilt; the marker is written only after the index is valid.

## Alternatives

- **Build the index inside the version transaction.** It blocks job writes for the build.
- **A marker written outside a transaction after the build.** A crash between the build and the marker would need its own recovery rule; the transaction keeps one.
- **A separate version without a marker for the index.** Every reader of the version would have to treat that version specially.
- **Leave the index to operators.** Every installation would need the same manual step, and the shape check would reject the extra relation.
