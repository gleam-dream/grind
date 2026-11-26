# Changelog

## Unreleased

This release breaks the public API and adds schema version 15. Read the [migration guide](https://github.com/gleam-dream/grind/blob/main/docs/MIGRATION.md) before upgrading from 0.2.

### Breaking

- Schema version 15 builds `grind_jobs_settle_due_idx` with `CREATE INDEX CONCURRENTLY`, outside a transaction; never wrap Grind migrations in one transaction. See [the guide](https://github.com/gleam-dream/grind/blob/main/docs/MIGRATION.md#schema-version-15-builds-an-index-concurrently).
- Grind depends on `sinal >= 0.2.0 and < 0.3.0`. See [the guide](https://github.com/gleam-dream/grind/blob/main/docs/MIGRATION.md#grind-depends-on-sinal-02).
- `abandoned.Cause` has a new variant, `HandlerCrashed`, and a crashed handler's job is held uncertain at once under `HoldUncertain`. See [the guide](https://github.com/gleam-dream/grind/blob/main/docs/MIGRATION.md#a-handler-crash-has-its-own-cause).
- `grind.SubmitError` has two new variants, `BatchRefused(position, reason)` and `BatchCommitUnknown(pending)`, both returned only by `submit_many`. See [the guide](https://github.com/gleam-dream/grind/blob/main/docs/MIGRATION.md#submiterror-has-batchrefused-and-batchcommitunknown-variants).

### Added

- `grind.submit_many` admits a list of jobs in one transaction, all or nothing, with admissions in list order. See [the guide](https://github.com/gleam-dream/grind/blob/main/docs/MIGRATION.md#submiterror-has-batchrefused-and-batchcommitunknown-variants).

### Changed

- A lease renewal that finds the lease lost stops the running handler without an acknowledgement; lease-expiry recovery records the job as `LeaseExpired`. See [the guide](https://github.com/gleam-dream/grind/blob/main/docs/MIGRATION.md#a-lost-lease-stops-the-handler).
- A consumer claims into all its free slots with one statement, starts the batch's runnable attempts first, and then parks rows whose codec contract no longer matches in one statement guarded by the claim, not by the lease. See [the guide](https://github.com/gleam-dream/grind/blob/main/docs/MIGRATION.md#other-changes).
- A restart under the same runtime name waits until the previous runtime's processes have exited. See [the guide](https://github.com/gleam-dream/grind/blob/main/docs/MIGRATION.md#other-changes).
- A crash description is at most 500 characters and 2000 bytes, prefix included, and holds the message the developer wrote for `panic`, `let assert`, `assert` or `todo`. See [the guide](https://github.com/gleam-dream/grind/blob/main/docs/MIGRATION.md#a-handler-crash-has-its-own-cause).

## 0.2.2

### Fixed

- Public calls no longer copy every registered worker into the calling process. `submit`, `submit_in`, `reconcile_submission`, the reads, `await`, `cancel`, `migrate`, the `admin` functions and `stop` each fetched the whole runtime record, which holds every queue's registry, from the runtime's process. BEAM copies a reply into its receiver, so each call copied every worker definition and everything it captures. They now receive only the database handle, or for `stop` the root supervisor and the queues' coordinator names and graces. `testing.drain` fetches its queue's workers in the process that runs the jobs, and `admin.settle_due` receives only its queue's settlements. The public API is unchanged.
