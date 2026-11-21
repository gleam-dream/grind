# Changelog

## Unreleased

### Fixed

- Public calls no longer copy every registered worker into the calling process. `submit`, `submit_in`, `reconcile_submission`, the reads, `await`, `cancel`, `migrate`, the `admin` functions and `stop` each fetched the whole runtime record, which holds every queue's registry, from the runtime's process. BEAM copies a reply into its receiver, so each call copied every worker definition and everything it captures. They now receive only the database handle, or for `stop` the queues' coordinator names and graces. `admin.settle_due` and `testing.drain` receive the one queue's plan they run. The public API is unchanged.
