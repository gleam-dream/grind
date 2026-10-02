# Changelog

All notable changes to this package are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the package
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Typed, versioned workers with JSON codecs for input, output and error
  (`grind/worker`), and heterogeneous per-queue registries
  (`grind/registry`).
- A PostgreSQL store (`grind/postgres`): a package-owned pool, schema
  migrations, admission with `submit`, `submit_at`, `submit_with_id` and
  `submit_unique`, typed `state` and `outcome` reads, cancellation, audited
  resolution of uncertain jobs, quarantine of expired attempts, and
  `prune_finished`.
- A supervised queue consumer (`grind/queue`) with bounded concurrency,
  automatic or manual polling, database-time leases renewed by a separate
  pool, fenced acknowledgement receipts, retries with a persisted attempt
  limit, snooze, discard, cancel and uncertain outcomes, and a draining
  `stop`.
- Uniqueness policies (`grind/unique`) and admission results
  (`grind/submission`).
- A supervised pruner (`grind/pruner`).
- Lifecycle and diagnostic Sinal events (`grind/observation`,
  `grind/diagnostic`) delivered through a bounded forwarder.
- Module docs for `grind`, `grind/job`, `grind/worker` and `grind/registry`,
  which had none, and rewritten module docs for `grind/queue` and
  `grind/postgres`. The root doc no longer calls the package a "serial
  consumer slice", and it gives a complete example.
- A regression test that `string.inspect` of `Settings`,
  `ValidatedSettings` and `Database` does not contain the database password.
- `worker.infallible(encode)` adapts a total gleam_json encoder to the
  fallible encoder shape `worker.codec` takes.
- `submission.InvalidInput(reason)`: every submit path (`submit`,
  `submit_at`, `submit_with_id`, `submit_unique`) returns it when the input
  codec or a `unique.selected` key codec rejects the value. It is checked
  before a connection is checked out, so no job row or receipt is written.
- `postgres.ResolutionInvalidValue(reason)`: `resolve_uncertain` returns it
  when the job's output or error codec rejects the confirmed value, before
  any write.
- `worker.ExecutedUnencodable(codec, reason)`: the handler's output or error
  was rejected by its codec after the handler ran. It commits as
  `runtime_failed`, which is terminal and not retried, with the description
  `"output codec rejected the handler's output: <reason>"` or
  `"error codec rejected the handler's error: <reason>"`.

### Changed

- `worker.codec(version, encode, decoder)` takes
  `encode: fn(value) -> Result(json.Json, String)` instead of
  `fn(value) -> json.Json`, so a validating codec (a json_blueprint codec
  with refinements) can reject a value instead of panicking or storing a
  value its decoder refuses. Wrap a plain encoder with `worker.infallible`.

- `postgres.Settings.database_url` is now `fn() -> String` instead of
  `String`, so `string.inspect`, crash reports and logger metadata no longer
  print the password in the URL. `postgres.settings(database_url)` keeps its
  signature; code that builds `Settings` directly or reads the field must
  wrap or call it (`settings.database_url()`). `ValidatedSettings` and
  `Database` also keep the pool configuration behind a closure.
- Grind builds on the Sinal wave 2 API. Applications attach to
  `grind/observation` and `grind/diagnostic` descriptors with
  `sinal.observe(event, run)`; handler ids are automatic. A closed-enum
  metadata field now also decodes an atom with the same text.

### Fixed

- `queue.Polling` and `queue.with_manual_polling` named a nonexistent
  `process_batch`; they now name `process_available`.
