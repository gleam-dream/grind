# Pinned Oban behavioral oracle

This harness runs Oban OSS `v2.24.1` at commit
`64b8481e5383f6bc46b7a5284e4ced3202dec9d5` against the dedicated `oban_test`
database started by `scripts/test-postgres.sh`. It migrates Oban's PostgreSQL
schema, selects the Basic engine explicitly, and drives `oracle/run.exs`
end to end. It does not connect to an application database.

Run through the disposable harness with:

```sh
nix develop --command bash scripts/test-postgres.sh
```

## What `run.exs` does

`run.exs` starts one automatically-polling Oban instance (`queues: [default:
1]`) and, through it, observes:

1. **Success** — a worker is inserted with input value `41`; it returns
   `{:ok, 42}`, Oban's `[:oban, :job, :stop]` telemetry fires with event
   state `:success`, and the committed row reads `completed`.
2. **Discard** — a worker returning a business failure with `max_attempts: 1`
   is inserted, the `:discard` event fires, and the committed row reads
   `discarded`.

It then stops that instance and starts a second, manually-drained Oban
instance (`testing: :manual`, no automatic queues) to observe, without a
polling race against the first instance's queue:

3. **First retry** — a business failure with `max_attempts: 2` is drained
   once; the row commits `retryable`, attempt `1`, with a future
   `scheduled_at`.
4. **Exhaustion** — the same row is drained again with
   `with_scheduled: true`; the row commits `discarded`, attempt `2`.
5. **Snooze** — a worker that snoozes for 60 seconds is drained; the row
   commits `scheduled`, attempt stays `0`, `meta["snoozed"]` is `1`, and
   `scheduled_at` moves into the future.

Each observation is printed with `IO.inspect` and asserted against the
committed row before the harness writes its `oban-oracle-passed` marker.

The referenced upstream contracts are:

- `test/oban/queues/executor_test.exs`, "accepting :ok as a success" and
  "raising, catching and error tuples are failures".
- `test/oban/engine_test.exs`, "inserting and executing jobs", "inserting a
  single job", and "discarding jobs that exceed max attempts".
- `test/oban/engine_test.exs`, "snooze_job/3 / rolling back the attempt and
  counting snoozes".
- `lib/oban/queues/executor.ex`, `perform/1`, `normalize_state/1`, and
  `ack_event/1`; `lib/oban/telemetry.ex` documents `[:oban, :job, :stop]`
  after success is recorded.

See `ORACLE-LEDGER.md` for the full source/test mapping, evidence category,
normalization, deliberate differences, and the latest reproduced pass counts
for the root Gleam suite, the Oban oracle observations, and the external
consumer. `OBAN-LICENSE.txt` preserves the oracle's Apache-2.0 notice.
