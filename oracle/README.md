# Pinned Oban behavioral oracle

This harness runs Oban OSS `v2.24.1` at commit
`64b8481e5383f6bc46b7a5284e4ced3202dec9d5` against the dedicated `oban_test`
database started by `scripts/test-postgres.sh`. It migrates Oban's PostgreSQL
schema, selects the Basic engine explicitly, starts a one-slot `default` queue,
inserts one successful worker and one max-attempts-one business failure, waits
for Oban's post-ack telemetry event, then reads each committed row. It does not
connect to an application database.

Run through the disposable harness with:

```sh
nix develop --command bash scripts/test-postgres.sh
```

The runner supplies `GRIND_OBAN_TEST_DATABASE_URL`, fetches the exact Git commit
and locked Hex dependencies, then runs `mix run run.exs` from this directory.
The last successful isolated run used the command above. It reported PostgreSQL
`16.15`, Elixir `1.18.5`, OTP `28`, and Postgrex `0.22.4`; Gleam reported
`12 passed, 0 failures`, the Oban harness wrote its success marker, and the
separate public-import consumer reported `3 passed, 0 failures`. The last
recorded cluster used port `24342`. Oban
returned `{:ok, 42}` with event state `:success` and committed `completed`,
attempt `1`, queue `default`; Grind's committed success test observes
`SucceededWith("42")`, state `succeeded`, attempt `1`, queue `default`. This is a
differential observation after normalizing `completed` to Grind's `succeeded`
and retaining the intentional integer-versus-JSON-string typed output
difference. Oban returned `{:error, "business failure"}` with event state
`:discard` and committed `discarded`, attempt `1`. Grind's separate typed-error
test reads `AccountMissing(42)` as `BusinessFailedWith` and committed state
`business_failed`. Both now run successfully, but the inputs and terminal-state
policy differ, so the failure tests are classified as inspired rather than a
differential pair.

The harness observes only public Oban insertion, queue processing, telemetry, and
the committed job row. The referenced upstream contracts are:

- `test/oban/queues/executor_test.exs`, “accepting :ok as a success” and “raising,
  catching and error tuples are failures”.
- `test/oban/engine_test.exs`, “inserting and executing jobs” and
  “inserting a single job”.
- `lib/oban/queues/executor.ex`, `perform/1`, `normalize_state/1`, and `ack_event/1`;
  `lib/oban/telemetry.ex` documents `[:oban, :job, :stop]` after success is recorded.

See `ORACLE-LEDGER.md` for source/test mapping, evidence category, normalization,
and the verified or pending status of each behavior. The Gleam success assertion
is inspired by the cited Oban behavior and implements the original Grind
typed-result contract; it is not a faithful port of Oban's executor return shape
or a Pro-equivalence claim. `OBAN-LICENSE.txt` preserves the oracle's Apache-2.0
notice.
