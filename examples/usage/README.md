# Grind usage examples

Compiled, runnable examples of the public API, one module under `src/grind_usage/` per topic. They are the usage documentation: a changed function or default fails the build here instead of leaving stale prose.

| Module            | Shows                                                                                    |
| ----------------- | ---------------------------------------------------------------------------------------- |
| `limits`          | Every default and limit, queue tuning, and `grind.check` refusing an unsafe combination. |
| `workers`         | Typed input, output and error, a validating codec, and what a refusal does.              |
| `jobs`            | Submit, idempotent ids, scheduling, reading state and outcome.                           |
| `transactions`    | Enqueueing inside the application's transaction, and a rollback taking the job with it.  |
| `runtime`         | A supervised runtime and its named handle, a script, a submit-only node.                 |
| `handlers`        | The job context, snooze, discard and cooperative cancellation.                           |
| `handler_testing` | `testing.perform` without a database and `testing.drain` on demand.                      |
| `isolation`       | One installation per schema, and one pool shared with the application.                   |
| `observations`    | Sinal events for acknowledgements and capacity.                                          |
| `uniqueness`      | Finding an existing job and rescheduling one across queues.                              |
| `uncertain`       | Resolving an uncertain job, alone and inside a transaction.                              |
| `retention`       | Queue statistics and bounded pruning.                                                    |
| `migrations`      | `grind.migrate` as a deploy step, and cigogne sharing Grind's pool.                      |

## Run

Without a database, the `limits` and offline `handler_testing` topics run:

```sh
nix develop --command sh -c 'cd examples/usage && gleam test'
```

With PostgreSQL, set the database URL, and a second empty database for the cigogne topic:

```sh
GRIND_USAGE_DATABASE_URL=postgres://user@localhost/grind_usage \
GRIND_USAGE_CIGOGNE_DATABASE_URL=postgres://user@localhost/grind_usage_cigogne \
  gleam test        # or: gleam run
```

The database gate, `scripts/test-postgres.sh`, creates both databases, runs every topic and fails unless each one reached its last assertion. See the [design](../../docs/design/design.typ), "Usage examples".
