# Grind

Grind runs typed background jobs on PostgreSQL and Erlang/OTP. Workers define
input, output and error types with JSON codecs; Grind stores admissions and
outcomes and runs handlers through a supervised queue runtime.

## Install

Grind is not yet published to Hex. Use the current checkout as a local path
dependency, with [Sinal](https://github.com/gleam-dream/sinal) checked out beside
Grind at `../sinal`:

```toml
[dependencies]
grind = { path = "../grind" }
gleam_stdlib = ">= 0.70.0 and < 2.0.0"
gleam_erlang = ">= 1.0.0 and < 2.0.0"
gleam_otp = ">= 1.0.0 and < 2.0.0"
gleam_json = ">= 3.1.0 and < 4.0.0"
gleam_time = ">= 1.0.0 and < 2.0.0"
exception = ">= 2.1.1 and < 3.0.0"
pog = ">= 4.1.0 and < 4.2.0"
```

The Erlang target requires Gleam 1.18 or later and PostgreSQL. Development uses
OTP 28 and PostgreSQL 16 through `nix develop`. The [package manifest](gleam.toml)
records the dependency ranges, including the pinned PostgreSQL driver versions.

## Quick start

Define a worker, add Grind to the application's supervision tree, and submit
through its registered name. This example uses application-owned `Email` values
and simulates delivery by returning a message ID. Pass your `pog.Config` to
`children`; the application supervisor owns startup, restart and shutdown.
For a script, `run_once` starts the runtime and calls `grind.stop` on exit.

```gleam
import exception
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import gleam/otp/static_supervisor as supervisor
import gleam/time/duration
import grind
import grind/job
import grind/worker
import pog

pub type Email {
  Email(to: String, subject: String)
}

fn encode_email(email: Email) -> json.Json {
  json.object([
    #("to", json.string(email.to)),
    #("subject", json.string(email.subject)),
  ])
}

fn email_decoder() -> decode.Decoder(Email) {
  use to <- decode.field("to", decode.string)
  use subject <- decode.field("subject", decode.string)
  decode.success(Email(to:, subject:))
}

pub fn mailer() -> worker.Worker(Email, String, Nil) {
  worker.new(
    "mailer.send",
    input: worker.codec(worker.infallible(encode_email), email_decoder()),
    output: worker.codec(worker.infallible(json.string), decode.string),
    perform: fn(email) { Ok("msg:" <> email.to) },
  )
  |> worker.with_queue("mailers")
}

/// One child in the application's supervision tree migrates the schema,
/// then runs the pool, one consumer per queue and the pruner.
pub fn children(pool: pog.Config, name: process.Name(grind.Message)) {
  let config =
    grind.new(pool)
    |> grind.with_worker(mailer())
    |> grind.with_startup_migration
  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(grind.supervised(config, name))
}

/// Anywhere in the application: a handle found by name.
pub fn send(name: process.Name(grind.Message), email: Email) {
  let jobs = grind.named(name)
  let assert Ok(admission) = grind.submit(jobs, job.new(mailer(), email))
  grind.await(jobs, grind.handle(admission), within: duration.seconds(5))
  // Ok(grind.Succeeded("msg:a@b.c"))
}

/// A script owns its runtime and stops it when this call finishes or raises.
pub fn run_once(pool: pog.Config, email: Email) {
  let name = process.new_name("mailer_jobs")
  let config =
    grind.new(pool)
    |> grind.with_worker(mailer())
    |> grind.with_startup_migration
  let assert Ok(jobs) = grind.start(config, name)
  use <- exception.defer(fn() { grind.stop(jobs) })
  send(name, email)
}
```

The [compiled quick-start test](test/grind/facade/readme_test.gleam) retains the
worker, codecs and supervised example. The [separate consumer](consumer/README.md)
exercises public APIs, typed failures, configuration and recovery.

`submit` can fail before admission or return `CommitUnknown(pending)` after a
lost reply. Retain that pending command and use `grind.reconcile_submission`.
The example asserts successful startup and admission; application code can use
`grind.submit_error_kind` and `grind.describe_submit_error` to handle failures.
A five-second `await` can return `Pending`; it does not cancel the job, and its
last storage read can extend beyond the wait budget.

## Runtime and recovery

Grind owns the main pool and its supervised processes. Each used queue adds one
reserved renewal connection. Applications can borrow `grind.connection(jobs)`;
the pool remains valid only while Grind owns it. A runtime added with
`grind.supervised` shuts down through the application supervisor. A runtime
started with `grind.start` must be stopped with `grind.stop`.

Defaults include ten slots per queue per node, a thirty-second renewable lease,
a fifteen-minute handler timeout, twenty business attempts, a 1 MiB payload
limit, and pruning of finished jobs after seven days. The four-second storage
deadline does not hard-bound a queued pool checkout. See the
[full defaults](docs/USAGE.md#defaults) and [deadline limits](docs/USAGE.md#deadlines-and-capacity).

An abandoned attempt is held `Uncertain` by default. Investigate the external
effect before confirming an outcome or authorizing replay with `grind/admin`.
Cancellation is cooperative and cannot retract an effect. Explicit uncertainty
retains its evidence and cancellation intent in either order. Pending
cancellation forbids replay but permits attributed terminal confirmation. Receipt
recovery and submission deduplication end when the job is pruned. See
[operations](docs/OPERATIONS.md) for these recovery limits.

## Business transactions

`admin.resolve_uncertain_in(jobs, tx, handle, resolution)` stages the same
attributed resolution inside an application's open READ COMMITTED transaction.
Use it when queue resolution and an application acknowledgment must commit
together. It returns `Staged(Applied(state))` or `Staged(AlreadyApplied(state))`;
the caller still owns commit, rollback and recovery if the commit reply is lost.

Keep provider investigation outside that transaction. Resolution caps PostgreSQL
lock and statement timeouts by `with_statement_deadline`, preserves stricter
caller settings, and restores the caller's settings after successful statements.
Locks remain held until the outer transaction ends. A borrowed connection adds
no checkout or network deadline, and staging emits no committed-resolution event.
Propagate errors so the transaction owner rolls back all staged writes.

The [separate public consumer](consumer/test/grind_consumer/resolution_transaction_test.gleam)
demonstrates rollback, exact-command retry, cancellation, concurrent resolution,
and durable application acknowledgment after lost commit replies and pruning.
No migration is required. Existing missing history still requires investigation.

`grind.submit_in(jobs, tx, job)` stages admission in the application's open
READ COMMITTED transaction. The application owns business invariants, account
locking, commit, and reconciliation after a lost commit reply. Grind rejects
SERIALIZABLE and REPEATABLE READ transactions; it never lowers their isolation.
Keep the isolation your application requires. An application outbox can commit
financial intent and a dispatch record together, then submit with a stable job
identity in a separate transaction. See [transaction guidance](docs/USAGE.md#one-pool-and-enqueueing-inside-your-transaction)
and the [financial recovery consumer](https://github.com/gleam-dream/oversight/tree/master/apps/financial_recovery).

## Migrations

`grind.with_startup_migration` migrates before consumers start. Leave it off for
runtime roles without DDL permission and apply migrations at deployment time.
Consumers require a current schema. Choose one migration owner for each schema:
`grind.migrate` or the [Cigogne SQL mirrors](priv/migrations).
See [migration configuration and upgrade limits](docs/USAGE.md#migrations).

## More usage

The [usage guide](docs/USAGE.md) keeps the full API guidance and advanced examples:

| Task                                                                 | Guide                                                                                                 |
| -------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| Validate application types and define retrying or responding workers | [Workers and codecs](docs/USAGE.md#workers-and-codecs)                                                |
| Schedule jobs, deduplicate admissions or select unique keys          | [Jobs and receipts](docs/USAGE.md#jobs-receipts-and-outcomes), [uniqueness](docs/USAGE.md#uniqueness) |
| Enqueue with application writes in one caller-owned transaction      | [Shared pool and transactions](docs/USAGE.md#one-pool-and-enqueueing-inside-your-transaction)         |
| Attach Sinal events or inspect queue pressure                        | [Observations](docs/USAGE.md#observations), [diagnostics](docs/USAGE.md#operational-diagnostics)      |
| Run handlers in tests or prune finished jobs                         | [Testing](docs/USAGE.md#testing), [retention](docs/USAGE.md#retention)                                |

Recurring schedules, live queue controls, global limits and dependency workflows
remain intended capabilities awaiting implementation. The
[design](docs/design/design-layer.pdf), [vocabulary](docs/design/CONTEXT.typ),
[coverage map](docs/COVERAGE.md) and [ADRs](docs/adr) distinguish current behavior
from that retained scope.

## Development and evidence

Run from the repository root with the Sinal sibling present:

```sh
nix develop --command gleam test
nix develop --command bash scripts/test-postgres.sh
nix fmt -- README.md docs/USAGE.md
```

Plain `gleam test` skips database cases without their explicit test URL. The
PostgreSQL script creates and removes its own disposable cluster and includes
the public consumer and paired oracle checks.

[Benchmarks](bench/README.md#historical-measurements) include throughput and
latency summaries for the recorded 2026-09-28 exploratory build, along with
workloads, repeat counts and reproduction commands. The
[resilience harness](resilience/README.md) covers independent-node faults and
soaks. Historical measurements qualify only their recorded inputs; use the
[qualification guide](docs/evidence/qualification.md) for a new candidate.

Oban OSS `v2.24.1` is a scoped behavioral reference. The
[oracle guide](oracle/README.md) and [ledger](oracle/ORACLE-LEDGER.md) retain its
revision, license and measured distinctions.
