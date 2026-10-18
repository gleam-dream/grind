# Grind

Grind runs typed background jobs on PostgreSQL and Erlang/OTP. Workers define
input, output and error types with JSON codecs; Grind stores admissions and
outcomes and runs handlers through a supervised queue runtime.

## Install

Grind is not yet published to Hex. Depend on it through Git; its own unpublished
dependency, [Sinal](https://github.com/gleam-dream/sinal), resolves through
Grind's manifest entry. Your manifest holds the resolved commit, and `gleam update grind` moves it:

```toml
[dependencies]
grind = { git = "https://github.com/gleam-dream/grind.git", ref = "main" }
gleam_stdlib = ">= 0.70.0 and < 2.0.0"
gleam_erlang = ">= 1.0.0 and < 2.0.0"
gleam_otp = ">= 1.0.0 and < 2.0.0"
gleam_json = ">= 3.1.0 and < 4.0.0"
gleam_time = ">= 1.0.0 and < 2.0.0"
exception = ">= 2.1.1 and < 3.0.0"
pog = ">= 4.1.0 and < 4.2.0"
```

The Erlang target requires Gleam 1.18 or later and PostgreSQL. Development uses
OTP 29 and PostgreSQL 16 through `nix develop`. The [package manifest](gleam.toml)
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
worker, codecs and supervised example. The [usage examples](examples/usage/README.md)
cover each topic as code that compiles and runs, and the [separate consumer](consumer/README.md)
exercises public APIs, typed failures, configuration and recovery.

`submit` can fail before admission or return `CommitUnknown(pending)` after a
lost reply. Retain that pending command and use `grind.reconcile_submission`.
The example asserts successful startup and admission; application code can use
`grind.submit_error_kind` and `grind.describe_submit_error` to handle failures.
A five-second `await` can return `Pending`; it does not cancel the job, and its
last storage read can extend beyond the wait budget.

## Runtime and recovery

Grind owns the main pool and its supervised processes. Each used queue adds one reserved renewal connection. Applications can borrow `grind.connection(jobs)` while Grind owns the pool. A runtime added with `grind.supervised` shuts down through the application supervisor, and one started with `grind.start` needs `grind.stop`.

An abandoned attempt is held `Uncertain` by default. Investigate the external effect, then confirm an outcome or authorize replay with `grind/admin`. Cancellation is cooperative and cannot retract an effect. Receipt recovery and submission deduplication end when a job is pruned. The defaults, deadline limits and operating obligations are defined in [the design](docs/design/design.typ) under "Public modules and defaults", "Runtime capacity and deadlines" and "Deployment obligations".

## Usage

Enqueue inside the application's own READ COMMITTED transaction. The reply means staged admission, not commit, and SERIALIZABLE and REPEATABLE READ transactions are refused:

```gleam
pog.transaction(grind.connection(jobs), fn(tx) {
  use _ <- result.try(orders.confirm(tx, order))
  grind.submit_in(jobs, tx, job.new(receipt(), order.id) |> job.with_id(order.id))
  |> result.map_error(ReceiptNotQueued)
})
```

Resolve an uncertain job after investigating its effect. The resolution ID names one exact command across the installation, so qualify it with the job ID and persist the command before applying it; `admin.resolve_uncertain_in` stages the same command inside a caller-owned transaction:

```gleam
let command = admin.resolution(
  admin.ConfirmSuccess(receipt),
  id: "job:" <> int.to_string(job.id(handle)) <> ":decision:" <> decision_id,
  by: operator,
  details: evidence,
)
admin.resolve_uncertain(jobs, handle, command)
```

Observe committed lifecycle transitions and diagnostics with plain Sinal calls. Events are best-effort, so outcomes are read from the durable rows:

```gleam
let _attachment =
  sinal.observe(telemetry.acknowledged(), fn(_measurements, metadata) {
    io.println("job " <> int.to_string(metadata.ref.job_id) <> " acknowledged")
  })
```

Read one queue as a content-free committed snapshot, and prune finished jobs in bounded batches:

```gleam
let assert Ok(snapshot) = admin.statistics(jobs, queue: "emails")
let assert Ok(deleted) =
  admin.prune_finished(jobs, older_than: duration.minutes(1), limit: 1000)
```

Every snippet has a compiled, runnable counterpart in [examples/usage](examples/usage/README.md): [transactions](examples/usage/src/grind_usage/transactions.gleam), [uncertain jobs](examples/usage/src/grind_usage/uncertain.gleam), [observations](examples/usage/src/grind_usage/observations.gleam), [retention and statistics](examples/usage/src/grind_usage/retention.gleam), [migrations](examples/usage/src/grind_usage/migrations.gleam) and [limits and defaults](examples/usage/src/grind_usage/limits.gleam). The examples run in the gate, so they stay current with the library.

## Migrations

`grind.with_startup_migration` migrates before consumers start. Leave it off for runtime roles without DDL permission and apply migrations at deployment time. Choose one migration owner for each schema, either `grind.migrate` or the [cigogne mirrors](priv/migrations). Cigogne can share Grind's pool:

```gleam
let assert Ok(base_config) = config.get("grind")
let cigogne_config =
  config.Config(
    ..base_config,
    database: config.ConnectionDbConfig(grind.connection(jobs)),
  )
let assert Ok(engine) = cigogne.create_engine(cigogne_config)
let assert Ok(Nil) = cigogne.apply_all(engine)
```

The execution rules, upgrade limits and schema isolation requirements are defined under "Storage and migrations" in [the design](docs/design/design.typ).

## Development

Run from the repository root:

```sh
nix develop --command gleam test
nix develop --command bash scripts/test-postgres.sh
nix develop --command bash scripts/ci.sh fast
```

Plain `gleam test` skips database cases without their explicit test URL. The PostgreSQL script creates and removes its own disposable cluster and includes the public consumer and paired oracle checks. See [AGENTS.md](AGENTS.md) for every gate command.

The [design](docs/design/design.typ), [vocabulary](docs/design/CONTEXT.typ), [coverage map](docs/COVERAGE.md) and [ADRs](docs/adr) distinguish current behavior from retained intended capabilities. [docs/ORACLE.md](docs/ORACLE.md) names the pinned Oban reference and what is compared, and [docs/BENCHMARKS.md](docs/BENCHMARKS.md) states the benchmark method and retained results; the two link each other. The [benchmark](bench/README.md), [resilience](resilience/README.md) and [oracle](oracle/README.md) directories give the harness commands.
