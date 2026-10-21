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

<!-- prettier-ignore-start -->

<!-- snips: test/grind/facade/readme_test.gleam#quick-start -->
```gleam
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

pub fn children(pool: pog.Config, name: process.Name(grind.Message)) {
  let config =
    grind.new(pool)
    |> grind.with_worker(mailer())
    |> grind.with_startup_migration
  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(grind.supervised(config, name))
}

pub fn send(name: process.Name(grind.Message), email: Email) {
  let jobs = grind.named(name)
  let assert Ok(admission) = grind.submit(jobs, job.new(mailer(), email))
  grind.await(jobs, grind.handle(admission), within: duration.seconds(5))
}

```

<!-- prettier-ignore-end -->

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

Enqueue inside the application's own READ COMMITTED transaction. This excerpt uses the receipt worker and database connection configured by the runnable transaction example. The reply means staged admission, not commit, and SERIALIZABLE and REPEATABLE READ transactions are refused:

<!-- prettier-ignore-start -->

<!-- snips: examples/usage/src/grind_usage/transactions.gleam#transaction -->
```gleam
let order = support.unique("order")
let assert Ok(grind.Inserted(handle)) =
  pog.transaction(db, fn(tx) {
    let assert Ok(_) =
      pog.query("INSERT INTO usage_orders (id) VALUES ($1)")
      |> pog.parameter(pog.text(order))
      |> pog.execute(tx)
    grind.submit_in(jobs, tx, job.new(receipt, 9) |> job.with_id(order))
  })
```

<!-- prettier-ignore-end -->

Resolve an uncertain job after investigating its effect. The resolution ID names one exact command across the installation, so qualify it with the job ID and persist the command before applying it; `admin.resolve_uncertain_in` stages the same command inside a caller-owned transaction:

<!-- prettier-ignore-start -->

<!-- snips: examples/usage/src/grind_usage/uncertain.gleam#resolve-uncertain -->
```gleam
let command =
  admin.resolution(
    admin.ConfirmSuccess(42),
    id: "job:" <> int.to_string(job.id(handle)) <> ":decision:1",
    by: "operator@example.com",
    details: "the provider ledger confirms op-42",
  )
let assert Ok(admin.Applied(job.Succeeded)) =
  admin.resolve_uncertain(jobs, handle, command)
```

<!-- prettier-ignore-end -->

Observe committed lifecycle transitions and diagnostics with plain Sinal calls. Events are best-effort, so outcomes are read from the durable rows:

<!-- prettier-ignore-start -->

<!-- snips: examples/usage/src/grind_usage/observations.gleam#observe -->
```gleam
let acknowledged = process.new_subject()
let capacity = process.new_subject()
// A lifecycle event: one committed disposition for one claimed attempt.
let ack_handler =
  sinal.observe(telemetry.acknowledged(), fn(_measurements, metadata) {
    case metadata.ref.queue == double.queue {
      True -> process.send(acknowledged, metadata)
      False -> Nil
    }
  })
```

<!-- prettier-ignore-end -->

Read one queue as a content-free committed snapshot, and prune finished jobs in bounded batches:

<!-- prettier-ignore-start -->

<!-- snips: examples/usage/src/grind_usage/retention.gleam#retention -->
```gleam
let assert Ok(statistics) = admin.statistics(jobs, queue: work.queue)
let assert Ok(finished) =
  list.find(statistics.states, fn(group) { group.state == job.Succeeded })
let assert 1 = finished.count

// One call is one bounded batch of finished jobs older than the age.
process.sleep(20)
let assert Ok(pruned) =
  admin.prune_finished(
    jobs,
    older_than: duration.milliseconds(10),
    limit: 1000,
  )
let assert True = pruned >= 1
```

<!-- prettier-ignore-end -->

Every snippet has a compiled, runnable counterpart in [examples/usage](examples/usage/README.md): [transactions](examples/usage/src/grind_usage/transactions.gleam), [uncertain jobs](examples/usage/src/grind_usage/uncertain.gleam), [observations](examples/usage/src/grind_usage/observations.gleam), [retention and statistics](examples/usage/src/grind_usage/retention.gleam), [migrations](examples/usage/src/grind_usage/migrations.gleam) and [limits and defaults](examples/usage/src/grind_usage/limits.gleam). The examples run in the gate, so they stay current with the library.

## Migrations

`grind.with_startup_migration` migrates before consumers start. Leave it off for runtime roles without DDL permission and apply migrations at deployment time. Choose one migration owner for each schema, either `grind.migrate` or the [cigogne mirrors](priv/migrations). Cigogne can share Grind's pool:

<!-- prettier-ignore-start -->

<!-- snips: examples/usage/src/grind_usage/migrations.gleam#migrate-cigogne -->
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

<!-- prettier-ignore-end -->

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
