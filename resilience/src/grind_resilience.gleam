import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/json
import gleam/option.{type Option, None, Some}
import gleam/result
import grind/job
import grind/postgres
import grind/queue
import grind/registry
import grind/worker

type Mode {
  Normal
  RetryOnce
  SnoozeOnce
}

type Input {
  Input(key: String, delay_ms: Int, release: String, mode: Mode)
}

type Command {
  Start
  Stop
  Submit(Input)
  Cancel(id: Int)
  Resolve(id: Int, resolution_id: String)
  Reconcile(id: Int, command_id: String)
  Prune
  KillWorker(key: String)
  EffectSynced(key: String)
  Trace(path: String, duration_ms: Int)
  Sample
  Exit
}

@external(erlang, "grind_resilience_ffi", "env")
fn env(key: String) -> String

@external(erlang, "grind_resilience_ffi", "read_line")
fn read_line() -> Result(String, Nil)

@external(erlang, "grind_resilience_ffi", "init")
fn initialise() -> Nil

@external(erlang, "grind_resilience_ffi", "next_attempt")
fn next_attempt(key: String) -> Int

@external(erlang, "grind_resilience_ffi", "decision")
fn record_decision(key: String, decision: String) -> Nil

@external(erlang, "grind_resilience_ffi", "effect")
fn record_effect(key: String) -> Nil

@external(erlang, "grind_resilience_ffi", "finished")
fn record_finished(key: String) -> Nil

@external(erlang, "grind_resilience_ffi", "await_release")
fn await_release(path: String) -> Nil

@external(erlang, "grind_resilience_ffi", "kill_worker")
fn kill_worker(key: String) -> Bool

@external(erlang, "grind_resilience_ffi", "effect_synced")
fn effect_synced(key: String) -> Bool

@external(erlang, "grind_resilience_ffi", "trace")
fn trace(path: String, duration_ms: Int) -> Nil

@external(erlang, "grind_resilience_ffi", "runtime")
fn runtime() -> #(
  Int,
  Int,
  Int,
  Int,
  Int,
  Int,
  Int,
  Int,
  Int,
  Bool,
  List(String),
)

fn input_decoder() -> decode.Decoder(Input) {
  use key <- decode.field("key", decode.string)
  use delay_ms <- decode.field("delay_ms", decode.int)
  use release <- decode.field("release", decode.string)
  use mode <- decode.field("mode", decode.string)
  case mode {
    "normal" -> decode.success(Input(key, delay_ms, release, Normal))
    "retry_once" -> decode.success(Input(key, delay_ms, release, RetryOnce))
    "snooze_once" -> decode.success(Input(key, delay_ms, release, SnoozeOnce))
    _ -> decode.failure(Input(key, delay_ms, release, Normal), "known mode")
  }
}

fn encode_input(input: Input) -> json.Json {
  json.object([
    #("key", json.string(input.key)),
    #("delay_ms", json.int(input.delay_ms)),
    #("release", json.string(input.release)),
    #(
      "mode",
      json.string(case input.mode {
        Normal -> "normal"
        RetryOnce -> "retry_once"
        SnoozeOnce -> "snooze_once"
      }),
    ),
  ])
}

fn definition() -> worker.Worker(Input, String, Nil) {
  let version = env("RESILIENCE_VERSION")
  let assert Ok(input) =
    worker.codec("resilience-input-" <> version, encode_input, input_decoder())
  let assert Ok(output) =
    worker.codec("resilience-output-" <> version, json.string, decode.string)
  let assert Ok(definition) =
    worker.define("resilience.effect", version, input, output, perform)
  let assert Ok(delay) = worker.retry_delay(50)
  definition
  |> worker.with_queue_handler(fn(input) {
    let attempt = next_attempt(input.key)
    case input.mode, attempt {
      RetryOnce, 1 -> {
        record_decision(input.key, "retry")
        worker.WorkerFailed(Nil)
      }
      SnoozeOnce, 1 -> {
        record_decision(input.key, "snooze")
        worker.WorkerSnoozed(delay, "controlled soak snooze")
      }
      _, _ -> {
        let assert Ok(output) = perform(input)
        worker.WorkerSucceeded(output)
      }
    }
  })
  |> worker.with_retry_policy(
    worker.retry_policy(fn(_, _) { worker.RetryAfter(delay) }),
  )
}

fn perform(input: Input) -> Result(String, Nil) {
  record_effect(input.key)
  await_release(input.release)
  process.sleep(input.delay_ms)
  record_finished(input.key)
  Ok(input.key)
}

fn command_decoder() -> decode.Decoder(Command) {
  use kind <- decode.field("op", decode.string)
  case kind {
    "start" -> decode.success(Start)
    "stop" -> decode.success(Stop)
    "submit" -> {
      use input <- decode.then(input_decoder())
      decode.success(Submit(input))
    }
    "cancel" -> {
      use id <- decode.field("id", decode.int)
      decode.success(Cancel(id))
    }
    "resolve" -> {
      use id <- decode.field("id", decode.int)
      use resolution_id <- decode.field("resolution_id", decode.string)
      decode.success(Resolve(id, resolution_id))
    }
    "reconcile" -> {
      use id <- decode.field("id", decode.int)
      use command_id <- decode.field("command_id", decode.string)
      decode.success(Reconcile(id, command_id))
    }
    "prune" -> decode.success(Prune)
    "kill_worker" -> {
      use key <- decode.field("key", decode.string)
      decode.success(KillWorker(key))
    }
    "effect_synced" -> {
      use key <- decode.field("key", decode.string)
      decode.success(EffectSynced(key))
    }
    "trace" -> {
      use path <- decode.field("path", decode.string)
      use duration <- decode.field("duration_ms", decode.int)
      decode.success(Trace(path, duration))
    }
    "sample" -> decode.success(Sample)
    "exit" -> decode.success(Exit)
    _ -> decode.failure(Exit, "known resilience command")
  }
}

fn emit(fields: List(#(String, json.Json))) -> Nil {
  io.println("RESILIENCE " <> json.to_string(json.object(fields)))
}

fn ok(op: String) -> Nil {
  emit([#("op", json.string(op)), #("ok", json.bool(True))])
}

fn sample() -> Nil {
  let #(
    pid,
    processes,
    atoms,
    memory,
    mailbox,
    deadlines,
    type_entries,
    query_entries,
    harness_entries,
    traps_exits,
    messages,
  ) = runtime()
  emit([
    #("op", json.string("sample")),
    #("os_pid", json.int(pid)),
    #("processes", json.int(processes)),
    #("atoms", json.int(atoms)),
    #("memory_bytes", json.int(memory)),
    #("mailbox_messages", json.int(mailbox)),
    #("deadline_entries", json.int(deadlines)),
    #("postgres_type_entries", json.int(type_entries)),
    #("postgres_query_entries", json.int(query_entries)),
    #("harness_entries", json.int(harness_entries)),
    #("owner_traps_exits", json.bool(traps_exits)),
    #("owner_mailbox", json.array(messages, json.string)),
  ])
}

pub fn main() -> Nil {
  initialise()
  let assert Ok(deadline) = int.parse(env("RESILIENCE_DEADLINE_MS"))
  let assert Ok(validated) =
    postgres.settings(env("RESILIENCE_DATABASE_URL"))
    |> postgres.with_schema(env("RESILIENCE_SCHEMA"))
    |> postgres.with_pool_size(4)
    |> postgres.with_unique_lock_wait(100)
    |> postgres.with_statement_deadline(deadline)
    |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  let assert Ok(_) = postgres.migrate(database)
  let worker = definition()
  let assert Ok(empty_registry) = registry.new(env("RESILIENCE_QUEUE"))
  let assert Ok(workers) = registry.register(empty_registry, worker)
  let assert Ok(concurrency) = int.parse(env("RESILIENCE_CONCURRENCY"))
  let assert Ok(lease_ms) = int.parse(env("RESILIENCE_LEASE_MS"))
  let assert Ok(grace_ms) = int.parse(env("RESILIENCE_GRACE_MS"))
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(25)
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.with_lease_duration(lease_ms)
    |> queue.with_shutdown_grace(grace_ms)
    |> queue.validate_policy
  ok("ready")
  loop(database, worker, workers, policy, None)
}

fn loop(
  database: postgres.Database,
  worker: worker.Worker(Input, String, Nil),
  workers: registry.Registry,
  policy: queue.ValidatedPolicy,
  consumer: Option(queue.Consumer),
) -> Nil {
  case read_line() {
    Error(Nil) -> close(database, consumer)
    Ok(line) -> {
      let assert Ok(command) = json.parse(line, command_decoder())
      case command {
        Exit -> close(database, consumer)
        Start -> {
          let assert None = consumer
          let assert Ok(started) = queue.start(database, workers, policy)
          ok("start")
          loop(database, worker, workers, policy, Some(started))
        }
        Stop -> {
          let assert Some(started) = consumer
          let result = queue.stop(started)
          let outcome = case result {
            Ok(queue.StoppedCleanly) -> "clean"
            Ok(queue.StoppedWithActiveWork(_)) -> "active_work"
            Ok(queue.StoppedDrainUnconfirmed) -> "drain_unconfirmed"
            Ok(queue.StoppedWithoutDrain) -> "without_drain"
            Error(_) -> "error"
          }
          emit([
            #("op", json.string("stop")),
            #("ok", json.bool(result.is_ok(result))),
            #("outcome", json.string(outcome)),
          ])
          loop(database, worker, workers, policy, None)
        }
        _ -> {
          run(command, database, worker)
          loop(database, worker, workers, policy, consumer)
        }
      }
    }
  }
}

fn run(
  command: Command,
  database: postgres.Database,
  worker: worker.Worker(Input, String, Nil),
) -> Nil {
  case command {
    Submit(input) -> {
      let assert Ok(handle) =
        postgres.submit(database, env("RESILIENCE_QUEUE"), worker, input)
      emit([
        #("op", json.string("submit")),
        #("id", json.int(job.id_value(handle))),
      ])
    }
    Cancel(id) -> {
      let assert Ok(handle) = postgres.bind_handle(database, worker, id)
      let assert Ok(outcome) = postgres.cancel(database, handle)
      emit([
        #("op", json.string("cancel")),
        #(
          "outcome",
          json.string(case outcome {
            postgres.CancelledBeforeRun -> "before_run"
            postgres.CancellationRequested -> "requested"
            postgres.AlreadyCancelled -> "already_cancelled"
            postgres.AlreadyUncertain -> "uncertain"
            postgres.AlreadyFinished(_) -> "finished"
          }),
        ),
      ])
    }
    Resolve(id, resolution_id) -> {
      let assert Ok(handle) = postgres.bind_handle(database, worker, id)
      let assert Ok(_) =
        postgres.resolve_uncertain(
          database,
          handle,
          postgres.ResolutionRequest(
            resolution_id,
            "resilience-controller",
            "Explicit replay after witnessed external effect and ownership loss",
            postgres.AuthorizeReplay,
          ),
        )
      ok("resolve")
    }
    Reconcile(id, command_id) -> {
      let assert Ok(handle) = postgres.bind_handle(database, worker, id)
      let assert Ok(receipt) =
        postgres.reconcile_acknowledgement(database, handle, command_id)
      emit([
        #("op", json.string("reconcile")),
        #("ok", json.bool(True)),
        #("command_id", json.string(receipt.command_id)),
      ])
    }
    Prune -> {
      let assert Ok(report) =
        postgres.prune_finished(database, older_than_ms: 1, limit: 100)
      emit([
        #("op", json.string("prune")),
        #("ok", json.bool(True)),
        #("deleted", json.int(report.jobs)),
      ])
    }
    KillWorker(key) ->
      emit([
        #("op", json.string("kill_worker")),
        #("ok", json.bool(kill_worker(key))),
      ])
    EffectSynced(key) ->
      emit([
        #("op", json.string("effect_synced")),
        #("ok", json.bool(effect_synced(key))),
      ])
    Trace(path, duration) -> {
      trace(path, duration)
      ok("trace")
    }
    Sample -> sample()
    Start | Stop | Exit -> panic as "lifecycle command must be handled by loop"
  }
}

fn close(database: postgres.Database, consumer: Option(queue.Consumer)) -> Nil {
  case consumer {
    Some(consumer) -> {
      let assert Ok(_) = queue.stop(consumer)
      Nil
    }
    None -> Nil
  }
  let assert Ok(_) = postgres.close(database)
  ok("exit")
}
