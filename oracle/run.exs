alias GrindOracle.{Repo, Worker}

IO.puts(
  "Oracle runtime: Oban 2.24.1 commit 64b8481e5383f6bc46b7a5284e4ced3202dec9d5, Elixir #{System.version()}, OTP #{:erlang.system_info(:otp_release)}, Postgrex #{Application.spec(:postgrex, :vsn)}"
)

defmodule GrindOracle.Normalizer do
  def result({:ok, value}), do: {:ok, value}
  def result(other), do: inspect(other)
end

defmodule GrindOracle.TelemetryHandler do
  def handle_event(_event, _measurements, metadata, pid) do
    send(pid, {:oban_job_recorded, metadata.job.id, metadata.state, metadata.result})
  end
end

defmodule GrindOracle.PrunerTelemetryHandler do
  def handle_event(_event, _measurements, %{plugin: Oban.Pruner} = metadata, pid) do
    send(pid, {:oban_pruner_ran, metadata})
  end

  def handle_event(_event, _measurements, _metadata, _pid), do: :ok
end

Ecto.Migrator.up(Repo, 1, GrindOracle.ObanMigration, log: false)

{:ok, oban_pid} =
  Oban.start_link(
    name: GrindOracle.Oban,
    repo: Repo,
    engine: Oban.Engines.Basic,
    queues: [default: 1],
    plugins: []
  )

test_pid = self()

:ok =
  :telemetry.attach_many(
    "grind-oracle-contract-#{System.unique_integer([:positive])}",
    [[:oban, :job, :stop], [:oban, :job, :exception]],
    &GrindOracle.TelemetryHandler.handle_event/4,
    test_pid
  )

for {mode, args, expected_event_state, expected_database_state} <- [
      {"success", %{"value" => 41}, :success, "completed"},
      {"failure", %{}, :discard, "discarded"}
    ] do
  {:ok, job} =
    Oban.insert(
      GrindOracle.Oban,
      Worker.new(Map.put(args, "mode", mode))
    )

  receive do
    {:oban_job_recorded, id, state, result} when id == job.id and state == expected_event_state ->
      persisted = Repo.get!(Oban.Job, id)
      true = persisted.state == expected_database_state

      IO.inspect(
        %{
          trigger: "#{mode} worker return",
          event_state: state,
          returned: GrindOracle.Normalizer.result(result),
          committed_state: persisted.state,
          attempt: persisted.attempt,
          queue: persisted.queue
        },
        label: "Oban v2.24.1 normalized observation"
      )
  after
    10_000 -> raise "Oban did not acknowledge the #{mode} job"
  end
end

# The first Oban instance polls `default` automatically. Stop it before the
# manual drain observations so it cannot claim those jobs from the same Repo.
if Process.alive?(oban_pid), do: Supervisor.stop(oban_pid)

{:ok, manual_pid} =
  Oban.start_link(
    name: GrindOracle.ManualOban,
    repo: Repo,
    engine: Oban.Engines.Basic,
    queues: [],
    plugins: [],
    testing: :manual
  )

{:ok, retry_job} =
  Oban.insert(
    GrindOracle.ManualOban,
    Worker.new(%{"mode" => "failure"}, max_attempts: 2)
  )

first_retry_drain = Oban.drain_queue(GrindOracle.ManualOban, queue: :default)
1 = Map.fetch!(first_retry_drain, :failure)
retryable = Repo.get!(Oban.Job, retry_job.id)
true = retryable.state == "retryable"
1 = retryable.attempt
2 = retryable.max_attempts
:gt = DateTime.compare(retryable.scheduled_at, DateTime.utc_now())

IO.inspect(
  %{
    trigger: "first failure with max_attempts 2",
    committed_state: retryable.state,
    attempt: retryable.attempt,
    max_attempts: retryable.max_attempts,
    scheduled_at: retryable.scheduled_at
  },
  label: "Oban v2.24.1 retry observation"
)

second_retry_drain =
  Oban.drain_queue(GrindOracle.ManualOban, queue: :default, with_scheduled: true)

1 = Map.fetch!(second_retry_drain, :discard)
exhausted = Repo.get!(Oban.Job, retry_job.id)
true = exhausted.state == "discarded"
2 = exhausted.attempt

IO.inspect(
  %{
    trigger: "second failure at max_attempts 2",
    committed_state: exhausted.state,
    attempt: exhausted.attempt,
    max_attempts: exhausted.max_attempts
  },
  label: "Oban v2.24.1 exhaustion observation"
)

{:ok, snooze_job} =
  Oban.insert(
    GrindOracle.ManualOban,
    Worker.new(%{"mode" => "snooze"})
  )

snooze_drain = Oban.drain_queue(GrindOracle.ManualOban, queue: :default)
1 = Map.fetch!(snooze_drain, :snoozed)
snoozed = Repo.get!(Oban.Job, snooze_job.id)
true = snoozed.state == "scheduled"
0 = snoozed.attempt
1 = snoozed.meta["snoozed"]
:gt = DateTime.compare(snoozed.scheduled_at, DateTime.utc_now())

IO.inspect(
  %{
    trigger: "worker snoozes for 60 seconds",
    committed_state: snoozed.state,
    attempt: snoozed.attempt,
    snoozed_count: snoozed.meta["snoozed"],
    scheduled_at: snoozed.scheduled_at
  },
  label: "Oban v2.24.1 snooze observation"
)

if Process.alive?(manual_pid), do: Supervisor.stop(manual_pid)

# "historic jobs are pruned when they are older than the configured age"
# (oracle/deps/oban/test/oban/pruner_test.exs) — paired against Grind's own
# postgres_prune_finished_deletes_old_terminal_rows_test. `Oban.Pruner` is a
# default *service* (not a `plugins:` entry) since it superseded the
# deprecated `Oban.Plugins.Pruner`; a plain `Oban.start_link` already runs it
# with its own defaults (interval 30_000ms, limit 10_000, max_age 60s)
# unless overridden here via the top-level `pruner:` option, exactly as
# `Oban.start_link`'s own moduledoc documents.
:ok =
  :telemetry.attach(
    "grind-oracle-pruner-#{System.unique_integer([:positive])}",
    [:oban, :plugin, :stop],
    &GrindOracle.PrunerTelemetryHandler.handle_event/4,
    test_pid
  )

now = DateTime.utc_now()

%Oban.Job{id: old_completed_id} =
  Worker.new(%{},
    state: "completed",
    # `Oban.Engines.Basic.prune_jobs/3` filters a `completed` row by its
    # `scheduled_at`, not `completed_at` (`cancelled`/`discarded` do use
    # their own matching timestamp column) — confirmed by reading the
    # engine directly, since the surviving/pruned rows in the upstream
    # `pruner_test.exs` "historic jobs are pruned..." matrix only correlate
    # with each row's own `scheduled_age`, not its `timestamp_age`. Set
    # here as the real controlling value; `completed_at` is the decoy.
    scheduled_at: DateTime.add(now, -61, :second),
    completed_at: DateTime.add(now, -61, :second)
  )
  |> Repo.insert!()

%Oban.Job{id: young_completed_id} =
  Worker.new(%{},
    state: "completed",
    # Comfortably inside the 60s `max_age` (not the tighter 59s the
    # upstream unit test itself uses): the upstream test also runs against
    # real wall-clock time (`DateTime.utc_now()`, no mocked/virtual clock),
    # but its own 59s/61s margin only has to survive `ExUnit`'s own
    # near-instant plugin tick. This harness's own pruner tick includes real
    # process and connection-pool startup latency first, so it needs a wider
    # margin than 1s for the same reason: without it, that startup latency
    # alone could push this job's own age past the threshold before the
    # first real tick ever runs.
    scheduled_at: DateTime.add(now, -10, :second),
    completed_at: DateTime.add(now, -10, :second)
  )
  |> Repo.insert!()

{:ok, pruner_pid} =
  Oban.start_link(
    name: GrindOracle.PrunerOban,
    repo: Repo,
    engine: Oban.Engines.Basic,
    queues: [],
    plugins: [],
    pruner: [interval: 100, max_age: 60]
  )

receive do
  {:oban_pruner_ran, %{conf: %{name: GrindOracle.PrunerOban}} = meta} ->
    still_present = Repo.get(Oban.Job, old_completed_id) != nil
    young_survived = Repo.get(Oban.Job, young_completed_id) != nil
    false = still_present
    true = young_survived
    1 = meta.pruned_count

    IO.inspect(
      %{
        trigger: "Oban.Pruner tick, max_age 60s",
        pruned_count: meta.pruned_count,
        old_job_deleted: not still_present,
        young_job_survived: young_survived
      },
      label: "Oban v2.24.1 pruner observation"
    )
after
  10_000 -> raise "Oban.Pruner did not tick"
end

if Process.alive?(pruner_pid), do: Supervisor.stop(pruner_pid)

File.write!(System.fetch_env!("GRIND_ORACLE_MARKER"), "oban-oracle-passed\n", [:append])
