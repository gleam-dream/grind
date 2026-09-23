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
File.write!(System.fetch_env!("GRIND_ORACLE_MARKER"), "oban-oracle-passed\n", [:append])
