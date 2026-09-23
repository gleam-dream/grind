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

if Process.alive?(oban_pid), do: Supervisor.stop(oban_pid)
File.write!(System.fetch_env!("GRIND_ORACLE_MARKER"), "oban-oracle-passed\n", [:append])
