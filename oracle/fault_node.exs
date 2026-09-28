# Loaded in a separate named BEAM OS process, with already compiled dependencies.
Application.load(:grind_oracle)

Application.put_env(:grind_oracle, GrindOracle.Repo,
  url: System.fetch_env!("GRIND_OBAN_TEST_DATABASE_URL"),
  pool_size: 5
)

{:ok, _} = Application.ensure_all_started(:grind_oracle)
Logger.configure(level: :warning)

defmodule GrindOracle.FaultMigration do
  use Ecto.Migration

  def up, do: Oban.Migration.up(prefix: prefix(), create_schema: false)
end

defmodule GrindOracle.FaultNode do
  alias GrindOracle.{FaultEvidence, FaultWorker, Repo}
  @name GrindOracle.FaultInstance

  def main do
    prefix = System.fetch_env!("ORACLE_FAULT_SCHEMA")
    Ecto.Migrator.up(Repo, 1, GrindOracle.FaultMigration, prefix: prefix, log: false)

    :ok =
      :telemetry.attach_many(
        "oracle-fault-evidence",
        [[:oban, :plugin, :stop], [:oban, :peer, :election, :stop]],
        &FaultEvidence.plugin_event/4,
        System.fetch_env!("ORACLE_FAULT_EVENTS")
      )

    lifeline =
      case System.fetch_env!("ORACLE_FAULT_SCENARIO") do
        "M2" ->
          [
            interval: integer_env("ORACLE_FAULT_PLUGIN_INTERVAL_MS"),
            rescue_after: integer_env("ORACLE_FAULT_RESCUE_AFTER_MS")
          ]

        "M6" ->
          false
      end

    pruner =
      case System.fetch_env!("ORACLE_FAULT_SCENARIO") do
        "M2" -> false
        "M6" -> [interval: integer_env("ORACLE_FAULT_PLUGIN_INTERVAL_MS"), max_age: 1]
      end

    {:ok, _} =
      Oban.start_link(
        name: @name,
        node: to_string(node()),
        repo: Repo,
        prefix: prefix,
        engine: Oban.Engines.Basic,
        notifier: Oban.Notifiers.Postgres,
        peer: {Oban.Peers.Database, interval: integer_env("ORACLE_FAULT_PEER_INTERVAL_MS")},
        queues: [oracle_fault: [limit: 1, paused: true]],
        plugins: [],
        lifeline: lifeline,
        pruner: pruner,
        stager: [interval: 100],
        testing: :disabled
      )

    reply("ready", %{beam_node: to_string(node()), os_pid: String.to_integer(System.pid())})
    loop()
  end

  defp loop do
    case IO.gets("") do
      :eof ->
        :ok

      line ->
        %{"op" => op} = command = Jason.decode!(line)
        reply(op, command(command))
        loop()
    end
  end

  defp command(%{"op" => "submit", "key" => key, "release" => release}) do
    {:ok, job} = Oban.insert(@name, FaultWorker.new(%{"key" => key, "release" => release}))
    %{id: job.id}
  end

  defp command(%{"op" => "start"}) do
    :ok = Oban.resume_queue(@name, queue: :oracle_fault, local_only: true)
    %{ok: true}
  end

  defp command(%{"op" => "peer"}) do
    %{leader: Oban.Peer.leader?(@name), leader_node: Oban.Peer.get_leader(@name)}
  end

  defp command(%{"op" => "queue"}) do
    state = Oban.check_queue(@name, queue: :oracle_fault)
    %{paused: state.paused, running: state.running}
  end

  defp reply(op, data), do: IO.puts("ORACLE_FAULT " <> Jason.encode!(Map.put(data, :op, op)))
  defp integer_env(key), do: key |> System.fetch_env!() |> String.to_integer()
end

GrindOracle.FaultNode.main()
