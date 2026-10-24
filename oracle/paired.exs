defmodule GrindOracle.Paired do
  alias GrindOracle.{PairedWorker, Repo}
  import Ecto.Query

  def run do
    true = Process.register(self(), __MODULE__)
    catalog = System.fetch_env!("GRIND_ORACLE_CATALOG") |> File.read!() |> Jason.decode!()
    1 = catalog["version"]
    Ecto.Migrator.up(Repo, 1, GrindOracle.ObanMigration, log: false)
    0 = Repo.aggregate(Oban.Job, :count)

    :ok =
      :telemetry.attach("paired-job-stop", [:oban, :job, :stop], &__MODULE__.job_event/4, self())

    {:ok, pid} =
      Oban.start_link(
        name: __MODULE__,
        repo: Repo,
        engine: Oban.Engines.Basic,
        queues: [],
        plugins: [],
        pruner: false,
        testing: :manual
      )

    try do
      lines =
        Enum.map(catalog["scenarios"], fn scenario ->
          # Each scenario owns the entire empty installation, including
          # pruning, whose scope is wider than its fixture queue.
          Repo.delete_all(Oban.Job)

          %{
            catalog_version: 1,
            engine: "oban",
            run_id: System.fetch_env!("GRIND_ORACLE_RUN_ID"),
            scenario: scenario["id"],
            result: scenario(scenario)
          }
          |> Jason.encode!()
        end)

      File.write!(System.fetch_env!("GRIND_ORACLE_RESULTS"), Enum.join(lines, "\n") <> "\n")
    after
      :telemetry.detach("paired-job-stop")
      Supervisor.stop(pid)
    end
  end

  defp scenario(%{"kind" => "prune"} = scenario), do: prune(scenario)

  defp scenario(scenario) do
    params = scenario["parameters"]
    kind = scenario["kind"]
    before_ms = database_ms()
    queue = "paired-" <> scenario["id"]
    args = %{"mode" => kind, "value" => params["value"], "delay_ms" => params["delay_ms"]}
    opts = [queue: queue, max_attempts: params["max_attempts"]]

    opts =
      case kind do
        "future" ->
          Keyword.put(
            opts,
            :scheduled_at,
            DateTime.add(DateTime.utc_now(), params["delay_ms"], :millisecond)
          )

        "past" ->
          Keyword.put(
            opts,
            :scheduled_at,
            DateTime.add(DateTime.utc_now(), -params["delay_ms"], :millisecond)
          )

        "unique" ->
          Keyword.put(opts, :unique,
            period: :infinity,
            fields: [:worker, :queue, :args],
            keys: [:value],
            states: [:available]
          )

        _ ->
          opts
      end

    {:ok, job} = Oban.insert(__MODULE__, PairedWorker.new(args, opts))
    if kind == "cancel", do: :ok = Oban.cancel_job(__MODULE__, job.id)

    reused =
      if kind == "unique" do
        {:ok, second} = Oban.insert(__MODULE__, PairedWorker.new(args, opts))
        second.id == job.id and second.conflict?
      end

    if params["runs"] > 0 do
      for _ <- 1..params["runs"] do
        # Stage only due rows, matching Grind's direct due-time claim path.
        Oban.drain_queue(__MODULE__, queue: queue, with_scheduled: DateTime.utc_now())
      end
    end

    values = collect_calls(job.id, [])
    persisted = Repo.get!(Oban.Job, job.id)
    result = snapshot(persisted, length(values))

    result =
      if params["verify_delay"] do
        after_ms = database_ms()
        scheduled_ms = DateTime.to_unix(persisted.scheduled_at, :millisecond)

        matches =
          after_ms >= before_ms and after_ms - before_ms <= 5_000 and
            scheduled_ms >= before_ms + params["delay_ms"] - 20 and
            scheduled_ms <= after_ms + params["delay_ms"] + 20

        Map.put(result, :delay_matches, matches)
      else
        result
      end

    case kind do
      "success" ->
        value =
          receive do
            {:stopped, id, {:ok, value}} when id == job.id -> value
          after
            1_000 -> raise "successful job has no post-ack return observation"
          end

        Map.put(result, :value, value)

      "unique" ->
        Map.put(result, :reused, reused)

      _ ->
        result
    end
  end

  defp snapshot(job, executions) do
    %{
      state: if(job.state == "completed", do: "succeeded", else: job.state),
      attempt: job.attempt,
      snoozes: Map.get(job.meta, "snoozed", 0),
      future:
        job.state in ["scheduled", "retryable"] and
          DateTime.compare(job.scheduled_at, DateTime.utc_now()) == :gt,
      executions: executions
    }
  end

  defp database_ms do
    %{rows: [[milliseconds]]} =
      Repo.query!("SELECT floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint")

    milliseconds
  end

  defp collect_calls(id, values) do
    receive do
      {:performed, ^id, value} -> collect_calls(id, [value | values])
    after
      0 -> Enum.reverse(values)
    end
  end

  def job_event(_event, _measurements, metadata, pid),
    do: send(pid, {:stopped, metadata.job.id, metadata.result})

  defp prune(scenario) do
    params = scenario["parameters"]
    queue = "paired-" <> scenario["id"]
    args = %{"mode" => "success", "value" => params["value"], "delay_ms" => params["delay_ms"]}
    {:ok, old} = Oban.insert(__MODULE__, PairedWorker.new(args, queue: queue))
    {:ok, young} = Oban.insert(__MODULE__, PairedWorker.new(args, queue: queue))
    %{success: 2} = Oban.drain_queue(__MODULE__, queue: queue)
    old = Repo.get!(Oban.Job, old.id)
    "completed" = old.state
    now = DateTime.utc_now()

    {1, nil} =
      from(j in Oban.Job, where: j.id == ^old.id)
      |> Repo.update_all(
        set: [
          scheduled_at: DateTime.add(now, -params["scheduled_age_ms"], :millisecond),
          completed_at: DateTime.add(now, -params["finished_age_ms"], :millisecond)
        ]
      )

    handler = "paired-pruner-" <> scenario["id"]
    :ok = :telemetry.attach(handler, [:oban, :plugin, :stop], &__MODULE__.pruner_event/4, self())

    {:ok, pid} =
      Oban.start_link(
        name: GrindOracle.PairedPruner,
        repo: Repo,
        engine: Oban.Engines.Basic,
        queues: [],
        plugins: [],
        peer: Oban.Peers.Isolated,
        notifier: Oban.Notifiers.Isolated,
        pruner: [interval: 100, max_age: div(params["retention_ms"], 1_000)]
      )

    try do
      receive do
        {:pruned, deleted} ->
          %{
            old_deleted: Repo.get(Oban.Job, old.id) == nil,
            young_survived: Repo.get(Oban.Job, young.id) != nil,
            deleted: deleted
          }
      after
        10_000 -> raise "paired pruner did not run"
      end
    after
      Supervisor.stop(pid)
      :telemetry.detach(handler)
    end
  end

  def pruner_event(
        _event,
        _measurements,
        %{plugin: Oban.Pruner, conf: %{name: GrindOracle.PairedPruner}} = metadata,
        pid
      ),
      do: send(pid, {:pruned, metadata.pruned_count})

  def pruner_event(_event, _measurements, _metadata, _pid), do: :ok
end

GrindOracle.Paired.run()
