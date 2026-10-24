defmodule GrindOracle.FaultEvidence do
  @moduledoc false

  def record(path, event) do
    event =
      Map.merge(event, %{
        node: System.fetch_env!("ORACLE_FAULT_NODE"),
        beam_node: to_string(node()),
        os_pid: String.to_integer(System.pid()),
        worker_pid: inspect(self()),
        at_ms: System.system_time(:millisecond)
      })

    {:ok, file} = :file.open(String.to_charlist(path), [:append, :raw, :binary])

    try do
      :ok = :file.write(file, [Jason.encode!(event), "\n"])
      :ok = :file.sync(file)
    after
      :ok = :file.close(file)
    end
  end

  def plugin_event([:oban, :plugin, :stop], _measurements, metadata, path) do
    if metadata.plugin in [Oban.Lifeline, Oban.Pruner] do
      record(path, %{
        event: "plugin_stop",
        plugin: inspect(metadata.plugin),
        rescued_ids: Enum.map(Map.get(metadata, :rescued_jobs, []), & &1.id),
        discarded_ids: Enum.map(Map.get(metadata, :discarded_jobs, []), & &1.id),
        pruned_ids: Enum.map(Map.get(metadata, :pruned_jobs, []), & &1.id),
        pruned_count: Map.get(metadata, :pruned_count),
        error: Map.has_key?(metadata, :error)
      })
    end
  end

  def plugin_event([:oban, :peer, :election, :stop], _measurements, metadata, path) do
    record(path, %{
      event: "peer_election",
      leader: metadata.leader,
      was_leader: metadata.was_leader
    })
  end
end

defmodule GrindOracle.FaultWorker do
  @moduledoc false
  use Oban.Worker, queue: :oracle_fault, max_attempts: 3

  @impl Oban.Worker
  def perform(%Oban.Job{id: id, attempt: attempt, args: args}) do
    path = System.fetch_env!("ORACLE_FAULT_EFFECTS")
    fields = %{key: Map.fetch!(args, "key"), job_id: id, attempt: attempt}
    GrindOracle.FaultEvidence.record(path, Map.put(fields, :event, "effect"))

    # The controller requires this marker, written only after file:sync has
    # returned. Seeing the JSONL line alone could race its pending fsync.
    File.write!("#{path}.#{id}.#{attempt}.synced", "synced\n")
    await_release(Map.fetch!(args, "release"))
    GrindOracle.FaultEvidence.record(path, Map.put(fields, :event, "handler_finished"))
    :ok
  end

  defp await_release(""), do: :ok

  defp await_release(path) do
    unless File.regular?(path) do
      Process.sleep(10)
      await_release(path)
    end
  end
end
