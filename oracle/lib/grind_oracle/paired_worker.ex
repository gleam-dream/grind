defmodule GrindOracle.PairedWorker do
  use Oban.Worker, queue: :default, max_attempts: 1

  @impl Oban.Worker
  def perform(%Oban.Job{id: id, args: args}) do
    value = Map.fetch!(args, "value") + 1
    send(Process.whereis(GrindOracle.Paired), {:performed, id, value})

    case Map.fetch!(args, "mode") do
      "failure" -> {:error, "business failure"}
      "snooze" -> {:snooze, div(Map.fetch!(args, "delay_ms"), 1_000)}
      "discard" -> {:discard, "paired discard"}
      _ -> {:ok, value}
    end
  end

  @impl Oban.Worker
  def backoff(%Oban.Job{args: args}), do: div(Map.fetch!(args, "delay_ms"), 1_000)
end
