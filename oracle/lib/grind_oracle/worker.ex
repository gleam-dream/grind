defmodule GrindOracle.Worker do
  use Oban.Worker, queue: :default, max_attempts: 1

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"mode" => "success", "value" => value}}), do: {:ok, value + 1}
  def perform(%Oban.Job{args: %{"mode" => "failure"}}), do: {:error, "business failure"}
end
