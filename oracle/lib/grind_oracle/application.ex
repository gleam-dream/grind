defmodule GrindOracle.Application do
  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link([GrindOracle.Repo],
      strategy: :one_for_one,
      name: GrindOracle.Supervisor
    )
  end
end
