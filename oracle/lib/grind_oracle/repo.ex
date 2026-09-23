defmodule GrindOracle.Repo do
  use Ecto.Repo,
    otp_app: :grind_oracle,
    adapter: Ecto.Adapters.Postgres
end
