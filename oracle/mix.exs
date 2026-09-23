defmodule GrindOracle.MixProject do
  use Mix.Project

  @oban_commit "64b8481e5383f6bc46b7a5284e4ced3202dec9d5"

  def project do
    [
      app: :grind_oracle,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: [
        {:oban, git: "https://github.com/oban-bg/oban.git", ref: @oban_commit},
        {:ecto_sql, "~> 3.10"},
        {:postgrex, "~> 0.20"},
        {:telemetry, "~> 1.3"},
        {:jason, "~> 1.1"}
      ],
      deps_path: "deps",
      lockfile: "mix.lock"
    ]
  end

  def application do
    [extra_applications: [:logger], mod: {GrindOracle.Application, []}]
  end
end
