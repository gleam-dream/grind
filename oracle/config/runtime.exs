import Config

config :grind_oracle, GrindOracle.Repo,
  url: System.fetch_env!("GRIND_OBAN_TEST_DATABASE_URL"),
  pool_size: 5

config :oban, Oban,
  repo: GrindOracle.Repo,
  prefix: "public"
