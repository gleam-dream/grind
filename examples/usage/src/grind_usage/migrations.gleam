//// Migrations: `grind.migrate` as a deploy step, and cigogne sharing
//// Grind's pool. Choose one owner for a schema, never both.

import cigogne
import cigogne/config
import exception
import gleam/erlang/process
import gleam/io
import grind
import grind_usage/support

/// A runtime without consumers may start before the schema exists; `migrate`
/// then creates it. Running it again, or on many nodes, is safe.
pub fn run(url: String) -> Nil {
  let config =
    grind.new(support.pool(url))
    |> grind.with_schema(support.identifier("usage_migrate"))
    |> grind.without_consumers
  let assert Ok(jobs) = grind.start(config, process.new_name("usage_migrate"))
  use <- exception.defer(fn() { grind.stop(jobs) })
  let assert Ok(Nil) = grind.migrate(jobs)
  let assert Ok(Nil) = grind.migrate(jobs)
  io.println("migrations: migrate applied twice, the second a no-op")
  support.mark("usage-migrations-passed")
}

/// Cigogne applies the published `priv/migrations` files through the pool
/// Grind already owns; Grind then accepts the schema and `migrate` is a no-op.
/// Use a database whose schema cigogne owns, here the `public` schema.
pub fn run_cigogne(url: String) -> Nil {
  let config = grind.new(support.pool(url)) |> grind.without_consumers
  let assert Ok(jobs) = grind.start(config, process.new_name("usage_cigogne"))
  use <- exception.defer(fn() { grind.stop(jobs) })

  // snips-start: migrate-cigogne
  let assert Ok(base_config) = config.get("grind")
  let cigogne_config =
    config.Config(
      ..base_config,
      database: config.ConnectionDbConfig(grind.connection(jobs)),
    )
  let assert Ok(engine) = cigogne.create_engine(cigogne_config)
  let assert Ok(Nil) = cigogne.apply_all(engine)
  // snips-end: migrate-cigogne
  let assert Ok(Nil) = grind.migrate(jobs)
  io.println("migrations: cigogne applied the schema, migrate was a no-op")
  support.mark("usage-cigogne-passed")
}
