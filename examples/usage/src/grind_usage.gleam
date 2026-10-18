//// Runs every usage example. `gleam run` needs a PostgreSQL database for the
//// topics that talk to one:
////
////   GRIND_USAGE_DATABASE_URL=postgres://user@host/db gleam run
////
//// Set `GRIND_USAGE_CIGOGNE_DATABASE_URL` to an empty database to run the
//// cigogne topic as well. Each topic is a module under `grind_usage/`; read
//// one as the answer to "how do I ...".

import gleam/io
import grind_usage/handler_testing
import grind_usage/handlers
import grind_usage/isolation
import grind_usage/jobs
import grind_usage/limits
import grind_usage/migrations
import grind_usage/observations
import grind_usage/retention
import grind_usage/runtime
import grind_usage/support
import grind_usage/transactions
import grind_usage/uncertain
import grind_usage/uniqueness
import grind_usage/workers

pub fn main() -> Nil {
  limits.run()
  handler_testing.offline()
  case support.database_url() {
    Error(Nil) ->
      io.println(
        "Set GRIND_USAGE_DATABASE_URL to run the topics that need PostgreSQL.",
      )
    Ok(url) -> {
      workers.run(url)
      jobs.run(url)
      transactions.run(url)
      runtime.run(url)
      handlers.run(url)
      handler_testing.run(url)
      isolation.run(url)
      observations.run(url)
      uniqueness.run(url)
      uncertain.run(url)
      retention.run(url)
      migrations.run(url)
    }
  }
  case support.cigogne_url() {
    Ok(url) -> migrations.run_cigogne(url)
    Error(Nil) -> Nil
  }
}
