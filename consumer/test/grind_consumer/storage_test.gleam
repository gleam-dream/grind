import exception
import gleeunit/should
import grind/internal/postgres
import grind_consumer/support/env

pub fn storage_start_failure_is_reported_test() {
  case env.storage_failure_url() {
    Error(Nil) -> Nil
    Ok(url) -> {
      let assert Ok(settings) =
        postgres.settings(url)
        |> postgres.validate
      let failure_observed = case postgres.start(settings) {
        Error(_) -> True
        Ok(database) -> {
          use <- exception.defer(fn() { postgres.close(database) })
          case postgres.migrate(database) {
            Error(_) -> True
            Ok(Nil) -> False
          }
        }
      }
      failure_observed |> should.equal(True)
      env.mark("consumer-storage-failure-passed")
    }
  }
}
