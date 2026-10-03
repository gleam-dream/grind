//// The database password must not appear in `string.inspect` of any public
//// value that embeds it: `Settings`, `ValidatedSettings` or `Database`.
//// Crash reports and logger metadata print values the same way.

import exception
import gleam/string
import gleeunit/should
import grind/internal/postgres
import grind/support/env.{database_url, mark_database_test_executed}

const password = "grind-secret-password"

const url = "postgres://grind:grind-secret-password@127.0.0.1:5432/grind?sslmode=disable"

pub fn settings_do_not_print_the_password_test() {
  let settings = postgres.settings(url)
  string.contains(string.inspect(settings), password) |> should.be_false
  // The URL is still available to the caller that holds the settings.
  settings.database_url() |> should.equal(url)
}

pub fn validated_settings_do_not_print_the_password_test() {
  let assert Ok(validated) = postgres.settings(url) |> postgres.validate
  string.contains(string.inspect(validated), password) |> should.be_false
}

/// Trust authentication ignores the password, so the disposable test cluster
/// accepts a URL that carries one.
pub fn postgres_database_does_not_print_the_password_test() {
  case database_url() {
    Error(Nil) -> Nil
    Ok(database_url) -> run_database_test(database_url)
  }
}

fn run_database_test(database_url: String) -> Nil {
  let with_password =
    string.replace(database_url, "://grind@", "://grind:" <> password <> "@")
  string.contains(with_password, password) |> should.be_true
  let assert Ok(validated) =
    postgres.settings(with_password) |> postgres.validate
  let assert Ok(database) = postgres.start(validated)
  use <- exception.defer(fn() { postgres.close(database) })
  string.contains(string.inspect(database), password) |> should.be_false
  mark_database_test_executed("inspect-database-hides-password-passed")
}
