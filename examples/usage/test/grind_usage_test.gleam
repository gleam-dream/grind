//// Runs every usage example as a test, so an API change that breaks an
//// example fails the gate. Topics that need PostgreSQL return early without
//// `GRIND_USAGE_DATABASE_URL`; the full database gate sets it.

import gleeunit
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

pub fn main() {
  gleeunit.main()
}

fn with_database(topic: fn(String) -> Nil) -> Nil {
  case support.database_url() {
    Ok(url) -> topic(url)
    Error(Nil) -> Nil
  }
}

pub fn limits_test() {
  limits.run()
}

pub fn handler_testing_offline_test() {
  handler_testing.offline()
}

pub fn workers_test() {
  with_database(workers.run)
}

pub fn jobs_test() {
  with_database(jobs.run)
}

pub fn transactions_test() {
  with_database(transactions.run)
}

pub fn runtime_test() {
  with_database(runtime.run)
}

pub fn handlers_test() {
  with_database(handlers.run)
}

pub fn handler_testing_test() {
  with_database(handler_testing.run)
}

pub fn isolation_test() {
  with_database(isolation.run)
}

pub fn observations_test() {
  with_database(observations.run)
}

pub fn uniqueness_test() {
  with_database(uniqueness.run)
}

pub fn uncertain_test() {
  with_database(uncertain.run)
}

pub fn retention_test() {
  with_database(retention.run)
}

pub fn migrations_test() {
  with_database(migrations.run)
}

pub fn cigogne_test() {
  case support.cigogne_url() {
    Ok(url) -> migrations.run_cigogne(url)
    Error(Nil) -> Nil
  }
}
