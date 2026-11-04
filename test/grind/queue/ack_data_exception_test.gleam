//// A PostgreSQL data exception (SQLSTATE class 22) is deterministic: the
//// same acknowledgement fails the same way on every retry. These tests pin
//// that such a failure is classified as a storage refusal, and that the
//// replacement proposal keeps the handler's disposition where it carries
//// only text, and becomes a terminal runtime failure where the refused value
//// was the encoded output or error.

import gleam/option.{None, Some}
import gleeunit/should
import grind/internal/postgres
import grind/internal/worker
import pog

pub fn class_22_query_error_is_a_data_exception_test() {
  postgres.data_exception_code(pog.PostgresqlError(
    "22P05",
    "untranslatable_character",
    "unsupported Unicode escape sequence",
  ))
  |> should.equal(Some("22P05"))
  postgres.data_exception_code(pog.PostgresqlError(
    "22021",
    "character_not_in_repertoire",
    "invalid byte sequence for encoding \"UTF8\": 0x00",
  ))
  |> should.equal(Some("22021"))
}

pub fn other_query_errors_are_not_data_exceptions_test() {
  postgres.data_exception_code(pog.PostgresqlError(
    "40001",
    "serialization_failure",
    "could not serialize access",
  ))
  |> should.equal(None)
  postgres.data_exception_code(pog.PostgresqlError(
    "08006",
    "connection_failure",
    "connection lost",
  ))
  |> should.equal(None)
  postgres.data_exception_code(pog.ConnectionUnavailable)
  |> should.equal(None)
  postgres.data_exception_code(pog.QueryTimeout) |> should.equal(None)
}

pub fn refused_output_becomes_runtime_failed_test() {
  worker.refused_by_storage(worker.ExecutedSuccess("v1", "\"x\""), "22P05")
  |> should.equal(worker.ExecutedUnencodable(
    worker.OutputCodec,
    worker.storage_refusal_reason("22P05"),
  ))
}

pub fn refused_error_becomes_runtime_failed_test() {
  worker.refused_by_storage(
    worker.ExecutedRetryable(Some("e1"), Some("\"x\""), "failed", 1000),
    "22P05",
  )
  |> should.equal(worker.ExecutedUnencodable(
    worker.ErrorCodec,
    worker.storage_refusal_reason("22P05"),
  ))
  worker.refused_by_storage(
    worker.ExecutedBusinessFailure(
      Some("e1"),
      Some("\"x\""),
      "failed",
      worker.BudgetExhausted,
    ),
    "22P05",
  )
  |> should.equal(worker.ExecutedUnencodable(
    worker.ErrorCodec,
    worker.storage_refusal_reason("22P05"),
  ))
}

pub fn refused_text_keeps_the_handler_disposition_test() {
  let reason = worker.storage_refusal_reason("22021")
  worker.refused_by_storage(worker.ExecutedDiscarded("a\u{0}b"), "22021")
  |> should.equal(worker.ExecutedDiscarded(reason))
  worker.refused_by_storage(worker.ExecutedCancelled("a\u{0}b"), "22021")
  |> should.equal(worker.ExecutedCancelled(reason))
  worker.refused_by_storage(worker.ExecutedUncertain("a\u{0}b"), "22021")
  |> should.equal(worker.ExecutedUncertain(reason))
  worker.refused_by_storage(worker.ExecutedSnoozed(500, "a\u{0}b"), "22021")
  |> should.equal(worker.ExecutedSnoozed(500, reason))
}

pub fn storage_refusal_reason_names_the_sqlstate_test() {
  worker.storage_refusal_reason("22P05")
  |> should.equal(
    "PostgreSQL refused the acknowledgement with SQLSTATE 22P05 (data exception); the handler's value was not stored",
  )
}
