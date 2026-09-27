import sinal

/// Detach is best-effort cleanup, not part of what a test proves: native
/// `:telemetry` can already have auto-detached a handler on its own (a
/// raising handler is auto-detached after it raises — see `postgres_acknowledged_observation_raising_handler_test`
/// in `grind/observations/delivery_test`), so a `NotAttached` result here is not a test failure.
pub fn detach(attachment: sinal.Attachment) -> Nil {
  let _ = sinal.detach(attachment)
  Nil
}
