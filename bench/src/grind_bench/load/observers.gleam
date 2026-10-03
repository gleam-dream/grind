import grind/telemetry
import grind_bench/load/runtime
import sinal
import sinal/forwarder

/// Attaches the two observers every scenario's audit needs:
/// `[grind, job, quarantined]` (I3) and `[sinal, forwarder, dropped]` (I7).
/// `pub`: item 4's mutation tests attach through this exact function (the
/// real wiring a load scenario uses), not a copy. Sinal gives each
/// attachment a fresh handler id, so repeated calls never collide. Both
/// observers live for the caller's own process lifetime -- never detached.
pub fn attach_audit_observers() -> Nil {
  sinal.observe(telemetry.quarantined(), fn(_m, _d) {
    let _ = runtime.bump(runtime.quarantine_counter)
    Nil
  })
  sinal.observe(forwarder.dropped_event(), fn(_m, _d) {
    let _ = runtime.bump(runtime.forwarder_drop_counter)
    Nil
  })
  Nil
}
