import grind/observation
import grind_bench/load/runtime
import sinal
import sinal/forwarder

/// Attaches the two observers every scenario's audit needs:
/// `[grind, job, quarantined]` (I3) and `[sinal, forwarder, dropped]` (I7).
/// `pub`: item 4's mutation tests attach through this exact function (the
/// real wiring a load scenario uses), not a copy -- `id_suffix` lets each
/// test call this with a distinct `sinal.handler_id` so repeated calls
/// within one `gleam test` process (each test its own call) never collide
/// on a duplicate handler id. Both observers live for the caller's own
/// process lifetime -- never detached.
pub fn attach_audit_observers(id_suffix: String) -> Nil {
  let assert Ok(quarantine_id) =
    sinal.handler_id("grind-bench-quarantine-" <> id_suffix)
  let assert Ok(_) =
    sinal.observe(quarantine_id, observation.quarantined(), fn(_m, _d) {
      let _ = runtime.bump(runtime.quarantine_counter)
      Nil
    })
  let assert Ok(dropped_id) =
    sinal.handler_id("grind-bench-forwarder-dropped-" <> id_suffix)
  let assert Ok(_) =
    sinal.observe(dropped_id, forwarder.dropped_event(), fn(_m, _d) {
      let _ = runtime.bump(runtime.forwarder_drop_counter)
      Nil
    })
  Nil
}
