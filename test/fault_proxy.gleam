//// Gleam bindings for `grind_fault_proxy.erl`, the test-only TCP
//// fault-injection proxy. See that module's doc comment for the mechanism;
//// this module only shapes the Gleam-facing types the Erlang controller
//// pattern-matches on directly (`FaultMode`'s and `Trigger`'s tags are load
//// bearing — check `grind_fault_proxy:trigger_matches/2` before renaming a
//// constructor).

import gleam/erlang/process

/// Opaque handle for one running proxy instance.
pub type Proxy

/// What the proxy does to the first client -> server chunk that matches
/// `Trigger`, once armed. One-shot: call `arm` again to re-arm.
pub type DropAction {
  /// Forward the triggering chunk to the real server (it genuinely executes),
  /// then silently discard every server -> client byte afterward.
  DropReply
  /// Never forward the triggering chunk, or anything the client sends
  /// afterward, to the real server.
  DropRequest
}

/// What byte pattern in a client -> server chunk arms the fault.
pub type Trigger {
  /// The extended-query-protocol `Parse` message for `commit` (pog's own
  /// literal, lowercase SQL for `pog.transaction`'s implicit commit).
  OnCommit
  /// Same, for `begin`.
  OnBegin
  /// An arbitrary caller-supplied byte pattern, for a specific
  /// squirrel-generated statement (for example a lease-renewal `UPDATE`).
  OnSql(pattern: String)
}

pub type FaultMode {
  Pass
  Armed(trigger: Trigger, action: DropAction)
}

/// Delivered to the subject passed to `arm` the moment its fault fires.
/// `local_port` is the relay's own local port on its connection to the real
/// PostgreSQL server — exactly `pg_stat_activity.client_port` for that
/// backend, letting a test find and terminate that exact backend.
pub type ProxyEvent {
  CommitSeen(conn_id: Int, local_port: Int)
}

/// Starts a proxy listening on an ephemeral 127.0.0.1 port and forwarding to
/// `upstream_host:upstream_port`. Returns the handle and the port test
/// databases should connect to instead of the real cluster.
@external(erlang, "grind_fault_proxy", "start")
pub fn start(
  upstream_host: String,
  upstream_port: Int,
) -> Result(#(Proxy, Int), Nil)

/// Stops accepting new connections and kills every live relay, closing both
/// of that relay's sockets — including its connection to the real server, so
/// PostgreSQL notices the disconnect and rolls back any open transaction.
@external(erlang, "grind_fault_proxy", "stop")
pub fn stop(proxy: Proxy) -> Nil

/// Arms a one-shot fault: the first client -> server chunk matching
/// `mode`'s trigger (across any connection this proxy is relaying) gets
/// `mode`'s action applied, and `notify` receives `CommitSeen`.
@external(erlang, "grind_fault_proxy", "arm")
pub fn arm(
  proxy: Proxy,
  mode: FaultMode,
  notify: process.Subject(ProxyEvent),
) -> Nil
