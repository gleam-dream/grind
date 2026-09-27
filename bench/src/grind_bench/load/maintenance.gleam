import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import grind/postgres
import grind/pruner
import grind/queue
import grind/registry
import grind_bench
import grind_bench/audit
import grind_bench/instrumentation
import grind_bench/latency
import grind_bench/load/context
import grind_bench/load/observers
import grind_bench/load/open_loop
import grind_bench/load/report
import grind_bench/load/runtime
import grind_bench/load/workload
import grind_bench/summarize
import grind_bench/worker as bench_worker
import pog

/// Open-loop load (200/s, the same `run_open_loop` L3 uses, 4xC10) with the
/// supervised pruner either off (`pruner_on = 0`, the control) or on
/// (`pruner_on = 1`: interval 1000ms, limit 10000, `max_age_ms` reduced to
/// 3000ms from the plan's 5s for wall-clock feasibility within this run's
/// own `duration_ms`). After the open-loop submission phase and drain, an
/// extra `max_age_ms + 2000` sleep lets the pruner catch up before dead
/// tuples are read and one direct, timed `postgres.prune_finished` call
/// reports a concrete prune-duration sample. **`pruner_on = 1` relaxes
/// I1/I2/I5** (`reduced_audit_and_report` -- pruning deletes rows the
/// ledger still references, exactly the case `grind_bench/audit`'s own doc
/// comment says this checker assumes never happens); `pruner_on = 0` runs
/// the full, unrelaxed I1-I7 audit as the control.
pub fn run_l5(pruner_on: Int, duration_ms: Int, repeat: Int) -> Nil {
  let consumers = 4
  let concurrency = 10
  let arrival_per_sec = 200
  let max_age_ms = 3000
  let total_concurrency = consumers * concurrency
  let harness =
    context.setup(
      int.max(total_concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(total_concurrency),
    )
  let context.Harness(database:, ledger:, drain:) = harness
  let queue_name = "l5-q0"
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l5.echo")
  let assert Ok(registry_) = registry.new(queue_name)
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  observers.attach_audit_observers("l5")
  let log_lines_before = report.postgres_log_lines_before()

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_poll_interval(10)
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy
  let consumers_list =
    runtime.int_range(consumers)
    |> list.map(fn(_i) {
      let assert Ok(c) = queue.start(database, registry_, policy)
      c
    })

  let maybe_pruner = case pruner_on == 1 {
    True -> {
      let assert Ok(pruner_policy) =
        pruner.default_policy()
        |> pruner.with_interval(1000)
        |> pruner.with_limit(10_000)
        |> pruner.with_max_age(max_age_ms)
        |> pruner.validate_policy
      let assert Ok(started) = pruner.start(database, pruner_policy)
      Some(started)
    }
    False -> None
  }

  let job_count =
    open_loop.run_open_loop(
      database,
      ledger,
      worker_def,
      queue_name,
      arrival_per_sec,
      duration_ms,
    )
  workload.assert_submission_count(ledger, job_count)

  let grind_schema = context.schema_of(database)
  let drained = workload.wait_for_drain(drain, grind_schema, 60_000)
  list.each(consumers_list, fn(c) {
    let _ = queue.stop(c)
    Nil
  })

  case drained {
    Error(Nil) -> {
      io.println("l5: timed out waiting for drain")
      runtime.halt(1)
    }
    Ok(Nil) -> {
      // Latency is read immediately after drain, before pruning (on the
      // `pruner_on = 1` side) gets any chance to delete the very rows these
      // queries join against -- this is the number the on/off comparison
      // is about, so it must be measured on the same footing both ways.
      let assert Ok(insert_finish) =
        latency.insert_to_finish_ms(ledger, grind_schema)
      let assert Ok(start_ack) = latency.start_to_ack_ms(ledger, grind_schema)
      let insert_finish_stats = summarize.percentiles(insert_finish)
      let start_ack_stats = summarize.percentiles(start_ack)

      // Only now let the pruner (if on) catch up, and only take a direct,
      // timed `prune_finished` sample when the pruner is on: doing this on
      // the `pruner_on = 0` control would delete rows the control's own
      // full I1-I7 audit (below) requires to still exist.
      process.sleep(max_age_ms + 2000)
      let #(prune_duration_ms, pruned_now) = case pruner_on == 1 {
        True -> {
          let prune_before = runtime.monotonic_ms()
          let prune_report =
            postgres.prune_finished(
              database,
              older_than_ms: max_age_ms,
              limit: 10_000,
            )
          let duration = runtime.monotonic_ms() - prune_before
          let jobs = case prune_report {
            Ok(postgres.PruneReport(jobs:)) -> jobs
            Error(_) -> -1
          }
          #(duration, jobs)
        }
        False -> #(-1, 0)
      }
      let dead_tuples =
        report.dead_tuple_count(postgres.connection(database), grind_schema)
      case maybe_pruner {
        Some(started) -> {
          let _ = pruner.stop(started)
          Nil
        }
        None -> Nil
      }

      let row =
        string.join(
          [
            runtime.provenance_prefix(),
            int.to_string(repeat),
            int.to_string(pruner_on),
            int.to_string(duration_ms),
            int.to_string(job_count),
            report.percentile_field(insert_finish_stats, "p99"),
            report.percentile_field(start_ack_stats, "p99"),
            int.to_string(dead_tuples),
            int.to_string(prune_duration_ms),
            int.to_string(pruned_now),
          ],
          ",",
        )
      report.write_row(
        runtime.results_dir() <> "/l5.csv",
        runtime.provenance_header_prefix()
          <> ",repeat,pruner_on,duration_ms,job_count,insert_to_finish_p99,start_to_ack_p99,dead_tuples,prune_call_duration_ms,pruned_now",
        row,
      )
      io.println("l5 " <> row)
      case pruner_on == 1 {
        True ->
          report.reduced_audit_and_report(
            ledger,
            database,
            "l5",
            log_lines_before,
          )
        False ->
          report.run_audit_and_report(
            ledger,
            database,
            "l5",
            job_count,
            log_lines_before,
          )
      }
    }
  }
}

// -- L6: lease-log/slow-ack instrumentation, T1 and T2 -----------------------

/// T1 (healthy renewal lag under real defaults): one consumer at
/// `concurrency`, `job_count` jobs each costing `cost_ms` (long enough to
/// span at least one `L / 3` renewal interval at the real, unmodified
/// default `L = 30000`/`D = 4000`), the lease-log trigger installed, no
/// slow acks (`K = 0`, healthy). Reports the worst (minimum) observed
/// per-renewal headroom and derives `2L/3 - h` (the renewal-lag proxy the
/// threshold's own derivation uses -- see `src/grind/queue.gleam`'s own
/// `LeaseTooShortForDeadline` doc comment) against the `L / 6` bound. Runs
/// the full, unrelaxed I1-I7 audit (K=0 is a healthy run; nothing should be
/// uncertain or quarantined).
pub fn run_l6t1(
  concurrency: Int,
  job_count: Int,
  cost_ms: Int,
  repeat: Int,
) -> Nil {
  let l = 30_000
  let harness =
    context.setup(
      int.max(concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(concurrency),
    )
  let context.Harness(database:, ledger:, drain:) = harness
  let grind_schema = context.schema_of(database)
  let connection = postgres.connection(database)
  let assert Ok(Nil) =
    instrumentation.install_lease_log(connection, grind_schema)
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l6t1.echo")
  let assert Ok(registry_) = registry.new("l6t1-q0")
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  observers.attach_audit_observers("l6t1")
  let log_lines_before = report.postgres_log_lines_before()

  workload.preload_and_track(
    database,
    ledger,
    worker_def,
    ["l6t1-q0"],
    job_count,
    cost_ms,
  )
  workload.assert_submission_count(ledger, job_count)
  workload.analyze_and_checkpoint(database, ledger)

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, registry_, policy)

  let waves = job_count / int.max(concurrency, 1) + 2
  let timeout_ms = int.max(120_000, cost_ms * waves)
  let drained = workload.wait_for_drain(drain, grind_schema, timeout_ms)
  let _ = queue.stop(consumer)

  case drained {
    Error(Nil) -> {
      io.println("l6t1: timed out waiting for drain")
      runtime.halt(1)
    }
    Ok(Nil) -> {
      let assert Ok(job_ids) = report.submitted_job_ids_ordered(ledger)
      let assert Ok(headroom_ms) =
        instrumentation.renewal_headroom_ms(ledger, job_ids)
      let stats = summarize.percentiles(headroom_ms)
      let worst_headroom = case stats {
        Ok(summarize.Percentiles(min:, ..)) -> min
        Error(Nil) -> -1.0
      }
      let p99_headroom = case stats {
        Ok(summarize.Percentiles(p99:, ..)) -> p99
        Error(Nil) -> -1.0
      }
      let two_thirds_l = 2.0 *. int.to_float(l) /. 3.0
      let worst_lag = two_thirds_l -. worst_headroom
      let l_over_6 = int.to_float(l) /. 6.0
      let t1_triggered = worst_lag >. l_over_6
      let row =
        string.join(
          [
            runtime.provenance_prefix(),
            int.to_string(repeat),
            int.to_string(concurrency),
            int.to_string(job_count),
            int.to_string(cost_ms),
            int.to_string(list.length(headroom_ms)),
            float.to_string(worst_headroom),
            float.to_string(p99_headroom),
            float.to_string(worst_lag),
            float.to_string(l_over_6),
            report.bool_str(t1_triggered),
          ],
          ",",
        )
      report.write_row(
        runtime.results_dir() <> "/l6_t1.csv",
        runtime.provenance_header_prefix()
          <> ",repeat,concurrency,job_count,cost_ms,renewal_samples,worst_headroom_ms,p99_headroom_ms,worst_lag_ms,l_over_6_ms,t1_triggered",
        row,
      )
      io.println("l6t1 " <> row)
      report.run_audit_and_report(
        ledger,
        database,
        "l6t1",
        job_count,
        log_lines_before,
      )
    }
  }
}

/// T2 (sibling starvation under `K` slow acks): `C = 10` fixed, `L = 6 *
/// d_ms` and handler cost `3 * L`, exactly the plan's own relationships,
/// with `d_ms` scaled down from the real 4000ms default (documented in
/// `docs/PERFORMANCE-EVIDENCE.md`) so the whole scenario finishes in tens
/// of seconds rather than minutes -- the geometry (`L = 6D`, `cost = 3L`,
/// slow-ack delay `0.8D`) is exact, only the absolute unit shrinks. The
/// first `k_slow_acks` submitted job ids (lowest id, i.e. first claimed
/// under FIFO-ish claim ordering) are marked as slow-ack targets, delaying
/// each of their own acknowledgement inserts by `0.8 * d_ms`.
///
/// **This scenario deliberately relaxes I3** (uncertain/quarantine): a slow
/// ack legitimately starves a sibling's renewal and quarantines it -- that
/// is exactly what T2 measures, not a defect (`reduced_l6t2_audit_and_report`,
/// below). It also does not use `wait_for_drain`'s own "every job
/// succeeded" poll: a quarantined job may never reach `succeeded` at all
/// (it stays `uncertain` until an audited replay this scenario never
/// issues), so a fixed, generously bounded sleep is used instead --
/// `cost_ms + k_slow_acks * 0.8d_ms + L + 5000` covers the handler run,
/// every slow ack serialized on the coordinator's own loop, and the next
/// poll tick's quarantine sweep after the longest lease this run uses would
/// have expired.
pub fn run_l6t2(k_slow_acks: Int, d_ms: Int, repeat: Int) -> Nil {
  let concurrency = 10
  let l = 6 * d_ms
  let cost_ms = 3 * l
  let delay_ms = d_ms * 8 / 10
  let harness =
    context.setup_with_deadline(
      int.max(concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(concurrency),
      d_ms,
    )
  let context.Harness(database:, ledger:, drain: _drain) = harness
  let grind_schema = context.schema_of(database)
  let connection = postgres.connection(database)
  let assert Ok(Nil) =
    instrumentation.install_lease_log(connection, grind_schema)
  let assert Ok(Nil) =
    instrumentation.install_slow_ack(connection, grind_schema, delay_ms)

  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l6t2.echo")
  let assert Ok(registry_) = registry.new("l6t2-q0")
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  observers.attach_audit_observers("l6t2")
  let log_lines_before = report.postgres_log_lines_before()

  workload.preload_and_track(
    database,
    ledger,
    worker_def,
    ["l6t2-q0"],
    concurrency,
    cost_ms,
  )
  workload.assert_submission_count(ledger, concurrency)
  workload.analyze_and_checkpoint(database, ledger)

  let assert Ok(all_job_ids) = report.submitted_job_ids_ordered(ledger)
  let slow_ids = list.take(all_job_ids, k_slow_acks)
  let non_stalled_ids = list.drop(all_job_ids, k_slow_acks)
  let assert Ok(Nil) = instrumentation.mark_slow_ack_targets(ledger, slow_ids)

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(l)
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(database, registry_, policy)

  let wait_ms = cost_ms + k_slow_acks * delay_ms + l + 5000
  process.sleep(wait_ms)
  let _ = queue.stop(consumer)

  let assert Ok(non_stalled_headroom) =
    instrumentation.renewal_headroom_ms(ledger, non_stalled_ids)
  let stats = summarize.percentiles(non_stalled_headroom)
  let min_headroom = case stats {
    Ok(summarize.Percentiles(min:, ..)) -> min
    Error(Nil) -> -1.0
  }
  let l_over_10 = int.to_float(l) /. 10.0
  let assert Ok(non_stalled_quarantined) =
    instrumentation.quarantine_transition_count(ledger, non_stalled_ids)
  let assert Ok(any_quarantined) =
    instrumentation.quarantine_transition_count(ledger, all_job_ids)
  let headroom_breach = min_headroom <. l_over_10
  let t2_triggered = headroom_breach || any_quarantined > 0

  let row =
    string.join(
      [
        runtime.provenance_prefix(),
        int.to_string(repeat),
        int.to_string(concurrency),
        int.to_string(k_slow_acks),
        int.to_string(d_ms),
        int.to_string(l),
        int.to_string(cost_ms),
        int.to_string(delay_ms),
        int.to_string(list.length(non_stalled_headroom)),
        float.to_string(min_headroom),
        float.to_string(l_over_10),
        int.to_string(non_stalled_quarantined),
        int.to_string(any_quarantined),
        report.bool_str(t2_triggered),
      ],
      ",",
    )
  report.write_row(
    runtime.results_dir() <> "/l6_t2.csv",
    runtime.provenance_header_prefix()
      <> ",repeat,concurrency,k_slow_acks,d_ms,l_ms,cost_ms,delay_ms,non_stalled_renewal_samples,non_stalled_min_headroom_ms,l_over_10_ms,non_stalled_quarantined,any_quarantined,t2_triggered",
    row,
  )
  io.println("l6t2 " <> row)

  let _ = instrumentation.drop_slow_ack(connection, grind_schema)
  let _ = instrumentation.drop_lease_log(connection, grind_schema)
  reduced_l6t2_audit_and_report(ledger, database, concurrency, log_lines_before)
}

/// L6T2's own audit: relaxes I3 in full (a slow ack may legitimately
/// quarantine any sibling, stalled or not -- that is the scenario, not a
/// defect) and I1b/I5's "every job succeeded" shape (a quarantined job
/// never reaches `succeeded`), but still checks I2 (no lost/duplicated
/// effect), I6 (ledger-write errors, PostgreSQL log anomalies), and I7
/// (forwarder drops) -- none of which a slow ack should affect.
fn reduced_l6t2_audit_and_report(
  ledger: pog.Connection,
  database: postgres.Database,
  expected_job_count: Int,
  log_lines_before: Int,
) -> Nil {
  let grind_schema = context.schema_of(database)
  let assert Ok(submission_count) =
    audit.check_submission_count(ledger, expected_job_count)
  let assert Ok(effects) = audit.check_effect_counts(ledger, grind_schema)
  let ledger_error_count = runtime.counter_value(runtime.ledger_error_counter)
  let forwarder_drop_count =
    runtime.counter_value(runtime.forwarder_drop_counter)
  let log_window = report.postgres_log_window(log_lines_before)
  let log_result = case log_window {
    Ok(window) -> audit.check_postgres_log(window)
    Error(Nil) -> Ok(Nil)
  }
  let _ = postgres.close(database)
  context.cleanup_schema(ledger, grind_schema)
  let ok =
    submission_count == Ok(Nil)
    && effects == Ok(Nil)
    && ledger_error_count == 0
    && forwarder_drop_count == 0
    && log_result == Ok(Nil)
  case ok {
    True ->
      io.println(
        "l6t2: reduced audit PASSED (I2/I6/I7; I1b/I3/I5 relaxed -- slow acks may legitimately produce uncertain/quarantined jobs)",
      )
    False -> {
      io.println(
        "l6t2: reduced audit FAILED submission_count="
        <> string.inspect(submission_count)
        <> " effects="
        <> string.inspect(effects)
        <> " ledger_errors="
        <> int.to_string(ledger_error_count)
        <> " forwarder_drops="
        <> int.to_string(forwarder_drop_count)
        <> " log="
        <> string.inspect(log_result),
      )
      runtime.halt(1)
    }
  }
}
