import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option.{None, Some}
import gleam/string
import grind/observation
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
import sinal

/// Both arms run the same 200/s workload over 10,000 pre-aged terminal
/// rows. The enabled pruner must delete rows during arrivals. Workload
/// jobs remain younger than retention, preserving every latency sample.
pub fn run_l5(pruner_on: Int, duration_ms: Int, repeat: Int) -> Nil {
  let consumers = 4
  let concurrency = 10
  let arrival_per_sec = 200
  let max_age_ms = duration_ms + 60_000
  let total_concurrency = consumers * concurrency
  let harness =
    context.setup(
      int.max(total_concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(total_concurrency),
    )
  let context.Harness(database:, ledger:, drain:, ..) = harness
  let queue_name = "l5-q0"
  // Both arms start with identical pre-aged terminal rows. Fresh workload
  // rows outlive this window, so timing/audits cannot lose fast survivors.
  report.insert_filler_succeeded(postgres.connection(database), 10_000)
  let assert Ok(_) =
    pog.execute(
      pog.query(
        "UPDATE grind_jobs SET finished_at = clock_timestamp() - interval '1 day' WHERE queue = 'l2-filler'",
      ),
      postgres.connection(database),
    )
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l5.echo")
  let assert Ok(registry_) = registry.new(queue_name)
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  observers.attach_audit_observers()
  let log_lines_before = report.postgres_log_lines_before()

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy
  let consumers_list =
    runtime.int_range(consumers)
    |> list.map(fn(_i) {
      let assert Ok(c) = queue.start(database, registry_, policy)
      c
    })

  let prune_events = process.new_subject()
  let prune_observer =
    sinal.observe(observation.prune_completed(), fn(measurements, _) {
      process.send(prune_events, #(runtime.monotonic_ms(), measurements.jobs))
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

  let traffic_started = runtime.monotonic_ms()
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
  let pruned_visible_after_arrivals =
    10_000
    - report.count_jobs_in_queue(postgres.connection(database), "l2-filler")
  let pruned_in_window =
    count_prune_events(
      prune_events,
      traffic_started,
      traffic_started + duration_ms,
      0,
    )
  let assert Ok(Nil) = sinal.detach(prune_observer)
  let assert True = pruned_in_window <= pruned_visible_after_arrivals
  let assert True = pruner_on != 1 || pruned_in_window > 0

  let grind_schema = context.schema_of(database)
  let drained = workload.wait_for_drain(drain, grind_schema, 60_000)
  context.stop_observer(harness)
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
      let assert Ok(insert_finish) =
        latency.insert_to_finish_ms(ledger, grind_schema)
      let assert Ok(start_ack) = latency.start_to_ack_ms(ledger, grind_schema)
      let insert_finish_stats = summarize.percentiles(insert_finish)
      let start_ack_stats = summarize.percentiles(start_ack)
      let assert Ok(observed_ack) =
        latency.insert_to_observed_ack_ms(ledger, grind_schema)
      let assert Ok(handler_duration) = latency.handler_duration_ms(ledger)
      let assert True =
        list.length(insert_finish) == job_count
        && list.length(start_ack) == job_count
        && list.length(observed_ack) == job_count
        && list.length(handler_duration) == job_count
      let observed_ack_stats = summarize.percentiles(observed_ack)
      let handler_stats = summarize.percentiles(handler_duration)

      let prune_duration_ms = -1
      let pruned_now = pruned_in_window
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
            int.to_string(pruned_visible_after_arrivals),
            int.to_string(list.length(observed_ack)),
            report.percentile_field(observed_ack_stats, "p50"),
            report.percentile_field(observed_ack_stats, "p95"),
            report.percentile_field(observed_ack_stats, "p99"),
            int.to_string(list.length(handler_duration)),
            report.percentile_field(handler_stats, "p99"),
            int.to_string(total_concurrency),
            int.to_string(consumers),
            int.to_string(total_concurrency + consumers),
          ],
          ",",
        )
      report.write_row(
        runtime.results_dir() <> "/l5.csv",
        runtime.provenance_header_prefix()
          <> ",repeat,pruner_on,duration_ms,job_count,insert_to_sql_finished_at_p99_ms,handler_start_to_sql_receipt_timestamp_p99_ms,dead_tuples,prune_call_duration_ms,pruned_in_window,pruned_visible_after_arrivals,durable_completion_count,insert_to_observed_durable_ack_p50_ms,insert_to_observed_durable_ack_p95_ms,insert_to_observed_durable_ack_p99_ms,handler_completion_count,handler_duration_p99_ms,main_pool_size,reserved_renewal_connections,total_grind_connections",
        row,
      )
      io.println("l5 " <> row)
      // Remove only synthetic filler, then run the complete workload audit
      // in both arms; measured jobs were not eligible for pruning.
      let assert Ok(_) =
        pog.execute(
          pog.query("DELETE FROM grind_jobs WHERE queue = 'l2-filler'"),
          postgres.connection(database),
        )
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

// Telemetry is emitted after durable prune commit. Its independently observed
// time is a conservative window witness: delayed delivery can exclude an edge
// commit, but cannot claim an after-window prune happened during traffic.
fn count_prune_events(
  events: process.Subject(#(Int, Int)),
  started: Int,
  ended: Int,
  total: Int,
) -> Int {
  case process.receive(events, within: 0) {
    Ok(#(observed, jobs)) -> {
      let included = case observed >= started && observed <= ended {
        True -> jobs
        False -> 0
      }
      count_prune_events(events, started, ended, total + included)
    }
    Error(_) -> total
  }
}

// -- L6: lease-log/slow-ack instrumentation, T1 and T2 -----------------------

/// Healthy long-lived work at the real L=30s/D=4s defaults. Job costs
/// are staggered across a full lease, with at least three leases of work.
/// Successful renewal timing is complemented by independent all-attempt
/// samples; missing coverage and nonpositive headroom fail validation.
pub fn run_l6t1(
  concurrency: Int,
  job_count: Int,
  cost_ms: Int,
  repeat: Int,
) -> Nil {
  let l = 30_000
  let assert True = cost_ms >= 3 * l
  let harness =
    context.setup(
      int.max(concurrency, 10),
      grind_bench.ledger_pool_size_for_concurrency(concurrency),
    )
  let context.Harness(database:, ledger:, drain:, ..) = harness
  let grind_schema = context.schema_of(database)
  let connection = postgres.connection(database)
  let assert Ok(Nil) =
    instrumentation.install_lease_log(connection, grind_schema)
  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l6t1.echo")
  let assert Ok(registry_) = registry.new("l6t1-q0")
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  observers.attach_audit_observers()
  let log_lines_before = report.postgres_log_lines_before()

  workload.preload_varying_cost(
    database,
    ledger,
    worker_def,
    ["l6t1-q0"],
    job_count,
    fn(index) { cost_ms + index % concurrency * l / concurrency },
  )
  workload.assert_submission_count(ledger, job_count)
  workload.analyze_and_checkpoint(database, ledger)

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy
  let sampler = start_lease_sampler(drain, grind_schema)
  let assert Ok(consumer) = queue.start(database, registry_, policy)

  let waves = job_count / int.max(concurrency, 1) + 2
  let timeout_ms = int.max(120_000, cost_ms * waves)
  let drained = workload.wait_for_drain(drain, grind_schema, timeout_ms)
  context.stop_observer(harness)
  let _ = queue.stop(consumer)
  process.kill(sampler)

  case drained {
    Error(Nil) -> {
      io.println("l6t1: timed out waiting for drain")
      runtime.halt(1)
    }
    Ok(Nil) -> {
      let assert Ok(job_ids) = report.submitted_job_ids_ordered(ledger)
      let assert Ok(headroom_ms) =
        instrumentation.renewal_headroom_ms(ledger, job_ids)
      let assert Ok(evidence) = instrumentation.lease_evidence(ledger, job_ids)
      let assert True =
        evidence.attempts == job_count && evidence.negative_samples == 0
      let assert True = list.length(headroom_ms) >= 3 * job_count
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
            int.to_string(evidence.attempts),
            float.to_string(evidence.minimum_headroom_ms),
            int.to_string(evidence.negative_samples),
            int.to_string(int.max(concurrency, 10)),
            "1",
            int.to_string(int.max(concurrency, 10) + 1),
          ],
          ",",
        )
      report.write_row(
        runtime.results_dir() <> "/l6_t1.csv",
        runtime.provenance_header_prefix()
          <> ",repeat,concurrency,job_count,cost_ms,renewal_samples,worst_headroom_ms,p99_headroom_ms,worst_lag_ms,l_over_6_ms,t1_triggered,observed_attempts,all_attempt_min_headroom_ms,negative_samples,main_pool_size,reserved_renewal_connections,total_grind_connections",
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

/// T2 controls ACK delay independently from the lease and handler work.
/// Slow jobs finish across a renewal tick while other jobs keep running.
pub fn run_l6t2(k_slow_acks: Int, d_ms: Int, repeat: Int) -> Nil {
  run_l6t2_profile(k_slow_acks, d_ms, 6 * d_ms, d_ms * 8 / 10, repeat)
}

pub fn run_l6t2_profile(
  k_slow_acks: Int,
  d_ms: Int,
  l: Int,
  delay_ms: Int,
  repeat: Int,
) -> Nil {
  run_l6t2_resources(k_slow_acks, d_ms, l, delay_ms, 10, 10, repeat)
}

pub fn run_l6t2_resources(
  k_slow_acks: Int,
  d_ms: Int,
  l: Int,
  delay_ms: Int,
  concurrency: Int,
  main_pool_size: Int,
  repeat: Int,
) -> Nil {
  let assert True = main_pool_size > 0
  let assert True = k_slow_acks >= 0 && k_slow_acks < concurrency
  let cost_ms = 3 * l
  let harness =
    context.setup_with_deadline(
      main_pool_size,
      grind_bench.ledger_pool_size_for_concurrency(concurrency),
      d_ms,
    )
  let context.Harness(database:, ledger:, drain:, ..) = harness
  let grind_schema = context.schema_of(database)
  let connection = postgres.connection(database)
  let assert Ok(Nil) =
    instrumentation.install_lease_log(connection, grind_schema)
  let assert Ok(Nil) =
    instrumentation.install_slow_ack(connection, grind_schema, delay_ms)

  let assert Ok(worker_def) = bench_worker.build(ledger, "bench.l6t2.echo")
  let assert Ok(registry_) = registry.new("l6t2-q0")
  let assert Ok(registry_) = registry.register(registry_, worker_def)
  observers.attach_audit_observers()
  let log_lines_before = report.postgres_log_lines_before()

  workload.preload_varying_cost(
    database,
    ledger,
    worker_def,
    ["l6t2-q0"],
    concurrency,
    fn(index) {
      case index < k_slow_acks {
        True -> int.max(1, l / 3 - delay_ms / 2)
        False -> cost_ms + { index - k_slow_acks } * l / concurrency
      }
    },
  )
  workload.assert_submission_count(ledger, concurrency)
  workload.analyze_and_checkpoint(database, ledger)

  let assert Ok(all_job_ids) = report.submitted_job_ids_ordered(ledger)
  let slow_ids = list.take(all_job_ids, k_slow_acks)
  let non_stalled_ids = list.drop(all_job_ids, k_slow_acks)
  let assert Ok(Nil) =
    instrumentation.mark_slow_ack_targets(ledger, grind_schema, slow_ids)

  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_lease_duration(l)
    |> queue.with_maximum_concurrency(concurrency)
    |> queue.validate_policy
  let sampler = start_lease_sampler(drain, grind_schema)
  let assert Ok(activations_before) =
    instrumentation.slow_ack_activations(ledger)
  let assert Ok(consumer) = queue.start(database, registry_, policy)

  let wait_ms = cost_ms + k_slow_acks * delay_ms + l + 5000
  process.sleep(wait_ms)
  context.stop_observer(harness)
  let _ = queue.stop(consumer)
  process.kill(sampler)
  let assert Ok(activations_after) =
    instrumentation.slow_ack_activations(ledger)
  let activations = activations_after - activations_before
  let assert Ok(target_activations) =
    instrumentation.slow_ack_target_activations(ledger, grind_schema, slow_ids)
  let every_target_activated =
    list.all(target_activations, fn(entry) { entry.1 > 0 })
  let assert Ok(evidence) =
    instrumentation.lease_evidence(ledger, non_stalled_ids)
  let assert True = evidence.attempts == list.length(non_stalled_ids)
  let assert True =
    k_slow_acks == 0
    || { activations >= k_slow_acks && evidence.overlap_samples > 0 }

  let assert Ok(non_stalled_headroom) =
    instrumentation.renewal_headroom_ms(ledger, non_stalled_ids)
  let min_headroom = evidence.minimum_headroom_ms
  let l_over_10 = int.to_float(l) /. 10.0
  let assert Ok(non_stalled_quarantined) =
    instrumentation.quarantine_transition_count(ledger, non_stalled_ids)
  let assert Ok(any_quarantined) =
    instrumentation.quarantine_transition_count(ledger, all_job_ids)
  let headroom_breach = min_headroom <. l_over_10
  let t2_triggered = headroom_breach || any_quarantined > 0
  let assert Ok(classifications) =
    audit.fault_completions(ledger, grind_schema, slow_ids)
  let succeeded_count =
    list.count(classifications, fn(row) { row.state == "succeeded" })
  let uncertain_count =
    list.count(classifications, fn(row) { row.state == "uncertain" })
  let invalid_count = list.count(classifications, fn(row) { !row.valid })
  list.each(classifications, fn(item) {
    let target_activation_count = case item.fault_target {
      False -> 0
      True -> {
        let assert Ok(#(_, count)) =
          list.find(target_activations, fn(entry) { entry.0 == item.job_id })
        count
      }
    }
    report.write_row(
      runtime.results_dir() <> "/l6_t2_outcomes.csv",
      runtime.provenance_header_prefix()
        <> ",repeat,concurrency,k_slow_acks,d_ms,l_ms,delay_ms,main_pool_size,bench_index,job_id,state,fault_target,receipt_count,valid,slow_ack_activations",
      string.join(
        [
          runtime.provenance_prefix(),
          int.to_string(repeat),
          int.to_string(concurrency),
          int.to_string(k_slow_acks),
          int.to_string(d_ms),
          int.to_string(l),
          int.to_string(delay_ms),
          int.to_string(main_pool_size),
          int.to_string(item.bench_index),
          int.to_string(item.job_id),
          item.state,
          report.bool_str(item.fault_target),
          int.to_string(item.receipt_count),
          report.bool_str(item.valid),
          int.to_string(target_activation_count),
        ],
        ",",
      ),
    )
  })

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
        int.to_string(evidence.attempts),
        int.to_string(evidence.negative_samples),
        int.to_string(evidence.overlap_samples),
        int.to_string(evidence.renewals_during_slow_ack),
        int.to_string(activations),
        report.bool_str(t2_triggered),
        int.to_string(main_pool_size),
        "1",
        int.to_string(main_pool_size + 1),
        int.to_string(list.length(classifications)),
        int.to_string(succeeded_count),
        int.to_string(uncertain_count),
        int.to_string(invalid_count),
      ],
      ",",
    )
  report.write_row(
    runtime.results_dir() <> "/l6_t2.csv",
    runtime.provenance_header_prefix()
      <> ",repeat,concurrency,k_slow_acks,d_ms,l_ms,cost_ms,delay_ms,non_stalled_renewal_samples,non_stalled_min_headroom_ms,l_over_10_ms,non_stalled_quarantined,any_quarantined,non_stalled_attempts,negative_samples,overlap_samples,renewals_during_slow_ack,slow_ack_activations,t2_triggered,main_pool_size,reserved_renewal_connections,total_grind_connections,classified_count,succeeded_count,uncertain_count,invalid_final_count",
    row,
  )
  io.println("l6t2 " <> row)

  let _ = instrumentation.drop_slow_ack(connection, grind_schema)
  let _ = instrumentation.drop_lease_log(connection, grind_schema)
  reduced_l6t2_audit_and_report(
    ledger,
    database,
    concurrency,
    classifications,
    log_lines_before,
  )
  let assert True = every_target_activated
    as "every slow ACK target must activate"
  // Preserve historical red runs only when explicitly requested. A current
  // release validation never turns healthy quarantine into a passing audit.
  let assert True =
    runtime.getenv("GRIND_BENCH_ALLOW_T2_FAILURE") == Ok("1")
    || { !headroom_breach && non_stalled_quarantined == 0 }
  Nil
}

/// Fault-run accounting retains uncertain outcomes instead of filtering
/// them out. The caller separately rejects any healthy sibling quarantine;
/// this reduced audit alone never constitutes a passing T2 result.
fn reduced_l6t2_audit_and_report(
  ledger: pog.Connection,
  database: postgres.Database,
  expected_job_count: Int,
  classifications: List(audit.FaultCompletion),
  log_lines_before: Int,
) -> Nil {
  let grind_schema = context.schema_of(database)
  let assert Ok(submission_count) =
    audit.check_submission_count(ledger, expected_job_count)
  let assert Ok(effects) = audit.check_effect_counts(ledger, grind_schema)
  let assert Ok(extra_jobs) = audit.check_no_extra_jobs(ledger, grind_schema)
  let invalid = list.filter(classifications, fn(row) { !row.valid })
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
    && extra_jobs == Ok(Nil)
    && list.length(classifications) == expected_job_count
    && invalid == []
    && ledger_error_count == 0
    && forwarder_drop_count == 0
    && log_result == Ok(Nil)
  case ok {
    True ->
      io.println(
        "l6t2: final audit PASSED (every job classified; healthy succeeded with receipt; slow targets succeeded or uncertain; I2/I6/I7)",
      )
    False -> {
      io.println(
        "l6t2: reduced audit FAILED submission_count="
        <> string.inspect(submission_count)
        <> " effects="
        <> string.inspect(effects)
        <> " extra_jobs="
        <> string.inspect(extra_jobs)
        <> " invalid_final_outcomes="
        <> string.inspect(invalid)
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

fn start_lease_sampler(
  observer: pog.Connection,
  schema: String,
) -> process.Pid {
  process.spawn_unlinked(fn() { sample_leases_loop(observer, schema) })
}

fn sample_leases_loop(observer: pog.Connection, schema: String) -> Nil {
  let assert Ok(Nil) = instrumentation.sample_leases(observer, schema)
  process.sleep(50)
  sample_leases_loop(observer, schema)
}
