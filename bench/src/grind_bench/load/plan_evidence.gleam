//// Untimed plans from PostgreSQL auto_explain (ANALYZE, BUFFERS). A fresh
//// single-connection pool reuses the measured installation. Session-local
//// instrumentation captures the SQL Grind actually executes; no query copy
//// or role/global configuration is involved. Never include this in timing.

import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/string
import grind/postgres
import grind/queue
import grind/registry
import grind_bench/load/context
import grind_bench/load/report
import grind_bench/load/runtime
import pog
import simplifile

pub type Counts {
  Counts(total: Int, retained_succeeded: Int)
}

pub fn count_rows(connection: pog.Connection) -> Counts {
  let assert Ok(pog.Returned(rows: [counts], ..)) =
    pog.query(
      "SELECT count(*)::bigint, count(*) FILTER (WHERE queue='l2-filler' AND state='succeeded')::bigint FROM grind_jobs",
    )
    |> pog.returning({
      use total <- decode.field(0, decode.int)
      use retained_succeeded <- decode.field(1, decode.int)
      decode.success(Counts(total:, retained_succeeded:))
    })
    |> pog.execute(connection)
  counts
}

pub fn capture(
  database: postgres.Database,
  registry_: registry.Registry,
  requested_rows: Int,
  measured: Counts,
  repeat: Int,
) -> String {
  // The gate/matrix always provides its disposable server log. An ad-hoc
  // run without it cannot claim to have produced plan evidence.
  let assert Ok(_) = runtime.getenv("GRIND_BENCH_POSTGRES_LOG")
  let schema = context.schema_of(database)
  let assert Ok(settings) =
    postgres.settings(runtime.database_url())
    |> postgres.with_schema(schema)
    |> postgres.with_pool_size(1)
    |> postgres.validate
  let assert Ok(probe) = postgres.start(settings)
  let connection = postgres.connection(probe)
  list.each(
    [
      "LOAD 'auto_explain'",
      "SET auto_explain.log_analyze = on",
      "SET auto_explain.log_buffers = on",
      "SET auto_explain.log_timing = on",
      "SET auto_explain.log_format = json",
      "SET auto_explain.log_min_duration = 0",
    ],
    fn(sql) {
      let assert Ok(_) = pog.execute(pog.query(sql), connection)
      Nil
    },
  )
  let log_start = report.postgres_log_lines_before()
  let assert Ok(policy) =
    queue.default_policy()
    |> queue.with_manual_polling
    |> queue.with_maximum_concurrency(1)
    |> queue.validate_policy
  let assert Ok(consumer) = queue.start(probe, registry_, policy)
  list.each([0, 1, 2], fn(_) {
    let assert Ok(False) = queue.process_one(consumer)
    Nil
  })
  let assert Ok(_) = queue.stop(consumer)
  let assert Ok(Nil) = postgres.close(probe)
  process.sleep(25)
  let assert Ok(lines) = report.postgres_log_window(log_start)
  let content = string.join(lines, "\n")
  let path =
    runtime.results_dir()
    <> "/raw/l2-plans-"
    <> schema
    <> "-r"
    <> int.to_string(repeat)
    <> ".log"
  let assert Ok(_) = simplifile.create_directory_all(runtime.parent_dir(path))
  let assert Ok(_) =
    simplifile.write(
      path,
      "source_sha256="
        <> string.inspect(runtime.getenv("GRIND_BENCH_SOURCE_SHA256"))
        <> "\nrequested_rows="
        <> int.to_string(requested_rows)
        <> "\nactual_total_rows="
        <> int.to_string(measured.total)
        <> "\nactual_retained_succeeded_rows="
        <> int.to_string(measured.retained_succeeded)
        <> "\nmode=untimed auto_explain ANALYZE BUFFERS TIMING FORMAT JSON; actual runtime SQL; three empty-queue polls; separate pool=1\n"
        <> content
        <> "\n",
    )
  let assert True =
    string.contains(content, "attempt_id = nextval")
    && string.contains(
      content,
      "expired attempt requires outcome reconciliation",
    )
    && string.contains(content, "Actual Rows")
    && string.contains(content, "Shared Hit Blocks")
  path
}
