"""Counterexamples for suite completeness and native warning rejection."""

import csv
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("ci_evidence", ROOT / "scripts/ci-evidence.py")
evidence = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evidence)


class EvidenceTests(unittest.TestCase):
    def test_complete_suite_passes_and_missing_duplicate_or_failed_suites_fail(self):
        rows = [dict(id=name, status="passed") for name in sorted(evidence.SHORT_CASES)]
        evidence.validate_results(rows, evidence.SHORT_CASES)
        for invalid in ([], rows[:-1], rows + rows[:1], [dict(row, status="failed") for row in rows]):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                evidence.validate_results(invalid, evidence.SHORT_CASES)

    def test_missing_matrix_cannot_count_as_pass(self):
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(FileNotFoundError):
            evidence.matrix(Path(directory))

    def test_native_erlang_warning_is_rejected_with_positive_control(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for argument, expected in (("_Value", 0), ("Unused", 1)):
                source = root / "native_warning.erl"
                source.write_text("\n".join([
                    "-module(native_warning).", "-export([value/1]).",
                    f"value({argument}) -> ok.", "",
                ]))
                result = subprocess.run(["erlc", "-Werror", "-o", directory, str(source)],
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
                if expected:
                    self.assertIn("unused", result.stdout + result.stderr)

    def test_elixir_script_warning_is_rejected_with_positive_control(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "warning.exs"
            examples = (
                ("IO.puts(:ok)\n", True),
                ("unused = 1\n:ok\n", False),
                ("defmodule PositiveScript do\n def value(_used), do: :ok\nend\n", True),
                ("defmodule WarningScript do\n def value(unused), do: :ok\nend\n", False),
            )
            for code, success in examples:
                source.write_text(code)
                result = subprocess.run(["elixir", str(ROOT / "scripts/check-exs.exs"), str(source)],
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, success, result.stdout + result.stderr)
                if not success:
                    self.assertIn("unused", result.stdout + result.stderr)


class CleanupTests(unittest.TestCase):
    def test_postgres_start_failure_preserves_original_exit_and_server_log(self):
        for script, variable in (("scripts/test-postgres.sh", "GRIND_TEST_LOG_DIR"),
                                 ("scripts/bench-postgres.sh", "GRIND_BENCH_RESULTS_DIR"),
                                 ("scripts/test-resilience.sh", "GRIND_RESILIENCE_OUTPUT"),
                                 ("oracle/run-faults.sh", "GRIND_ORACLE_FAULT_OUTPUT")):
            with self.subTest(script=script), tempfile.TemporaryDirectory() as directory:
                temporary = Path(directory)
                tools = temporary / "tools"
                tools.mkdir()
                stubs = {
                    "gleam": "exit 0\n", "python3": "exit 0\n", "initdb": "exit 0\n",
                    "pg_isready": "exit 1\n",
                    "pg_ctl": 'while [[ $# -gt 0 ]]; do if [[ "$1" == -l ]]; then printf "startup witness\\n" > "$2"; break; fi; shift; done\nexit 1\n',
                }
                for name, body in stubs.items():
                    executable = tools / name
                    executable.write_text("#!/usr/bin/env bash\n" + body)
                    executable.chmod(0o755)
                results = temporary / "results"
                env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ["PATH"])
                env[variable] = str(results)
                result = subprocess.run(["bash", str(ROOT / script)], cwd=ROOT, env=env,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertEqual((results / "postgres.log").read_text(), "startup witness\n")


class MatrixTests(unittest.TestCase):
    def write_csv(self, directory, name, rows):
        with (directory / f"{name}.csv").open("w", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    def fixture(self, directory):
        digest = "a" * 64
        (directory / "provenance.json").write_text(json.dumps(
            dict(source_sha256=digest, sinal=dict(commit="b" * 40, dirty=False))))
        shapes = {
            "l1": dict(consumers="1", concurrency="4", queues="1", cost_ms="0", job_count="40"),
            "l2": dict(consumers="1", interval_ms="50", filler_rows="0", duration_ms="1000"),
            "l3": dict(arrival_per_sec="50", duration_ms="1000"),
            "l4": dict(submitters="4", mode="hot", total_submissions="40"),
            "l5": dict(pruner_on="1", duration_ms="1000"),
            "l6_t1": dict(concurrency="4", job_count="4", cost_ms="90000"),
            "l6_t2": dict(k_slow_acks="0", d_ms="4000", l_ms="30000", delay_ms="3200",
                          concurrency="4", main_pool_size="10"),
            "l7": dict(consumers="1", concurrency="4", job_count="40"),
        }
        (directory / "matrix-points.txt").write_text("\n".join([
            "suite=all", "repeats=3",
            *(name + " " + " ".join(shape.values()) for name, shape in shapes.items()),
        ]))
        rows = {}
        for name, shape in shapes.items():
            extras = {}
            if name == "l6_t1":
                extras = dict(t1_triggered="false", all_attempt_min_headroom_ms="1", negative_samples="0")
            if name == "l6_t2":
                extras = dict(invalid_final_count="0", non_stalled_quarantined="0",
                              non_stalled_min_headroom_ms="1", l_over_10_ms="1")
            rows[name] = [dict(shape, repeat=str(repeat), source_sha256=digest, **extras)
                          for repeat in range(1, 4)]
            self.write_csv(directory, name, rows[name])
        self.write_csv(directory, "arrivals", [dict(generator_valid="true", status="valid")])
        raw = directory / "raw"
        raw.mkdir()
        for repeat in range(1, 4):
            (raw / f"l7-1x4-r{repeat}.jsonl.drain.json").write_text(json.dumps(
                dict(valid=True, outcome="drained", sampler_covers_drain=True, observer_stopped=True)))
        return rows

    def test_matrix_requires_each_workload_repeat_and_retained_audit_verdicts(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            rows = self.fixture(directory)
            evidence.matrix(directory)
            for name, invalid in (
                ("l1", rows["l1"][:-1]),
                ("l1", rows["l1"] + rows["l1"][:1]),
                ("l6_t1", [dict(row, t1_triggered="true") for row in rows["l6_t1"]]),
                ("l6_t2", [dict(row, non_stalled_min_headroom_ms="0") for row in rows["l6_t2"]]),
            ):
                with self.subTest(name=name, invalid=invalid):
                    self.write_csv(directory, name, invalid)
                    with self.assertRaises(ValueError):
                        evidence.matrix(directory)
                    self.write_csv(directory, name, rows[name])
            drain = directory / "raw/l7-1x4-r3.jsonl.drain.json"
            drain.unlink()
            with self.assertRaises(ValueError):
                evidence.matrix(directory)


class ProvenanceTests(unittest.TestCase):
    def test_credential_paths_are_filtered_before_source_bytes_are_read(self):
        module_spec = importlib.util.spec_from_file_location("bench_provenance", ROOT / "scripts/bench-provenance.py")
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "source.gleam").write_text("source")
            # No credential file is created or read; the Git listing alone names one.
            original = Path.read_bytes
            def guarded(path):
                if path.name.startswith(".env"):
                    raise AssertionError("credential read attempted")
                return original(path)
            with patch.object(module, "root", directory), patch.object(module, "git", return_value="source.gleam\n.env.local"), patch.object(Path, "read_bytes", guarded):
                self.assertEqual(len(module.source_digest()), 64)
