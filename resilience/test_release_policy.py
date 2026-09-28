"""Prospective release-duration policy; no database or BEAM processes."""

import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import run


class ReleasePolicyTest(unittest.TestCase):
    scenarios = "M1,M2,M3,M4,M5,M6,M7,F1,F2,F3,F4,F5"

    def provenance(self, seconds, *, release=True, dirty=False, scenarios=None):
        def metadata(command, **_):
            if command[0] == "git":
                operation = command[3]
                if operation == "ls-files":
                    return ""
                if operation == "status":
                    return " M fixture.py\n" if dirty else ""
                if operation == "rev-parse":
                    return "synthetic-commit\n"
            if command[:2] == ["psql", "--version"]:
                return "PostgreSQL synthetic\n"
            if command[0] == "erl":
                return "28"
            raise AssertionError(f"Unexpected external command: {command}")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "grind"
            root.mkdir()
            runner = object.__new__(run.Runner)
            runner.output = root / "results"
            runner.output.mkdir()
            runner.runtime_sha256 = "synthetic-runtime"
            runner.lease_ms, runner.deadline_ms = 30000, 4000
            runner.args = SimpleNamespace(
                release_evidence=release, soak_seconds=seconds,
                scenarios=self.scenarios if scenarios is None else scenarios)
            with patch.object(run, "ROOT", root), patch.object(
                    run.subprocess, "check_output", side_effect=metadata):
                runner.provenance()
            return json.loads((runner.output / "provenance.json").read_text())

    def test_clean_complete_release_accepts_two_hour_boundary_and_above(self):
        for seconds in (7200, 7201):
            with self.subTest(seconds=seconds):
                self.assertEqual(self.provenance(seconds)["soak_seconds"], seconds)

    def test_release_rejects_one_second_below_two_hours(self):
        with self.assertRaises(RuntimeError):
            self.provenance(7199)

    def test_release_still_requires_every_standalone_scenario(self):
        scenarios = self.scenarios.split(",")
        for missing in scenarios:
            with self.subTest(missing=missing), self.assertRaises(RuntimeError):
                self.provenance(7200, scenarios=",".join(
                    scenario for scenario in scenarios if scenario != missing))

    def test_dirty_release_still_rejected_at_two_hours(self):
        with self.assertRaisesRegex(RuntimeError, "clean pinned source"):
            self.provenance(7200, dirty=True)

    def test_short_dirty_exploratory_run_remains_allowed(self):
        data = self.provenance(300, release=False, dirty=True, scenarios="")
        self.assertEqual(data["evidence_class"], "exploratory")
        self.assertEqual(data["soak_seconds"], 300)


if __name__ == "__main__":
    unittest.main()
