import copy
import json
from pathlib import Path
import tempfile
import unittest

from faults import check_result, json_lines


class FaultEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.catalog = json.loads((Path(__file__).parent / "fault-scenarios.json").read_text())

    def test_actual_outputs_must_match_every_expected_field(self):
        for scenario in self.catalog["scenarios"]:
            expected = scenario["expected"]["oban"]
            check_result(self.catalog, scenario["id"], expected)
            for key in expected:
                changed = copy.deepcopy(expected)
                del changed[key]
                with self.assertRaises(AssertionError):
                    check_result(self.catalog, scenario["id"], changed)
                changed = copy.deepcopy(expected)
                changed[key] = not expected[key] if isinstance(expected[key], bool) else "wrong"
                with self.assertRaises(AssertionError):
                    check_result(self.catalog, scenario["id"], changed)
            changed = dict(expected, unexpected=True)
            with self.assertRaises(AssertionError):
                check_result(self.catalog, scenario["id"], changed)

    def test_grind_intentional_outputs_cannot_pass_as_oban(self):
        for scenario in self.catalog["scenarios"]:
            with self.assertRaises(AssertionError):
                check_result(self.catalog, scenario["id"], scenario["expected"]["grind"])

    def test_integer_is_not_a_boolean_witness(self):
        scenario = self.catalog["scenarios"][0]
        changed = dict(scenario["expected"]["oban"], lifeline_rescue_observed=1)
        with self.assertRaises(AssertionError):
            check_result(self.catalog, scenario["id"], changed)

    def test_partial_writer_line_is_not_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            self.assertEqual([], json_lines(path))
            path.write_text('{"event":"effect"}\n{"event":')
            self.assertEqual([{"event": "effect"}], json_lines(path))

    def test_fault_catalog_pin_and_source_references(self):
        root = Path(__file__).resolve().parent.parent
        core = json.loads((root / "oracle/scenarios.json").read_text())
        self.assertEqual(core["oracle"], self.catalog["oracle"])
        self.assertEqual({"M2", "M6"}, {row["id"] for row in self.catalog["scenarios"]})
        paths = list(self.catalog["adapters"].values())
        for scenario in self.catalog["scenarios"]:
            self.assertEqual("intentional-grind-semantics", scenario["classification"])
            self.assertEqual({"oban", "grind"}, set(scenario["expected"]))
            paths.extend(scenario["sources"])
        for path in paths:
            self.assertTrue((root / path).is_file(), path)


if __name__ == "__main__":
    unittest.main()
