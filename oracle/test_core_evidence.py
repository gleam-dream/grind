import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import core_evidence


class CoreEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.output = Path(self.directory.name)
        (self.output / "catalog.json").write_text('{"version":1}\n')
        self.provenance = dict(run_id="current", source_sha256="source",
                               oban_commit="pin", status="started",
                               catalog_sha256=core_evidence.sha256(self.output / "catalog.json"))
        (self.output / "provenance.json").write_text(json.dumps(self.provenance))
        for engine in ("grind", "oban"):
            (self.output / f"{engine}.jsonl").write_text('{"run_id":"current"}\n')
        self.addCleanup(patch.stopall)
        self.source = patch.object(core_evidence, "source_digest", return_value="source").start()
        self.git = patch.object(core_evidence, "command",
                                side_effect=lambda *args: "pin" if "rev-parse" in args else "").start()

    def test_completed_results_have_byte_hashes(self):
        core_evidence.finish(self.output, 0)
        data = json.loads((self.output / "provenance.json").read_text())
        self.assertEqual("passed", data["status"])
        self.assertEqual(core_evidence.sha256(self.output / "grind.jsonl"),
                         data["artifact_sha256"]["grind.jsonl"])

    def test_source_changes_fail_and_are_retained(self):
        self.source.return_value = "different source"
        with self.assertRaisesRegex(ValueError, "source changed"):
            core_evidence.finish(self.output, 0)
        self.assertEqual("failed", json.loads((self.output / "provenance.json").read_text())["status"])

    def test_stale_result_run_is_rejected(self):
        (self.output / "oban.jsonl").write_text('{"run_id":"old"}\n')
        with self.assertRaisesRegex(ValueError, "run identity"):
            core_evidence.finish(self.output, 0)

    def test_modified_pinned_dependency_is_rejected(self):
        self.git.side_effect = lambda *args: "pin" if "rev-parse" in args else " M lib/oban.ex"
        with self.assertRaisesRegex(ValueError, "pinned Oban source changed"):
            core_evidence.finish(self.output, 0)

    def test_failed_execution_cannot_have_passed_provenance(self):
        core_evidence.finish(self.output, 1)
        self.assertEqual("failed", json.loads((self.output / "provenance.json").read_text())["status"])


if __name__ == "__main__":
    unittest.main()
