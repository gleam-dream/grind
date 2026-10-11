from contextlib import redirect_stdout
import io
import importlib.util
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "prepare_dependencies", Path(__file__).with_name("prepare-dependencies.py")
)
dependencies = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dependencies)


class PreparationTest(unittest.TestCase):
    def setUp(self):
        self.enterContext(redirect_stdout(io.StringIO()))
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.project = Path(temporary.name)
        (self.project / "gleam.toml").write_text(
            'name = "example"\n[dependencies]\n'
            'first = {path = "../first"}\nsecond = {path = "../second"}\n'
        )
        self.packages = self.project / "build/packages"
        self.packages.mkdir(parents=True)
        self.fingerprint = self.packages / "first.config_fingerprint"
        self.fingerprint.write_text("previous")

    @staticmethod
    def result(code=0, output=""):
        return subprocess.CompletedProcess(["gleam", "deps", "download"], code, output)

    def test_prepare_settles_every_direct_path_without_running_tests(self):
        with patch.object(dependencies.subprocess, "run", return_value=self.result()) as run:
            dependencies.prepare(self.project)
        self.assertEqual(run.call_count, 3)
        self.assertTrue(all(call.args[0] == ["gleam", "deps", "download"]
                            for call in run.call_args_list))

    def test_only_explicit_hex_rate_limits_are_retried(self):
        calls = []
        original_mtime = self.fingerprint.stat().st_mtime_ns

        def run(*args, **kwargs):
            calls.append(args)
            if len(calls) == 1:
                self.fingerprint.write_text("unresolved")
                (self.packages / "second.config_fingerprint").write_text("unresolved")
                return self.result(1, "The rate limit for the Hex API has been exceeded")
            self.assertEqual(self.fingerprint.read_text(), "previous")
            self.assertEqual(self.fingerprint.stat().st_mtime_ns, original_mtime)
            self.assertFalse((self.packages / "second.config_fingerprint").exists())
            return self.result()

        with patch.object(dependencies.subprocess, "run", side_effect=run):
            with patch.object(dependencies.time, "sleep") as sleep:
                dependencies.download(self.project)
        self.assertEqual(len(calls), 2)
        sleep.assert_called_once_with(20)

    def test_dependency_errors_fail_immediately(self):
        with patch.object(dependencies.subprocess, "run", return_value=self.result(1, "No solution")) as run:
            with patch.object(dependencies.time, "sleep") as sleep:
                with self.assertRaises(subprocess.CalledProcessError):
                    dependencies.download(self.project)
        run.assert_called_once()
        sleep.assert_not_called()

    def test_rate_limit_retries_are_bounded(self):
        result = self.result(1, "The rate limit for the Hex API has been exceeded")
        with patch.object(dependencies.subprocess, "run", return_value=result) as run:
            with patch.object(dependencies.time, "sleep") as sleep:
                with self.assertRaises(subprocess.CalledProcessError):
                    dependencies.download(self.project)
        self.assertEqual(run.call_count, 4)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [20, 40, 80])

    def test_clean_discards_sources_inventory_and_beam_files(self):
        (self.packages / "pog").mkdir()
        (self.packages / "pog/stale.erl").write_text("fork")
        (self.packages / "packages.toml").write_text("stale inventory")
        beam = self.project / "build/dev/erlang/pog/ebin/stale.beam"
        beam.parent.mkdir(parents=True)
        beam.write_text("fork")
        fingerprint_mtime = self.fingerprint.stat().st_mtime_ns

        def run(command, **kwargs):
            if command == ["gleam", "clean"]:
                shutil.rmtree(self.project / "build")
            else:
                self.assertEqual(list(self.packages.iterdir()), [self.fingerprint])
                self.assertEqual(self.fingerprint.read_text(), "previous")
                self.assertEqual(self.fingerprint.stat().st_mtime_ns, fingerprint_mtime)
                self.assertFalse(beam.exists())
            return self.result()

        with patch.object(dependencies.subprocess, "run", side_effect=run):
            dependencies.prepare(self.project, clean=True)


if __name__ == "__main__":
    unittest.main()
