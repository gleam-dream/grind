"""Observable guarantees of the benchmark's transport and provenance tools."""
import asyncio
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.dont_write_bytecode = True
REPO = Path(__file__).resolve().parents[2]


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


proxy = load_module("bench_network_delay", REPO / "bench/network_delay.py")
provenance = load_module("bench_provenance", REPO / "scripts/bench-provenance.py")


class ProvenanceTests(unittest.TestCase):
    def test_drain_timeout_is_explicit_positive_and_defaulted_only_when_unset(self):
        self.assertEqual(provenance.drain_timeout_ms(None), 60_000)
        for raw, expected in [("1", 1), ("600000", 600_000), ("060000", 60_000)]:
            self.assertEqual(provenance.drain_timeout_ms(raw), expected)
        for raw in ["", "0", "000", "-1", "+1", " 10", "10 ", "1.5", "1e6", "1_000", "１"]:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                provenance.drain_timeout_ms(raw)

    def test_toolchain_and_machine_metadata_exclude_host_identity(self):
        with mock.patch.object(provenance, "command_output", side_effect=[
            "28\n28.4.1\n16.3\naarch64-apple-darwin", "gleam 1.15.0", "postgres (PostgreSQL) 18.1"
        ]):
            versions = provenance.toolchain_metadata()
        self.assertEqual(versions["otp_version"], "28.4.1")
        self.assertEqual(versions["erts_version"], "16.3")
        self.assertEqual(versions["gleam"], "gleam 1.15.0")
        self.assertIn("PostgreSQL", versions["postgresql"])
        with mock.patch.object(provenance.platform, "node", side_effect=AssertionError("hostname read")):
            machine = provenance.machine_metadata()
        self.assertEqual(set(machine), {"os", "os_release", "os_version", "architecture", "cpu_count"})
        self.assertTrue(machine["architecture"])
        self.assertGreater(machine["cpu_count"], 0)

    def test_output_directory_cannot_be_reused_or_overwritten(self):
        with tempfile.TemporaryDirectory(prefix="grind-results-") as directory:
            output = Path(directory) / "nested" / "run"
            provenance.reserve_output_directory(output)
            marker = output / "provenance.json"
            marker.write_text("previous evidence")
            with self.assertRaises(FileExistsError):
                provenance.reserve_output_directory(output)
            self.assertEqual(marker.read_text(), "previous evidence")
            empty = Path(directory) / "empty"
            empty.mkdir()
            with self.assertRaises(FileExistsError):
                provenance.reserve_output_directory(empty)
            occupied = Path(directory) / "occupied"
            occupied.write_text("file")
            with self.assertRaises(FileExistsError):
                provenance.reserve_output_directory(occupied)

    def test_digest_tracks_source_and_path_dependency_but_not_results(self):
        with tempfile.TemporaryDirectory(prefix="grind-provenance-") as directory:
            root = Path(directory) / "grind"
            root.mkdir()
            subprocess.run(["git", "init", "--quiet", str(root)], check=True)
            original_root = provenance.root
            provenance.root = root
            try:
                (root / "source.gleam").write_text("first")
                first = provenance.source_digest()
                results = root / "bench/results"
                results.mkdir(parents=True)
                (results / "measurement.csv").write_text("measured")
                self.assertEqual(first, provenance.source_digest())
                (root / "source.gleam").write_text("changed")
                second = provenance.source_digest()
                self.assertNotEqual(first, second)
                sibling = root / "bench/build/packages/sinal/src"
                sibling.mkdir(parents=True)
                (sibling / "sinal.gleam").write_text("dependency changed")
                self.assertNotEqual(second, provenance.source_digest())
            finally:
                provenance.root = original_root


class SinalProvenanceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="grind-sinal-provenance-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        subprocess.run(["git", "init", "--quiet", str(self.root)], check=True)
        subprocess.run([
            "git", "-C", str(self.root), "-c", "user.name=Provenance test",
            "-c", "user.email=provenance@example.invalid", "-c", "commit.gpgsign=false",
            "-c", "core.hooksPath=/dev/null", "commit", "--quiet", "--allow-empty", "-m", "Parent checkout",
        ], check=True)
        self.parent_commit = subprocess.check_output(
            ["git", "-C", str(self.root), "rev-parse", "HEAD"], text=True
        ).strip()
        self.bench = self.root / "bench"
        self.installed = self.bench / "build/packages/sinal"
        self.installed.mkdir(parents=True)
        patch = mock.patch.object(provenance, "root", self.root)
        patch.start()
        self.addCleanup(patch.stop)

    def lock(self, package: str) -> None:
        (self.bench / "manifest.toml").write_text(f"packages = [{package}]\n")

    def test_hex_identity_comes_from_the_lock_without_parent_git_metadata(self):
        checksum = "358D776FDFD6B0BE56E6FA63BBE3FA8D5A9A05C2DDD256270E54E133EB4A71FC"
        self.lock(f'{{ name = "sinal", version = "0.1.0", source = "hex", outer_checksum = "{checksum}" }}')
        metadata = provenance.sinal_metadata()
        self.assertEqual(metadata["source"], "hex")
        self.assertEqual(metadata["version"], "0.1.0")
        self.assertEqual(metadata["outer_checksum"], checksum)
        self.assertIsNone(metadata["commit"])
        self.assertIsNone(metadata["dirty"])

    def test_git_identity_uses_the_locked_commit_without_a_dependency_checkout(self):
        commit = "0123456789abcdef0123456789abcdef01234567"
        self.lock('{ name = "sinal", version = "0.1.0", source = "git", '
                  f'repo = "https://example.invalid/sinal.git", commit = "{commit}", path = "packages/sinal" }}')
        self.installed.rmdir()
        metadata = provenance.sinal_metadata()
        self.assertEqual(metadata["source"], "git")
        self.assertEqual(metadata["version"], "0.1.0")
        self.assertEqual(metadata["repo"], "https://example.invalid/sinal.git")
        self.assertEqual(metadata["path"], "packages/sinal")
        self.assertEqual(metadata["commit"], commit)
        self.assertNotEqual(metadata["commit"], self.parent_commit)
        self.assertIsNone(metadata["dirty"])

    def test_local_source_does_not_inherit_the_enclosing_checkout_identity(self):
        self.lock('{ name = "sinal", version = "0.1.0", source = "local", path = "../../sinal" }')
        metadata = provenance.sinal_metadata()
        self.assertEqual(metadata["source"], "local")
        self.assertEqual(metadata["path"], "../../sinal")
        self.assertIsNone(metadata["commit"])
        self.assertIsNone(metadata["dirty"])

    def test_absent_lock_or_package_has_no_inferred_source_identity(self):
        absent = {"source": None, "version": None, "commit": None, "dirty": None}
        self.assertEqual(provenance.sinal_metadata(), absent)
        self.lock("")
        self.assertEqual(provenance.sinal_metadata(), absent)


class DelayTests(unittest.IsolatedAsyncioTestCase):
    async def test_configured_delay_is_in_the_actual_tcp_path(self):
        async def echo(reader, writer):
            writer.write(await reader.read(1024))
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        upstream = await asyncio.start_server(echo, "127.0.0.1", 0)
        port = upstream.sockets[0].getsockname()[1]
        with tempfile.TemporaryDirectory(prefix="grind-delay-") as directory:
            ready = Path(directory) / "ready"
            task = asyncio.create_task(proxy.serve(proxy.DelayConfig(0, port, 40, ready)))
            try:
                for _ in range(100):
                    if ready.exists():
                        break
                    await asyncio.sleep(.01)
                self.assertTrue(ready.exists(), "proxy failed to bind")
                reader, writer = await asyncio.open_connection("127.0.0.1", int(ready.read_text()))
                start = asyncio.get_running_loop().time()
                writer.write(b"round-trip")
                await writer.drain()
                result = await asyncio.wait_for(reader.readexactly(10), timeout=3)
                elapsed = asyncio.get_running_loop().time() - start
                self.assertEqual(result, b"round-trip")
                self.assertGreaterEqual(elapsed, .075)
                writer.close()
                await writer.wait_closed()
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                upstream.close()
                await upstream.wait_closed()


if __name__ == "__main__":
    unittest.main()
