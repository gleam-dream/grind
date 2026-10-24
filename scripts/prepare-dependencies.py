#!/usr/bin/env python3
"""Prepare Gleam dependencies without caching source trees or compiled output."""

import argparse
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import tomllib


def fingerprints(project: Path) -> list[Path]:
    return list((project / "build/packages").glob("*.config_fingerprint"))


def restore(project: Path, saved: Path) -> None:
    for path in fingerprints(project):
        path.unlink()
    destination = project / "build/packages"
    destination.mkdir(parents=True, exist_ok=True)
    for path in saved.iterdir():
        shutil.copy2(path, destination / path.name)


def download(project: Path) -> None:
    # Gleam writes a path fingerprint before resolving. Roll it back on failure
    # so a retry cannot mistake an unsuccessful resolution for a current lock.
    with tempfile.TemporaryDirectory() as temporary:
        saved = Path(temporary)
        for path in fingerprints(project):
            shutil.copy2(path, saved / path.name)
        for attempt in range(4):
            result = subprocess.run(
                ["gleam", "deps", "download"], cwd=project,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            print(result.stdout, end="", flush=True)
            if result.returncode == 0:
                return
            restore(project, saved)
            if (
                "rate limit for the hex api has been exceeded"
                not in result.stdout.lower()
                or attempt == 3
            ):
                raise subprocess.CalledProcessError(result.returncode, result.args)
            delay = 20 * 2**attempt
            print(f"Hex rate limit: retrying dependency download in {delay}s", flush=True)
            time.sleep(delay)


def prepare(project: str | Path, clean: bool = False) -> None:
    project = Path(project).resolve()
    config = tomllib.loads((project / "gleam.toml").read_text())
    dependencies = config.get("dependencies", {}) | config.get("dev-dependencies", {})
    path_count = sum(isinstance(value, dict) and "path" in value
                     for value in dependencies.values())
    if clean:
        # Keep only compiler-produced configuration hashes. Gleam still removes
        # every source package, packages.toml and compiled artifact.
        with tempfile.TemporaryDirectory() as temporary:
            saved = Path(temporary)
            for path in fingerprints(project):
                shutil.copy2(path, saved / path.name)
            subprocess.run(["gleam", "clean"], cwd=project, check=True)
            restore(project, saved)
    # Gleam 1.18 checks one changed path fingerprint per resolution. Settle all
    # direct paths here, where rate-limit retries cannot rerun tests. Once warm,
    # these commands use the manifest and do not request registry metadata.
    for _ in range(path_count + 1):
        download(project)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("projects", nargs="+")
    arguments = parser.parse_args()
    for project in arguments.projects:
        prepare(project, arguments.clean)


if __name__ == "__main__":
    main()
