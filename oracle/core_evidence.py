#!/usr/bin/env python3
"""Retain and bind core paired results to their exact catalog and source inputs."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parent.parent


def command(*args: str) -> str:
    return subprocess.check_output(args, text=True).strip()


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_digest() -> str:
    names = command("git", "-C", str(ROOT), "ls-files", "--cached", "--others", "--exclude-standard")
    paths = [(name, ROOT / name) for name in sorted(set(names.splitlines()))]
    sibling = ROOT.parent / "sinal"
    paths += [("sinal/" + str(path.relative_to(sibling)), path)
              for path in sorted((sibling / "src").rglob("*")) if path.is_file()]
    paths += [("sinal/" + name, sibling / name) for name in ("gleam.toml", "manifest.toml")]
    digest = hashlib.sha256()
    for name, path in paths:
        if any(part.startswith(".env") or part in {".aws", ".codex"} for part in Path(name).parts):
            continue
        if path.is_file() and not name.startswith(("oracle/results/", "bench/results/", "resilience/results/")):
            digest.update(name.encode() + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


def prepare(output: Path, catalog_path: Path, run_id: str) -> None:
    catalog = json.loads(catalog_path.read_text())
    pin = command("git", "-C", str(ROOT / "oracle/deps/oban"), "rev-parse", "HEAD")
    if pin != catalog["oracle"]["commit"]:
        raise ValueError("Oban source pin differs from catalog")
    if command("git", "-C", str(ROOT / "oracle/deps/oban"), "status", "--porcelain"):
        raise ValueError("Oban source has local modifications")
    (output / "catalog.json").write_bytes(catalog_path.read_bytes())
    dirty = bool(command("git", "-C", str(ROOT), "status", "--porcelain"))
    dependencies = ["manifest.toml", "gleam.toml", "oracle/mix.lock", "oracle/mix.exs"]
    tools = dict(gleam=command("gleam", "--version"),
                 elixir=command("elixir", "--erl", "+S 1:1", "--version"),
                 postgres_client=command("psql", "--version"),
                 postgres_server=command("psql", "-XAt", os.environ["GRIND_ORACLE_DATABASE_URL"],
                                         "-v", "ON_ERROR_STOP=1", "-c", "SHOW server_version"))
    data = dict(version=1, run_id=run_id, status="started", started_at_ns=time.time_ns(),
                commit=command("git", "-C", str(ROOT), "rev-parse", "HEAD"), dirty=dirty,
                evidence_class="exploratory" if dirty else "candidate", source_sha256=source_digest(),
                catalog_sha256=sha256(output / "catalog.json"), oban_commit=pin,
                dependency_sha256={name: sha256(ROOT / name) for name in dependencies},
                toolchain=tools,
                toolchain_sha256=hashlib.sha256(json.dumps(tools, sort_keys=True).encode()).hexdigest())
    (output / "provenance.json").write_text(json.dumps(data, indent=2) + "\n")


def finish(output: Path, exit_code: int) -> None:
    path = output / "provenance.json"
    data = json.loads(path.read_text())
    source_unchanged = data["source_sha256"] == source_digest()
    catalog_unchanged = data["catalog_sha256"] == sha256(output / "catalog.json")
    failures = []
    if not source_unchanged:
        failures.append("source changed during paired execution")
    if not catalog_unchanged:
        failures.append("catalog snapshot changed during paired execution")
    dependency = ROOT / "oracle/deps/oban"
    if command("git", "-C", str(dependency), "rev-parse", "HEAD") != data["oban_commit"] or command(
            "git", "-C", str(dependency), "status", "--porcelain"):
        failures.append("pinned Oban source changed during paired execution")
    artifacts = {}
    for engine in ("grind", "oban"):
        result = output / f"{engine}.jsonl"
        if result.exists():
            artifacts[result.name] = sha256(result)
            rows = [json.loads(line) for line in result.read_text().splitlines() if line]
            if not rows or any(row.get("run_id") != data["run_id"] for row in rows):
                failures.append(f"{engine} result run identity differs from provenance")
        elif exit_code == 0:
            failures.append(f"missing {engine} results")
    data.update(status="passed" if exit_code == 0 and not failures else "failed",
                exit_code=exit_code, finished_at_ns=time.time_ns(),
                source_unchanged=source_unchanged, catalog_unchanged=catalog_unchanged,
                artifact_sha256=artifacts, failures=failures)
    path.write_text(json.dumps(data, indent=2) + "\n")
    if failures:
        raise ValueError("; ".join(failures))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "finish"])
    parser.add_argument("output", type=Path)
    parser.add_argument("arguments", nargs="+")
    args = parser.parse_args()
    if args.action == "prepare":
        catalog, run_id = args.arguments
        prepare(args.output, Path(catalog), run_id)
    else:
        [exit_code] = args.arguments
        finish(args.output, int(exit_code))


if __name__ == "__main__":
    main()
