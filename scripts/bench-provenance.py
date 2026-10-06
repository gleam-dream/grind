#!/usr/bin/env python3
"""Record exact local benchmark input, without labelling a dirty run a release run."""
import hashlib
import json
import os
import platform
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parent.parent

def git(*args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def source_digest() -> str:
    names = git("ls-files", "--cached", "--others", "--exclude-standard").splitlines()
    digest = hashlib.sha256()
    for name in sorted(set(names)):
        if any(part.startswith(".env") or part in {".aws", ".codex"} for part in Path(name).parts):
            continue
        if name.startswith(("bench/results/", ".git/")):
            continue
        path = root / name
        if path.is_file():
            digest.update(name.encode() + b"\0" + path.read_bytes() + b"\0")
    # The unpublished path dependency is also executable benchmark input.
    sibling = root.parent / "sinal"
    sibling_inputs = list((sibling / "src").rglob("*")) + [sibling / "gleam.toml", sibling / "manifest.toml"]
    for path in sorted(sibling_inputs):
        if any(part.startswith(".env") or part in {".aws", ".codex"} for part in path.relative_to(sibling).parts):
            continue
        if path.is_file():
            digest.update(str(path.relative_to(sibling)).encode() + b"\0" + path.read_bytes())
    return digest.hexdigest()

def command_output(*arguments: str) -> str:
    return subprocess.check_output(arguments, text=True, stderr=subprocess.STDOUT, timeout=15).strip()


def toolchain_metadata() -> dict[str, str]:
    # Read the installed OTP patch version as well as the release and ERTS.
    # This runs only while recording provenance, never for --digest.
    expression = (
        'R=erlang:system_info(otp_release),'
        '{ok,V}=file:read_file(filename:join([code:root_dir(),"releases",R,"OTP_VERSION"])),'
        'io:format("~s~n~s~n~s~n~s~n",[R,string:trim(V),erlang:system_info(version),'
        'erlang:system_info(system_architecture)]),halt().'
    )
    otp_release, otp_version, erts_version, runtime_architecture = command_output(
        "erl", "+S", "1:1", "-noshell", "-eval", expression
    ).splitlines()
    return {
        "gleam": command_output("gleam", "--version"),
        "otp_release": otp_release,
        "otp_version": otp_version,
        "erts_version": erts_version,
        "erlang_architecture": runtime_architecture,
        "postgresql": command_output("postgres", "--version"),
    }


def machine_metadata() -> dict[str, str | int | None]:
    # Deliberately exclude platform.node()/uname().node and environment dumps.
    return {"os": platform.system(), "os_release": platform.release(),
            "os_version": platform.version(), "architecture": platform.machine(),
            "cpu_count": os.cpu_count()}


def sinal_metadata() -> dict[str, str | bool | None]:
    sibling = root.parent / "sinal"
    try:
        return {"commit": command_output("git", "-C", str(sibling), "rev-parse", "HEAD"),
                "dirty": bool(command_output("git", "-C", str(sibling), "status", "--porcelain"))}
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        return {"commit": None, "dirty": None}


def drain_timeout_ms(raw: str | None) -> int:
    """Match the benchmark parser; --digest deliberately never calls this."""
    if raw is None:
        return 60_000
    if not raw or any(character not in "0123456789" for character in raw) or int(raw) <= 0:
        raise ValueError("GRIND_BENCH_DRAIN_TIMEOUT_MS must be a positive decimal integer")
    return int(raw)


def reserve_output_directory(output: Path) -> None:
    """Atomically reserve a new run directory; never mix or overwrite evidence."""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.mkdir()


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--reserve-dir":
        try:
            reserve_output_directory(Path(sys.argv[2]))
        except FileExistsError:
            raise SystemExit(f"Refusing existing benchmark output directory: {sys.argv[2]}")
        raise SystemExit(0)
    dirty = bool(git("status", "--porcelain"))
    digest = source_digest()
    if len(sys.argv) == 2 and sys.argv[1] == "--digest":
        print(digest)
    else:
        output = Path(sys.argv[1])
        try:
            drain_budget = drain_timeout_ms(os.getenv("GRIND_BENCH_DRAIN_TIMEOUT_MS"))
        except ValueError as error:
            raise SystemExit(str(error)) from error
        data = {
            "commit": git("rev-parse", "HEAD"), "dirty": dirty,
            "source_sha256": digest, "arguments": sys.argv[2:],
            "evidence_class": "exploratory" if dirty else "candidate",
            "network_delay_ms_per_direction_per_chunk": int(os.getenv("GRIND_BENCH_NETWORK_DELAY_MS", "0")),
            "observer_interval_ms": 10,
            "drain_timeout_ms": drain_budget,
            "toolchain": toolchain_metadata(),
            "machine": machine_metadata(),
            "sinal": sinal_metadata(),
            "environment": {name: os.environ[name] for name in ("ERL_FLAGS", "GRIND_BENCH_REPEATS", "GRIND_BENCH_DRAIN_TIMEOUT_MS", "GRIND_BENCH_MAX_INFLIGHT", "GRIND_BENCH_T2_STRESS", "GRIND_BENCH_T2_PROFILES") if name in os.environ},
        }
        output.write_text(json.dumps(data, indent=2) + "\n")
        if os.getenv("GRIND_BENCH_RELEASE_EVIDENCE") == "1" and dirty:
            raise SystemExit("Release evidence requires a clean source tree; this run is exploratory.")
