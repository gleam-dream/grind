#!/usr/bin/env python3
"""Independent-VM Oban fault fixtures; committed rows and effects are separate evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import threading
import time
import traceback
from typing import Callable, TypeVar
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent.parent
T = TypeVar("T")


def wait_for(probe: Callable[[], T], description: str, timeout: float = 30) -> T:
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        value = probe()
        if value:
            return value
        time.sleep(0.025)
    raise AssertionError(f"Timed out waiting for {description}")


def json_lines(path: Path) -> list[dict]:
    if not path.exists():
        return []
    # A writer may be appending its final line. Only complete records count.
    return [json.loads(line) for line in path.read_text().splitlines(keepends=True)
            if line.endswith("\n")]


def check_result(catalog: dict, scenario: str, actual: dict) -> None:
    expected = next(entry for entry in catalog["scenarios"] if entry["id"] == scenario)
    if json.dumps(actual, sort_keys=True) != json.dumps(expected["expected"]["oban"], sort_keys=True):
        raise AssertionError(f"{scenario}: observed {actual}, expected {expected['expected']['oban']}")


class Node:
    def __init__(self, runner: Runner, label: str):
        self.runner, self.label = runner, label
        self.effects = runner.output / f"{runner.case}-{label}-effects.jsonl"
        self.events = runner.output / f"{runner.case}-{label}-plugins.jsonl"
        self.replies: queue.Queue[dict] = queue.Queue()
        self.beam_name = f"oban_fault_{runner.suffix}_{runner.case.lower()}_{label}@127.0.0.1"
        paths = sorted((ROOT / "oracle/_build/dev/lib").glob("*/ebin"))
        if not (ROOT / "oracle/_build/dev/lib/grind_oracle/ebin/Elixir.GrindOracle.FaultWorker.beam").exists():
            raise RuntimeError("Compile oracle/ before running independent-VM fixtures")
        env = dict(os.environ,
                   GRIND_OBAN_TEST_DATABASE_URL=runner.url,
                   ORACLE_FAULT_NODE=label, ORACLE_FAULT_SCHEMA=runner.schema,
                   ORACLE_FAULT_SCENARIO=runner.case, ORACLE_FAULT_EFFECTS=str(self.effects),
                   ORACLE_FAULT_EVENTS=str(self.events),
                   ORACLE_FAULT_PLUGIN_INTERVAL_MS=str(runner.args.plugin_interval_ms),
                   ORACLE_FAULT_PEER_INTERVAL_MS=str(runner.args.peer_interval_ms),
                   ORACLE_FAULT_RESCUE_AFTER_MS=str(runner.args.rescue_after_ms),
                   ERL_CRASH_DUMP=str(runner.output / f"{runner.case}-{label}-crash.dump"))
        command = ["elixir", "--erl", "+S 2:2", "--name", self.beam_name,
                   "--cookie", "grind_oracle_fault"]
        for path in paths:
            command.extend(["-pa", str(path)])
        command.append(str(ROOT / "oracle/fault_node.exs"))
        self.process = subprocess.Popen(command, cwd=ROOT / "oracle", env=env,
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, text=True, bufsize=1)
        self.log = open(runner.output / f"{runner.case}-{label}.log", "w")
        runner.nodes.append(self)
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        runner.record("beam_started", node=label, pid=self.process.pid,
                      beam_name=self.beam_name, command=command, concurrency=1, pool_size=5)
        ready = self.receive("ready")
        assert ready["os_pid"] == self.process.pid, "controller must own the actual BEAM PID"
        assert ready["beam_node"] == self.beam_name

    def _read(self) -> None:
        assert self.process.stdout is not None
        for line in self.process.stdout:
            self.log.write(line)
            self.log.flush()
            if line.startswith("ORACLE_FAULT "):
                self.replies.put(json.loads(line[len("ORACLE_FAULT "):]))
        self.replies.put({"op": "process_exited", "returncode": self.process.wait()})

    def receive(self, op: str) -> dict:
        reply = self.replies.get(timeout=30)
        if reply.get("op") != op:
            raise AssertionError(f"{self.label}: expected {op}, got {reply}")
        self.runner.record("node_reply", node=self.label, reply=reply)
        return reply

    def command(self, op: str, **args: object) -> dict:
        assert self.process.stdin is not None
        self.process.stdin.write(json.dumps(dict(op=op, **args)) + "\n")
        self.process.stdin.flush()
        return self.receive(op)

    def start(self) -> None:
        assert self.command("start")["ok"]
        wait_for(lambda: not self.command("queue")["paused"], f"{self.label} queue resumed")

    def kill(self) -> int:
        at_ms = time.time_ns() // 1_000_000
        self.process.send_signal(signal.SIGKILL)
        self.runner.record("os_signal_sent", node=self.label, pid=self.process.pid, signal="SIGKILL")
        code = self.process.wait(timeout=5)
        assert code == -signal.SIGKILL, f"not a confirmed SIGKILL exit: {code}"
        self.runner.record("beam_death_confirmed", node=self.label, pid=self.process.pid, returncode=code)
        return at_ms

    def close(self) -> None:
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.reader.join(timeout=5)
        self.log.close()


class Runner:
    def __init__(self, args: argparse.Namespace):
        self.args, self.url, self.output = args, args.database_url, args.output.resolve()
        parsed = urlsplit(self.url)
        if parsed.hostname != "127.0.0.1" or parsed.path != "/oban_resilience":
            raise ValueError("Requires the disposable local oban_resilience database")
        self.output.mkdir(parents=True, exist_ok=False)
        self.suffix = str(time.time_ns())[-10:]
        self.case = "setup"
        self.nodes: list[Node] = []
        self.results: list[dict] = []
        self.events = open(self.output / "events.jsonl", "w")
        self.catalog = json.loads((ROOT / "oracle/fault-scenarios.json").read_text())
        assert self.catalog["version"] == 1
        self.provenance()

    def record(self, event: str, **data: object) -> None:
        self.events.write(json.dumps(dict(event=event, case=self.case,
                                          monotonic_ns=time.monotonic_ns(),
                                          at_ns=time.time_ns(), **data)) + "\n")
        self.events.flush()

    def provenance(self) -> None:
        def git(*args: str) -> str:
            return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True).strip()

        pin = subprocess.check_output(["git", "-C", str(ROOT / "oracle/deps/oban"),
                                       "rev-parse", "HEAD"], text=True).strip()
        assert pin == self.catalog["oracle"]["commit"], "Oban source pin mismatch"
        dependency_dirty = subprocess.check_output(
            ["git", "-C", str(ROOT / "oracle/deps/oban"), "status", "--porcelain"], text=True).strip()
        assert not dependency_dirty, "modified Oban source cannot validate the pinned oracle"
        digest = hashlib.sha256()
        for name in sorted(git("ls-files", "--cached", "--others", "--exclude-standard").splitlines()):
            if any(part.startswith(".env") or part in {".aws", ".codex"} for part in Path(name).parts):
                continue
            path = ROOT / name
            if path.is_file() and not path.is_relative_to(self.output) and not name.startswith(
                    ("oracle/results/", "resilience/results/", "bench/results/")):
                digest.update(name.encode() + b"\0" + path.read_bytes())
        dirty = bool(git("status", "--porcelain"))
        data = dict(commit=git("rev-parse", "HEAD"), dirty=dirty,
                    source_sha256=digest.hexdigest(), oban_commit=pin,
                    evidence_class="exploratory" if dirty else "candidate",
                    catalog_sha256=hashlib.sha256((ROOT / "oracle/fault-scenarios.json").read_bytes()).hexdigest(),
                    parameters=dict(peer_interval_ms=self.args.peer_interval_ms,
                                    plugin_interval_ms=self.args.plugin_interval_ms,
                                    rescue_after_ms=self.args.rescue_after_ms,
                                    pruner_max_age_seconds=1, queue_concurrency=1, pool_size=5),
                    scenarios=self.args.scenarios,
                    postgres=subprocess.check_output(["psql", "--version"], text=True).strip(),
                    elixir=subprocess.check_output(["elixir", "--erl", "+S 1:1", "--version"], text=True).strip())
        (self.output / "provenance.json").write_text(json.dumps(data, indent=2) + "\n")

    def sql(self, sql: str) -> str:
        return subprocess.check_output(["psql", "-XAt", self.url, "-v", "ON_ERROR_STOP=1",
                                        "-c", sql], text=True, timeout=30).strip()

    def rows(self, sql: str) -> list[dict]:
        return json.loads(self.sql(f"SELECT COALESCE(json_agg(row_to_json(t)), '[]') FROM ({sql}) t"))

    def jobs(self) -> list[dict]:
        return self.rows(f'SELECT * FROM "{self.schema}".oban_jobs ORDER BY id')

    def effects(self, key: str) -> list[dict]:
        return [event for node in self.nodes for event in json_lines(node.effects)
                if event["event"] == "effect" and event["key"] == key]

    def synced_effect(self, node: Node, identity: int, attempt: int) -> bool:
        return Path(f"{node.effects}.{identity}.{attempt}.synced").is_file()

    def snapshot(self, label: str) -> dict:
        data = dict(jobs=self.jobs(),
                    peers=self.rows(f'SELECT * FROM "{self.schema}".oban_peers ORDER BY name'),
                    effects=[row for node in self.nodes for row in json_lines(node.effects)],
                    plugin_events=[row for node in self.nodes for row in json_lines(node.events)],
                    receipt_model="Oban Basic has no Grind acknowledgement receipt or resolution table")
        (self.output / f"{self.case}-{label}-database.json").write_text(json.dumps(data, indent=2) + "\n")
        return data

    def await_completed(self, identity: int, label: str) -> dict:
        def completed() -> dict | None:
            return next((job for job in self.jobs()
                         if job["id"] == identity and job["state"] == "completed"), None)
        row = wait_for(completed, f"committed completion of {identity}")
        self.record("terminal_row_observed", row=row)
        self.snapshot(label)
        return row

    def plugin_witness(self, node: Node, plugin: str, field: str, identity: int) -> dict | None:
        return next((event for event in json_lines(node.events)
                     if event.get("plugin") == plugin and identity in event.get(field, [])
                     and not event["error"]), None)

    def m2(self) -> dict:
        doomed, survivor = Node(self, "doomed"), Node(self, "survivor")
        barrier = self.output / "M2-effect.release"
        identity = doomed.command("submit", key="ambiguous-effect", release=str(barrier))["id"]
        doomed.start()
        wait_for(lambda: self.synced_effect(doomed, identity, 1), "fsynced first external effect")
        first = self.snapshot("before-kill")
        assert len(self.effects("ambiguous-effect")) == 1
        assert [(row["state"], row["attempt"]) for row in first["jobs"]] == [("executing", 1)]
        assert not any(row["event"] == "handler_finished" for row in first["effects"])
        self.record("effect_fsync_confirmed", node=doomed.label, job_id=identity, attempt=1)
        survivor.start()
        doomed.kill()
        wait_for(lambda: self.plugin_witness(survivor, "Oban.Lifeline", "rescued_ids", identity),
                 "survivor Lifeline rescue", self.args.rescue_after_ms / 1000 + 15)
        wait_for(lambda: self.synced_effect(survivor, identity, 2), "fsynced automatic second effect")
        rescued = self.snapshot("before-release")
        effects = self.effects("ambiguous-effect")
        assert len(effects) == 2 and not barrier.exists()
        assert len({event["os_pid"] for event in effects}) == 2
        assert len({event["beam_node"] for event in effects}) == 2
        assert [(row["state"], row["attempt"]) for row in rescued["jobs"]] == [("executing", 2)]
        self.record("automatic_replay_witnessed", job_id=identity, effects=2, operator_replay=False)
        barrier.touch()
        self.record("handler_barrier_released", path=str(barrier))
        final = self.await_completed(identity, "completed")
        return dict(effects_before_kill=1, effects_before_operator_replay=len(effects),
                    final_effects=len(self.effects("ambiguous-effect")), final_state=final["state"],
                    final_attempt=final["attempt"], lifeline_rescue_observed=True,
                    independent_worker_vms=len({event["os_pid"] for event in effects}))

    def m6(self) -> dict:
        first, second = Node(self, "a"), Node(self, "b")
        def one_leader() -> list[tuple[Node, dict]] | None:
            states = [(node, node.command("peer")) for node in (first, second)]
            return states if sum(reply["leader"] for _, reply in states) == 1 else None
        states = wait_for(one_leader, "one initial leader")
        leader = next(node for node, reply in states if reply["leader"])
        survivor = second if leader is first else first
        assert all(reply["leader_node"] == leader.beam_name for _, reply in states)
        initial_peers = self.snapshot("initial-leader")["peers"]
        assert len(initial_peers) == 1 and initial_peers[0]["node"] == leader.beam_name
        first.start()
        second.start()
        old = first.command("submit", key="before-leader-kill", release="")["id"]
        self.await_completed(old, "old-seed-completed")
        old_prune = wait_for(lambda: self.plugin_witness(leader, "Oban.Pruner", "pruned_ids", old),
                             "old leader's committed prune")
        assert old_prune["pruned_count"] == 1 and self.jobs() == []
        self.record("maintenance_before_kill", node=leader.label, witness=old_prune)
        kill_at_ms = leader.kill()
        elected = wait_for(lambda: survivor.command("peer")["leader"], "survivor elected")
        assert elected and survivor.command("peer")["leader_node"] == survivor.beam_name
        peer_rows = self.snapshot("survivor-elected")["peers"]
        assert len(peer_rows) == 1 and peer_rows[0]["node"] == survivor.beam_name
        election = wait_for(lambda: next((event for event in json_lines(survivor.events)
                           if event["event"] == "peer_election" and event["leader"]
                           and event["was_leader"] is False and event["at_ms"] >= kill_at_ms), None),
                            "post-kill election transition")
        self.record("peer_failover_witnessed", witness=election)
        new = survivor.command("submit", key="after-leader-kill", release="")["id"]
        self.await_completed(new, "new-seed-completed")
        new_prune = wait_for(lambda: self.plugin_witness(survivor, "Oban.Pruner", "pruned_ids", new),
                             "survivor's committed prune")
        assert new_prune["pruned_count"] == 1 and self.jobs() == []
        self.record("maintenance_after_kill", node=survivor.label, witness=new_prune)
        return dict(initial_leaders=1, leader_changed_after_kill=True,
                    old_leader_pruned_seed=True, survivor_pruned_seed=True,
                    final_job_count=len(self.jobs()), independent_vms=len(self.nodes))

    def run_case(self, name: str) -> None:
        self.case, self.nodes = name, []
        self.schema = f"oracle_{self.suffix}_{name.lower()}"
        self.sql(f'CREATE SCHEMA "{self.schema}"')
        result = dict(scenario=name, status="failed")
        self.record("scenario_started")
        try:
            actual = {"M2": self.m2, "M6": self.m6}[name]()
            check_result(self.catalog, name, actual)
            result.update(status="passed", result=actual,
                          classification="intentional-grind-semantics", engine="oban",
                          catalog_version=self.catalog["version"])
            self.record("scenario_passed", result=actual)
        except BaseException as error:
            result["error"] = str(error)
            (self.output / f"{name}-failure.txt").write_text(traceback.format_exc())
            raise
        finally:
            try:
                self.snapshot("final")
            except (subprocess.SubprocessError, OSError, ValueError) as error:
                self.record("final_database_snapshot_unavailable", reason=str(error))
            for node in self.nodes:
                node.close()
            self.results.append(result)
            (self.output / "results.json").write_text(json.dumps(self.results, indent=2) + "\n")


def main() -> None:
    if not __debug__:
        raise RuntimeError("Acceptance assertions require Python without optimization")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scenarios", default="M2,M6")
    parser.add_argument("--peer-interval-ms", type=int, default=500)
    parser.add_argument("--plugin-interval-ms", type=int, default=200)
    parser.add_argument("--rescue-after-ms", type=int, default=5000)
    args = parser.parse_args()
    scenarios = args.scenarios.split(",")
    if not scenarios or len(scenarios) != len(set(scenarios)) or set(scenarios) - {"M2", "M6"}:
        parser.error("scenarios must be a nonempty subset of M2,M6 without duplicates")
    if min(args.peer_interval_ms, args.plugin_interval_ms, args.rescue_after_ms) <= 0:
        parser.error("timing parameters must be positive")
    runner = Runner(args)
    try:
        for name in scenarios:
            runner.run_case(name)
    finally:
        runner.events.close()
    print(f"Oban independent-VM fixtures passed: {', '.join(scenarios)}; evidence: {runner.output}")


if __name__ == "__main__":
    main()
