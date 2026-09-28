#!/usr/bin/env python3
"""Independent BEAM process acceptance scenarios. All assertions fail closed."""

import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import queue
import signal
import shutil
import subprocess
import threading
import time
import traceback
from urllib.parse import urlsplit, urlunsplit

from transport import Proxy

ROOT = Path(__file__).resolve().parent.parent


def wait_for(probe, description, timeout=30):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        value = probe()
        if value:
            return value
        time.sleep(0.025)
    raise AssertionError(f"Timed out waiting for {description}")


class Node:
    def __init__(self, runner, label, version="v1", concurrency=1, proxied=False, grace=1000):
        self.runner, self.label = runner, label
        self.replies = queue.Queue()
        self.closed = False
        self.paused = False
        self.effects = runner.output / f"{runner.case}-{label}-effects.jsonl"
        self.proxy = None
        url = runner.url
        if proxied:
            parsed = urlsplit(url)
            self.proxy = Proxy(parsed.hostname, parsed.port or 5432,
                               lambda event, **data: runner.record(event, node=label, **data))
            url = urlunsplit((parsed.scheme, f"{parsed.username}@127.0.0.1:{self.proxy.port}",
                             parsed.path, parsed.query, ""))
        env = dict(os.environ, RESILIENCE_NODE=label, RESILIENCE_DATABASE_URL=url,
                   RESILIENCE_SCHEMA=runner.schema, RESILIENCE_QUEUE="resilience",
                   RESILIENCE_VERSION=version, RESILIENCE_CONCURRENCY=str(concurrency),
                   RESILIENCE_DEADLINE_MS=str(runner.deadline_ms),
                   RESILIENCE_LEASE_MS=str(runner.lease_ms), RESILIENCE_GRACE_MS=str(grace),
                   RESILIENCE_EFFECTS=str(self.effects),
                   ERL_CRASH_DUMP=str(runner.output / f"{runner.case}-{label}-crash.dump"))
        paths = runner.runtime_paths
        if not paths:
            raise RuntimeError("Build resilience/ before running scenarios")
        name = f"grind_res_{runner.suffix}_{runner.serial}_{label}@127.0.0.1"
        command = ["erl", "+S", "2:2", "-name", name, "-setcookie", "grind_resilience",
                   "-pa", *paths, "-noshell", "-eval",
                   "{ok,_}=application:ensure_all_started(grind_resilience), grind_resilience:main(), halt()."]
        self.process = subprocess.Popen(command, cwd=ROOT / "resilience", env=env,
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, text=True, bufsize=1)
        self.log = open(runner.output / f"{runner.case}-{label}.log", "w")
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        runner.nodes.append(self)
        runner.record("beam_started", node=label, pid=self.process.pid, beam_name=name,
                      concurrency=concurrency, main_pool_size=4, version=version,
                      proxied=proxied, command=command)
        self.receive("ready", timeout=30)

    def _read(self):
        for line in self.process.stdout:
            self.log.write(line)
            self.log.flush()
            if line.startswith("RESILIENCE "):
                self.replies.put(json.loads(line[len("RESILIENCE "):]))
        self.replies.put({"op": "process_exited", "returncode": self.process.wait()})

    def receive(self, op, timeout=30):
        reply = self.replies.get(timeout=timeout)
        if reply.get("op") != op:
            raise AssertionError(f"{self.label}: expected {op}, got {reply}")
        self.runner.record("node_reply", node=self.label, reply=reply)
        return reply

    def command(self, op, timeout=30, **args):
        self.process.stdin.write(json.dumps(dict(op=op, **args)) + "\n")
        self.process.stdin.flush()
        return self.receive(op, timeout=timeout)

    def signal(self, sig):
        self.process.send_signal(sig)
        if sig in (signal.SIGSTOP, signal.SIGCONT):
            self.paused = sig == signal.SIGSTOP
        self.runner.record("os_signal_sent", node=self.label, pid=self.process.pid, signal=sig)
        if sig == signal.SIGKILL:
            self.process.wait(timeout=5)
            self.runner.record("beam_death_confirmed", node=self.label, pid=self.process.pid,
                               returncode=self.process.returncode)

    def close(self):
        if self.closed:
            return
        self.closed = True
        if self.process.poll() is None:
            try:
                if self.paused:
                    self.process.kill()
                    self.process.wait(timeout=5)
                    raise OSError("Paused scenario VM killed during cleanup")
                self.command("exit")
                self.process.wait(timeout=10)
            except (OSError, AssertionError, queue.Empty, subprocess.TimeoutExpired):
                self.process.kill()
                self.process.wait(timeout=5)
        if self.proxy:
            self.runner.record("transport_totals", node=self.label, **self.proxy.snapshot())
            self.proxy.close()
        self.reader.join(timeout=3)
        self.log.close()


class Runner:
    def __init__(self, args):
        self.args, self.url, self.output = args, args.database_url, args.output.resolve()
        parsed = urlsplit(self.url)
        if parsed.hostname != "127.0.0.1" or parsed.path != "/grind_resilience":
            raise ValueError("This destructive harness requires its disposable local grind_resilience database")
        self.output.mkdir(parents=True, exist_ok=False)
        self.suffix = str(time.time_ns())[-10:]
        self.serial = 0
        self.nodes = []
        self.results = []
        self.effect_files = {}
        self.lock = threading.Lock()
        self.events = open(self.output / "events.jsonl", "w")
        self.lease_ms, self.deadline_ms = args.lease_ms, args.deadline_ms
        self.case = "setup"
        self.snapshot_runtime()
        self.provenance()

    def record(self, event, **data):
        with self.lock:
            self.events.write(json.dumps(dict(event=event, case=self.case,
                                              monotonic_ns=time.monotonic_ns(),
                                              at_ns=time.time_ns(), **data)) + "\n")
            self.events.flush()

    def snapshot_runtime(self):
        # New VMs throughout a long soak must load the same built artifacts even
        # if another task later builds the shared checkout. Keep executable input
        # provenance, not merely a digest of possibly newer source files.
        destination = self.output / "runtime"
        build = ROOT / "resilience/build/dev/erlang"
        for ebin in sorted(build.glob("*/ebin")):
            package = destination / ebin.parent.name
            shutil.copytree(ebin, package / "ebin")
            if (ebin.parent / "priv").is_dir():
                shutil.copytree(ebin.parent / "priv", package / "priv")
        self.runtime_paths = sorted(str(path) for path in destination.glob("*/ebin"))
        if not self.runtime_paths:
            raise RuntimeError("Build resilience/ before running scenarios")
        digest, files = hashlib.sha256(), {}
        for path in sorted(destination.rglob("*")):
            if path.is_file():
                name, content = str(path.relative_to(destination)), path.read_bytes()
                files[name] = hashlib.sha256(content).hexdigest()
                digest.update(name.encode() + b"\0" + content)
        self.runtime_sha256 = digest.hexdigest()
        (self.output / "runtime-manifest.json").write_text(json.dumps(files, indent=2) + "\n")

    def provenance(self):
        git = lambda *args: subprocess.check_output(["git", "-C", str(ROOT), *args], text=True)
        names = set(git("ls-files", "--cached", "--others", "--exclude-standard").splitlines())
        digest = hashlib.sha256()
        for name in sorted(names):
            path = ROOT / name
            if path.is_file() and not path.is_relative_to(self.output) and not name.startswith(("bench/results/", "resilience/results/")):
                digest.update(name.encode() + b"\0" + path.read_bytes())
        sibling = ROOT.parent / "sinal"
        for path in sorted((sibling / "src").rglob("*")):
            if path.is_file():
                digest.update(str(path.relative_to(sibling)).encode() + b"\0" + path.read_bytes())
        dirty = bool(git("status", "--porcelain").strip())
        data = dict(commit=git("rev-parse", "HEAD").strip(), dirty=dirty,
                    source_sha256=digest.hexdigest(), runtime_sha256=self.runtime_sha256, evidence_class="exploratory" if dirty else "candidate",
                    lease_ms=self.lease_ms, deadline_ms=self.deadline_ms,
                    scenarios=self.args.scenarios, soak_seconds=self.args.soak_seconds,
                    postgres=subprocess.check_output(["psql", "--version"], text=True).strip(),
                    otp=subprocess.check_output(["erl", "+S", "1:1", "-noshell", "-eval",
                         'io:format("~s",[erlang:system_info(otp_release)]),halt().'], text=True))
        (self.output / "provenance.json").write_text(json.dumps(data, indent=2) + "\n")
        if self.args.release_evidence and dirty:
            raise RuntimeError("Release evidence requires clean pinned source")
        if self.args.release_evidence:
            required = {"M1", "M2", "M3", "M4", "M5", "M6", "M7", "F1", "F2", "F3", "F4", "F5"}
            if self.args.soak_seconds < 7200 or not required.issubset(self.args.scenarios.split(",")):
                raise RuntimeError("Release evidence requires the complete short suite and at least 7200 seconds of mixed soak")

    def sql(self, sql):
        return subprocess.check_output(["psql", "-XAt", self.url, "-v", "ON_ERROR_STOP=1", "-c", sql], text=True, timeout=30).strip()

    def rows(self, sql):
        return json.loads(self.sql(f"SELECT COALESCE(json_agg(row_to_json(t)), '[]') FROM ({sql}) t"))

    def jobs(self):
        return self.rows(f'SELECT * FROM "{self.schema}".grind_jobs ORDER BY id')

    def receipts(self):
        return self.rows(f'SELECT * FROM "{self.schema}".grind_job_acknowledgements ORDER BY job_id')

    def effect_events(self, node, key=None):
        cache = self.effect_files.setdefault(node.effects, dict(offset=0, events=[], keys={}, effect_count=0))
        if node.effects.exists():
            with node.effects.open() as source:
                source.seek(cache["offset"])
                while line := source.readline():
                    # A concurrent append may not yet have completed its line.
                    if not line.endswith("\n"):
                        break
                    event = json.loads(line)
                    cache["events"].append(event)
                    cache["effect_count"] += int(event["event"] == "effect")
                    cache["keys"].setdefault(event["key"], []).append(event)
                    cache["offset"] = source.tell()
        return cache["events"] if key is None else cache["keys"].get(key, [])

    def effects(self, key=None):
        return [event for node in self.nodes for event in self.effect_events(node, key)
                if event["event"] == "effect"]

    def await_effect(self, node, key):
        wait_for(lambda: self.effects(key), f"effect log for {key}")
        wait_for(lambda: node.command("effect_synced", key=key)["ok"], f"fsync completed for {key}")
        self.record("effect_fsync_confirmed", node=node.label, key=key)

    def state(self, identity):
        jobs = self.rows(f'SELECT state FROM "{self.schema}".grind_jobs WHERE id={int(identity)}')
        return jobs[0]["state"] if jobs else None

    def await_state(self, identity, state):
        wait_for(lambda: self.state(identity) == state, f"job {identity} {state}", self.lease_ms / 1000 + 15)

    def submit(self, node, key, delay=0, release="", mode="normal"):
        return node.command("submit", key=key, delay_ms=delay, release=str(release), mode=mode)["id"]

    def barrier(self, name):
        return self.output / f"{self.case}-{name}.release"

    def audit(self):
        jobs, receipts = self.jobs(), self.receipts()
        resolutions = self.rows(f'SELECT * FROM "{self.schema}".grind_job_resolutions ORDER BY job_id')
        (self.output / f"{self.case}-database.json").write_text(json.dumps(
            dict(jobs=jobs, receipts=receipts, resolutions=resolutions, effects=self.effects()), indent=2) + "\n")
        keys = [(row["job_id"], row["attempt_id"], row["attempt_epoch"]) for row in receipts]
        assert len(keys) == len(set(keys)), "duplicate receipt per attempt"
        return jobs, receipts, resolutions

    def run_case(self, name, action, schema=None):
        self.serial += 1
        self.case, self.schema, self.nodes = name, schema or f"resilience_{self.suffix}_{self.serial}", []
        started = time.monotonic()
        self.record("scenario_started", schema=self.schema)
        try:
            action()
            self.audit()
            result = dict(id=name, status="passed", elapsed_seconds=time.monotonic() - started)
        except Exception as error:
            self.record("scenario_failed", error=repr(error), traceback=traceback.format_exc())
            try:
                self.audit()
            except Exception as audit_error:
                self.record("audit_unavailable", error=repr(audit_error))
            result = dict(id=name, status="failed", error=repr(error), elapsed_seconds=time.monotonic() - started)
        finally:
            for node in reversed(self.nodes):
                node.close()
        self.results.append(result)
        self.record("scenario_completed", **result)
        (self.output / "results.json").write_text(json.dumps(self.results, indent=2) + "\n")
        print(json.dumps(result), flush=True)
        if result["status"] != "passed":
            raise AssertionError(result)

    def m1(self, count=40):
        admin = Node(self, "admin")
        first, second = Node(self, "a", concurrency=2), Node(self, "b", concurrency=2)
        first.command("start")
        second.command("start")
        ids = [self.submit(admin, f"m1-{number}", delay=150) for number in range(count)]
        wait_for(lambda: all(row["state"] == "succeeded" for row in self.jobs()), "all durable completions")
        effects = self.effects()
        assert len(effects) == count and len({row["key"] for row in effects}) == count
        assert {row["node"] for row in effects} == {"a", "b"}, "both independent VMs must execute"
        assert len({row["os_pid"] for row in effects}) == 2
        assert len({row["beam_node"] for row in effects}) == 2
        assert len(self.receipts()) == len(ids)
        for node in (first, second):
            outstanding = high = 0
            for line in node.effects.read_text().splitlines():
                event = json.loads(line)
                outstanding += 1 if event["event"] == "effect" else -1
                high = max(high, outstanding)
            assert outstanding == 0 and high <= 2, (node.label, high, outstanding)
            node.command("sample")
            self.record("local_capacity_checked", node=node.label, maximum_observed=high, configured=2)

    def m2(self, kill_process=False):
        admin, doomed, survivor = Node(self, "admin"), Node(self, "doomed"), Node(self, "survivor")
        release = self.barrier("effect")
        identity = self.submit(admin, "ambiguous-effect", release=release)
        doomed.command("start")
        self.await_effect(doomed, "ambiguous-effect")
        survivor.command("start")
        if kill_process:
            assert doomed.command("kill_worker", key="ambiguous-effect")["ok"]
        else:
            doomed.signal(signal.SIGKILL)
        self.await_state(identity, "uncertain")
        time.sleep(0.3)
        assert len(self.effects("ambiguous-effect")) == 1, "implicit replay after ownership loss"
        quarantined = self.rows(f'SELECT * FROM "{self.schema}".grind_jobs WHERE id={identity}')
        self.record("pre_replay_quarantine_observed", job_id=identity, state="uncertain",
                    effects=len(self.effects("ambiguous-effect")), rows=quarantined)
        release.touch()
        self.record("audited_replay_requested", job_id=identity, resolution_id=f"{self.case}-authorized")
        admin.command("resolve", id=identity, resolution_id=f"{self.case}-authorized")
        self.await_state(identity, "succeeded")
        assert len(self.effects("ambiguous-effect")) == 2, "authorized replay must run exactly once"
        rows = self.rows(f'SELECT * FROM "{self.schema}".grind_job_resolutions')
        assert len(rows) == 1 and rows[0]["resolved_by"] == "resilience-controller"

    def m3(self, direction):
        admin = Node(self, "admin")
        isolated, healthy = Node(self, "isolated", proxied=True, grace=self.lease_ms + 2 * self.deadline_ms), Node(self, "healthy")
        release = self.barrier("partition")
        identity = self.submit(admin, "partitioned-effect", release=release)
        isolated.command("start")
        self.await_effect(isolated, "partitioned-effect")
        healthy.command("start")
        isolated.proxy.configure(direction)
        healthy_id = self.submit(admin, "healthy-sibling")
        self.await_state(healthy_id, "succeeded")
        self.await_state(identity, "uncertain")
        stats = isolated.proxy.snapshot()
        assert sum(stats["fault_bytes"].values()) > 0, "partition never activated"
        isolated.command("trace", path=str(self.output / f"{self.case}-isolated-stacks.jsonl"),
                         duration_ms=self.lease_ms + 2 * self.deadline_ms + 5000)
        isolated.proxy.configure()
        release.touch()
        wait_for(lambda: any(json.loads(line)["event"] == "handler_finished"
                            for line in isolated.effects.read_text().splitlines()), "old handler returned")
        with concurrent.futures.ThreadPoolExecutor(1) as executor:
            pending_stop = executor.submit(isolated.command, "stop",
                                           timeout=(self.lease_ms + 2 * self.deadline_ms) / 1000 + 10)
            while not pending_stop.done():
                self.record("healed_drain_database_activity", rows=self.rows(
                    "SELECT pid,state,wait_event_type,wait_event,query FROM pg_stat_activity "
                    "WHERE datname=current_database() AND pid<>pg_backend_pid()"))
                time.sleep(0.5)
            stopped = pending_stop.result()
        assert stopped["outcome"] == "clean", "old pending acknowledgement did not settle"
        assert self.state(identity) == "uncertain", "stale owner acknowledged after healing"
        assert len(self.effects("partitioned-effect")) == 1
        assert all(row["job_id"] != identity for row in self.receipts()), "stale receipt accepted"

    def m4(self):
        admin, doomed = Node(self, "admin"), Node(self, "doomed", proxied=True)
        release = self.barrier("commit")
        identity = self.submit(admin, "committed-effect", release=release)
        doomed.command("start")
        self.await_effect(doomed, "committed-effect")
        doomed.proxy.drop_next_commit_reply()
        release.touch()
        assert doomed.proxy.commit_seen.wait(10), "COMMIT fault did not activate"
        doomed.signal(signal.SIGSTOP)
        receipt = wait_for(lambda: next((row for row in self.receipts() if row["job_id"] == identity), None),
                           "durable receipt while VM is paused")
        self.record("commit_independently_confirmed", job_id=identity, command_id=receipt["command_id"])
        doomed.signal(signal.SIGKILL)
        restarted = Node(self, "restarted")
        assert restarted.process.pid != doomed.process.pid
        reconciled = restarted.command("reconcile", id=identity, command_id=receipt["command_id"])
        assert reconciled["command_id"] == receipt["command_id"]
        restarted.command("start")
        self.await_state(identity, "succeeded")
        time.sleep(0.3)
        assert len(self.effects("committed-effect")) == 1

    def m5(self):
        old, new = Node(self, "old", version="v1"), Node(self, "new", version="v2")
        release = self.barrier("old-version")
        old_id = self.submit(old, "old-version", release=release)
        old.command("start")
        self.await_effect(old, "old-version")
        new.command("start")
        new_id = self.submit(new, "new-version")
        self.await_state(new_id, "succeeded")
        assert self.state(old_id) == "executing"
        old.signal(signal.SIGKILL)
        self.await_state(old_id, "uncertain")
        assert len(self.effects("old-version")) == 1
        assert self.effects("new-version")[0]["node"] == "new"

    def m6(self):
        first, second = Node(self, "a"), Node(self, "b")
        first.command("start")
        ids = [self.submit(second, f"prune-{number}") for number in range(20)]
        wait_for(lambda: len(self.receipts()) == 20, "durable jobs before pruning")
        first.command("stop")
        # A real row lock held in a separate connection forces both pruners to
        # skip this candidate. The process is released through stdin.
        lock = subprocess.Popen(["psql", "-XqAt", self.url, "-v", "ON_ERROR_STOP=1"],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        lock.stdin.write(f'BEGIN; SELECT id FROM "{self.schema}".grind_jobs WHERE id={ids[0]} FOR UPDATE;\n')
        lock.stdin.flush()
        assert lock.stdout.readline().strip() == str(ids[0])
        self.record("prune_row_lock_acquired", job_id=ids[0], process_pid=lock.pid)
        try:
            with concurrent.futures.ThreadPoolExecutor(2) as executor:
                replies = list(executor.map(lambda node: node.command("prune"), (first, second)))
            assert sum(reply["deleted"] for reply in replies) == 19
            remaining = [row["id"] for row in self.jobs()]
            self.record("concurrent_prune_observed", replies=replies,
                        deleted=sum(reply["deleted"] for reply in replies),
                        remaining_job_ids=remaining, receipts=len(self.receipts()))
            assert remaining == [ids[0]]
        finally:
            lock.stdin.write("ROLLBACK;\n\\q\n")
            lock.stdin.flush()
            lock.wait(timeout=5)
        final = first.command("prune")
        remaining_jobs, remaining_receipts = self.jobs(), self.receipts()
        self.record("unlocked_prune_observed", deleted=final["deleted"],
                    jobs=remaining_jobs, receipts=remaining_receipts)
        assert final["deleted"] == 1
        assert not remaining_jobs and not remaining_receipts
        self.record("intentional_scope", note="Grind has no Peer; Oban leader-failover parity remains separate")

    def m7(self):
        admin = Node(self, "admin")
        worker = Node(self, "worker", concurrency=2, grace=250)
        worker.command("start")
        quick = self.submit(admin, "graceful", delay=100)
        wait_for(lambda: self.effects("graceful"), "graceful job started")
        assert worker.command("stop")["outcome"] == "clean"
        self.await_state(quick, "succeeded")
        worker.command("start")
        release = self.barrier("forced")
        forced = self.submit(admin, "forced", release=release)
        self.await_effect(worker, "forced")
        assert worker.command("stop")["outcome"] == "active_work"
        replacement = Node(self, "replacement", concurrency=4)
        replacement.command("start")
        self.await_state(forced, "uncertain")
        assert len(self.effects("forced")) == 1
        release.touch()
        admin.command("resolve", id=forced, resolution_id="forced-shutdown-replay")
        self.await_state(forced, "succeeded")
        assert len(self.effects("forced")) == 2

    def slow_ack(self):
        admin = Node(self, "admin")
        worker = Node(self, "worker", concurrency=2, proxied=True)
        release = self.barrier("healthy")
        healthy = self.submit(admin, "long-healthy", release=release)
        worker.command("start")
        self.await_effect(worker, "long-healthy")
        self.sql(f'''CREATE FUNCTION "{self.schema}".resilience_slow_ack() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN IF OLD.input->>'key' = 'slow-ack' AND NEW.state = 'succeeded' THEN
            PERFORM pg_sleep({self.deadline_ms * 0.8 / 1000:.3f}); END IF; RETURN NEW; END $$;
          CREATE TRIGGER resilience_slow_ack BEFORE UPDATE ON "{self.schema}".grind_jobs
          FOR EACH ROW EXECUTE FUNCTION "{self.schema}".resilience_slow_ack();''')
        worker.proxy.configure("delay", 5)
        for number in range(int(self.lease_ms / (0.8 * self.deadline_ms)) + 3):
            identity = self.submit(admin, "slow-ack")
            witness = wait_for(lambda: self.rows("SELECT pid,wait_event FROM pg_stat_activity WHERE wait_event='PgSleep'"),
                               "real acknowledgement sleep")
            self.record("slow_ack_activated", iteration=number, database_waiters=witness)
            self.await_state(identity, "succeeded")
        release.touch()
        self.await_state(healthy, "succeeded")
        assert all(row["state"] == "succeeded" for row in self.jobs())
        assert sum(worker.proxy.snapshot()["fault_bytes"].values()) > 0

    def db_loss(self):
        admin, worker = Node(self, "admin"), Node(self, "worker", proxied=True)
        release = self.barrier("connection")
        identity = self.submit(admin, "lost-connection", release=release)
        worker.command("start")
        self.await_effect(worker, "lost-connection")
        worker.proxy.cut_connections()
        terminated = self.rows("SELECT pid,pg_terminate_backend(pid) AS terminated FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")
        assert any(row["terminated"] for row in terminated), "no database connection was terminated"
        self.record("backend_termination_confirmed", backends=terminated)
        # Keep the callback alive across several renewal ticks after reconnect.
        time.sleep(self.lease_ms / 1000 + 0.5)
        release.touch()
        self.await_state(identity, "succeeded")
        assert len(self.effects("lost-connection")) == 1

    def restart_postgres(self):
        data = Path(os.environ["RESILIENCE_PGDATA"]).resolve()
        if not data.parent.name.startswith("grind-resilience.") or not (data / "PG_VERSION").is_file():
            raise RuntimeError("DB restart requires this harness's disposable cluster")
        start = time.monotonic()
        subprocess.run(["pg_ctl", "-D", str(data), "-m", "immediate", "stop"], check=True, capture_output=True, timeout=15)
        self.record("postgres_stopped", data_directory=str(data))
        time.sleep(0.75)
        subprocess.run(["pg_ctl", "-D", str(data), "-o", f"-h 127.0.0.1 -p {urlsplit(self.url).port}",
                        "-l", str(data.parent / "postgres.log"), "start"], check=True, capture_output=True, timeout=15)
        self.record("postgres_restarted", outage_seconds=time.monotonic() - start)

    def db_restart(self):
        admin, worker = Node(self, "admin"), Node(self, "worker")
        release = self.barrier("database-restart")
        identity = self.submit(admin, "database-restart", release=release)
        worker.command("start")
        self.await_effect(worker, "database-restart")
        self.restart_postgres()
        release.touch()
        self.await_state(identity, "succeeded")
        assert len(self.effects("database-restart")) == 1

    def warm_reconnect_paths(self, admin, worker):
        # OTP loads some error-reporting modules only on the first real failure.
        # Warm these paths with asserted recovery before fixing the resource
        # baseline, rather than allowing an ever-growing per-fault atom margin.
        for fault in ("backend-loss", "server-restart"):
            key = f"warmup-{fault}"
            release = self.barrier(key)
            identity = self.submit(admin, key, release=release)
            worker.command("start")
            self.await_effect(worker, key)
            if fault == "backend-loss":
                terminated = self.rows("SELECT pid,pg_terminate_backend(pid) AS terminated FROM pg_stat_activity "
                                       "WHERE datname=current_database() AND pid<>pg_backend_pid()")
                assert any(row["terminated"] for row in terminated)
                self.record("backend_termination_confirmed", phase="warmup", backends=terminated)
                time.sleep(self.lease_ms / 1000 + 0.5)
            else:
                self.restart_postgres()
            release.touch()
            self.await_state(identity, "succeeded")
            assert len(self.effects(key)) == 1
            assert worker.command("stop")["outcome"] == "clean"
            self.prune_all(admin)
            self.record("soak_reconnect_warmup_passed", fault=fault, job_id=identity)

    def mixed_batch(self, admin, worker, prefix):
        # Starts stopped so queued cancellation is deterministic.
        cancelled = self.submit(admin, prefix + "-cancel-before")
        assert admin.command("cancel", id=cancelled)["outcome"] == "before_run"
        worker.command("start")
        ids = {}
        for n in range(24):
            mode = ("normal", "retry_once", "snooze_once")[n % 3]
            key = f"{prefix}-{mode}-{n}"
            ids[self.submit(admin, key, delay=n % 5 * 10, mode=mode)] = (key, mode)
        release = self.barrier(prefix + "-cancel-running")
        running_key = prefix + "-cancel-running"
        running = self.submit(admin, running_key, release=release)
        wait_for(lambda: self.effects(running_key), "running cancellation effect")
        assert admin.command("cancel", id=running)["outcome"] == "requested"
        release.touch()
        self.await_state(running, "cancelled")
        wait_for(lambda: all(row["state"] in ("succeeded", "cancelled") for row in self.jobs()),
                 "mixed batch durable drain")
        rows = {row["id"]: row for row in self.jobs()}
        receipts = self.receipts()
        for identity, (key, mode) in ids.items():
            row = rows[identity]
            assert row["state"] == "succeeded" and len(self.effects(key)) == 1, row
            attempts = [receipt for receipt in receipts if receipt["job_id"] == identity]
            assert len(attempts) == (1 if mode == "normal" else 2), (row, attempts)
            assert row["attempt_count"] == (2 if mode == "retry_once" else 1), row
            assert row["snooze_count"] == (1 if mode == "snooze_once" else 0), row
        assert not self.effects(prefix + "-cancel-before")
        assert len(self.effects(running_key)) == 1
        assert not [receipt for receipt in receipts if receipt["job_id"] == cancelled]
        assert len([receipt for receipt in receipts if receipt["job_id"] == running]) == 1
        assert worker.command("stop")["outcome"] == "clean"
        self.record("soak_mixed_batch_verified", batch=prefix, submitted=26,
                    normal=8, retried=8, snoozed=8, cancelled_before=1, cancelled_running=1,
                    effects=25, receipts=41)

    def prune_all(self, admin):
        time.sleep(0.01)
        while admin.command("prune")["deleted"]:
            pass
        assert not self.jobs() and not self.receipts()
        self.sql(f'VACUUM "{self.schema}".grind_jobs')
        self.sql(f'VACUUM "{self.schema}".grind_job_acknowledgements')
        # Retain raw files and cumulative counts; bound the controller's parsed
        # event index as well as the VMs during a 24-hour run.
        if self.case == "soak":
            for node in self.nodes:
                self.effect_events(node)
                cache = self.effect_files[node.effects]
                self.record("soak_effect_index_checkpoint", node=node.label,
                            effect_count=cache["effect_count"], file_offset=cache["offset"])
                cache["events"].clear()
                cache["keys"].clear()

    def soak_fault(self, name, action, fault):
        # Every reused acceptance case still owns independent VMs, real fault
        # witnesses, its own complete database audit and an individual verdict.
        # The primary soak VMs remain alive and their long job keeps renewing.
        parent = self.case, self.schema, self.nodes
        try:
            self.run_case(name, action, schema=self.soak_schemas[fault])
            audited_schema = self.schema
            if fault == "slow-ack-delay":
                self.sql(f'DROP TRIGGER resilience_slow_ack ON "{audited_schema}".grind_jobs; '
                         f'DROP FUNCTION "{audited_schema}".resilience_slow_ack()')
            self.sql(f'TRUNCATE "{audited_schema}".grind_jobs, '
                     f'"{audited_schema}".grind_job_resolutions, '
                     f'"{audited_schema}".grind_job_acknowledgements, '
                     f'"{audited_schema}".grind_unique_submissions RESTART IDENTITY')
            self.record("audited_fault_schema_drained", schema=audited_schema)
            for node in self.nodes:
                self.effect_files.pop(node.effects, None)
        finally:
            self.case, self.schema, self.nodes = parent

    def database_sample(self):
        return dict(relations=self.rows(
            f"SELECT relname,pg_total_relation_size(oid) AS bytes FROM pg_class WHERE relnamespace='\"{self.schema}\"'::regnamespace AND relkind='r'"),
            sessions=int(self.sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()")))

    def resource_verdict(self, baseline, sample, starts):
        # Fixed bounds apply after durable drain/consumer stop. The only linear
        # allowance is the known three registered-name atoms per consumer start.
        # Cache counts may warm once during the three pre-baseline batches.
        checks = {
            "processes": sample["processes"] <= baseline["processes"] + 4,
            "memory_bytes": sample["memory_bytes"] <= baseline["memory_bytes"] + 64 * 1024 * 1024,
            "atoms": sample["atoms"] <= baseline["atoms"] + starts * 3 + 64,
            "deadline_entries": sample["deadline_entries"] == baseline["deadline_entries"],
            "postgres_type_entries": sample["postgres_type_entries"] == baseline["postgres_type_entries"],
            "postgres_query_entries": sample["postgres_query_entries"] <= baseline["postgres_query_entries"] + 4,
            "owner_mailbox": sample["owner_mailbox"] == [],
            "mailbox_messages": sample["mailbox_messages"] <= baseline["mailbox_messages"] + 32,
            "harness_entries": sample["harness_entries"] == 0,
        }
        return dict(passed=all(checks.values()), checks=checks, baseline=baseline,
                    sample=sample, starts_since_baseline=starts)

    def soak(self):
        faults = [
            ("node-kill", self.m2),
            ("worker-kill", lambda: self.m2(kill_process=True)),
            ("request-partition", lambda: self.m3("request")),
            ("reply-partition", lambda: self.m3("reply")),
            ("full-partition", lambda: self.m3("partition")),
            ("lost-commit-reply", self.m4),
            ("slow-ack-delay", self.slow_ack),
            ("connection-loss", self.db_loss),
            ("database-restart", self.db_restart),
        ]
        # Stable relation OIDs prevent synthetic DDL churn from polluting pgo's
        # per-pool type caches after the deliberate cluster reconnect faults.
        # All schemas are created before the long-lived pools take their baseline.
        parent = self.case, self.schema, self.nodes
        self.soak_schemas = {}
        try:
            for number, (name, _) in enumerate(faults):
                self.schema = f"resilience_{self.suffix}_fault_{number}"
                self.soak_schemas[name] = self.schema
                self.nodes = []
                bootstrap = Node(self, f"bootstrap{number}")
                bootstrap.close()
            self.schema, self.nodes = parent[1], []
            bootstrap = Node(self, "bootstrap_primary")
            bootstrap.close()
        finally:
            for bootstrap in self.nodes:
                bootstrap.close()
            self.case, self.schema, self.nodes = parent
        self.record("soak_fixed_schemas_created", schemas=self.soak_schemas)
        admin, worker = Node(self, "admin"), Node(self, "worker", concurrency=4, proxied=True,
                                                 grace=self.lease_ms + 2 * self.deadline_ms)
        for number in range(3):
            self.mixed_batch(admin, worker, f"warmup-{number}")
            self.prune_all(admin)
        self.warm_reconnect_paths(admin, worker)
        sampler_warmup = {node.label: node.command("sample") for node in (admin, worker)}
        baseline = {node.label: node.command("sample") for node in (admin, worker)}
        self.record("soak_sampler_warmup", first=sampler_warmup, baseline=baseline)
        database_baseline = self.database_sample()
        counts = {name: 0 for name, _ in faults}
        start, round_number = time.monotonic(), 0
        while time.monotonic() - start < self.args.soak_seconds or min(counts.values()) < 2:
            round_number += 1
            self.mixed_batch(admin, worker, f"round-{round_number}")
            # Keep independent primary work live during every destructive case,
            # including cluster-wide backend termination and server restart.
            key = f"round-{round_number}-healthy"
            release = self.barrier(key)
            identity = self.submit(admin, key, release=release)
            worker.command("start")
            self.await_effect(worker, key)
            name, action = faults[(round_number - 1) % len(faults)]
            self.soak_fault(f"soak-{round_number}-{name}", action, name)
            counts[name] += 1
            release.touch()
            self.await_state(identity, "succeeded")
            assert len(self.effects(key)) == 1, "primary work duplicated across destructive fault"
            assert worker.command("stop")["outcome"] == "clean"
            # Preserve this batch's rows before retention removes them.
            jobs, receipts, resolutions = self.jobs(), self.receipts(), self.rows(
                f'SELECT * FROM "{self.schema}".grind_job_resolutions ORDER BY job_id')
            (self.output / f"soak-round-{round_number}-database.json").write_text(json.dumps(
                dict(jobs=jobs, receipts=receipts, resolutions=resolutions), indent=2) + "\n")
            self.prune_all(admin)
            worker.proxy.configure("delay", round_number % 3)
            # Reconnecting pools and their type loaders must settle before a
            # stopped-consumer comparison; no mailbox or cache is drained here.
            time.sleep(0.2)
            verdicts = {}
            for node in (admin, worker):
                sample = node.command("sample")
                verdict = self.resource_verdict(baseline[node.label], sample,
                                                2 * round_number if node is worker else 0)
                verdicts[node.label] = verdict
            database = self.database_sample()
            database_checks = dict(
                sessions=database["sessions"] <= database_baseline["sessions"] + 2,
                retained_relation_bytes=sum(row["bytes"] for row in database["relations"]) <= 16 * 1024 * 1024)
            self.record("soak_resource_verdict", round=round_number, nodes=verdicts,
                        database=database, database_checks=database_checks)
            assert all(verdict["passed"] for verdict in verdicts.values()), verdicts
            assert all(database_checks.values()), database
        total_effects = sum(self.effect_files[node.effects]["effect_count"] for node in self.nodes)
        assert total_effects == 3 * 25 + 2 + round_number * 26
        self.record("soak_acceptance_passed", seconds=time.monotonic() - start, rounds=round_number,
                    target_seconds=self.args.soak_seconds, fault_counts=counts, effects=total_effects,
                    resource_bounds=dict(processes_above_baseline=4, memory_bytes_above_baseline=67108864,
                                         atoms_per_consumer_start=3, atom_warmup_margin=64,
                                         retained_relation_bytes=16777216))

    def pool_churn(self):
        worker, sibling = Node(self, "worker"), Node(self, "sibling")
        sibling.command("start")
        warmup = self.submit(worker, "live-sibling-warmup")
        self.await_state(warmup, "succeeded")
        worker.command("start")
        time.sleep(0.1)
        assert worker.command("stop")["outcome"] == "clean"
        before = worker.command("sample")
        for cycle in range(10):
            worker.command("start")
            time.sleep(0.1)
            assert worker.command("stop")["outcome"] == "clean"
            sample = worker.command("sample")
            self.record("consumer_churn_sample", cycle=cycle + 1, sample=sample)
            identity = self.submit(worker, f"live-sibling-{cycle}")
            self.await_state(identity, "succeeded")
        after = worker.command("sample")
        assert after["deadline_entries"] == before["deadline_entries"]
        assert after["postgres_type_entries"] == before["postgres_type_entries"], (before, after)
        assert after["postgres_query_entries"] == before["postgres_query_entries"], (before, after)
        assert after["owner_mailbox"] == before["owner_mailbox"], (before, after)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scenarios", default="M1,M2,M3,M4,M5,M6,M7,F1,F2,F3,F4,F5")
    parser.add_argument("--lease-ms", type=int, default=12000)
    parser.add_argument("--deadline-ms", type=int, default=1500)
    parser.add_argument("--soak-seconds", type=int, default=0)
    parser.add_argument("--release-evidence", action="store_true")
    args = parser.parse_args()
    runner = Runner(args)
    actions = {"M1": runner.m1, "M2": runner.m2, "M4": runner.m4, "M5": runner.m5,
               "M6": runner.m6, "M7": runner.m7, "F1": runner.slow_ack, "F2": runner.db_loss,
               "F3": lambda: runner.m2(kill_process=True), "F4": runner.db_restart, "F5": runner.pool_churn}
    try:
        for scenario in filter(None, args.scenarios.split(",")):
            if scenario == "M3":
                for direction in ("request", "reply", "partition"):
                    runner.run_case(f"M3-{direction}", lambda direction=direction: runner.m3(direction))
            else:
                runner.run_case(scenario, actions[scenario])
        if args.soak_seconds:
            runner.run_case("soak", runner.soak)
    finally:
        runner.events.close()


if __name__ == "__main__":
    main()
