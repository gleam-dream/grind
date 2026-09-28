#!/usr/bin/env python3
"""Compare normalized M2/M6 observations reconstructed from both retained VM runs."""

import argparse
import hashlib
import json
from pathlib import Path
import signal

from faults import ROOT, json_lines


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def one(rows: list[dict], description: str) -> dict:
    require(len(rows) == 1, f"expected one {description}, got {len(rows)}")
    return rows[0]


class Evidence:
    def __init__(self, directory: Path, engine: str):
        self.directory, self.engine = directory.resolve(), engine
        self.provenance = self.read("provenance.json")
        require(len(self.provenance["source_sha256"]) == 64, f"{engine}: missing source digest")
        require(bool(self.provenance["commit"]), f"{engine}: missing commit")
        self.events = json_lines(self.directory / "events.jsonl")
        self.results = self.read("results.json")
        for scenario in ("M2", "M6"):
            row = one([row for row in self.results
                       if row.get("scenario", row.get("id")) == scenario], f"{engine} {scenario} result")
            require(row["status"] == "passed", f"{engine} {scenario} did not pass")

    def read(self, name: str):
        return json.loads((self.directory / name).read_text())

    def event(self, scenario: str, name: str) -> dict:
        return one([event for event in self.events
                    if event.get("case") == scenario and event["event"] == name],
                   f"{self.engine} {scenario} {name}")

    def effects(self, scenario: str, key: str) -> list[dict]:
        return sorted([event for path in self.directory.glob(f"{scenario}-*-effects.jsonl")
                       for event in json_lines(path)
                       if event["event"] == "effect" and event["key"] == key],
                      key=lambda row: row["at_ms"])

    def death(self, scenario: str) -> dict:
        died = self.event(scenario, "beam_death_confirmed")
        require(died["returncode"] == -signal.SIGKILL, "VM death was not confirmed SIGKILL")
        return died


def grind_m2(evidence: Evidence) -> dict:
    died = evidence.death("M2")
    synced = evidence.event("M2", "effect_fsync_confirmed")
    require(synced["at_ns"] < died["at_ns"], "first effect was not synced before kill")
    quarantined = evidence.event("M2", "pre_replay_quarantine_observed")
    requested = evidence.event("M2", "audited_replay_requested")
    require(died["at_ns"] < quarantined["at_ns"] < requested["at_ns"], "wrong quarantine/replay order")
    row = one(quarantined["rows"], "pre-replay committed Grind row")
    require(row["state"] == quarantined["state"] == "uncertain", "no durable quarantine witness")
    require(row["id"] == requested["job_id"], "replay requested for a different job")
    require(quarantined["effects"] == 1, "implicit effect before replay authorization")
    effects = evidence.effects("M2", "ambiguous-effect")
    require(len(effects) == 2, "Grind replay must produce exactly two total effects")
    require(effects[0]["os_pid"] == died["pid"], "killed VM did not produce first effect")
    require(effects[0]["at_ms"] <= died["at_ns"] // 1_000_000, "effect occurred after VM death")
    require(effects[1]["at_ms"] >= requested["at_ns"] // 1_000_000, "second effect preceded replay request")
    require(len({row["os_pid"] for row in effects}) == 2, "not independent worker VMs")
    final = evidence.read("M2-database.json")
    job = one(final["jobs"], "final Grind M2 job")
    receipt = one(final["receipts"], "final Grind M2 acknowledgement")
    resolution = one(final["resolutions"], "Grind M2 replay audit")
    require(job["id"] == receipt["job_id"] == resolution["job_id"] == row["id"], "M2 identity mismatch")
    require(resolution["attempt_id"] == row["attempt_id"] and
            resolution["attempt_epoch"] == row["attempt_epoch"] and
            receipt["attempt_epoch"] > row["attempt_epoch"] and
            receipt["attempt_id"] != row["attempt_id"], "replay did not establish a new acknowledgement fence")
    require(resolution["decision"] == "authorize_replay" and
            resolution["resolved_by"] == "resilience-controller" and
            resolution["resolution_id"] == requested["resolution_id"], "missing attributed replay")
    return dict(effects_before_kill=1, effects_before_operator_replay=quarantined["effects"],
                state_before_operator_replay=row["state"], final_effects=len(effects),
                final_state=job["state"], attributed_replay_rows=len(final["resolutions"]))


def grind_m6(evidence: Evidence) -> dict:
    locked = evidence.event("M6", "prune_row_lock_acquired")
    concurrent = evidence.event("M6", "concurrent_prune_observed")
    unlocked = evidence.event("M6", "unlocked_prune_observed")
    require(locked["at_ns"] < concurrent["at_ns"] < unlocked["at_ns"], "wrong pruning barrier order")
    require(len(concurrent["replies"]) == 2, "missing concurrent prune result")
    require(sum(row["deleted"] for row in concurrent["replies"]) == concurrent["deleted"],
            "prune total disagrees with replies")
    require(concurrent["receipts"] == 1, "locked job's receipt did not survive")
    final = evidence.read("M6-database.json")
    require(final["jobs"] == unlocked["jobs"] and final["receipts"] == unlocked["receipts"],
            "final pruning snapshots disagree")
    # This is the architecture selected by the Grind fixture; no claim of
    # experimentally proving the universal absence of leader machinery.
    evidence.event("M6", "intentional_scope")
    return dict(leader_election=False, concurrent_unlocked_deletes=concurrent["deleted"],
                locked_row_survived=concurrent["remaining_job_ids"] == [locked["job_id"]],
                after_unlock_deletes=unlocked["deleted"], final_jobs=len(final["jobs"]),
                final_receipts=len(final["receipts"]))


def oban_m2(evidence: Evidence) -> dict:
    died = evidence.death("M2")
    synced = evidence.event("M2", "effect_fsync_confirmed")
    require(synced["at_ns"] < died["at_ns"], "Oban effect was not fsynced before kill")
    before = evidence.read("M2-before-kill-database.json")
    pending = evidence.read("M2-before-release-database.json")
    final = evidence.read("M2-final-database.json")
    first = one(before["jobs"], "Oban first attempt")
    second = one(pending["jobs"], "Oban rescued attempt")
    job = one(final["jobs"], "Oban completed job")
    require(first["state"] == second["state"] == "executing", "missing executing barrier states")
    require(first["attempt"] == 1 and second["attempt"] == 2, "missing real rescue/reclaim")
    require(first["id"] == second["id"] == job["id"], "Oban rescue changed job identity")
    effects = evidence.effects("M2", "ambiguous-effect")
    require(len(effects) == 2 and effects[0]["os_pid"] == died["pid"], "wrong effect/death evidence")
    require([(event["job_id"], event["attempt"]) for event in effects] == [(job["id"], 1), (job["id"], 2)],
            "effects do not belong to the rescued job's two attempts")
    require(len({event["beam_node"] for event in effects}) == 2, "not independent named Oban VMs")
    require(not any(row["event"] == "handler_finished" for row in before["effects"] + pending["effects"]),
            "callback finished before controlled release")
    replay = evidence.event("M2", "automatic_replay_witnessed")
    released = evidence.event("M2", "handler_barrier_released")
    require(died["at_ns"] < replay["at_ns"] < released["at_ns"] and
            replay["operator_replay"] is False, "rescue depended on operator replay")
    require(effects[1]["at_ms"] <= released["at_ns"] // 1_000_000, "second effect wasn't before release")
    rescues = [event for event in final["plugin_events"]
               if event.get("plugin") == "Oban.Lifeline" and job["id"] in event["rescued_ids"]
               and not event["error"]]
    require(len(rescues) == 1 and rescues[0]["os_pid"] != died["pid"], "no survivor rescue witness")
    return dict(effects_before_kill=len([row for row in before["effects"] if row["event"] == "effect"]),
                effects_before_operator_replay=len([row for row in pending["effects"] if row["event"] == "effect"]),
                final_effects=len(effects), final_state=job["state"], final_attempt=job["attempt"],
                lifeline_rescue_observed=bool(rescues),
                independent_worker_vms=len({event["os_pid"] for event in effects}))


def oban_m6(evidence: Evidence) -> dict:
    initial = one(evidence.read("M6-initial-leader-database.json")["peers"], "initial Oban Peer row")
    elected = one(evidence.read("M6-survivor-elected-database.json")["peers"], "survivor Peer row")
    died = evidence.death("M6")
    before = evidence.event("M6", "maintenance_before_kill")
    after = evidence.event("M6", "maintenance_after_kill")
    election = evidence.event("M6", "peer_failover_witnessed")
    require(before["at_ns"] < died["at_ns"] < election["at_ns"] < after["at_ns"],
            "maintenance/election/death order is wrong")
    require(before["witness"]["os_pid"] == died["pid"] and after["witness"]["os_pid"] != died["pid"],
            "maintenance didn't move from killed VM to survivor")
    require(initial["node"] == before["witness"]["beam_node"] and
            elected["node"] == after["witness"]["beam_node"], "Peer and maintenance identities differ")
    for label, event in (("old", before), ("new", after)):
        row = one(evidence.read(f"M6-{label}-seed-completed-database.json")["jobs"], f"{label} completed seed")
        require(row["state"] == "completed" and event["witness"]["pruned_ids"] == [row["id"]]
                and event["witness"]["pruned_count"] == 1 and not event["witness"]["error"],
                f"{label} maintenance lacks actual committed seed and prune IDs")
    final = evidence.read("M6-final-database.json")
    starts = [event for event in evidence.events if event.get("case") == "M6"
              and event["event"] == "beam_started"]
    return dict(initial_leaders=1, leader_changed_after_kill=initial["node"] != elected["node"],
                old_leader_pruned_seed=True, survivor_pruned_seed=True,
                final_job_count=len(final["jobs"]), independent_vms=len({row["pid"] for row in starts}))


def compare(grind_path: Path, oban_path: Path, catalog: dict) -> dict:
    grind, oban = Evidence(grind_path, "grind"), Evidence(oban_path, "oban")
    require(oban.provenance["oban_commit"] == catalog["oracle"]["commit"], "Oban pin mismatch")
    require(oban.provenance["catalog_sha256"] == hashlib.sha256(
        (ROOT / "oracle/fault-scenarios.json").read_bytes()).hexdigest(), "stale Oban fault catalog")
    observed = {"M2": {"grind": grind_m2(grind), "oban": oban_m2(oban)},
                "M6": {"grind": grind_m6(grind), "oban": oban_m6(oban)}}
    for scenario in catalog["scenarios"]:
        require(scenario["classification"] == "intentional-grind-semantics", "unexpected fault classification")
        require(json.dumps(observed[scenario["id"]], sort_keys=True) ==
                json.dumps(scenario["expected"], sort_keys=True), f"{scenario['id']}: normalized observations differ")
    inputs = {}
    for engine, evidence in (("grind", grind), ("oban", oban)):
        files = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                 for path in sorted(evidence.directory.iterdir())
                 if path.is_file() and (path.suffix in (".json", ".jsonl") or path.name.endswith(".synced"))}
        inputs[engine] = dict(directory=str(evidence.directory), provenance=evidence.provenance, sha256=files)
    return dict(catalog_version=catalog["version"], status="passed", compared=2,
                classification="intentional-grind-semantics", observations=observed, inputs=inputs)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--grind", type=Path, required=True)
    parser.add_argument("--oban", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.exists(), "refusing to overwrite comparison evidence")
    catalog = json.loads((ROOT / "oracle/fault-scenarios.json").read_text())
    result = compare(args.grind, args.oban, catalog)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print("Fault oracle: M2 and M6 compared from both independent-VM evidence sets")


if __name__ == "__main__":
    main()
