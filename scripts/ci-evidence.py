#!/usr/bin/env python3
"""Reject missing, empty or incomplete scheduled evidence using retained contracts."""

import argparse
import csv
import json
from pathlib import Path

SHORT_CASES = {
    "M1", "M2", "M3-request", "M3-reply", "M3-partition", "M4", "M5",
    "M6", "M7", "F1", "F2", "F3", "F4", "F5",
}


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def validate_results(rows, expected, key="id"):
    require(isinstance(rows, list) and bool(rows), "empty or malformed suite results")
    require(all(isinstance(row, dict) for row in rows), "malformed scenario result")
    names = [row.get(key) for row in rows]
    require(len(names) == len(set(names)), "duplicate scenario result")
    require(set(names) == expected, "missing or unexpected scenario result")
    require(all(row.get("status") == "passed" for row in rows), "scenario did not pass")


def resilience(directory, soak=False):
    provenance = json.loads((directory / "provenance.json").read_text())
    require(len(provenance["source_sha256"]) == 64, "missing source digest")
    validate_results(json.loads((directory / "results.json").read_text()),
                     SHORT_CASES | ({"soak"} if soak else set()))
    if soak:
        events = [json.loads(line) for line in (directory / "events.jsonl").read_text().splitlines()]
        verdicts = [event for event in events if event.get("event") == "soak_acceptance_passed"]
        require(len(verdicts) == 1, "missing or duplicate soak acceptance")
        verdict = verdicts[0]
        require(provenance["soak_seconds"] >= 7200 and verdict["seconds"] >= 7200,
                "soak did not meet the approved two-hour duration")
        faults = {"node-kill", "worker-kill", "request-partition", "reply-partition",
                  "full-partition", "lost-commit-reply", "slow-ack-delay",
                  "connection-loss", "database-restart"}
        require(set(verdict["fault_counts"]) == faults and all(
            count >= 2 for count in verdict["fault_counts"].values()), "incomplete mixed-fault rotation")


def matrix(directory):
    provenance = json.loads((directory / "provenance.json").read_text())
    require(len(provenance["source_sha256"]) == 64, "missing source digest")
    require(provenance["sinal"]["commit"] and provenance["sinal"]["dirty"] is False,
            "matrix requires an identified clean Sinal dependency")
    points = (directory / "matrix-points.txt").read_text().splitlines()
    require(len(points) >= 2, "missing matrix workload catalog")
    require(points[0] == "suite=all", "full qualification requires the complete matrix")
    repeats = int(points[1].removeprefix("repeats="))
    require(repeats >= 3, "matrix requires at least three measured repeats")
    columns = {
        "l1": ("consumers", "concurrency", "queues", "cost_ms", "job_count"),
        "l2": ("consumers", "interval_ms", "filler_rows", "duration_ms"),
        "l3": ("arrival_per_sec", "duration_ms"),
        "l4": ("submitters", "mode", "total_submissions"),
        "l5": ("pruner_on", "duration_ms"),
        "l6_t1": ("concurrency", "job_count", "cost_ms"),
        "l6_t2": ("k_slow_acks", "d_ms", "l_ms", "delay_ms", "concurrency", "main_pool_size"),
        "l7": ("consumers", "concurrency", "job_count"),
    }
    for name, shape_columns in columns.items():
        expected_shapes = [tuple(line.split()[1:]) for line in points[2:] if line.split()[0] == name]
        require(bool(expected_shapes), f"missing {name} workload catalog")
        require(len(expected_shapes) == len(set(expected_shapes)), f"duplicate {name} workload catalog")
        with (directory / f"{name}.csv").open(newline="") as source:
            rows = list(csv.DictReader(source))
        require(bool(rows), f"empty {name} matrix")
        observed = [(tuple(row[column] for column in shape_columns), row["repeat"]) for row in rows]
        expected = {(shape, str(repeat)) for shape in expected_shapes for repeat in range(1, repeats + 1)}
        require(len(observed) == len(set(observed)) and set(observed) == expected,
                f"{name} missing, duplicate or unexpected workload/repeat")
        require(all(row.get("source_sha256") == provenance["source_sha256"] for row in rows),
                f"{name} source provenance mismatch")
        if name == "l6_t1":
            require(all(row["t1_triggered"] == "false" and float(row["all_attempt_min_headroom_ms"]) > 0
                        and int(row["negative_samples"]) == 0 for row in rows), "T1 renewal contract failed")
        if name == "l6_t2":
            require(all(int(row["invalid_final_count"]) == 0 and int(row["non_stalled_quarantined"]) == 0
                        and float(row["non_stalled_min_headroom_ms"]) >= float(row["l_over_10_ms"])
                        and (int(row["k_slow_acks"]) == 0 or (
                            int(row["slow_ack_activations"]) >= int(row["k_slow_acks"])
                            and int(row["overlap_samples"]) > 0
                            and int(row["renewals_during_slow_ack"]) > 0))
                        for row in rows), "T2 healthy-sibling contract failed")
    with (directory / "arrivals.csv").open(newline="") as source:
        arrivals = list(csv.DictReader(source))
    require(bool(arrivals) and all(row["generator_valid"] == "true" and row["status"] == "valid"
                                  for row in arrivals), "invalid arrival generator")
    drains = list((directory / "raw").glob("*.drain.json"))
    l7_shapes = [line.split()[1:] for line in points[2:] if line.split()[0] == "l7"]
    required_drains = {f"l7-{consumers}x{concurrency}-r{repeat}.jsonl.drain.json"
                       for consumers, concurrency, _ in l7_shapes for repeat in range(1, repeats + 1)}
    require(required_drains.issubset({path.name for path in drains}), "missing coordinator drain evidence")
    for path in drains:
        data = json.loads(path.read_text())
        require(data["valid"] is True and data["outcome"] == "drained"
                and data["sampler_covers_drain"] is True and data["observer_stopped"] is True,
                f"invalid drain evidence: {path.name}")
    # T3 is a reviewed cross-shape performance observation, not a CI speed limit.


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("kind", choices=("resilience", "soak", "matrix"))
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    if args.kind == "matrix":
        matrix(args.directory)
    else:
        resilience(args.directory, soak=args.kind == "soak")
    print(f"{args.kind} evidence accepted: {args.directory}")
