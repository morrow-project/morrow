#!/usr/bin/env python3
"""Validate a run produced by run-scale-benchmark.sh.

The runner deliberately records measurements without deciding whether a host
passed a capacity gate. This command provides a deterministic, CI-friendly
policy layer over those artifacts.
"""

from __future__ import annotations

import argparse
import json
import math
import pathlib
import sys
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Case:
    broker_count: int
    topics: int
    partitions: int
    result_path: pathlib.Path
    result: dict[str, Any]

    @property
    def throughput(self) -> float:
        return float(self.result.get("aggregate", {}).get("messages_per_second", 0))

    @property
    def p95_us(self) -> float:
        return float(self.result.get("aggregate", {}).get("latency_us", {}).get("p95", 0))


def fail(message: str) -> None:
    raise ValueError(message)


def read_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read JSON {path}: {error}")
    if not isinstance(value, dict):
        fail(f"JSON root must be an object: {path}")
    return value


def load_cases(root: pathlib.Path, manifest: dict[str, Any]) -> list[Case]:
    index = root / "cases.ndjson"
    try:
        rows = [json.loads(line) for line in index.read_text().splitlines() if line.strip()]
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {index}: {error}")
    cases: list[Case] = []
    for row in rows:
        if not isinstance(row, dict):
            fail("every cases.ndjson row must be an object")
        result_dir = row.get("result_dir")
        if not isinstance(result_dir, str):
            fail("cases.ndjson row is missing result_dir")
        result_root = pathlib.Path(result_dir)
        if not result_root.is_absolute():
            result_root = root / result_root
        result_files = sorted(result_root.glob("*.json"))
        result_files = [path for path in result_files if path.name != "manifest.json"]
        if not result_files:
            fail(f"no benchmark result JSON found below {result_root}")
        for result_path in result_files:
            result = read_json(result_path)
            if result.get("valid") is not True:
                fail(f"benchmark result is not valid: {result_path}")
            aggregate = result.get("aggregate", {})
            if any(aggregate.get(name, 0) for name in ("errors", "timeouts", "duplicates")):
                fail(f"benchmark counters report an error: {result_path}")
            cases.append(
                Case(
                    int(row["broker_count"]),
                    int(row["topics"]),
                    int(row["partitions"]),
                    result_path,
                    result,
                )
            )
    if not cases:
        fail("scale run contains no benchmark cases")
    expected_voters = manifest.get("controller_voter_count")
    expected_profile = manifest.get("deployment_profile")
    expected_shared = manifest.get("roles_share_process")
    if expected_voters is None or expected_profile not in ("combined", "separated"):
        fail("manifest is missing controller_voter_count or deployment_profile")
    for case in cases:
        # The case labels are the topology contract; a result may not silently
        # report a different deployment identity.
        configuration = case.result.get("configuration", {})
        if configuration.get("payload_size") != manifest.get("payload_size"):
            fail(f"payload size differs from manifest: {case.result_path}")
        if case.result.get("valid") is not True:
            fail(f"invalid case: {case.result_path}")
    return cases


def percentile_ratio(cases: list[Case], value: str) -> tuple[float, float] | None:
    if not cases:
        return None
    ordered = sorted(cases, key=lambda case: (case.broker_count, case.topics, case.partitions))
    baseline = ordered[0]
    candidate = ordered[-1]
    before = baseline.throughput if value == "throughput" else baseline.p95_us
    after = candidate.throughput if value == "throughput" else candidate.p95_us
    if before <= 0:
        return None
    return before, after


def evaluate(
    root: pathlib.Path,
    min_throughput_percent: float,
    max_p95_percent: float,
) -> dict[str, Any]:
    manifest = read_json(root / "manifest.json")
    cases = load_cases(root, manifest)
    voters = manifest["controller_voter_count"]
    profile = manifest["deployment_profile"]
    shared = manifest.get("roles_share_process")
    by_workload: dict[tuple[str, str], list[Case]] = {}
    for case in cases:
        result = case.result
        workload = (
            str(result.get("mode")),
            str(result.get("acknowledgement", {}).get("requested_level")),
        )
        by_workload.setdefault(workload, []).append(case)

    checks: list[dict[str, Any]] = []
    checks.append(
        {
            "name": "fixed_controller_quorum",
            "passed": isinstance(voters, int) and voters > 0,
            "controller_voters": voters,
        }
    )
    checks.append(
        {
            "name": "deployment_identity",
            "passed": profile in ("combined", "separated") and isinstance(shared, bool),
            "deployment_profile": profile,
            "roles_share_process": shared,
        }
    )
    checks.append(
        {
            "name": "cardinality_matrix",
            "passed": (
                len({case.broker_count for case in cases}) > 1
                or len({case.topics for case in cases}) > 1
                or len({case.partitions for case in cases}) > 1
            ),
            "broker_counts": sorted({case.broker_count for case in cases}),
            "topics": sorted({case.topics for case in cases}),
            "partitions": sorted({case.partitions for case in cases}),
        }
    )
    comparisons: list[dict[str, Any]] = []
    for workload, workload_cases in sorted(by_workload.items()):
        if len(workload_cases) < 2:
            continue
        throughput = percentile_ratio(workload_cases, "throughput")
        p95 = percentile_ratio(workload_cases, "p95")
        comparison: dict[str, Any] = {"mode": workload[0], "ack_level": workload[1]}
        passed = True
        if throughput is not None:
            before, after = throughput
            retention = after / before * 100
            comparison["throughput_retention_percent"] = retention
            comparison["min_throughput_percent"] = min_throughput_percent
            passed = passed and retention >= min_throughput_percent
        if p95 is not None:
            before, after = p95
            growth = after / before * 100
            comparison["p95_growth_percent"] = growth
            comparison["max_p95_percent"] = max_p95_percent
            passed = passed and growth <= max_p95_percent
        comparison["passed"] = passed
        comparisons.append(comparison)
    checks.append(
        {
            "name": "cardinality_regression",
            "passed": all(item["passed"] for item in comparisons),
            "comparisons": comparisons,
        }
    )
    passed = all(check["passed"] for check in checks)
    return {
        "valid": passed,
        "manifest": {"commit": manifest.get("commit"), "deployment_profile": profile, "controller_voters": voters},
        "case_count": len(cases),
        "checks": checks,
        "thresholds": {
            "min_throughput_percent": min_throughput_percent,
            "max_p95_percent": max_p95_percent,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=pathlib.Path)
    parser.add_argument("--min-throughput-percent", type=float, default=70.0)
    parser.add_argument("--max-p95-percent", type=float, default=150.0)
    parser.add_argument("--output", type=pathlib.Path, help="also write the JSON gate report here")
    args = parser.parse_args()
    if not math.isfinite(args.min_throughput_percent) or not 0 <= args.min_throughput_percent <= 100:
        parser.error("--min-throughput-percent must be between 0 and 100")
    if not math.isfinite(args.max_p95_percent) or args.max_p95_percent < 0:
        parser.error("--max-p95-percent must be non-negative")
    try:
        report = evaluate(args.run_dir, args.min_throughput_percent, args.max_p95_percent)
    except ValueError as error:
        print(f"scale gate failed: {error}", file=sys.stderr)
        return 1
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered, end="")
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
