#!/usr/bin/env python3
"""Behavior tests for the scale-gate artifact evaluator."""

import json
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
CHECKER = ROOT / "scripts/check-scale-benchmark.py"


def write_run(root: pathlib.Path, throughputs: tuple[float, float], p95s: tuple[float, float]) -> None:
    result_dirs = []
    for index, (throughput, p95) in enumerate(zip(throughputs, p95s), start=1):
        result_root = root / f"brokers-{index}" / "topics-1" / "partitions-1" / "results"
        result_root.mkdir(parents=True)
        result_dirs.append(result_root)
        (result_root / "case.json").write_text(
            json.dumps(
                {
                    "valid": True,
                    "mode": "fire-and-forget",
                    "acknowledgement": {"requested_level": None},
                    "configuration": {"payload_size": 128},
                    "aggregate": {
                        "messages_per_second": throughput,
                        "latency_us": {"p95": p95},
                        "errors": 0,
                        "timeouts": 0,
                        "duplicates": 0,
                    },
                }
            )
        )
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "commit": "test",
                "payload_size": 128,
                "controller_voter_count": 3,
                "deployment_profile": "separated",
                "roles_share_process": False,
            }
        )
    )
    (root / "cases.ndjson").write_text(
        "".join(
            json.dumps(
                {
                    "broker_count": index,
                    "topics": 1,
                    "partitions": 1,
                    "result_dir": str(result_root),
                }
            )
            + "\n"
            for index, result_root in enumerate(result_dirs, start=1)
        )
    )


class ScaleBenchmarkTest(unittest.TestCase):
    def run_checker(self, root: pathlib.Path, *options: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["python3", str(CHECKER), str(root), *options],
            cwd=ROOT,
            check=False,
            text=True,
            capture_output=True,
        )

    def test_passes_within_thresholds(self) -> None:
        with tempfile.TemporaryDirectory() as path:
            root = pathlib.Path(path)
            write_run(root, (1000, 800), (100, 140))
            result = self.run_checker(root)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('"valid": true', result.stdout)

    def test_fails_throughput_regression(self) -> None:
        with tempfile.TemporaryDirectory() as path:
            root = pathlib.Path(path)
            write_run(root, (1000, 500), (100, 100))
            result = self.run_checker(root)
            self.assertEqual(result.returncode, 1)
            self.assertIn('"name": "cardinality_regression"', result.stdout)


if __name__ == "__main__":
    unittest.main()
