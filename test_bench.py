"""Benchmark harness checks: statistics, classification and workload shape.

No server and no network are used.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from bench import settings
from bench.__main__ import order_for
from bench.server import Expected, html_body, local_urls, workload
from bench.stats import DriverRow, bootstrap_median_ci, classify, median


def row(*, collected: bool = True, blocked: bool = False, size: int = 10) -> DriverRow:
    return DriverRow(index=0, ms=1.0, status=200, bytes=size if collected else 0,
                     collected=collected, blocked=blocked)


class StatsTests(unittest.TestCase):
    def test_known_answer_median(self) -> None:
        self.assertEqual(median([3.0, 1.0, 2.0]), 2.0)
        self.assertEqual(median([4.0, 1.0, 3.0, 2.0]), 2.5)
        with self.assertRaises(ValueError):
            median([])

    def test_bootstrap_is_reproducible_for_a_fixed_seed(self) -> None:
        deltas = [float(value % 17) - 5.0 for value in range(200)]
        first = bootstrap_median_ci(deltas, seed=1, resamples=500, confidence=0.95)
        second = bootstrap_median_ci(deltas, seed=1, resamples=500, confidence=0.95)
        self.assertEqual(first, second)
        self.assertLessEqual(first.low, first.median)
        self.assertLessEqual(first.median, first.high)

    def test_zero_spread_gives_a_degenerate_interval(self) -> None:
        interval = bootstrap_median_ci([4.0] * 30, seed=1, resamples=200, confidence=0.95)
        self.assertEqual((interval.median, interval.low, interval.high), (4.0, 4.0, 4.0))

    def test_unsupported_confidence_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            bootstrap_median_ci([1.0, 2.0], seed=1, resamples=100, confidence=0.93)


class ClassifyTests(unittest.TestCase):
    def test_classification_table(self) -> None:
        cases = [
            (row(), Expected("ok", 10), "ok"),
            (row(), Expected("ok"), "ok"),
            (row(size=9), Expected("ok", 10), "mismatch"),
            (row(collected=False), Expected("ok", 10), "error"),
            (row(blocked=True), Expected("ok"), "blocked"),
            (row(blocked=True), Expected("blocked"), "blocked"),
            (row(), Expected("blocked"), "mismatch"),
            (row(collected=False), Expected("blocked"), "error"),
            (row(collected=False), Expected("error"), "error"),
            (row(), Expected("error"), "mismatch"),
        ]
        for driver_row, expected, outcome in cases:
            with self.subTest(expected=expected, row=driver_row):
                self.assertEqual(classify(driver_row, expected), outcome)


class WorkloadTests(unittest.TestCase):
    def test_workload_shape(self) -> None:
        bases = [f"http://{host}:1" for host in settings.LOCAL_HOSTS]
        targets = local_urls(bases)
        self.assertEqual(len(targets), 84)
        self.assertEqual(len({url for url, _ in targets}), 84)
        paths = workload()
        self.assertEqual(sum(path.endswith("/gzip") for path in paths), 9)
        self.assertEqual(sum(path.endswith("/plain") for path in paths), 9)
        kinds = [expected.kind for _, expected in targets]
        self.assertEqual((kinds.count("ok"), kinds.count("blocked"), kinds.count("error")),
                         (72, 4, 8))

    def test_bodies_have_exact_sizes_and_are_deterministic(self) -> None:
        for size in settings.SIZES:
            self.assertEqual(len(html_body(size, 3)), size)
            self.assertEqual(html_body(size, 3), html_body(size, 3))

    def test_order_alternates(self) -> None:
        self.assertEqual(order_for(0), ("rust", "python"))
        self.assertEqual(order_for(1), ("python", "rust"))

    def test_rust_driver_uses_the_same_challenge_markers(self) -> None:
        source = (Path(__file__).parent / "rust" / "examples" / "bench_driver.rs").read_text("utf-8")
        match = re.search(r"CHALLENGE_MARKERS: \[&str; \d+\] =\s*\[(.*?)\];", source, re.S)
        assert match is not None
        self.assertEqual(tuple(re.findall(r'"([^"]*)"', match.group(1))),
                         settings.CHALLENGE_MARKERS)


if __name__ == "__main__":
    unittest.main()
