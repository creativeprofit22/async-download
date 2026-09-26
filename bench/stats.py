"""Outcome classification and paired bootstrap statistics (standard library only).

Method follows fastcrw/crw ``bench/stats.py``: the median of per-item paired
differences (not the difference of medians), with a percentile bootstrap
interval whose seed and resample count are fixed before running.
"""

from __future__ import annotations

import random
import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from bench.server import Expected

Outcome = Literal["ok", "blocked", "error", "mismatch"]


@dataclass(slots=True, frozen=True)
class DriverRow:
    """One page as reported by a driver."""

    index: int
    ms: float
    status: int | None
    bytes: int
    collected: bool
    blocked: bool


@dataclass(slots=True, frozen=True)
class Interval:
    median: float
    low: float
    high: float


def median(values: Sequence[float]) -> float:
    if not values:
        raise ValueError("median of an empty sample")
    return statistics.median(values)


def classify(row: DriverRow, expected: Expected) -> Outcome:
    """Label one page. Block and error pages are never ``ok``.

    ``mismatch`` means the client returned something a correct client would not
    (wrong size, a page that should have failed, or a missed challenge page).
    """
    match expected.kind:
        case "ok":
            if not row.collected:
                return "error"
            if row.blocked:
                return "blocked"
            if expected.size is not None and row.bytes != expected.size:
                return "mismatch"
            return "ok"
        case "blocked":
            if not row.collected:
                return "error"
            return "blocked" if row.blocked else "mismatch"
        case _:
            return "error" if not row.collected else "mismatch"


def bootstrap_median_ci(
    deltas: Sequence[float], seed: int, resamples: int, confidence: float
) -> Interval:
    """Median of ``deltas`` with a percentile bootstrap interval.

    Deterministic for a fixed seed. A sample with no spread gives a degenerate
    interval equal to the median.
    """
    point = median(deltas)
    tail = (1 - confidence) / 2
    cuts = round(1 / tail)
    if resamples < 2 or abs(cuts * tail - 1) > 1e-9:
        raise ValueError("confidence must leave tails of 1/n, e.g. 0.95 -> n = 40")
    rng = random.Random(seed)
    size = len(deltas)
    medians = [statistics.median(rng.choices(deltas, k=size)) for _ in range(resamples)]
    quantiles = statistics.quantiles(medians, n=cuts, method="inclusive")
    return Interval(point, quantiles[0], quantiles[-1])
