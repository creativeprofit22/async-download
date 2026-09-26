"""Run the Rust vs Python benchmark.

    python -m bench local    # main benchmark against a local server
    python -m bench public   # small, paced check against a few public pages

Each run starts one driver subprocess per implementation; timing is measured
inside the driver, so process start-up is excluded. Order alternates each run.
Results go to ``bench/results/{local,public}.csv`` and ``.md``.
"""

from __future__ import annotations

import argparse
import csv
import json
import platform
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import aiohttp

from bench import settings
from bench.server import BenchServer, Expected, local_urls
from bench.stats import DriverRow, Interval, Outcome, bootstrap_median_ci, classify, median

IMPLEMENTATIONS = ("rust", "python")


@dataclass(slots=True, frozen=True)
class RunResult:
    rows: list[DriverRow]
    wall_ms: float


@dataclass(slots=True, frozen=True)
class Record:
    run: int
    position: int
    implementation: str
    url: str
    status: int | None
    elapsed_ms: float
    bytes: int
    outcome: Outcome


def rust_driver() -> Path:
    suffix = ".exe" if sys.platform == "win32" else ""
    return settings.RUST_DRIVER.with_name(settings.RUST_DRIVER.name + suffix)


def driver_command(implementation: str, urls_file: Path, min_interval_ms: int) -> list[str]:
    common = ["--concurrency", str(settings.CONCURRENCY), "--per-host", str(settings.PER_HOST),
              "--urls", str(urls_file)]
    if implementation == "rust":
        return [str(rust_driver()), *common, "--min-interval-ms", str(min_interval_ms)]
    return [sys.executable, "-m", "bench.py_driver", *common]


def run_driver(command: list[str], count: int) -> RunResult:
    completed = subprocess.run(
        command, cwd=settings.ROOT, capture_output=True, text=True, check=False,
        timeout=settings.DRIVER_TIMEOUT_S,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"driver failed ({completed.returncode}): {completed.stderr[-2000:]}")
    rows: list[DriverRow] = []
    wall_ms: float | None = None
    for line in completed.stdout.splitlines():
        data = json.loads(line)
        if "wall_ms" in data:
            wall_ms = float(data["wall_ms"])
        else:
            rows.append(DriverRow(
                index=int(data["index"]), ms=float(data["ms"]),
                status=None if data["status"] is None else int(data["status"]),
                bytes=int(data["bytes"]), collected=bool(data["collected"]),
                blocked=bool(data["blocked"]),
            ))
    if wall_ms is None or [row.index for row in rows] != list(range(count)):
        raise RuntimeError("driver output did not match the protocol")
    if any(row.collected and row.status is None for row in rows):
        # A collected page always had an HTTP answer; a missing status means the
        # driver missed per-page events and its timings fell back to wall time.
        raise RuntimeError("driver did not capture per-page events")
    return RunResult(rows, wall_ms)


def order_for(run: int) -> tuple[str, str]:
    """Rust first on even runs, Python first on odd runs."""
    rust, python = IMPLEMENTATIONS
    return (rust, python) if run % 2 == 0 else (python, rust)


def tool_versions() -> dict[str, str]:
    versions = {
        "OS": platform.platform(),
        "CPU": platform.processor() or platform.machine(),
        "Python": platform.python_version(),
    }
    versions["aiohttp"] = aiohttp.__version__
    try:
        rustc = subprocess.run(["rustc", "--version"], capture_output=True, text=True, check=False)
        versions["rustc"] = rustc.stdout.strip() or "unknown"
    except OSError:
        versions["rustc"] = "unknown"
    return versions


def fmt_interval(interval: Interval) -> str:
    return f"{interval.median:+.1f} ms (95% CI {interval.low:+.1f} to {interval.high:+.1f})"


def summarize(
    mode: str, records: list[Record], walls: dict[str, list[float]], url_count: int,
    served: dict[str, Counter[str]], run_settings: dict[str, object],
) -> str:
    lines = [f"# Benchmark summary: {mode}", "", "Single-machine measurement.", "",
             "## Settings", ""]
    lines += [f"- {name}: {value}" for name, value in run_settings.items()]
    lines += [f"- {name}: {value}" for name, value in tool_versions().items()]

    lines += ["", "## Outcomes (every URL, every measured run)", "",
              "| Implementation | ok | blocked | error | mismatch |", "| --- | --- | --- | --- | --- |"]
    for implementation in IMPLEMENTATIONS:
        counts = Counter(r.outcome for r in records if r.implementation == implementation)
        lines.append(f"| {implementation} | {counts['ok']} | {counts['blocked']} | "
                     f"{counts['error']} | {counts['mismatch']} |")

    lines += ["", "## Per-URL completion time (successful pages only)", ""]
    for implementation in IMPLEMENTATIONS:
        times = [r.elapsed_ms for r in records if r.implementation == implementation and r.outcome == "ok"]
        if times:
            lines.append(f"- {implementation}: median {median(times):.1f} ms over {len(times)} pages")

    by_key = {(r.run, r.url, r.implementation): r for r in records}
    keys = sorted({(r.run, r.url) for r in records})
    deltas = [
        by_key[(run, url, "python")].elapsed_ms - by_key[(run, url, "rust")].elapsed_ms
        for run, url in keys
        if by_key[(run, url, "python")].outcome == "ok" and by_key[(run, url, "rust")].outcome == "ok"
    ]
    lines += ["", "## Paired per-URL difference (Python - Rust; positive means Rust finished first)", ""]
    if deltas:
        interval = bootstrap_median_ci(deltas, settings.SEED, settings.RESAMPLES, settings.CONFIDENCE)
        lines.append(f"- Median of paired differences: {fmt_interval(interval)}")
    lines.append(f"- Pairs used: {len(deltas)}; excluded (either side not ok): {len(keys) - len(deltas)}")

    lines += ["", "## Batch throughput (per run)", ""]
    for implementation in IMPLEMENTATIONS:
        rates = [url_count / (wall / 1000) for wall in walls[implementation]]
        lines.append(f"- {implementation}: median {median(rates):.2f} URLs/s, "
                     f"median wall {median(walls[implementation]):.1f} ms")
    wall_deltas = [p - r for p, r in zip(walls["python"], walls["rust"], strict=True)]
    wall_interval = bootstrap_median_ci(
        wall_deltas, settings.SEED, settings.RESAMPLES, settings.CONFIDENCE)
    lines.append(f"- Paired wall-time difference (Python - Rust): {fmt_interval(wall_interval)}")

    if any(served.values()):
        lines += ["", "## Compression check (gzip-eligible pages served)", ""]
        for implementation in IMPLEMENTATIONS:
            counts = served[implementation]
            lines.append(f"- {implementation}: gzip {counts['gzip']}, identity {counts['identity']}")

    lines += ["", "## Caveat", "",
              "Pages within one run share the client, the server and the machine, so paired "
              "per-URL differences are not independent and their interval understates "
              "run-to-run variation. The per-run wall-time interval is the conservative figure.", ""]
    return "\n".join(lines)


def write_csv(path: Path, records: list[Record]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["run", "position", "implementation", "url", "status", "elapsed_ms",
                         "bytes", "outcome"])
        for r in records:
            writer.writerow([r.run, r.position, r.implementation, r.url,
                             "" if r.status is None else r.status, f"{r.elapsed_ms:.3f}",
                             r.bytes, r.outcome])


def benchmark(
    mode: str, targets: list[tuple[str, Expected]], runs: int, warmup: int, gap_s: float,
    min_interval_ms: int, server: BenchServer | None, run_settings: dict[str, object],
) -> str:
    settings.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    urls_file = settings.RESULTS_DIR / f"{mode}-urls.txt"
    urls_file.write_text("".join(url + "\n" for url, _ in targets), encoding="utf-8")
    records: list[Record] = []
    walls: dict[str, list[float]] = {name: [] for name in IMPLEMENTATIONS}
    served: dict[str, Counter[str]] = {name: Counter() for name in IMPLEMENTATIONS}
    first = True
    for run in range(-warmup, runs):
        for position, implementation in enumerate(order_for(run)):
            if not first and gap_s:
                time.sleep(gap_s)
            first = False
            result = run_driver(driver_command(implementation, urls_file, min_interval_ms), len(targets))
            counts = server.take_counts() if server else Counter()
            label = "warm-up" if run < 0 else f"run {run + 1}/{runs}"
            print(f"{label} {implementation}: {result.wall_ms:.0f} ms", file=sys.stderr)
            if run < 0:
                continue
            walls[implementation].append(result.wall_ms)
            served[implementation].update(counts)
            for row, (url, expected) in zip(result.rows, targets, strict=True):
                records.append(Record(run, position, implementation, url, row.status, row.ms,
                                      row.bytes, classify(row, expected)))
    write_csv(settings.RESULTS_DIR / f"{mode}.csv", records)
    summary = summarize(mode, records, walls, len(targets), served, run_settings)
    (settings.RESULTS_DIR / f"{mode}.md").write_text(summary, encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m bench", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", choices=["local", "public"])
    mode = parser.parse_args().mode
    if not rust_driver().exists():
        sys.exit(f"Rust driver not found at {rust_driver()}.\nBuild it first: {settings.RUST_BUILD_COMMAND}")
    common = {
        "concurrency": settings.CONCURRENCY, "per-host limit": settings.PER_HOST,
        "seed": settings.SEED, "bootstrap resamples": settings.RESAMPLES,
        "confidence": settings.CONFIDENCE,
    }
    if mode == "local":
        server = BenchServer()
        bases = server.start()
        try:
            targets = local_urls(bases)
            summary = benchmark(mode, targets, settings.RUNS, settings.WARMUP, 0,
                                settings.LOCAL_MIN_INTERVAL_MS, server, {
                                    **common, "URLs per run": len(targets),
                                    "measured runs per implementation": settings.RUNS,
                                    "warm-up runs per implementation": settings.WARMUP,
                                    "Rust min interval (ms)": settings.LOCAL_MIN_INTERVAL_MS,
                                    "delays (ms)": settings.DELAYS_MS, "sizes (bytes)": settings.SIZES,
                                })
        finally:
            server.stop()
    else:
        targets = [(url, Expected("ok")) for url in settings.PUBLIC_URLS]
        summary = benchmark(mode, targets, settings.PUBLIC_RUNS, 0, settings.PUBLIC_GAP_S,
                            settings.PUBLIC_MIN_INTERVAL_MS, None, {
                                **common, "URLs per run": len(targets),
                                "runs per implementation": settings.PUBLIC_RUNS,
                                "gap between runs (s)": settings.PUBLIC_GAP_S,
                                "Rust min interval (ms)": settings.PUBLIC_MIN_INTERVAL_MS,
                            })
    print(summary)


if __name__ == "__main__":
    main()
