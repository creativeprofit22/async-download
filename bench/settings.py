"""Benchmark settings, fixed before any run. Change them only between studies."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT / "bench" / "results"
RUST_DRIVER = ROOT / "rust" / "target" / "release" / "examples" / "bench_driver"
RUST_BUILD_COMMAND = (
    "cargo build --release --locked --manifest-path rust/Cargo.toml --example bench_driver"
)

# Statistics: chosen before running and never tuned afterwards.
SEED = 20260926
RESAMPLES = 10_000
CONFIDENCE = 0.95

# Local run.
RUNS = 20
WARMUP = 1
CONCURRENCY = 8
PER_HOST = 2
# Python has no pacing, so limits are only equal with pacing off on both sides.
LOCAL_MIN_INTERVAL_MS = 0
DRIVER_TIMEOUT_S = 300

# Local workload: every host serves every combination below.
LOCAL_HOSTS = ("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4")
DELAYS_MS = (10, 50, 150)
SIZES = (4 * 1024, 64 * 1024, 512 * 1024)
ENCODINGS = ("plain", "gzip")
BODY_SEED = 7

# Public run: small and paced.
PUBLIC_RUNS = 3
PUBLIC_GAP_S = 10
PUBLIC_MIN_INTERVAL_MS = 500
# One final (non-redirecting) URL per host.
PUBLIC_URLS = (
    "https://example.com/",
    "https://www.python.org/",
    "https://www.iana.org/domains/reserved",
    "https://en.wikipedia.org/wiki/Web_crawler",
    "https://www.gnu.org/",
    "https://www.w3.org/",
    "https://rust-lang.org/",
)

# Challenge-page detection; kept identical in bench/py_driver.py and
# rust/examples/bench_driver.rs.
CHALLENGE_MARKERS = ("just a moment", "cf-chl", "captcha", "access denied")
