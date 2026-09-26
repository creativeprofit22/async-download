"""Python benchmark driver: runs one batch and prints one JSON line per input URL.

Usage: ``python -m bench.py_driver --concurrency N --per-host N --urls FILE``

Same output protocol as ``rust/examples/bench_driver.rs``: lines
``{"index","ms","status","bytes","collected","blocked"}``, then ``{"wall_ms"}``.
The clock starts immediately before the batch call; each page's ``ms`` is the
time until its final debug record. The Python downloader has no pacing option.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

from bench.settings import CHALLENGE_MARKERS
from downloader import download_texts

CHALLENGE_MAX_BYTES = 50_000
CHALLENGE_PREFIX_BYTES = 4096


@dataclass(slots=True, frozen=True)
class Final:
    at: float
    status: int | None


class Capture(logging.Handler):
    """Keeps the last per-page record for each input index."""

    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.finals: dict[int, Final] = {}

    def emit(self, record: logging.LogRecord) -> None:
        index = getattr(record, "page_index", None)
        if isinstance(index, int):
            self.finals[index] = Final(perf_counter(), getattr(record, "page_status", None))


def is_blocked(text: str) -> bool:
    body = text.encode("utf-8")
    if len(body) >= CHALLENGE_MAX_BYTES:
        return False
    prefix = body[:CHALLENGE_PREFIX_BYTES].lower()
    return any(marker.encode("ascii") in prefix for marker in CHALLENGE_MARKERS)


async def run(urls: list[str], concurrency: int, per_host: int, capture: Capture) -> None:
    started = perf_counter()
    results = await download_texts(urls, concurrency, per_host_limit=per_host)
    ended = perf_counter()
    for index, text in enumerate(results):
        final = capture.finals.get(index)
        at = final.at if final is not None else ended
        print(json.dumps({
            "index": index,
            "ms": round((at - started) * 1000, 3),
            "status": final.status if final is not None else None,
            "bytes": 0 if text is None else len(text.encode("utf-8")),
            "collected": text is not None,
            "blocked": text is not None and is_blocked(text),
        }))
    print(json.dumps({"wall_ms": round((ended - started) * 1000, 3)}))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--concurrency", type=int, required=True)
    parser.add_argument("--per-host", type=int, required=True)
    parser.add_argument("--urls", type=Path, required=True)
    args = parser.parse_args()
    urls = [line.strip() for line in args.urls.read_text("utf-8").splitlines() if line.strip()]
    capture = Capture()
    logger = logging.getLogger("downloader")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.addHandler(capture)
    asyncio.run(run(urls, args.concurrency, args.per_host, capture))


if __name__ == "__main__":
    main()
