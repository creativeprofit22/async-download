"""Local HTTP/1.1 keep-alive server with a fixed, deterministic workload.

Each loopback host (127.0.0.1-4; macOS needs loopback aliases for .2-.4) serves:

- ``/page/{delay_ms}/{size}/{plain|gzip}``: HTML text of exactly ``size``
  decoded bytes after ``delay_ms``. Gzip pages are sent gzipped only when the
  request's ``Accept-Encoding`` contains ``gzip``.
- ``/missing``: 404.
- ``/block``: a 200 challenge page (must not count as a success).
- ``/bomb``: gzip that decodes past the 2 MiB cap (must be refused).
"""

from __future__ import annotations

import asyncio
import gzip
import random
import threading
from collections import Counter
from dataclasses import dataclass

from bench import settings

MAX_BODY_BYTES = 2 * 1024 * 1024
BLOCK_PAGE = (
    b"<!doctype html><html><head><title>Just a moment...</title></head>"
    b"<body><p>Checking your browser before accessing this site.</p></body></html>"
)
_WORDS = (
    "research", "public", "page", "throughput", "download", "measure", "server",
    "client", "request", "response", "latency", "bounded", "stream", "text", "host",
    "limit", "archive", "sample", "median", "interval", "polite", "batch", "the",
    "of", "and", "a", "to", "in", "is", "for", "on", "with", "as", "by",
)


@dataclass(slots=True, frozen=True)
class Expected:
    """What a correct client returns for one URL.

    ``kind`` is ``ok`` (collected text of ``size`` UTF-8 bytes; ``size`` None
    means any size), ``blocked`` or ``error``.
    """

    kind: str
    size: int | None = None


def html_body(size: int, seed: int) -> bytes:
    """Deterministic ASCII HTML of exactly ``size`` bytes, with realistic compressibility."""
    head, tail = b"<!doctype html><html><body>\n", b"\n</body></html>\n"
    rng = random.Random(seed)
    parts: list[bytes] = [head]
    length = len(head)
    while length < size:
        line = ("<p>" + " ".join(rng.choices(_WORDS, k=12)) + ".</p>\n").encode("ascii")
        parts.append(line)
        length += len(line)
    body = b"".join(parts)[: max(size - len(tail), 0)] + tail
    return body[:size]


def page_path(delay_ms: int, size: int, encoding: str) -> str:
    return f"/page/{delay_ms}/{size}/{encoding}"


def workload() -> list[str]:
    """Paths served by every host, in a fixed order."""
    pages = [
        page_path(delay, size, encoding)
        for delay in settings.DELAYS_MS
        for size in settings.SIZES
        for encoding in settings.ENCODINGS
    ]
    return [*pages, "/missing", "/block", "/bomb"]


def expected_for(path: str) -> Expected:
    match path.split("/"):
        case ["", "page", _, size, _]:
            return Expected("ok", int(size))
        case ["", "block"]:
            return Expected("blocked")
        case _:
            return Expected("error")


def local_urls(bases: list[str]) -> list[tuple[str, Expected]]:
    """Every workload path on every host, host-interleaved so all hosts start at once."""
    return [(base + path, expected_for(path)) for path in workload() for base in bases]


class Bodies:
    """Bodies built once at startup: plain text and its gzip form."""

    def __init__(self) -> None:
        self.plain = {size: html_body(size, settings.BODY_SEED + size) for size in settings.SIZES}
        self.gzipped = {size: gzip.compress(body, mtime=0) for size, body in self.plain.items()}
        self.bomb = gzip.compress(b"x" * (MAX_BODY_BYTES + 1024), mtime=0)


class BenchServer:
    """Runs the asyncio servers on their own thread; ``start`` returns base URLs."""

    def __init__(self, hosts: tuple[str, ...] = settings.LOCAL_HOSTS) -> None:
        self.hosts = hosts
        self.bodies = Bodies()
        self._lock = threading.Lock()
        self._served: Counter[str] = Counter()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._servers: list[asyncio.Server] = []
        self._handlers: set[asyncio.Task[None]] = set()

    def start(self) -> list[str]:
        self._thread.start()
        future = asyncio.run_coroutine_threadsafe(self._open(), self._loop)
        return future.result(timeout=10)

    def stop(self) -> None:
        asyncio.run_coroutine_threadsafe(self._close(), self._loop).result(timeout=10)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=10)
        self._loop.close()

    def take_counts(self) -> Counter[str]:
        """Return and reset how many gzip-eligible pages were sent per encoding."""
        with self._lock:
            counts, self._served = self._served, Counter()
        return counts

    async def _open(self) -> list[str]:
        bases = []
        for host in self.hosts:
            server = await asyncio.start_server(self._handle, host, 0)
            self._servers.append(server)
            bases.append(f"http://{host}:{server.sockets[0].getsockname()[1]}")
        return bases

    async def _close(self) -> None:
        for server in self._servers:
            server.close()
        for task in list(self._handlers):
            task.cancel()
        await asyncio.gather(*self._handlers, return_exceptions=True)
        for server in self._servers:
            await server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        self._handlers.add(task)
        try:
            while True:
                head = await reader.readuntil(b"\r\n\r\n")
                lines = head.decode("latin-1").split("\r\n")
                path = lines[0].split(" ")[1] if lines[0].count(" ") >= 2 else "/"
                headers = {
                    name.strip().lower(): value.strip()
                    for name, _, value in (line.partition(":") for line in lines[1:] if line)
                }
                accepts_gzip = "gzip" in headers.get("accept-encoding", "").lower()
                status, extra, body = await self._respond(path, accepts_gzip)
                writer.write(
                    f"HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\n".encode()
                    + extra + f"Content-Length: {len(body)}\r\n\r\n".encode() + body
                )
                await writer.drain()
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, ConnectionError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            self._handlers.discard(task)

    async def _respond(self, path: str, accepts_gzip: bool) -> tuple[str, bytes, bytes]:
        match path.split("/"):
            case ["", "page", delay, size, encoding] if int(size) in self.bodies.plain:
                await asyncio.sleep(int(delay) / 1000)
                if encoding == "gzip":
                    served = "gzip" if accepts_gzip else "identity"
                    with self._lock:
                        self._served[served] += 1
                    if accepts_gzip:
                        return "200 OK", b"Content-Encoding: gzip\r\n", self.bodies.gzipped[int(size)]
                return "200 OK", b"", self.bodies.plain[int(size)]
            case ["", "block"]:
                return "200 OK", b"", BLOCK_PAGE
            case ["", "bomb"]:
                return "200 OK", b"Content-Encoding: gzip\r\n", self.bodies.bomb
            case _:
                return "404 Not Found", b"", b"<!doctype html><title>Not found</title>"
