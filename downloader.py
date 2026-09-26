"""Bounded public-page collection for research throughput measurements."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from time import perf_counter
from urllib.parse import urlsplit

import aiohttp

LOGGER = logging.getLogger(__name__)
MAX_BODY_BYTES = 2 * 1024 * 1024
CHUNK_BYTES = 64 * 1024
REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=20, connect=5, sock_connect=5, sock_read=5, ceil_threshold=float("inf")
)


def _host(raw_url: str) -> str:
    if not isinstance(raw_url, str) or not raw_url or len(raw_url) > 8192:
        raise ValueError("Expected a nonempty URL of at most 8192 characters")
    if any(character.isspace() or ord(character) < 32 or ord(character) == 127
           for character in raw_url):
        raise ValueError("Whitespace and control characters are not accepted")
    parts = urlsplit(raw_url)
    if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
        raise ValueError("Expected an absolute HTTP or HTTPS URL")
    if parts.username is not None or parts.password is not None:
        raise ValueError("URL user information is not supported")
    if parts.port is not None and parts.port < 1:
        raise ValueError("Port must be positive")
    return parts.hostname.rstrip(".").encode("idna").decode("ascii").lower()


async def _fetch(session: aiohttp.ClientSession, url: str, index: int) -> str | None:
    try:
        # Public research URLs are supplied by the operator. Proxies, authenticated
        # pages, robots handling and an allowlist are out of scope; review the list
        # and site policies before running. This is not a public URL-submission service.
        async with session.get(url, allow_redirects=False) as response:
            if not 200 <= response.status < 300:
                LOGGER.debug("Input %d: HTTP %d", index, response.status)
                return None
            # simplification: request identity and reject compressed responses; this
            # bounds memory before decoding. Add bounded streaming decompression if needed.
            if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                LOGGER.debug("Input %d: compressed response declined", index)
                return None
            if response.content_length is not None and response.content_length > MAX_BODY_BYTES:
                LOGGER.debug("Input %d: body exceeds limit", index)
                return None
            body = bytearray()
            async for chunk in response.content.iter_chunked(CHUNK_BYTES):
                if len(body) + len(chunk) > MAX_BODY_BYTES:
                    LOGGER.debug("Input %d: body exceeds limit", index)
                    return None
                body.extend(chunk)
            try:
                return body.decode(response.charset or "utf-8", errors="replace")
            except LookupError:
                return body.decode("utf-8", errors="replace")
    except Exception as error:
        # A per-page failure must not escape the batch. Caller cancellation remains
        # cancellable (CancelledError is a BaseException, not an Exception).
        LOGGER.debug("Input %d: %s", index, type(error).__name__)
        return None


async def download_texts(
    urls: list[str], concurrency: int, *, per_host_limit: int = 2
) -> list[str | None]:
    """Return decoded bodies in input order, with None for each failed page.

    Invalid limits return an all-None list without sending requests. Caller
    cancellation propagates after active tasks and the shared session are closed.
    Host limits group hostname aliases differing only in case or a trailing dot,
    across both schemes and all ports. Redirects are treated as failed pages.
    """
    results: list[str | None] = [None] * len(urls)
    if (type(concurrency) is not int or concurrency < 1
            or type(per_host_limit) is not int or per_host_limit < 1):
        return results
    per_host_limit = min(per_host_limit, concurrency)
    groups: dict[str, deque[tuple[int, str]]] = defaultdict(deque)
    for index, url in enumerate(urls):
        try:
            groups[_host(url)].append((index, url))
        except Exception as error:
            LOGGER.debug("Input %d: %s", index, type(error).__name__)
    if not groups:
        return results

    ready = deque(groups)
    active: dict[str, int] = defaultdict(int)
    pending: dict[asyncio.Task[str | None], tuple[str, int]] = {}
    try:
        connector = aiohttp.TCPConnector(
            limit=concurrency, limit_per_host=per_host_limit, keepalive_timeout=5,
            use_dns_cache=False,
        )
        async with aiohttp.ClientSession(
            connector=connector, timeout=REQUEST_TIMEOUT, trust_env=False,
            cookie_jar=aiohttp.DummyCookieJar(), auto_decompress=False,
            headers={"Accept-Encoding": "identity", "User-Agent": "PublicPageResearch/1.0"},
            read_bufsize=CHUNK_BYTES,
        ) as session:
            try:
                while ready or pending:
                    # Round-robin eligible hosts, never tasks waiting on host permits.
                    # At most concurrency tasks exist, even for a very large URL list.
                    while ready and len(pending) < concurrency:
                        host = ready.popleft()
                        index, url = groups[host].popleft()
                        active[host] += 1
                        if groups[host] and active[host] < per_host_limit:
                            ready.append(host)
                        pending[asyncio.create_task(_fetch(session, url, index))] = (host, index)
                    finished, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                    for task in finished:
                        host, index = pending.pop(task)
                        results[index] = task.result()
                        if groups[host] and active[host] == per_host_limit:
                            ready.append(host)
                        active[host] -= 1
            finally:
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
    except Exception as error:
        LOGGER.debug("Batch could not continue: %s", type(error).__name__)
    return results


def main() -> None:
    """Run a small public-page demonstration without printing collected content."""
    urls = [
        "https://example.com/",
        "https://www.python.org/",
        "https://www.iana.org/domains/reserved",
    ]
    started = perf_counter()
    results = asyncio.run(download_texts(urls, concurrency=4))
    elapsed = perf_counter() - started
    for url, text in zip(urls, results, strict=True):
        print(f"{url}: {'None' if text is None else f'{len(text):,} characters'}")
    successes = sum(text is not None for text in results)
    print(f"{successes}/{len(urls)} succeeded in {elapsed:.3f}s; "
          f"{len(urls) / elapsed:.2f} URLs/s")


if __name__ == "__main__":
    main()
