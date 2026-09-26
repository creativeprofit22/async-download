"""Bounded public-page collection for research throughput measurements."""

from __future__ import annotations

import asyncio
import logging
import zlib
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


def _decompressor(encoding: str) -> zlib._Decompress | None:
    """Return a streaming decoder for a supported Content-Encoding, else None.

    Only single gzip or zlib-wrapped deflate codings are decoded, matching the
    tokens the Rust client decodes except brotli (not in the standard library).
    """
    match encoding:
        case "gzip":
            return zlib.decompressobj(wbits=31)
        case "deflate":
            return zlib.decompressobj(wbits=15)
        case _:
            return None


async def _read_body(response: aiohttp.ClientResponse) -> tuple[str | None, str]:
    """Read a response with the size cap applied to decoded bytes.

    Returns the text (or None) and a short outcome label for logging.
    """
    if not 200 <= response.status < 300:
        return None, f"HTTP {response.status}"
    encoding = response.headers.get("Content-Encoding", "").strip().lower()
    decoder = None
    if encoding in {"", "identity"}:
        # The header only bounds identity bodies; a compressed length says
        # nothing about the decoded size.
        if response.content_length is not None and response.content_length > MAX_BODY_BYTES:
            return None, "body exceeds limit"
    else:
        decoder = _decompressor(encoding)
        if decoder is None:
            return None, "unsupported encoding"
    body = bytearray()
    received = 0
    try:
        async for chunk in response.content.iter_chunked(CHUNK_BYTES):
            received += len(chunk)
            if decoder is None:
                if len(body) + len(chunk) > MAX_BODY_BYTES:
                    return None, "body exceeds limit"
                body.extend(chunk)
                continue
            data = chunk
            while data:
                if decoder.eof:
                    return None, "trailing data after compressed stream"
                # At most cap + 1 decoded bytes are ever held in memory.
                body.extend(decoder.decompress(data, MAX_BODY_BYTES - len(body) + 1))
                if len(body) > MAX_BODY_BYTES:
                    return None, "body exceeds limit"
                if decoder.unused_data:
                    return None, "trailing data after compressed stream"
                data = decoder.unconsumed_tail
        if decoder is not None and received:
            body.extend(decoder.flush())
            if len(body) > MAX_BODY_BYTES:
                return None, "body exceeds limit"
            if not decoder.eof:
                return None, "truncated compressed stream"
    except zlib.error:
        return None, "corrupt compressed stream"
    try:
        return body.decode(response.charset or "utf-8", errors="replace"), "collected"
    except LookupError:
        return body.decode("utf-8", errors="replace"), "collected"


async def _fetch(session: aiohttp.ClientSession, url: str, index: int) -> str | None:
    started = perf_counter()
    status: int | None = None
    try:
        # Public research URLs are supplied by the operator. Proxies, authenticated
        # pages, robots handling and an allowlist are out of scope; review the list
        # and site policies before running. This is not a public URL-submission service.
        async with session.get(url, allow_redirects=False) as response:
            status = response.status
            text, outcome = await _read_body(response)
    except Exception as error:
        # A per-page failure must not escape the batch. Caller cancellation remains
        # cancellable (CancelledError is a BaseException, not an Exception).
        text, outcome = None, type(error).__name__
    # One final record per page; no URL or body content is logged.
    LOGGER.debug("Input %d: %s", index, outcome, extra={
        "page_index": index,
        "page_status": status,
        "page_elapsed_ms": (perf_counter() - started) * 1000,
        "page_outcome": outcome,
    })
    return text


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
            # Final record for a rejected input; same shape as _fetch, no URL.
            outcome = type(error).__name__
            LOGGER.debug("Input %d: %s", index, outcome, extra={
                "page_index": index,
                "page_status": None,
                "page_elapsed_ms": 0.0,
                "page_outcome": outcome,
            })
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
            headers={"Accept-Encoding": "gzip, deflate", "User-Agent": "PublicPageResearch/1.0"},
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
