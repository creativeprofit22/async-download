"""Local HTTP verification; no public network is needed."""

from __future__ import annotations

import asyncio
import gzip
import unittest
import zlib
from collections import Counter
from time import perf_counter

import aiohttp

from downloader import MAX_BODY_BYTES, REQUEST_TIMEOUT, _fetch, _host, download_texts


class DownloadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.handlers: set[asyncio.Task[None]] = set()
        self.requests: list[str] = []
        self.accept_encodings: list[str] = []
        self.connections = 0
        self.active: Counter[str] = Counter()
        self.peak: Counter[str] = Counter()
        self.global_peak = 0
        self.fetch_peak = 0
        self.started = asyncio.Event()
        self.servers = [
            await asyncio.start_server(self.handle, address, 0)
            for address in ("127.0.0.1", "127.0.0.2", "127.0.0.1")
        ]
        self.bases = [
            f"http://{server.sockets[0].getsockname()[0]}:{server.sockets[0].getsockname()[1]}"
            for server in self.servers
        ]

    async def asyncTearDown(self) -> None:
        for server in self.servers:
            server.close()
        for task in self.handlers:
            task.cancel()
        await asyncio.gather(*self.handlers, return_exceptions=True)
        for server in self.servers:
            await server.wait_closed()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        self.handlers.add(task)
        self.connections += 1
        host = writer.get_extra_info("sockname")[0]
        try:
            while True:
                request = await reader.readuntil(b"\r\n\r\n")
                path = request.split(b" ", 2)[1].decode("ascii")
                self.requests.append(path)
                for line in request.split(b"\r\n")[1:]:
                    name, _, value = line.partition(b":")
                    if name.strip().lower() == b"accept-encoding":
                        self.accept_encodings.append(value.strip().decode("ascii"))
                self.active[host] += 1
                self.peak[host] = max(self.peak[host], self.active[host])
                self.global_peak = max(self.global_peak, sum(self.active.values()))
                self.fetch_peak = max(self.fetch_peak, sum(
                    item.get_coro().__qualname__ == "_fetch" for item in asyncio.all_tasks()
                ))
                self.started.set()
                try:
                    if path.startswith("/slow"):
                        await asyncio.sleep(0.04)
                    if path == "/stall":
                        await asyncio.Event().wait()
                    if path == "/trickle":
                        writer.write(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                        for _ in range(100):
                            writer.write(b"1\r\nx\r\n")
                            await writer.drain()
                            await asyncio.sleep(0.02)
                        writer.write(b"0\r\n\r\n")
                        continue
                    if path == "/chunked":
                        writer.write(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                        for _ in range(MAX_BODY_BYTES // 65536 + 1):
                            writer.write(b"10000\r\n" + b"x" * 65536 + b"\r\n")
                            await writer.drain()
                        writer.write(b"0\r\n\r\n")
                        continue
                    body = path.encode()
                    headers = b""
                    status = b"200 OK"
                    if path == "/fail":
                        status = b"503 Unavailable"
                    elif path == "/missing":
                        status = b"404 Not Found"
                    elif path == "/empty":
                        body = b""
                    elif path == "/latin":
                        body, headers = b"caf\xe9", b"Content-Type: text/plain; charset=iso-8859-1\r\n"
                    elif path == "/unknown":
                        body, headers = b"hi\xff", b"Content-Type: text/plain; charset=not-a-charset\r\n"
                    elif path == "/nontext":
                        body, headers = b"hi\xff", b"Content-Type: text/plain; charset=base64_codec\r\n"
                    elif path == "/utf8":
                        body = "Research \u2600".encode()
                    elif path == "/large":
                        body = b"x" * (MAX_BODY_BYTES + 1)
                    elif path == "/boundary":
                        body = b"x" * MAX_BODY_BYTES
                    elif path == "/compressed":
                        body = gzip.compress(b"x" * (MAX_BODY_BYTES + 1))
                        headers = b"Content-Encoding: gzip\r\n"
                    elif path == "/gzip":
                        body = gzip.compress(b"gzip page")
                        headers = b"Content-Encoding: gzip\r\n"
                    elif path == "/deflate":
                        body = zlib.compress(b"deflate page")
                        headers = b"Content-Encoding: deflate\r\n"
                    elif path == "/gzip-boundary":
                        body = gzip.compress(b"x" * MAX_BODY_BYTES)
                        headers = b"Content-Encoding: gzip\r\n"
                    elif path == "/brotli":
                        body, headers = b"\x0b\x02\x80hi\x03", b"Content-Encoding: br\r\n"
                    elif path == "/gzip-corrupt":
                        body = gzip.compress(b"corrupt page")[:10] + b"\xff" * 20
                        headers = b"Content-Encoding: gzip\r\n"
                    elif path == "/gzip-truncated":
                        body = gzip.compress(b"truncated page " * 100)[:-12]
                        headers = b"Content-Encoding: gzip\r\n"
                    elif path == "/gzip-trailing":
                        body = gzip.compress(b"page") + b"junk"
                        headers = b"Content-Encoding: gzip\r\n"
                    elif path == "/gzip-empty":
                        body, headers = b"", b"Content-Encoding: gzip\r\n"
                    elif path == "/redirect":
                        status, headers = b"302 Found", b"Location: /unexpected\r\n"
                    elif path == "/truncated":
                        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\nshort")
                        await writer.drain()
                        return
                    writer.write(b"HTTP/1.1 " + status + b"\r\n" + headers
                                 + f"Content-Length: {len(body)}\r\n\r\n".encode() + body)
                    await writer.drain()
                finally:
                    self.active[host] -= 1
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            self.handlers.discard(task)

    async def test_order_duplicates_failures_and_decoding(self) -> None:
        paths = ["/slow/first", "/fail", "/utf8", "/latin", "/unknown", "/nontext", "/empty", "/utf8"]
        results = await download_texts([self.bases[0] + path for path in paths], 4)
        self.assertEqual(results, ["/slow/first", None, "Research \u2600", "caf\xe9", "hi\ufffd", "hi\ufffd", "", "Research \u2600"])

    async def test_global_and_hostname_caps_across_ports(self) -> None:
        urls = [base + f"/slow/{i}" for i in range(6) for base in self.bases]
        results = await download_texts(urls, 3, per_host_limit=2)
        self.assertTrue(all(result is not None for result in results))
        self.assertEqual(self.global_peak, 3)
        self.assertEqual(self.peak["127.0.0.1"], 2)
        self.assertLessEqual(self.peak["127.0.0.2"], 2)
        self.assertLessEqual(self.fetch_peak, 3)

    async def test_saturated_host_does_not_block_other_hosts(self) -> None:
        urls = [self.bases[0] + f"/slow/{i}" for i in range(100)] + [self.bases[1] + "/other"]
        results = await download_texts(urls, 3, per_host_limit=1)
        self.assertEqual(results[-1], "/other")
        self.assertLess(self.requests.index("/other"), 3)
        self.assertLessEqual(self.fetch_peak, 3)

    async def test_body_ceiling_compression_redirect_and_truncation(self) -> None:
        paths = ["/large", "/chunked", "/compressed", "/redirect", "/truncated", "/boundary", "/ok"]
        results = await download_texts([self.bases[0] + path for path in paths], 4)
        self.assertEqual(results[:5], [None] * 5)
        self.assertEqual(results[5], "x" * MAX_BODY_BYTES)
        self.assertEqual(results[6], "/ok")
        self.assertNotIn("/unexpected", self.requests)

    async def test_compressed_pages_are_decoded_within_the_cap(self) -> None:
        cases = [
            ("/gzip", "gzip page"),
            ("/deflate", "deflate page"),
            ("/gzip-boundary", "x" * MAX_BODY_BYTES),
            ("/compressed", None),
            ("/brotli", None),
            ("/gzip-corrupt", None),
            ("/gzip-truncated", None),
            ("/gzip-trailing", None),
            ("/gzip-empty", ""),
        ]
        results = await download_texts([self.bases[0] + path for path, _ in cases], 4)
        for (path, expected), result in zip(cases, results, strict=True):
            with self.subTest(path=path):
                self.assertEqual(result, expected)
        self.assertEqual(set(self.accept_encodings), {"gzip, deflate"})

    async def test_one_structured_record_per_page_without_urls(self) -> None:
        # bench/py_driver.py consumes page_index and page_status from these records.
        cases = [("/ok", 200, "collected"), ("/missing", 404, "HTTP 404")]
        urls = [self.bases[0] + path for path, _, _ in cases]
        with self.assertLogs("downloader", "DEBUG") as captured:
            results = await download_texts(urls, 2)
        self.assertEqual(results, ["/ok", None])
        for index, (path, status, outcome) in enumerate(cases):
            with self.subTest(path=path):
                records = [record for record in captured.records
                           if getattr(record, "page_index", None) == index]
                self.assertEqual(len(records), 1)
                record = records[0]
                self.assertEqual(getattr(record, "page_status"), status)
                elapsed = getattr(record, "page_elapsed_ms")
                self.assertIsInstance(elapsed, float)
                self.assertGreaterEqual(elapsed, 0.0)
                self.assertEqual(getattr(record, "page_outcome"), outcome)
        for record in captured.records:
            message = record.getMessage()
            for url in urls:
                self.assertNotIn(url, message)

    async def test_rejected_input_emits_one_structured_record(self) -> None:
        with self.assertLogs("downloader", "DEBUG") as captured:
            results = await download_texts(["not a url"], 1)
        self.assertEqual(results, [None])
        records = [record for record in captured.records
                   if getattr(record, "page_index", None) == 0]
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertIsNone(getattr(record, "page_status"))
        self.assertEqual(getattr(record, "page_elapsed_ms"), 0.0)
        self.assertEqual(getattr(record, "page_outcome"), "ValueError")
        self.assertNotIn("not a url", record.getMessage())

    async def test_invalid_urls_and_limits_send_nothing(self) -> None:
        urls = ["file:///tmp/page", "ftp://example.com", "", "http://", "http://[",
                "http://example.com:99999", "http://example.com:0", "http://a@b/",
                "https://example.com/\n", " /relative"]
        self.assertEqual(await download_texts(urls, 3), [None] * len(urls))
        for limit in (0, -1, True):
            self.assertEqual(await download_texts([self.bases[0]], limit), [None])
            self.assertEqual(await download_texts([self.bases[0]], 2, per_host_limit=limit), [None])
        self.assertEqual(await download_texts([], 2), [])
        self.assertEqual(self.requests, [])
        self.assertEqual(_host("https://EXAMPLE.com.:443/a"), "example.com")

    async def test_pool_reuses_connections(self) -> None:
        results = await download_texts([self.bases[0] + "/ok"] * 8, 1)
        self.assertEqual(results, ["/ok"] * 8)
        self.assertEqual(self.connections, 1)

    async def test_read_and_total_timeouts_on_real_streams(self) -> None:
        self.assertEqual(REQUEST_TIMEOUT.connect, 5)
        self.assertEqual(REQUEST_TIMEOUT.sock_connect, 5)
        self.assertEqual(REQUEST_TIMEOUT.sock_read, 5)
        self.assertEqual(REQUEST_TIMEOUT.total, 20)
        for path, timeout in [
            ("/stall", aiohttp.ClientTimeout(total=1, connect=0.2, sock_connect=0.2, sock_read=0.1)),
            ("/trickle", aiohttp.ClientTimeout(total=0.15, connect=0.1, sock_connect=0.1, sock_read=0.1)),
        ]:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                started = perf_counter()
                results = await asyncio.gather(
                    _fetch(session, self.bases[0] + path, 0),
                    _fetch(session, self.bases[1] + "/ok", 1),
                )
                self.assertEqual(results, [None, "/ok"])
                self.assertLess(perf_counter() - started, 0.8)

    async def test_cancellation_cleans_up_active_fetches(self) -> None:
        batch = asyncio.create_task(download_texts([self.bases[0] + "/stall"] * 1000, 3))
        await asyncio.wait_for(self.started.wait(), 1)
        batch.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await batch
        self.assertFalse(any(task.get_coro().__qualname__ == "_fetch" for task in asyncio.all_tasks()))

    async def test_connection_failure_is_isolated(self) -> None:
        server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        server.close()
        await server.wait_closed()
        results = await download_texts([f"http://127.0.0.1:{port}/", self.bases[1] + "/ok"], 2)
        self.assertEqual(results, [None, "/ok"])


if __name__ == "__main__":
    unittest.main()
