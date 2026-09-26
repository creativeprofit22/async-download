# Public-page research downloader

Asynchronous bulk URL collection for NGO-backed public-safety infrastructure and academic research. Measures how quickly and reliably public pages can be collected into a research dataset.

## Primary implementation: Rust (`rust/`)

See [RESEARCH.md](RESEARCH.md) for why Rust replaced Python as the primary version. Requires a Rust toolchain (1.85+). Exact demo command, from this directory:

```bash
cargo run --release --manifest-path rust/Cargo.toml -- --concurrency 8 --per-host 2 --min-interval-ms 500
```

It fetches eight public pages and prints each page's character count (or `None`), then total time and URLs per second. It never prints page contents. Add `--verbose` for per-page reasons and elapsed times on stderr, or pass your own URLs as extra arguments.

Library use:

```rust
let texts = public_page_download::download_texts(&urls, 8).await; // Vec<Option<String>>, same order as urls
```

`download_texts_with_limits(&urls, Limits { max_in_flight, per_host, min_interval })` sets the limits explicitly; `Limits::default()` is 8 in flight, 2 per host, 500 ms between starts. Tests use a local server and need no network: `cargo test --manifest-path rust/Cargo.toml`. They include charset decoding from the header and from a page's meta charset.

Pacing and slow-down: request starts to one host are at least `min_interval` apart (default 500 ms, so at most 2 starts per second per host; `--min-interval-ms 0` turns pacing off). A host waiting for its next start holds no global slot, so other hosts keep going. When a host answers 429 or 503 with a `Retry-After` header (seconds, or an HTTP date in the standard `Sun, 06 Nov 1994 08:49:37 GMT` form; older date forms are treated as no usable header), only that host is paused, for at most 60 s, and the page is retried once after the pause. A second slow-down answer, or a requested pause over 60 s, leaves that page `None`. 429/503 without a usable `Retry-After` is an ordinary failure. Hosts are grouped by exact hostname; subdomains of one registered domain are not grouped together (that would need the public suffix list).

Redirects: a 301, 302, 303, 307 or 308 answer is followed by the dispatcher, not by the HTTP client. The `Location` is resolved against the page URL, checked with the same rules as an input URL (http or https, no user name or password, a valid port), and the page is queued on the target host with its original result slot, so the target host's per-host limit and pacing apply to every hop. Each input follows at most 5 hops. A loop, a missing or unusable `Location`, or a target that would not be accepted as input leaves that page `None`.

Other behaviour: global and per-host limits enforced by a dispatcher (never one task per URL); one shared client; connect 5 s, per-read 5 s, total 20 s; bodies refused above 2 MiB after decompression; text decoded with, in order, a byte-order mark, the `Content-Type` charset, a `<meta>` charset in the first 1024 bytes (the WHATWG HTML prescan, run for any response without a recognised header charset, including `text/plain`), then UTF-8 with replacement characters; proxies, authenticated pages, robots.txt and an allowlist are out of scope.

## Python baseline

## Run the demo

Python 3.11 or newer is required. From this directory on Windows:

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe downloader.py
```

In Bash on this Windows checkout, the exact demo command is:

```bash
.venv/Scripts/python downloader.py
```

On Linux/macOS, use `.venv/bin/python` instead of `.venv/Scripts/python` after creating the environment with `python3 -m venv .venv`.

The demo requests example.com, python.org and IANA. It prints character counts or None, elapsed wall time, success count and attempted URLs per second. It does not print or save page contents. Public-network timings vary and are not a controlled comparative benchmark.

## Entry point

```python
import asyncio
from downloader import download_texts

results = asyncio.run(download_texts(
    ["https://example.com/", "https://www.python.org/", "not a URL"],
    concurrency=8,
    per_host_limit=2,  # optional; defaults to 2
))
# One str or None per input, in the same order; duplicates are independent requests.
```

- Returns decoded response bodies, including HTML markup, not extracted article text.
- Global cap applies from request start through body consumption. A separate hostname cap spans ports and schemes, with case/trailing-dot normalization. Different hostnames sharing one server are not grouped together.
- A round-robin scheduler starts only eligible hosts, creating at most `concurrency` request tasks. A backlog for one host cannot occupy all workers waiting for that host.
- One session and connector per batch. Connection limits match the request caps; idle connections can be reused for five seconds. The connector limits acquired connections, not the cumulative count of idle sockets across every hostname. Cookies and environment-based proxies are disabled; DNS caching is disabled to avoid a growing per-batch cache.
- Timeouts: five seconds for connection acquisition, five for socket connection, five between received chunks, and twenty total per started request. Waiting in the scheduler does not consume the request deadline. A batch with many pages can therefore take longer than twenty seconds.
- Bodies are read in 64 KiB chunks and rejected above 2 MiB. Oversized pages are None, never silently truncated. Declared lengths are checked, but streaming limits also cover missing or inaccurate lengths.
- Declared text character sets are honored. Missing, unknown or non-text character sets fall back to UTF-8; invalid byte sequences become replacement characters.
- Only absolute HTTP/HTTPS URLs without whitespace, control characters or user information are accepted. URLs longer than 8192 characters fail without a request.
- Invalid concurrency or host limits return an all-None list without requests. Per-URL and ordinary batch setup errors do not escape the batch. Caller cancellation deliberately propagates after cleanup, rather than pretending unfinished work succeeded.

## Explicit scope

**This assumes an operator-reviewed list of public research URLs.** Address restrictions and an allowlist are not implemented; do not expose this function as a public URL-submission service. Confirm permission, site policies and applicable research approvals before collecting pages. Robots handling, proxies, authenticated pages, persistence and dataset governance belong outside this small benchmark component.

The Python baseline does not follow redirects (the Rust version does): every non-2xx response, including 3xx and 429, becomes None. Supply final public URLs. There are no automatic retries, so failures do not introduce retry delays or extra traffic. Concurrency limits are not a requests-per-second policy or a robots implementation.

For a predictable memory ceiling, requests advertise `Accept-Encoding: identity`, automatic decompression is disabled, and compressed responses are declined. This deliberately favors bounded resource use over covering every public page. Supporting them later requires bounded streaming decompression, not merely turning automatic decompression on.

Temporary body storage is bounded per active request, but retained output necessarily grows with successful input count: approximately O(number of pages × body limit), with additional Unicode storage. Scheduling metadata is O(number of inputs); request tasks are O(concurrency). Use smaller batches when collecting large datasets. No constant-memory or cross-language speed claim is made.

## Verification and diagnostics

```bash
.venv/Scripts/python -m unittest -v
```

Local HTTP tests cover ordering, duplicates, failures, encoding fallback, global and hostname caps across ports, fair scheduling, bounded task counts, connection reuse, oversized/chunked bodies, compression refusal, redirects, read/total timeouts and cancellation cleanup. They use only loopback servers. Real connection-establishment timeout timing is not tested by these local cases.

Observed on Windows with Python 3.13.7 and aiohttp 3.14.3: all nine tests passed using `python -W error::ResourceWarning -m unittest -v` (10.285 seconds). The public demo collected two of three pages in 0.192 seconds; python.org sent a compressed response despite the identity request and was correctly returned as None. A diagnostic rerun confirmed that reason. These are single-run observations, not a throughput guarantee. Python 3.11 and Linux/macOS were not exercised here.

For per-input failure categories, enable standard logging before calling the function:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

Downloader messages contain input indices and failure categories, not raw URLs or page bodies. The public demo reports aggregate timing and success counts.

## Evidence and Steroids integration

See [RESEARCH.md](RESEARCH.md) for the four-language comparison, decision, exact repositories/files and patterns used. Steroids is integrated into the development workflow through corpus search, source inspection and this provenance record. It is not required when installing or running the downloader; no runtime SDK was established by the available tool interface.
