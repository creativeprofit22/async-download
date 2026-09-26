# Implementation decision

This downloader supports NGO-backed public-safety infrastructure and academic research: measuring how quickly and reliably public pages can become a research dataset.

## Update (September 2026): Rust is now the primary implementation

A second corpus survey found Rust evidence covering every requirement that the first survey had marked "not established". The primary implementation is now `rust/`; the Python version is kept as the comparison baseline.

| Property | Rust: tokio + reqwest (chosen) | Python: asyncio + aiohttp (baseline) | Go: goroutines + net/http | JavaScript: promises + permit gate |
| --- | --- | --- | --- | --- |
| Concurrency model | Work-stealing multi-thread runtime; `JoinSet` of bounded tasks | Single-thread event loop | Goroutines, multi-thread | Single-thread event loop |
| Backpressure and limits | Bounded `JoinSet` refill loop (seen in corpus); per-host counters in our dispatcher | Connector limits + our dispatcher | Not established in the helpers read | FIFO permit gate only |
| Timeouts | Separate connect, per-read and total settings on the client builder (seen in corpus) | total, connect, socket-read | Not established as three separate settings in the helpers read | Abort signal only |
| Cancellation | Dropping the future aborts every task in the `JoinSet` | Task cancellation on scope exit | Context-based | Abort signal |
| Per-host politeness | Per-host in-flight cap, minimum interval between starts, and `Retry-After` pause for 429/503, all in our dispatcher; the interval rule follows crw's `RateLimiter` (seen in corpus) | Per-host in-flight cap in local dispatcher; no pacing | Not established | Not established |
| Redirects | Followed by the dispatcher, up to 5 hops, each target checked like an input URL and queued on its own host (hop cap and per-target check seen in corpus) | Not followed; the page is `None` | Not established | Not established |
| Large bodies | Chunked read refused past a limit, checked against bytes actually read (seen in corpus) | Chunked read with limit; compressed bodies decoded while streaming, limit on decoded bytes | Read limit + 1 and refuse (seen in corpus) | Not addressed |
| Compressed pages | Streamed decompression, limit applies to decoded bytes | gzip and deflate decoded while streaming, limit applies to decoded bytes; brotli not supported (not in the standard library), so those pages are `None` | n/a | n/a |
| Error isolation | Typed per-request failure reduced to `None`; task panics contained | Exceptions reduced to `None` | Explicit returned errors | Not a per-URL collector |
| Observability | `tracing` events with reason and elapsed time per page | Debug logging | Status metadata | Error types |
| Code cost | ~1,450 lines incl. unit tests (library) | ~215 lines | Not built | Not built |

**Single-machine measurements (September 2026).** Windows 10 (10.0.19045), Intel Core (Family 6 Model 158), Python 3.13.7, aiohttp 3.14.3, rustc 1.97.1, Rust release build. These replace earlier single-run public timings, which were taken while the Python version still refused compressed pages. Both versions now decode gzip and deflate with the 2 MiB limit on decoded bytes. Commands and method are in the README ("Benchmark"); all settings were fixed in `bench/settings.py` before running (seed 20260926, 10 000 bootstrap resamples, 95 % percentile interval).

*Local server* (84 URLs on 4 loopback hosts: delays 10/50/150 ms, sizes 4/64/512 KiB, half gzip, plus a 404, a challenge page and a decompression bomb per host; concurrency 8, 2 per host, Rust pacing off; 1 warm-up and 20 measured runs each, alternating order):

| | Rust | Python |
| --- | --- | --- |
| Outcomes per 1 680 pages (ok / blocked / error / mismatch) | 1 440 / 80 / 160 / 0 | 1 440 / 80 / 160 / 0 |
| Gzip responses received | 720 | 720 |
| Median per-page completion time (ok pages) | 182.4 ms | 202.8 ms |
| Median batch wall time | 729.6 ms | 792.1 ms |
| Median throughput | 115.13 URLs/s | 106.05 URLs/s |

- Median of paired per-URL differences (Python − Rust, 1 440 pairs where both sides were ok; 240 excluded): **+22.3 ms, 95 % CI +21.5 to +23.4**. Pairs within one run are not independent, so this interval understates run-to-run variation.
- Paired per-run wall-time difference (Python − Rust, 20 runs): **+66.9 ms, 95 % CI +52.4 to +74.3**, about 8 % in Rust's favour. This is the conservative figure.
- Both versions returned the same outcome for every URL: challenge pages were flagged as blocked (not counted as successes), 404s and decompression bombs were refused.
- The workload is dominated by server delays; the gap reflects client overhead (decoding, scheduling, connection handling) on one machine, not a general speed ratio.

*Public pages* (7 pages, one per host: example.com, python.org, iana.org, en.wikipedia.org, gnu.org, w3.org, rust-lang.org; 3 runs each, alternating order, 10 s between runs, Rust pacing 500 ms): Rust collected 21/21 pages, Python 18/21. Wikipedia answered Python with 403 on all three runs and Rust with 200, although both send the same User-Agent; the cause was not investigated. Over the 18 pairs where both succeeded, the median per-URL difference (Python − Rust) was −68.9 ms (95 % CI −75.1 to −26.6), and the per-run wall-time difference −11.8 ms (95 % CI −89.3 to +20.7), i.e. no clear difference. Rust also advertises brotli, so some servers send it different bytes. Three runs over the public network are a sanity check, not a controlled comparison.

### Benchmark method sources

- [0xMassi/webclaw, `crates/webclaw-fetch/examples/latency_bench.rs`, lines 120–240](https://github.com/0xMassi/webclaw/blob/3c32041967c2c5d5124a984b14a29e6845b77be6/crates/webclaw-fetch/examples/latency_bench.rs#L120-L240) — one row per URL (status, time, bytes, error) and latency summarised only over successful responses, because fast failures would otherwise look like wins. We add challenge-page detection so a 200 block page is not a success.
- [fastcrw/crw, `bench/stats.py`, lines 1–160](https://github.com/fastcrw/crw/blob/6281dee1cec6d4490bca6b4cc5ff8fec70fd112a/bench/stats.py#L1-L160) — the median of paired per-item differences with a percentile bootstrap interval, seed and resample count fixed in advance, successes reported separately. crw uses numpy; `bench/stats.py` reimplements the same method with `random` and `statistics`.

Runners-up: Python stays as the baseline (smaller code; no redirects, pacing or brotli). Go was not chosen because it is not installed here and the corpus helpers read did not show separate connect/read/total timeouts. JavaScript was not chosen because the reviewed code covers permits and cancellation but no HTTP pool policy.

Per-host pacing now has a corpus reference (crw, below); the per-host in-flight cap and the `Retry-After` handling are still local design. Text decoding follows the browser order: a byte-order mark, then the `Content-Type` charset, then a `<meta>` charset found by the WHATWG prescan of the first 1024 bytes, then UTF-8 if the body is valid UTF-8 or mostly UTF-8 (its valid non-ASCII characters outnumber its malformed sequences). Only when none of these declares an encoding and the body is not mostly UTF-8 does the `chardetng` crate (pinned `=1.0.0`) guess a legacy encoding, with the host's top-level domain as a hint. Without this step, older pages that declare nothing would come back full of replacement characters. UTF-8 is tried first, and valid or mostly-UTF-8 bodies are never re-decoded, so a UTF-8 page with a stray bad byte keeps its text with one replacement character instead of being garbled by a windows-1252 guess (chardetng alone guesses windows-1252 for such pages, even with UTF-8 allowed). The rule is a heuristic: a legacy page whose bytes happen to form more valid multi-byte UTF-8 characters than malformed ones would be read as UTF-8. This is the one dependency added for decoding.

### Rust source patterns used

- [browseros-ai/BrowserOS, `packages/browseros-agent/apps/claw-server-rust/src/services/feedback_cohort.rs`, `read_capped`](https://github.com/browseros-ai/BrowserOS/blob/1c631ac79577dffc1c410164d464b6c7bc079dbe/packages/browseros-agent/apps/claw-server-rust/src/services/feedback_cohort.rs#L303) — chunk-by-chunk body read that refuses past a ceiling measured on bytes actually read, not `Content-Length`. Taken almost directly as `read_capped`.
- [elliothux/open-compute, `crates/artifacts/src/git_repo/import_http.rs`, `PinnedHttp::new`](https://github.com/elliothux/open-compute/blob/2cb98927e8371b23341edeae67692ba02a47c815/crates/artifacts/src/git_repo/import_http.rs#L119) — one client built with `no_proxy()`, `redirect::Policy::none()` and explicit `connect_timeout` + `timeout`. We added `read_timeout` and pool settings.
- [dark-hxx/CLI-Manager, `src-tauri/src/features/notifications/service/dispatcher.rs`, `process_job`](https://github.com/dark-hxx/CLI-Manager/blob/64836f5e302140456588f5f6ddde4c4d55225749/src-tauri/src/features/notifications/service/dispatcher.rs#L117) and [clash-verge-rev, `crates/clash-verge-media-unlock/src/lib.rs`](https://github.com/clash-verge-rev/clash-verge-rev/blob/897a117dc5fc474650512064cb96601aa5c39af5/crates/clash-verge-media-unlock/src/lib.rs#L114) — shared cloned client with a `JoinSet` refilled only while below a fixed limit.
- [Helvesec/rmux, `crates/rmux-sdk/src/broadcast.rs`](https://github.com/Helvesec/rmux/blob/1f4571e74f36be0c033c6294d1616c7d3a6fbda1/crates/rmux-sdk/src/broadcast.rs#L367) — carrying the input index through each task so results can be placed back in input order.
- [aaif-goose/goose, `crates/goose-cli/src/commands/review/orchestrator.rs`](https://github.com/aaif-goose/goose/blob/04ed836c8cde23e540cc77d256992e00be99298b/crates/goose-cli/src/commands/review/orchestrator.rs#L92) — the contract "one result per input, in the same order; one broken item must never block the rest".
- [fastcrw/crw, `crates/crw-renderer/src/host_limiter.rs`, `RateLimiter`](https://github.com/fastcrw/crw/blob/6281dee1cec6d4490bca6b4cc5ff8fec70fd112a/crates/crw-renderer/src/host_limiter.rs#L114) — minimum interval per host: the next request may start no earlier than the last start plus the interval. We keep that rule but wait in the dispatcher (a host whose time has not come sits in a time-ordered waiting list) instead of sleeping while holding a permit, so a paced host never holds a global slot.
- [fastcrw/crw, `crates/crw-crawl/src/crawl.rs`, lines 307–310](https://github.com/fastcrw/crw/blob/6281dee1cec6d4490bca6b4cc5ff8fec70fd112a/crates/crw-crawl/src/crawl.rs#L307-L310) — crw keys its limiter by registered domain (eTLD+1) so subdomains share one budget. We key by exact hostname; grouping by registered domain needs the public suffix list and is out of scope.
- [fastcrw/crw, `crates/crw-crawl/src/crawl.rs`, lines 45–47](https://github.com/fastcrw/crw/blob/6281dee1cec6d4490bca6b4cc5ff8fec70fd112a/crates/crw-crawl/src/crawl.rs#L45-L47) — per-host in-flight cap where 1 means strict politeness. Our default is 2 per host plus 500 ms between starts; `--per-host 1` gives crw's strict setting.
- [fastcrw/crw, `crates/crw-core/src/url_safety.rs`, `safe_redirect_policy`](https://github.com/fastcrw/crw/blob/6281dee1cec6d4490bca6b4cc5ff8fec70fd112a/crates/crw-core/src/url_safety.rs#L6) and [0xMassi/webclaw, `crates/webclaw-fetch/src/tls.rs`, `ssrf_safe_redirect_policy`](https://github.com/0xMassi/webclaw/blob/3c32041967c2c5d5124a984b14a29e6845b77be6/crates/webclaw-fetch/src/tls.rs#L597) — both cap the number of redirect hops (crw at 10) and check each target before following it. They follow redirects inside the HTTP client. We keep the client at `redirect::Policy::none()` and apply the same two rules in the dispatcher instead: the target is resolved against the page URL, checked with the input URL rules, and queued on the target host's queue under the original result slot, so the target host's limit and pacing apply. We allow 5 hops and also stop when a URL repeats within one chain.
- `Retry-After` handling is local design following RFC 9110 section 10.2.3: delay-seconds or IMF-fixdate. A 429 or 503 carrying it pauses only that host, capped at 60 s, and the page is retried once. The obsolete RFC 850 and asctime date forms are not parsed.
- [Hmbown/Codewhale, `crates/tui/src/tools/web/extract.rs`, `content_type_encoding`](https://github.com/Hmbown/Codewhale/blob/5765d80278f7184d187fa6682ba96b403a006523/crates/tui/src/tools/web/extract.rs#L485) — reading the `charset` parameter from `Content-Type` and mapping it with `Encoding::for_label`.
- [WHATWG HTML, "prescan a byte stream to determine its encoding"](https://html.spec.whatwg.org/multipage/parsing.html#prescan-a-byte-stream-to-determine-its-encoding) — the rule followed for the meta step: only the first 1024 bytes, case-insensitive tag and attribute names, comments and other tags' attribute values skipped, `content=...charset=...` only with `http-equiv="Content-Type"`, UTF-16 labels read as UTF-8 and `x-user-defined` as windows-1252. Implemented locally as `prescan_meta_charset`, with labels mapped by `Encoding::for_label`; no dependency added.
- [spider-rs/auto-encoder, `src/detect.rs`, `detect_encoding`](https://github.com/spider-rs/auto-encoder) (0.2.4, installed source read) — a 1024-byte meta search seen in the ecosystem. It matches case-sensitively and does not skip comments, so it was not used.
- [spider-rs/auto-encoder, `src/lib.rs`, `auto_encode_bytes`](https://github.com/spider-rs/auto-encoder) (0.2.4, installed source read) — byte-order mark first, then a `chardetng` guess. We follow the same shape but call `chardetng` directly, because auto-encoder depends on the older chardetng 0.1, and we add the UTF-8 check before guessing.
- [hsivonen/chardetng](https://github.com/hsivonen/chardetng) (1.0.0, [docs](https://docs.rs/chardetng/1.0.0/chardetng/)) — the legacy-encoding detector used by Firefox. We use `EncodingDetector::new(Iso2022JpDetection::Deny)`, `feed(body, true)` and `guess(tld, Utf8Detection::Deny)`: UTF-8 is denied because it has already failed. The top-level-domain hint must be lower-case ASCII (Punycode for IDNs) without dots or it panics, so `tld_hint` passes only such labels and nothing for IP hosts.
- [spider-rs/spider, `spider/src/utils/robots_cache.rs`, test `preserves_response_charset_decoding`](https://github.com/spider-rs/spider/blob/2e39b2db3eff15c3aaa90e3edcd1d7ba30a5bb2a/spider/src/utils/robots_cache.rs#L447-L496) — a local-server charset test; the model for our loopback meta-charset test.

Crate versions were checked against crates.io (`cargo search` / `cargo info`) and pinned exactly in `rust/Cargo.toml`.

## Original comparison (Python decision, kept for history)

These are source observations, not cross-language speed measurements. The properties below refer to the implementations actually read, not everything each language can do.

| Approach | Concurrency and backpressure | Timeouts and cancellation | Per-host politeness | Body memory | Failure isolation and observation | Implementation cost |
| --- | --- | --- | --- | --- | --- | --- |
| Event-loop tasks with a pooled HTTP session (Python) | Gain holds a semaphore around each request; BullMQ bounds concurrent benchmark workers; aiohttp's connector enforces acquired-connection limits | aiohttp exposes total, pool/connect, socket-connect and socket-read limits; async contexts release resources | Connector supports endpoint limits; our scheduler adds hostname-wide limits across ports | Stream iterator supports bounded chunks; Gain's whole-body text read needs replacing | Gain returns None per request and reports HTTP status; BullMQ uses monotonic timing | Smallest composition covering these requirements after adding fair scheduling and a body ceiling |
| Lightweight concurrent functions with reader-based HTTP (Go) | Reviewed response helpers do not implement batch scheduling | Context-based SDK request flow exists; separate connect/read/total settings were not established in the selected helpers | Not established in the selected helpers | Stagehand explicitly reads at most limit + 1 and rejects oversized bodies | Explicit returned errors; status and request metadata in the SDK | Strong runner-up, especially for dependency-light deployment; hostname scheduling and text decoding still need implementation; Go is not installed here |
| Runtime futures with a typed HTTP builder (Rust) | Kache's reviewed request function is single-request, not a bounded batch | Explicit total deadline; contextual errors on build, request, status and decode | Not established in the selected function | JSON response decoded whole; no body ceiling in this function | Typed Result and contextual errors; caller must isolate batch failures | Viable runner-up, but this sample leaves more coordination and streaming work to add |
| Promises with a cancellable permit gate (JavaScript) | Vision Toolkit provides FIFO permits and removes cancelled waiters | Abort signal works while queued | Could compose gates, but reviewed gate has no HTTP pool policy | Not addressed by the gate | Explicit cancellation/input errors; not a per-URL result collector | Good cancellation model, but more HTTP-specific policy must be assembled |

**Decision: Python 3.11+ with aiohttp 3.14.3.** The expanded corpus provides direct evidence for the HTTP controls, a compact real fetch boundary, and bounded benchmark concurrency. This is a code-cost and control-coverage decision, not a claim that Python is faster than Go or Rust.

The original corpus lacked Python HTTP connector examples. Discovery first found a file-download project that did not fit public-page collection. With approval, `aio-libs/aiohttp` and `elliotgao2/gain` were indexed. No exact sample implements this complete contract: hostname-fair scheduling, aligned outputs, compression refusal and the final policy are local compositions and must be verified locally.

## Source patterns used

- [elliotgao2/gain, gain/request.py, lines 4–14](https://github.com/elliotgao2/gain/blob/86f60f523e703276db8087d347812ce2f3ac07e2/gain/request.py#L4): request-scoped async cleanup, status checking and None on per-request exceptions. We do not copy its unbounded `response.text()` or per-parser sessions.
- [aio-libs/aiohttp, aiohttp/connector.py, lines 607–690](https://github.com/aio-libs/aiohttp/blob/9e08ba02abe573ce9e4cc541d4e3c3646adb43a7/aiohttp/connector.py#L607): global and endpoint connection limits, timed pool acquisition and cleanup on connection errors.
- [aio-libs/aiohttp, aiohttp/client_reqrep.py, lines 88–108](https://github.com/aio-libs/aiohttp/blob/9e08ba02abe573ce9e4cc541d4e3c3646adb43a7/aiohttp/client_reqrep.py#L88): separate total, connect, socket-connect and socket-read timeout fields.
- [aio-libs/aiohttp, aiohttp/streams.py, lines 158–170](https://github.com/aio-libs/aiohttp/blob/9e08ba02abe573ce9e4cc541d4e3c3646adb43a7/aiohttp/streams.py#L158): async chunk iteration instead of whole-body reads.
- [taskforcesh/bullmq, python/benchmark_backends.py, lines 60–74](https://github.com/taskforcesh/bullmq/blob/25e6dc75e4649fbe10ae1164f365159ab2cab892/python/benchmark_backends.py#L60): bounded concurrent work and monotonic batch timing. Our scheduler does not create one task per input.
- [browserbase/stagehand, packages/sdk-go/browserbase_client.go, lines 315–327](https://github.com/browserbase/stagehand/blob/18cde3bd446e279ed8c2d8698bbd43749ff93c27/packages/sdk-go/browserbase_client.go#L315): reject an oversized body rather than silently accepting truncation.

Comparison-only references:

- [kunobi-ninja/kache, src/planner_client.rs, lines 32–57](https://github.com/kunobi-ninja/kache/blob/82b8b284a8997a664f84b273c00985334a4c11b5/src/planner_client.rs#L32): timed typed HTTP request and contextual errors.
- [Anionex/dsh-vision-toolkit, lib/runtime.js, lines 98–151](https://github.com/Anionex/dsh-vision-toolkit/blob/22d95a44c6f84a32820556eaffeef94a83d2f584/lib/runtime.js#L98): cancellable FIFO permit gate.

The corpus checkout is newer than the package release. The selected 3.14.3 package source was separately inspected through `source_path`; its timeout class lives in `aiohttp/client.py`. Public API parameters were cross-checked against the [client reference](https://docs.aiohttp.org/en/stable/client_reference.html) and release availability against [PyPI](https://pypi.org/project/aiohttp/3.14.3/).

## How Steroids integrates with this build

Steroids is the development-time evidence source, not an HTTP runtime dependency. Its available tool contract supplies `search`, `show`, `files`, `discover`, and approval-gated `add`. We exercised that workflow and recorded immutable source links above. No callable application SDK, package installation interface, or runtime endpoint was established by the available tool contract; inventing one would make the downloader less reproducible.

For a future change: search literal API names, show the actual implementation, compare it with the pinned installed source, update this decision record, then run local verification. Corpus additions require approval. Normal installation and benchmark execution require neither Steroids nor its indexed repositories.
