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
| Per-host politeness | No corpus example; local dispatcher | No corpus example; local dispatcher | Not established | Not established |
| Large bodies | Chunked read refused past a limit, checked against bytes actually read (seen in corpus) | Chunked read with limit; compressed pages refused | Read limit + 1 and refuse (seen in corpus) | Not addressed |
| Compressed pages | Streamed decompression, limit applies to decoded bytes | Refused, so those pages are lost | n/a | n/a |
| Error isolation | Typed per-request failure reduced to `None`; task panics contained | Exceptions reduced to `None` | Explicit returned errors | Not a per-URL collector |
| Observability | `tracing` events with reason and elapsed time per page | Debug logging | Status metadata | Error types |
| Code cost | ~340 lines incl. unit tests | ~155 lines | Not built | Not built |

**Measured on this machine, same 8 public URLs, concurrency 8, 2 per host.** Rust release build: 8/8 pages in 0.30–0.33 s over four runs. Python baseline: 4–5/8 pages, one run in 0.43 s and two runs in about 5.0 s (a stalled read hit the 5 s read timeout). Single-machine observations on the public network, not a controlled benchmark. Rust's advantage here comes mostly from collecting compressed pages rather than refusing them, and from not stalling; raw CPU speed matters little at this scale because the work is network-bound.

Runners-up: Python stays as the baseline (smaller code, but loses compressed pages). Go was not chosen because it is not installed here and the corpus helpers read did not show separate connect/read/total timeouts. JavaScript was not chosen because the reviewed code covers permits and cancellation but no HTTP pool policy.

The corpus has nothing on: per-host limits in any language (our dispatcher is local design, carried over from the Python version), or charset fallback when no charset is declared (we use UTF-8 with replacement characters; HTML meta-tag sniffing is not implemented).

### Rust source patterns used

- [browseros-ai/BrowserOS, `packages/browseros-agent/apps/claw-server-rust/src/services/feedback_cohort.rs`, `read_capped`](https://github.com/browseros-ai/BrowserOS/blob/1c631ac79577dffc1c410164d464b6c7bc079dbe/packages/browseros-agent/apps/claw-server-rust/src/services/feedback_cohort.rs#L303) — chunk-by-chunk body read that refuses past a ceiling measured on bytes actually read, not `Content-Length`. Taken almost directly as `read_capped`.
- [elliothux/open-compute, `crates/artifacts/src/git_repo/import_http.rs`, `PinnedHttp::new`](https://github.com/elliothux/open-compute/blob/2cb98927e8371b23341edeae67692ba02a47c815/crates/artifacts/src/git_repo/import_http.rs#L119) — one client built with `no_proxy()`, `redirect::Policy::none()` and explicit `connect_timeout` + `timeout`. We added `read_timeout` and pool settings.
- [dark-hxx/CLI-Manager, `src-tauri/src/features/notifications/service/dispatcher.rs`, `process_job`](https://github.com/dark-hxx/CLI-Manager/blob/64836f5e302140456588f5f6ddde4c4d55225749/src-tauri/src/features/notifications/service/dispatcher.rs#L117) and [clash-verge-rev, `crates/clash-verge-media-unlock/src/lib.rs`](https://github.com/clash-verge-rev/clash-verge-rev/blob/897a117dc5fc474650512064cb96601aa5c39af5/crates/clash-verge-media-unlock/src/lib.rs#L114) — shared cloned client with a `JoinSet` refilled only while below a fixed limit.
- [Helvesec/rmux, `crates/rmux-sdk/src/broadcast.rs`](https://github.com/Helvesec/rmux/blob/1f4571e74f36be0c033c6294d1616c7d3a6fbda1/crates/rmux-sdk/src/broadcast.rs#L367) — carrying the input index through each task so results can be placed back in input order.
- [aaif-goose/goose, `crates/goose-cli/src/commands/review/orchestrator.rs`](https://github.com/aaif-goose/goose/blob/04ed836c8cde23e540cc77d256992e00be99298b/crates/goose-cli/src/commands/review/orchestrator.rs#L92) — the contract "one result per input, in the same order; one broken item must never block the rest".
- [Hmbown/Codewhale, `crates/tui/src/tools/web/extract.rs`, `content_type_encoding`](https://github.com/Hmbown/Codewhale/blob/5765d80278f7184d187fa6682ba96b403a006523/crates/tui/src/tools/web/extract.rs#L485) — reading the `charset` parameter from `Content-Type` and mapping it with `Encoding::for_label`.

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
