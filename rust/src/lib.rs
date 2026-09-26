//! Bounded collection of public page text for research throughput measurements.
//!
//! The single entry point is [`download_texts`] (or [`download_texts_with_limits`]
//! to set the global limit, the per-host limit and the per-host start interval
//! explicitly; see [`Limits`]). Results are aligned to the input list: each slot
//! holds the decoded page text, or `None` if that page could not be collected.
//! The batch never returns an error and never panics because of one page.
//!
//! Scope and assumptions:
//! - Inputs are public research URLs chosen by the operator. This is not a public
//!   URL-submission service, and no private-network filtering is applied.
//! - Out of scope, deliberately: proxies (system proxy settings are ignored),
//!   authenticated pages and cookies, robots.txt handling, and a host allowlist.
//!   Review the URL list and each site's usage policy before running a batch.
//! - Redirects (301, 302, 303, 307, 308) are followed by the dispatcher, not by the
//!   HTTP client: the target is resolved against the page URL, checked with the
//!   same rules as an input URL and queued on the target host, so that host's
//!   limit and pacing apply. At most [`MAX_REDIRECTS`] hops per input; loops,
//!   a missing or unusable `Location`, and non-http(s) targets give `None`.
//! - Pacing and slow-down pauses apply per exact hostname. Grouping subdomains
//!   under one registered domain (eTLD+1) is out of scope, because it needs the
//!   public suffix list.
//! - Text is decoded with, in order: a byte-order mark, the `Content-Type`
//!   charset, a `<meta>` charset in the first 1024 bytes (the WHATWG HTML
//!   prescan), then UTF-8 with replacement characters.
//!
//! Politeness: request starts to one host are at least [`Limits::min_interval`]
//! apart. A `429` or `503` response with a usable `Retry-After` pauses only that
//! host (capped at [`MAX_RETRY_AFTER`]) and the page is retried once after the pause.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::{Duration, Instant, SystemTime};

use encoding_rs::{Encoding, UTF_8, UTF_16BE, UTF_16LE, WINDOWS_1252, X_USER_DEFINED};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue, LOCATION, RETRY_AFTER, USER_AGENT};
use reqwest::{Client, Response, StatusCode, Url, redirect};
use tokio::task::{Id, JoinSet};

/// Largest decoded body accepted, in bytes. Larger pages yield `None`, never a truncated text.
pub const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
/// Longest input URL accepted, in characters.
pub const MAX_URL_CHARS: usize = 8192;
/// Per-host limit used by [`download_texts`].
pub const DEFAULT_PER_HOST_LIMIT: usize = 2;
/// Least time between request starts to one host used by [`download_texts`]
/// (at most two starts per second per host).
pub const DEFAULT_MIN_INTERVAL: Duration = Duration::from_millis(500);
/// Longest pause honoured from a `Retry-After` header. A host asking for more is
/// paused this long, and the page that received the answer is not retried.
pub const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);
/// Most redirect hops followed for one input. A page that redirects again after
/// this many hops yields `None`.
pub const MAX_REDIRECTS: usize = 5;
/// Global limit used by [`Limits::default`].
const DEFAULT_MAX_IN_FLIGHT: usize = 8;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Longest wait for any single read on an open connection.
const READ_TIMEOUT: Duration = Duration::from_secs(5);
/// Whole request, from send until the last body byte.
const TOTAL_TIMEOUT: Duration = Duration::from_secs(20);
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(5);
const USER_AGENT_VALUE: &str = "PublicPageResearch/1.0";

/// Concurrency limits for one batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Most requests in flight across all hosts.
    pub max_in_flight: usize,
    /// Most requests in flight to any one host. Clamped to `max_in_flight`.
    pub per_host: usize,
    /// Least time between request starts to any one host. `Duration::ZERO`
    /// disables pacing. A host waiting for its next start holds no global slot.
    pub min_interval: Duration,
}

impl Default for Limits {
    /// 8 in flight, [`DEFAULT_PER_HOST_LIMIT`] per host, [`DEFAULT_MIN_INTERVAL`] apart.
    fn default() -> Self {
        Self {
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            per_host: DEFAULT_PER_HOST_LIMIT,
            min_interval: DEFAULT_MIN_INTERVAL,
        }
    }
}

/// Download each URL's text with at most `concurrency` requests in flight, at
/// most [`DEFAULT_PER_HOST_LIMIT`] per host, and request starts to one host at
/// least [`DEFAULT_MIN_INTERVAL`] apart.
pub async fn download_texts<S: AsRef<str>>(urls: &[S], concurrency: usize) -> Vec<Option<String>> {
    download_texts_with_limits(
        urls,
        Limits {
            max_in_flight: concurrency,
            ..Limits::default()
        },
    )
    .await
}

/// Download each URL's text under explicit global and per-host limits.
///
/// Returns one slot per input, in input order, regardless of completion order.
/// A limit of zero returns all `None` without sending requests. Dropping the
/// returned future cancels every request still in flight.
///
/// Host limits group hostnames that differ only in letter case or a trailing
/// dot, across both schemes and all ports.
///
/// Request starts to one host are at least `limits.min_interval` apart. When a
/// host answers `429` or `503` with a usable `Retry-After`, only that host is
/// paused, for at most [`MAX_RETRY_AFTER`], and the page is retried once after
/// the pause. A second slow-down answer, or a requested pause above the cap,
/// leaves that page `None`. Requests already in flight are not cancelled.
///
/// A redirect answer puts the page back in the queue of the target host, first
/// in line, keeping its result slot. Each hop is a new request under the target
/// host's limit and pacing. After [`MAX_REDIRECTS`] hops, on a loop, or when the
/// target is not an acceptable input URL, the slot stays `None`.
pub async fn download_texts_with_limits<S: AsRef<str>>(
    urls: &[S],
    limits: Limits,
) -> Vec<Option<String>> {
    let mut results: Vec<Option<String>> = vec![None; urls.len()];
    if limits.max_in_flight == 0 || limits.per_host == 0 {
        return results;
    }
    let per_host = limits.per_host.min(limits.max_in_flight);

    let (mut hosts, mut ready) = group_by_host(urls, Instant::now());
    if hosts.is_empty() {
        return results;
    }

    let client = match build_client(per_host) {
        Ok(client) => client,
        Err(error) => {
            tracing::warn!(%error, "HTTP client could not be created");
            return results;
        }
    };

    // Dispatcher: a host is "listed" (in exactly one of `ready` or `waiting`) only
    // while it has queued pages and spare per-host capacity. `waiting` holds listed
    // hosts whose next start time has not come yet, ordered by time and then by
    // arrival, so wake order is deterministic. A task is spawned only when a global
    // slot is free and the host's time has come, so both limits and the pacing hold
    // by construction, a paced or paused host holds no slot and no task, at most
    // `max_in_flight` tasks ever exist, and ready hosts are served round-robin.
    let mut waiting: BTreeMap<(Instant, u64), String> = BTreeMap::new();
    let mut arrivals: u64 = 0;
    let mut owners: HashMap<Id, (String, Pending)> = HashMap::new();
    let mut tasks: JoinSet<PageOutcome> = JoinSet::new();

    loop {
        let now = Instant::now();
        while let Some(entry) = waiting.first_entry() {
            if entry.key().0 > now {
                break;
            }
            ready.push_back(entry.remove());
        }

        while tasks.len() < limits.max_in_flight {
            let Some(host) = ready.pop_front() else { break };
            let Some(state) = hosts.get_mut(&host) else {
                continue;
            };
            if now < state.next_start {
                waiting.insert((state.next_start, arrivals), host);
                arrivals += 1;
                continue;
            }
            state.listed = false;
            let Some(page) = state.queue.pop_front() else {
                continue;
            };
            state.in_flight += 1;
            state.next_start = instant_after(now, limits.min_interval);
            let handle = tasks.spawn(fetch_text(client.clone(), page.url.clone(), page.index));
            owners.insert(handle.id(), (host.clone(), page));
            state.enlist(&host, per_host, &mut ready);
        }

        // The fill loop stops only when `ready` is empty or every slot is taken.
        let wake = waiting
            .first_key_value()
            .map(|(&(at, _), _)| tokio::time::Instant::from_std(at));
        let joined = match (tasks.is_empty(), wake) {
            (true, None) => break,
            (true, Some(at)) => {
                tokio::time::sleep_until(at).await;
                continue;
            }
            (false, None) => tasks.join_next_with_id().await,
            (false, Some(at)) => tokio::select! {
                joined = tasks.join_next_with_id() => joined,
                () = tokio::time::sleep_until(at) => continue,
            },
        };
        let Some(joined) = joined else { continue };
        let (id, outcome) = match joined {
            Ok((id, outcome)) => (id, outcome),
            // A panic inside one request is contained to that slot.
            Err(error) => {
                tracing::warn!(%error, "request task ended abnormally");
                (error.id(), PageOutcome::Done(None))
            }
        };
        let Some((host, page)) = owners.remove(&id) else {
            continue;
        };
        let Some(state) = hosts.get_mut(&host) else {
            continue;
        };
        state.in_flight -= 1;
        match outcome {
            PageOutcome::Done(text) => results[page.index] = text,
            PageOutcome::SlowDown(asked) => state.slow_down(page, asked, Instant::now()),
            PageOutcome::Redirect(location) => {
                requeue_redirect(&mut hosts, &mut ready, per_host, page, &location);
            }
        }
        if let Some(state) = hosts.get_mut(&host) {
            state.enlist(&host, per_host, &mut ready);
        }
    }
    results
}

/// Queue a redirected page on its target host, or log why the redirect is not
/// followed (the page's slot then stays `None`). The page joins the target host's
/// queue, so that host's per-host limit, pacing and any pause apply to it. It goes
/// first: a started chain finishes before new work.
fn requeue_redirect(
    hosts: &mut HashMap<String, HostState>,
    ready: &mut VecDeque<String>,
    per_host: usize,
    page: Pending,
    location: &str,
) {
    let index = page.index;
    match follow_redirect(page, location) {
        Ok((target, page)) => {
            let state = hosts
                .entry(target.clone())
                .or_insert_with(|| HostState::new(Instant::now()));
            state.queue.push_front(page);
            state.enlist(&target, per_host, ready);
        }
        Err(reason) => tracing::debug!(index, reason, "redirect not followed"),
    }
}

/// One page waiting to be requested.
#[derive(Debug)]
struct Pending {
    index: usize,
    url: Url,
    /// Redirect hops already followed for this input.
    hops: usize,
    /// URLs already requested for this input, without fragments. Holds at most
    /// `MAX_REDIRECTS + 1` entries.
    seen: Vec<Url>,
    /// Whether this page already received a slow-down answer once.
    retried: bool,
}

impl Pending {
    fn new(index: usize, url: Url) -> Self {
        let seen = vec![without_fragment(&url)];
        Self {
            index,
            url,
            hops: 0,
            seen,
            retried: false,
        }
    }
}

fn without_fragment(url: &Url) -> Url {
    let mut url = url.clone();
    url.set_fragment(None);
    url
}

/// Resolve one redirect for `page` and return the target's host key with the
/// page moved to the target URL. The target must pass the same checks as an
/// input URL, must not repeat a URL this input already requested, and the chain
/// may have at most [`MAX_REDIRECTS`] hops.
fn follow_redirect(page: Pending, location: &str) -> Result<(String, Pending), &'static str> {
    if page.hops >= MAX_REDIRECTS {
        return Err("too many redirects");
    }
    let joined = page
        .url
        .join(location)
        .map_err(|_| "Location is not a valid URL")?;
    let (host, url) = parse_public_url(joined.as_str())?;
    let target = without_fragment(&url);
    if page.seen.contains(&target) {
        return Err("redirect loop");
    }
    let mut seen = page.seen;
    seen.push(target);
    Ok((
        host,
        Pending {
            index: page.index,
            url,
            hops: page.hops + 1,
            seen,
            retried: page.retried,
        },
    ))
}

/// Scheduling state of one host.
#[derive(Debug)]
struct HostState {
    queue: VecDeque<Pending>,
    in_flight: usize,
    /// Earliest time the next request to this host may start.
    next_start: Instant,
    /// Whether the host is in the dispatcher's `ready` queue or `waiting` map.
    listed: bool,
}

impl HostState {
    /// An empty, unlisted host that may start a request at `now`.
    fn new(now: Instant) -> Self {
        Self {
            queue: VecDeque::new(),
            in_flight: 0,
            next_start: now,
            listed: false,
        }
    }

    /// Pause this host after a slow-down answer and requeue the page for one retry.
    /// Only this host is paused; other hosts keep their own schedule.
    fn slow_down(&mut self, page: Pending, asked: Duration, now: Instant) {
        let pause = asked.min(MAX_RETRY_AFTER);
        self.next_start = self.next_start.max(instant_after(now, pause));
        // Retrying earlier than the site asked would be impolite, so a pause
        // above the cap is honoured without a retry.
        let retrying = !page.retried && asked <= MAX_RETRY_AFTER;
        tracing::debug!(
            index = page.index,
            pause_ms = u64::try_from(pause.as_millis()).unwrap_or(u64::MAX),
            retrying,
            "host asked to slow down"
        );
        if retrying {
            self.queue.push_front(Pending {
                retried: true,
                ..page
            });
        }
    }

    /// List the host as ready if it has queued pages and spare capacity and is
    /// not listed already, so a host is never listed twice.
    fn enlist(&mut self, host: &str, per_host: usize, ready: &mut VecDeque<String>) {
        if !self.listed && !self.queue.is_empty() && self.in_flight < per_host {
            self.listed = true;
            ready.push_back(host.to_owned());
        }
    }
}

/// Group valid inputs by host. Hosts are returned in first-seen order, already
/// listed as ready, so scheduling is deterministic.
fn group_by_host<S: AsRef<str>>(
    urls: &[S],
    now: Instant,
) -> (HashMap<String, HostState>, VecDeque<String>) {
    let mut hosts: HashMap<String, HostState> = HashMap::new();
    let mut ready: VecDeque<String> = VecDeque::new();
    for (index, raw) in urls.iter().enumerate() {
        match parse_public_url(raw.as_ref()) {
            Ok((host, url)) => hosts
                .entry(host)
                .or_insert_with_key(|host| {
                    ready.push_back(host.clone());
                    HostState {
                        listed: true,
                        ..HostState::new(now)
                    }
                })
                .queue
                .push_back(Pending::new(index, url)),
            Err(reason) => tracing::debug!(index, reason, "input rejected"),
        }
    }
    (hosts, ready)
}

/// `now + delay`, saturating far in the future instead of overflowing.
fn instant_after(now: Instant, delay: Duration) -> Instant {
    const FAR_FUTURE: Duration = Duration::from_secs(100 * 365 * 24 * 60 * 60);
    now.checked_add(delay.min(FAR_FUTURE))
        .or_else(|| now.checked_add(MAX_RETRY_AFTER))
        .unwrap_or(now)
}

/// Validate one input and return its host grouping key with the parsed URL.
fn parse_public_url(raw: &str) -> Result<(String, Url), &'static str> {
    if raw.is_empty() || raw.chars().count() > MAX_URL_CHARS {
        return Err("expected a nonempty URL within the length limit");
    }
    if raw.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err("whitespace and control characters are not accepted");
    }
    let url = Url::parse(raw).map_err(|_| "not an absolute URL")?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("only http and https URLs are accepted");
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("URL user information is not supported");
    }
    if url.port() == Some(0) {
        return Err("port must be positive");
    }
    // The URL parser has already applied IDNA and lowercased domain names.
    // The key is the exact hostname: grouping subdomains under one registered
    // domain (eTLD+1) is out of scope, because it needs the public suffix list.
    let host = url
        .host_str()
        .map(|host| host.trim_end_matches('.').to_ascii_lowercase())
        .filter(|host| !host.is_empty())
        .ok_or("URL has no host")?;
    Ok((host, url))
}

/// One shared client per batch: pooled connections, explicit timeouts and pool limits.
fn build_client(per_host: usize) -> reqwest::Result<Client> {
    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_VALUE));
    Client::builder()
        .default_headers(headers)
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_TIMEOUT)
        .timeout(TOTAL_TIMEOUT)
        // The dispatcher caps open requests; idle keep-alive connections are capped here.
        .pool_max_idle_per_host(per_host)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .redirect(redirect::Policy::none())
        .no_proxy()
        .build()
}

/// Result of one request task, as seen by the dispatcher.
#[derive(Debug)]
enum PageOutcome {
    /// The page's final slot value.
    Done(Option<String>),
    /// The host answered 429 or 503 with a usable `Retry-After` of this length (uncapped).
    SlowDown(Duration),
    /// The host answered with a redirect to this `Location` value (not yet resolved).
    Redirect(String),
}

/// A successful answer from one request.
#[derive(Debug)]
enum Fetched {
    Text(String),
    /// Raw `Location` value of a 301, 302, 303, 307 or 308 answer.
    Redirect(String),
}

#[derive(Debug)]
enum FetchFailure {
    Transport(reqwest::Error),
    Status(u16),
    SlowDown(u16, Duration),
    /// A redirect status without a usable `Location` header.
    BadRedirect(u16),
    TooLarge,
}

impl std::fmt::Display for FetchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) if error.is_timeout() => f.write_str("timed out"),
            Self::Transport(error) if error.is_connect() => f.write_str("connection failed"),
            Self::Transport(error) => write!(f, "transfer failed: {error}"),
            Self::Status(code) => write!(f, "HTTP status {code}"),
            Self::SlowDown(code, pause) => {
                write!(f, "HTTP status {code}, retry after {}s", pause.as_secs())
            }
            Self::BadRedirect(code) => {
                write!(f, "HTTP status {code} without a usable Location")
            }
            Self::TooLarge => write!(f, "body exceeds {MAX_BODY_BYTES} bytes"),
        }
    }
}

/// Fetch one page and reduce every failure to `None`, logging the reason and
/// elapsed time. Slow-down and redirect answers are passed to the dispatcher instead.
async fn fetch_text(client: Client, url: Url, index: usize) -> PageOutcome {
    let started = Instant::now();
    let outcome = fetch_body(&client, url).await;
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    match outcome {
        Ok(Fetched::Text(text)) => {
            tracing::debug!(index, elapsed_ms, bytes = text.len(), "page collected");
            PageOutcome::Done(Some(text))
        }
        Ok(Fetched::Redirect(location)) => {
            tracing::debug!(index, elapsed_ms, "page redirected");
            PageOutcome::Redirect(location)
        }
        Err(failure @ FetchFailure::SlowDown(_, pause)) => {
            tracing::debug!(
                index,
                elapsed_ms,
                reason = %failure,
                "page not collected yet: host asked to slow down"
            );
            PageOutcome::SlowDown(pause)
        }
        Err(failure) => {
            tracing::debug!(index, elapsed_ms, reason = %failure, "page not collected");
            PageOutcome::Done(None)
        }
    }
}

async fn fetch_body(client: &Client, url: Url) -> Result<Fetched, FetchFailure> {
    let response = client
        .get(url)
        .send()
        .await
        .map_err(FetchFailure::Transport)?;
    let status = response.status();
    if matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
    ) {
        let pause = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| retry_after(value, SystemTime::now()));
        if let Some(pause) = pause {
            return Err(FetchFailure::SlowDown(status.as_u16(), pause));
        }
    }
    if matches!(
        status,
        StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT
    ) {
        // Every request is a GET, so all five codes are followed the same way.
        // The body of a redirect answer is not read. Location is decoded as
        // UTF-8 rather than visible ASCII, so raw non-ASCII paths are accepted;
        // joining percent-encodes them before the target is checked.
        return response
            .headers()
            .get(LOCATION)
            .and_then(|value| std::str::from_utf8(value.as_bytes()).ok())
            .map(|location| Fetched::Redirect(location.to_owned()))
            .ok_or(FetchFailure::BadRedirect(status.as_u16()));
    }
    if !status.is_success() {
        return Err(FetchFailure::Status(status.as_u16()));
    }
    // Compressed bodies are decoded while streaming, so the cap below applies to
    // decompressed bytes and a small compressed page cannot expand without limit.
    if response
        .content_length()
        .is_some_and(|length| length > MAX_BODY_BYTES as u64)
    {
        return Err(FetchFailure::TooLarge);
    }
    // An unknown or missing header charset gives `None`, so `decode_text` falls
    // through to the meta prescan.
    let declared = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(charset_from_content_type);
    let body = read_capped(response).await?;
    Ok(Fetched::Text(decode_text(&body, declared)))
}

/// Read the body chunk by chunk, refusing rather than buffering past [`MAX_BODY_BYTES`].
/// The limit is checked against bytes actually read, since `Content-Length` may be
/// absent or wrong.
async fn read_capped(mut response: Response) -> Result<Vec<u8>, FetchFailure> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(FetchFailure::Transport)? {
        if body.len().saturating_add(chunk.len()) > MAX_BODY_BYTES {
            return Err(FetchFailure::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Parse a `Retry-After` value (RFC 9110 section 10.2.3) into a pause measured from `now`.
///
/// Accepts delay-seconds (ASCII digits only; huge values saturate) and the
/// IMF-fixdate form (`Sun, 06 Nov 1994 08:49:37 GMT`). A date in the past gives a
/// zero pause. The obsolete RFC 850 and asctime date forms are not accepted and,
/// like any other unparseable value, give `None`.
fn retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
        return Some(Duration::from_secs(value.parse().unwrap_or(u64::MAX)));
    }
    let at = parse_imf_fixdate(value)?;
    Some(at.duration_since(now).unwrap_or(Duration::ZERO))
}

/// Parse `Sun, 06 Nov 1994 08:49:37 GMT` exactly; anything else gives `None`.
fn parse_imf_fixdate(value: &str) -> Option<SystemTime> {
    const DAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let fixed_digits = |text: &str, len: usize| -> Option<u32> {
        (text.len() == len && text.bytes().all(|b| b.is_ascii_digit()))
            .then(|| text.parse().ok())
            .flatten()
    };

    let mut parts = value.split(' ');
    let day_name = parts.next()?.strip_suffix(',')?;
    let day = fixed_digits(parts.next()?, 2)?;
    let month_name = parts.next()?;
    let year = fixed_digits(parts.next()?, 4)?;
    let time = parts.next()?;
    if parts.next()? != "GMT" || parts.next().is_some() || !DAYS.contains(&day_name) {
        return None;
    }
    let month = MONTHS.iter().position(|m| *m == month_name)? + 1;
    let mut clock = time.split(':');
    let hour = fixed_digits(clock.next()?, 2)?;
    let minute = fixed_digits(clock.next()?, 2)?;
    let second = fixed_digits(clock.next()?, 2)?;
    if clock.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let month = u32::try_from(month).ok()?;
    if day == 0 || day > days_in_month(year, month) {
        return None;
    }

    let days = days_from_civil(i64::from(year), month, day);
    let seconds =
        days * 86_400 + i64::from(hour) * 3_600 + i64::from(minute) * 60 + i64::from(second);
    // Years 0000-1969 give negative seconds; such a date is in the past anyway.
    let seconds = u64::try_from(seconds).unwrap_or(0);
    SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(seconds))
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let month_from_march = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * month_from_march + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Return the encoding named by a `charset` parameter, if it is a known label.
fn charset_from_content_type(content_type: &str) -> Option<&'static Encoding> {
    content_type.split(';').skip(1).find_map(|parameter| {
        let (name, value) = parameter.split_once('=')?;
        if !name.trim().eq_ignore_ascii_case("charset") {
            return None;
        }
        let label = value.trim().trim_matches(|c| c == '"' || c == '\'');
        Encoding::for_label(label.as_bytes())
    })
}

/// How far into the body the meta prescan looks, as the WHATWG HTML standard sets.
const PRESCAN_LIMIT: usize = 1024;

/// ASCII whitespace as the HTML standard defines it (tab, LF, FF, CR, space).
fn is_html_space(byte: u8) -> bool {
    matches!(byte, b'\t' | b'\n' | 0x0C | b'\r' | b' ')
}

/// Index of the first `needle` in `bytes` at or after `from`.
fn find_from(bytes: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    bytes
        .get(from..)?
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|found| found + from)
}

/// Find a `<meta>` charset declaration in the first [`PRESCAN_LIMIT`] bytes,
/// following the WHATWG HTML "prescan a byte stream to determine its encoding"
/// algorithm: tag and attribute names match case-insensitively, comments and
/// other tags' attribute values are skipped, and a tag cut off by the limit is
/// ignored. UTF-16 labels give UTF-8 and `x-user-defined` gives windows-1252,
/// as the standard says. Unknown labels are ignored.
fn prescan_meta_charset(body: &[u8]) -> Option<&'static Encoding> {
    let bytes = &body[..body.len().min(PRESCAN_LIMIT)];
    let mut pos = 0;
    while pos < bytes.len() {
        let rest = &bytes[pos..];
        if rest.starts_with(b"<!--") {
            // Searching from the second dash lets `<!-->` close itself, as in the standard.
            pos = find_from(bytes, pos + 2, b"-->")? + 3;
            continue;
        }
        if rest.len() > 5
            && rest[..5].eq_ignore_ascii_case(b"<meta")
            && (is_html_space(rest[5]) || rest[5] == b'/')
        {
            pos += 6;
            if let Some(encoding) = meta_charset(bytes, &mut pos) {
                return Some(encoding);
            }
        } else if rest.len() > 1 && rest[0] == b'<' {
            let name_start = if rest[1] == b'/' { 2 } else { 1 };
            if rest.get(name_start).is_some_and(u8::is_ascii_alphabetic) {
                // Any other tag: skip its attributes so text inside their values
                // cannot be mistaken for a meta tag.
                pos += name_start;
                while bytes
                    .get(pos)
                    .is_some_and(|&byte| !is_html_space(byte) && byte != b'>')
                {
                    pos += 1;
                }
                while get_attribute(bytes, &mut pos).is_some() {}
            } else if matches!(rest[1], b'!' | b'/' | b'?') {
                pos = find_from(bytes, pos + 1, b">")?;
            }
        }
        pos += 1;
    }
    None
}

/// Read the attributes of one `<meta>` tag, starting after `<meta` and the byte
/// that follows it, and return the declared encoding if the tag is a valid
/// declaration. `pos` is left at the tag's `>` (or at the end of the input).
fn meta_charset(bytes: &[u8], pos: &mut usize) -> Option<&'static Encoding> {
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut got_pragma = false;
    // `None` until a charset attribute or a content charset is seen (the
    // standard's "need pragma is null").
    let mut need_pragma: Option<bool> = None;
    let mut charset: Option<&'static Encoding> = None;
    while let Some((name, value)) = get_attribute(bytes, pos) {
        // Only the first attribute with a given name counts.
        if seen.contains(&name) {
            continue;
        }
        match name.as_slice() {
            b"http-equiv" => got_pragma |= value == b"content-type",
            b"content" if charset.is_none() => {
                if let Some(encoding) =
                    extract_charset_from_content(&value).and_then(Encoding::for_label)
                {
                    charset = Some(encoding);
                    need_pragma = Some(true);
                }
            }
            b"charset" => {
                charset = Encoding::for_label(&value);
                need_pragma = Some(false);
            }
            _ => {}
        }
        seen.push(name);
    }
    if *pos >= bytes.len() {
        // The tag was cut off by the end of the input or the prescan limit.
        return None;
    }
    if need_pragma? && !got_pragma {
        return None;
    }
    let encoding = charset?;
    Some(if encoding == UTF_16LE || encoding == UTF_16BE {
        UTF_8
    } else if encoding == X_USER_DEFINED {
        WINDOWS_1252
    } else {
        encoding
    })
}

/// The standard's "get an attribute": read one attribute at `pos`, with the name
/// and value lowercased. Returns `None` at the tag's `>` (leaving `pos` there) or
/// when the input ends first (leaving `pos` at the end).
fn get_attribute(bytes: &[u8], pos: &mut usize) -> Option<(Vec<u8>, Vec<u8>)> {
    let at = |index: usize| bytes.get(index).copied();
    while at(*pos).is_some_and(|byte| is_html_space(byte) || byte == b'/') {
        *pos += 1;
    }
    if at(*pos)? == b'>' {
        return None;
    }
    let mut name = Vec::new();
    loop {
        match at(*pos)? {
            b'=' if !name.is_empty() => break,
            byte if is_html_space(byte) => {
                while at(*pos).is_some_and(is_html_space) {
                    *pos += 1;
                }
                if at(*pos)? != b'=' {
                    return Some((name, Vec::new()));
                }
                break;
            }
            b'/' | b'>' => return Some((name, Vec::new())),
            byte => name.push(byte.to_ascii_lowercase()),
        }
        *pos += 1;
    }
    // `pos` is at the `=`.
    *pos += 1;
    while at(*pos).is_some_and(is_html_space) {
        *pos += 1;
    }
    let mut value = Vec::new();
    match at(*pos)? {
        quote @ (b'"' | b'\'') => loop {
            *pos += 1;
            let byte = at(*pos)?;
            if byte == quote {
                *pos += 1;
                return Some((name, value));
            }
            value.push(byte.to_ascii_lowercase());
        },
        b'>' => return Some((name, value)),
        _ => {}
    }
    loop {
        let byte = at(*pos)?;
        if is_html_space(byte) || byte == b'>' {
            return Some((name, value));
        }
        value.push(byte.to_ascii_lowercase());
        *pos += 1;
    }
}

/// The standard's "extract a character encoding from a meta element": the label
/// after `charset=` in a `content` value, quoted or up to whitespace or `;`.
fn extract_charset_from_content(value: &[u8]) -> Option<&[u8]> {
    let skip_spaces = |mut index: usize| {
        while value.get(index).copied().is_some_and(is_html_space) {
            index += 1;
        }
        index
    };
    let mut pos = 0;
    loop {
        let found = value
            .get(pos..)?
            .windows(7)
            .position(|window| window.eq_ignore_ascii_case(b"charset"))?;
        pos = skip_spaces(pos + found + 7);
        if value.get(pos) != Some(&b'=') {
            continue;
        }
        pos = skip_spaces(pos + 1);
        let first = *value.get(pos)?;
        if first == b'"' || first == b'\'' {
            let rest = &value[pos + 1..];
            let end = rest.iter().position(|&byte| byte == first)?;
            return Some(&rest[..end]);
        }
        let rest = &value[pos..];
        let end = rest
            .iter()
            .position(|&byte| is_html_space(byte) || byte == b';')
            .unwrap_or(rest.len());
        return Some(&rest[..end]);
    }
}

/// Decode a body in the order browsers use: a byte-order mark wins
/// (`encoding_rs` `decode` sniffs it), then the `Content-Type` charset, then a `<meta>` charset
/// found by [`prescan_meta_charset`], then UTF-8. The prescan runs for every
/// response without a recognised header charset, including `text/plain`.
/// Malformed sequences become U+FFFD rather than failing the page. Decoding a
/// body of at most 2 MiB is brief enough to run on the async worker.
fn decode_text(body: &[u8], declared: Option<&'static Encoding>) -> String {
    let encoding = declared
        .or_else(|| prescan_meta_charset(body))
        .unwrap_or(UTF_8);
    let (text, _, _) = encoding.decode(body);
    text.into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_http_and_https_and_normalizes_host() {
        let cases = [
            ("https://Example.COM./page", "example.com"),
            ("http://example.com:8080/", "example.com"),
            ("https://[::1]/", "[::1]"),
        ];
        for (raw, host) in cases {
            assert_eq!(
                parse_public_url(raw).map(|(h, _)| h),
                Ok(host.to_owned()),
                "{raw}"
            );
        }
    }

    #[test]
    fn rejects_unsupported_inputs() {
        let long = format!("https://example.com/{}", "a".repeat(MAX_URL_CHARS));
        let cases = [
            "",
            "not a url",
            "/relative/path",
            "ftp://example.com/",
            "file:///etc/hosts",
            "https://user:pass@example.com/",
            "https://example.com/ space",
            "https://example.com:0/",
            long.as_str(),
        ];
        for raw in cases {
            assert!(parse_public_url(raw).is_err(), "{raw:?} should be rejected");
        }
    }

    #[test]
    fn reads_charset_parameter() {
        let cases = [
            (
                "text/html; charset=ISO-8859-1",
                Some(encoding_rs::WINDOWS_1252),
            ),
            (
                "text/html; Charset=\"shift_jis\"",
                Some(encoding_rs::SHIFT_JIS),
            ),
            ("text/html", None),
            ("text/html; charset=not-a-charset", None),
        ];
        for (header, expected) in cases {
            assert_eq!(charset_from_content_type(header), expected, "{header}");
        }
    }

    #[test]
    fn decodes_with_declared_charset_or_utf8_fallback() {
        assert_eq!(
            decode_text(b"caf\xe9", Some(encoding_rs::WINDOWS_1252)),
            "café"
        );
        assert_eq!(decode_text("café".as_bytes(), None), "café");
        assert_eq!(decode_text(b"caf\xe9", None), "caf\u{FFFD}");
    }

    /// `head` followed by `text` encoded as `encoding`.
    fn encoded(head: &str, text: &str, encoding: &'static Encoding) -> Vec<u8> {
        let mut body = head.as_bytes().to_vec();
        body.extend_from_slice(&encoding.encode(text).0);
        body
    }

    #[test]
    fn decodes_with_meta_charset_when_header_has_none() {
        let cases = [
            (
                "<meta charset=\"Shift_JIS\">",
                "日本語のページ",
                encoding_rs::SHIFT_JIS,
            ),
            (
                "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=windows-1251\">",
                "Привет, мир",
                encoding_rs::WINDOWS_1251,
            ),
            ("<META CHARSET=SHIFT_JIS>", "日本語", encoding_rs::SHIFT_JIS),
        ];
        for (head, text, encoding) in cases {
            let body = encoded(head, text, encoding);
            assert_eq!(decode_text(&body, None), format!("{head}{text}"), "{head}");
        }
    }

    #[test]
    fn ignores_meta_charset_past_the_first_1024_bytes() {
        let head = format!("<p>{}</p><meta charset=\"shift_jis\">", "a".repeat(1024));
        let body = encoded(&head, "日本語", encoding_rs::SHIFT_JIS);
        assert_eq!(prescan_meta_charset(&body), None);
        assert!(decode_text(&body, None).contains('\u{FFFD}'));
        // A tag cut off by the limit is ignored too.
        let cut = format!("{}<meta charset=\"shift_jis\">", "a".repeat(1010));
        assert_eq!(prescan_meta_charset(cut.as_bytes()), None);
    }

    #[test]
    fn header_charset_wins_over_meta_charset() {
        let body = b"<meta charset=\"shift_jis\">caf\xe9";
        assert_eq!(
            decode_text(body, Some(encoding_rs::WINDOWS_1252)),
            "<meta charset=\"shift_jis\">café"
        );
    }

    #[test]
    fn byte_order_mark_wins_over_meta_charset() {
        let body = "\u{FEFF}<meta charset=\"windows-1252\">café".as_bytes();
        assert_eq!(
            decode_text(body, None),
            "<meta charset=\"windows-1252\">café"
        );
    }

    #[test]
    fn prescan_follows_the_whatwg_rules() {
        use encoding_rs::{SHIFT_JIS, WINDOWS_1251};
        let cases: [(&str, Option<&'static Encoding>); 17] = [
            ("<meta charset=\"shift_jis\">", Some(SHIFT_JIS)),
            ("<meta charset='shift_jis'/>", Some(SHIFT_JIS)),
            ("<meta/charset=shift_jis>", Some(SHIFT_JIS)),
            ("<!-- <meta charset=\"shift_jis\"> -->", None),
            ("<!--> <meta charset=\"shift_jis\">", Some(SHIFT_JIS)),
            // `content` needs `http-equiv="Content-Type"`.
            ("<meta content=\"text/html; charset=windows-1251\">", None),
            (
                "<meta content='text/html;charset = \"windows-1251\"' http-equiv=content-type>",
                Some(WINDOWS_1251),
            ),
            ("<meta charset=\"utf-16le\">", Some(UTF_8)),
            ("<meta charset=\"UTF-16\">", Some(UTF_8)),
            ("<meta charset=\"x-user-defined\">", Some(WINDOWS_1252)),
            ("<meta charset=\"not-a-charset\">", None),
            ("<div title='<meta charset=shift_jis>'>", None),
            (
                "<meta charset=\"shift_jis\" charset=\"windows-1251\">",
                Some(SHIFT_JIS),
            ),
            ("<metadata charset=\"shift_jis\">", None),
            ("<meta charset=\"shift_jis", None),
            ("<meta name=x><p>text</p>", None),
            // An unknown charset leaves room for a later content declaration.
            (
                "<meta charset=bogus http-equiv=content-type content='charset=windows-1251'>",
                Some(WINDOWS_1251),
            ),
        ];
        for (html, expected) in cases {
            assert_eq!(prescan_meta_charset(html.as_bytes()), expected, "{html}");
        }
    }

    #[test]
    fn prescan_never_panics_on_truncated_input() {
        let html = b"<!doctype html><!-- c --><html lang='ja'><meta http-equiv=\"content-type\" content=\"text/html; charset='shift_jis'\"><?pi?></p>";
        for end in 0..=html.len() {
            let _ = prescan_meta_charset(&html[..end]);
        }
        assert_eq!(prescan_meta_charset(html), Some(encoding_rs::SHIFT_JIS));
    }

    #[test]
    fn extracts_charset_from_content_values() {
        let cases: [(&[u8], Option<&[u8]>); 6] = [
            (b"text/html; charset=koi8-r", Some(b"koi8-r")),
            (b"text/html; CHARSET = 'koi8-r' ; x", Some(b"koi8-r")),
            (b"charsetx; charset=big5;", Some(b"big5")),
            (b"text/html; charset=\"koi8-r", None),
            (b"text/html; charset=", None),
            (b"text/html", None),
        ];
        for (value, expected) in cases {
            assert_eq!(extract_charset_from_content(value), expected);
        }
    }

    fn page(raw: &str) -> Pending {
        Pending::new(3, Url::parse(raw).expect("test URL"))
    }

    #[test]
    fn follows_relative_and_absolute_redirects() {
        let cases = [
            ("/b?x=1", "example.com", "https://example.com/b?x=1"),
            ("c", "example.com", "https://example.com/a/c"),
            (
                "//Other.example./d",
                "other.example",
                "https://other.example./d",
            ),
            (
                "http://other.example:8080/",
                "other.example",
                "http://other.example:8080/",
            ),
            ("/a/c#part", "example.com", "https://example.com/a/c#part"),
        ];
        for (location, host, url) in cases {
            let (actual_host, next) =
                follow_redirect(page("https://example.com/a/b"), location).expect(location);
            assert_eq!(actual_host, host, "{location}");
            assert_eq!(next.url.as_str(), url, "{location}");
            assert_eq!((next.index, next.hops, next.seen.len()), (3, 1, 2));
        }
    }

    #[test]
    fn percent_encodes_non_ascii_redirect_targets() {
        let (host, next) =
            follow_redirect(page("https://example.com/a"), "/caf\u{e9}").expect("UTF-8 Location");
        assert_eq!(host, "example.com");
        assert_eq!(next.url.as_str(), "https://example.com/caf%C3%A9");
    }

    #[test]
    fn stops_redirect_loops_including_fragment_only_changes() {
        let start = page("https://example.com/a#top");
        assert_eq!(
            follow_redirect(start, "/a#other").map(|(h, _)| h),
            Err("redirect loop")
        );
        let (_, next) = follow_redirect(page("https://example.com/a"), "/b").expect("first hop");
        assert_eq!(
            follow_redirect(next, "https://example.com/a").map(|(h, _)| h),
            Err("redirect loop")
        );
    }

    #[test]
    fn caps_redirect_hops() {
        let mut current = page("https://example.com/0");
        for hop in 1..=MAX_REDIRECTS {
            current = follow_redirect(current, &format!("/{hop}"))
                .expect("within the cap")
                .1;
        }
        assert_eq!(current.hops, MAX_REDIRECTS);
        assert_eq!(
            follow_redirect(current, "/next").map(|(h, _)| h),
            Err("too many redirects")
        );
    }

    #[test]
    fn rejects_disallowed_redirect_targets() {
        let cases = [
            "ftp://example.com/file",
            "file:///etc/hosts",
            "https://user@example.com/",
            "https://example.com:0/",
            "http://[::1",
            "mailto:someone@example.com",
        ];
        for location in cases {
            assert!(
                follow_redirect(page("https://example.com/a"), location).is_err(),
                "{location:?} should be rejected"
            );
        }
    }

    #[test]
    fn parses_retry_after_seconds_and_http_dates() {
        // 1994-11-06T08:49:37Z
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(784_111_777);
        let cases = [
            ("5", Some(Duration::from_secs(5))),
            ("0", Some(Duration::ZERO)),
            (" 120 ", Some(Duration::from_secs(120))),
            (
                "99999999999999999999999",
                Some(Duration::from_secs(u64::MAX)),
            ),
            (
                "Sun, 06 Nov 1994 08:50:07 GMT",
                Some(Duration::from_secs(30)),
            ),
            (
                "Mon, 07 Nov 1994 08:49:37 GMT",
                Some(Duration::from_secs(86_400)),
            ),
            ("Sun, 06 Nov 1994 08:49:37 GMT", Some(Duration::ZERO)),
            ("Sat, 05 Nov 1994 08:49:37 GMT", Some(Duration::ZERO)),
            (
                "Tue, 01 Mar 2000 00:00:00 GMT",
                Some(Duration::from_secs(951_868_800 - 784_111_777)),
            ),
            ("", None),
            ("soon", None),
            ("-5", None),
            ("1.5", None),
            ("Sun, 06 Nov 1994 08:49:37 UTC", None),
            ("Sun, 31 Feb 1994 08:49:37 GMT", None),
            ("Sun, 6 Nov 1994 08:49:37 GMT", None),
            ("Sunday, 06-Nov-94 08:49:37 GMT", None),
            ("Sun Nov  6 08:49:37 1994", None),
        ];
        for (value, expected) in cases {
            assert_eq!(retry_after(value, now), expected, "{value:?}");
        }
    }
}
