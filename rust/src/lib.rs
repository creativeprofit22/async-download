//! Bounded collection of public page text for research throughput measurements.
//!
//! The single entry point is [`download_texts`] (or [`download_texts_with_limits`]
//! for an explicit per-host limit). Results are aligned to the input list: each
//! slot holds the decoded page text, or `None` if that page could not be collected.
//! The batch never returns an error and never panics because of one page.
//!
//! Scope and assumptions:
//! - Inputs are public research URLs chosen by the operator. This is not a public
//!   URL-submission service, and no private-network filtering is applied.
//! - Out of scope, deliberately: proxies (system proxy settings are ignored),
//!   authenticated pages and cookies, robots.txt handling, and a host allowlist.
//!   Review the URL list and each site's usage policy before running a batch.
//! - Redirects are not followed; a redirect response counts as a failed page. This
//!   keeps every request on the host whose per-host limit was reserved for it.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use encoding_rs::{Encoding, UTF_8};
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT};
use reqwest::{Client, Response, Url, redirect};
use tokio::task::{Id, JoinSet};

/// Largest decoded body accepted, in bytes. Larger pages yield `None`, never a truncated text.
pub const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
/// Longest input URL accepted, in characters.
pub const MAX_URL_CHARS: usize = 8192;
/// Per-host limit used by [`download_texts`].
pub const DEFAULT_PER_HOST_LIMIT: usize = 2;

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
}

/// Download each URL's text with at most `concurrency` requests in flight and
/// at most [`DEFAULT_PER_HOST_LIMIT`] per host.
pub async fn download_texts<S: AsRef<str>>(urls: &[S], concurrency: usize) -> Vec<Option<String>> {
    download_texts_with_limits(
        urls,
        Limits {
            max_in_flight: concurrency,
            per_host: DEFAULT_PER_HOST_LIMIT,
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
pub async fn download_texts_with_limits<S: AsRef<str>>(
    urls: &[S],
    limits: Limits,
) -> Vec<Option<String>> {
    let mut results: Vec<Option<String>> = vec![None; urls.len()];
    if limits.max_in_flight == 0 || limits.per_host == 0 {
        return results;
    }
    let per_host = limits.per_host.min(limits.max_in_flight);

    // Group valid inputs by host; keep hosts in first-seen order so scheduling is deterministic.
    let mut queues: HashMap<String, VecDeque<(usize, Url)>> = HashMap::new();
    let mut ready: VecDeque<String> = VecDeque::new();
    for (index, raw) in urls.iter().enumerate() {
        match parse_public_url(raw.as_ref()) {
            Ok((host, url)) => queues
                .entry(host)
                .or_insert_with_key(|host| {
                    ready.push_back(host.clone());
                    VecDeque::new()
                })
                .push_back((index, url)),
            Err(reason) => tracing::debug!(index, reason, "input rejected"),
        }
    }
    if queues.is_empty() {
        return results;
    }

    let client = match build_client(per_host) {
        Ok(client) => client,
        Err(error) => {
            tracing::warn!(%error, "HTTP client could not be created");
            return results;
        }
    };

    // Dispatcher: a host sits in `ready` only while it has queued URLs and spare
    // per-host capacity, and a task is spawned only when a global slot is free.
    // So both limits hold by construction, at most `max_in_flight` tasks ever
    // exist (even for very long lists), and hosts are served round-robin.
    let mut active: HashMap<String, usize> = HashMap::new();
    let mut owners: HashMap<Id, (String, usize)> = HashMap::new();
    let mut tasks: JoinSet<Option<String>> = JoinSet::new();

    while !ready.is_empty() || !tasks.is_empty() {
        while tasks.len() < limits.max_in_flight {
            let Some(host) = ready.pop_front() else { break };
            let Some(queue) = queues.get_mut(&host) else {
                continue;
            };
            let Some((index, url)) = queue.pop_front() else {
                continue;
            };
            let in_flight = active.entry(host.clone()).or_insert(0);
            *in_flight += 1;
            if !queue.is_empty() && *in_flight < per_host {
                ready.push_back(host.clone());
            }
            let handle = tasks.spawn(fetch_text(client.clone(), url, index));
            owners.insert(handle.id(), (host, index));
        }

        let Some(joined) = tasks.join_next_with_id().await else {
            break;
        };
        let (id, text) = match joined {
            Ok((id, text)) => (id, text),
            // A panic inside one request is contained to that slot.
            Err(error) => {
                tracing::warn!(%error, "request task ended abnormally");
                (error.id(), None)
            }
        };
        let Some((host, index)) = owners.remove(&id) else {
            continue;
        };
        results[index] = text;
        let has_more = queues.get(&host).is_some_and(|queue| !queue.is_empty());
        if let Some(in_flight) = active.get_mut(&host) {
            // A host at its limit left `ready`; it returns now that a slot is free.
            if has_more && *in_flight == per_host {
                ready.push_back(host);
            }
            *in_flight -= 1;
        }
    }
    results
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

#[derive(Debug)]
enum FetchFailure {
    Transport(reqwest::Error),
    Status(u16),
    TooLarge,
}

impl std::fmt::Display for FetchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) if error.is_timeout() => f.write_str("timed out"),
            Self::Transport(error) if error.is_connect() => f.write_str("connection failed"),
            Self::Transport(error) => write!(f, "transfer failed: {error}"),
            Self::Status(code) => write!(f, "HTTP status {code}"),
            Self::TooLarge => write!(f, "body exceeds {MAX_BODY_BYTES} bytes"),
        }
    }
}

/// Fetch one page and reduce every failure to `None`, logging the reason and elapsed time.
async fn fetch_text(client: Client, url: Url, index: usize) -> Option<String> {
    let started = Instant::now();
    let outcome = fetch_body(&client, url).await;
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    match outcome {
        Ok(text) => {
            tracing::debug!(index, elapsed_ms, bytes = text.len(), "page collected");
            Some(text)
        }
        Err(failure) => {
            tracing::debug!(index, elapsed_ms, reason = %failure, "page not collected");
            None
        }
    }
}

async fn fetch_body(client: &Client, url: Url) -> Result<String, FetchFailure> {
    let response = client
        .get(url)
        .send()
        .await
        .map_err(FetchFailure::Transport)?;
    let status = response.status();
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
    let declared = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(charset_from_content_type);
    let body = read_capped(response).await?;
    Ok(decode_text(&body, declared))
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

/// Decode with the declared charset, falling back to UTF-8 when none is declared
/// or the label is unknown. A byte-order mark, when present, takes precedence (as
/// browsers do). Malformed sequences become U+FFFD rather than failing the page.
/// Decoding a body of at most 2 MiB is brief enough to run on the async worker.
fn decode_text(body: &[u8], declared: Option<&'static Encoding>) -> String {
    let (text, _, _) = declared.unwrap_or(UTF_8).decode(body);
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
}
