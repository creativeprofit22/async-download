//! Behavioural tests against a small local HTTP server; no public network needed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use public_page_download::{Limits, MAX_BODY_BYTES, download_texts, download_texts_with_limits};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Default)]
struct Counters {
    requests: AtomicUsize,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
    per_host: [AtomicUsize; 2],
    per_host_peak: [AtomicUsize; 2],
    /// `(host slot, accept time, path)` for every request, in the order their
    /// headers were read; sort by time before comparing neighbours.
    starts: Mutex<Vec<(usize, Instant, String)>>,
    hits: Mutex<HashMap<String, usize>>,
}

impl Counters {
    /// Accept times of requests to one host slot, oldest first.
    fn starts_for(&self, slot: usize) -> Vec<(Instant, String)> {
        let mut starts: Vec<(Instant, String)> = self
            .starts
            .lock()
            .expect("starts lock")
            .iter()
            .filter(|(s, _, _)| *s == slot)
            .map(|(_, at, path)| (*at, path.clone()))
            .collect();
        starts.sort_by_key(|(at, _)| *at);
        starts
    }
}

/// Host slot 0 in [`Counters`]; slot 1 is `localhost`.
const HOST_A: &str = "127.0.0.1";
const HOST_B: &str = "localhost";
/// Allowance for connection setup and timer jitter when checking start gaps. The
/// server stamps a request when it accepts the connection, which is after the
/// client connected, and the first connection to a host name also includes name
/// resolution. Measured on Windows with the CPU loaded: gaps came up at most
/// 26 ms short of the interval, and usually under 12 ms.
const TOLERANCE: Duration = Duration::from_millis(50);

fn raise_peak(peak: &AtomicUsize, value: usize) {
    peak.fetch_max(value, Ordering::SeqCst);
}

async fn start_server() -> (u16, Arc<Counters>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let port = listener.local_addr().expect("local address").port();
    let counters = Arc::new(Counters::default());
    serve(listener, Arc::clone(&counters));
    // "localhost" may resolve to IPv6 first; listening there too avoids a slow
    // refused-connection fallback that would skew the overlap measurements.
    if let Ok(ipv6) = TcpListener::bind(("::1", port)).await {
        serve(ipv6, Arc::clone(&counters));
    }
    (port, counters)
}

fn serve(listener: TcpListener, counters: Arc<Counters>) {
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            // Stamp the start at accept, before reading headers, so the gap the
            // tests measure is not skewed by how long a request took to arrive.
            let accepted = Instant::now();
            tokio::spawn(handle(stream, accepted, Arc::clone(&counters)));
        }
    });
}

async fn handle(mut stream: TcpStream, accepted: Instant, counters: Arc<Counters>) {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1024];
    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
        match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(n) => request.extend_from_slice(&buffer[..n]),
        }
    }
    let request = String::from_utf8_lossy(&request).into_owned();
    let path = request.split_whitespace().nth(1).unwrap_or("/").to_owned();
    let host_slot = usize::from(request.to_ascii_lowercase().contains("host: localhost"));

    counters
        .starts
        .lock()
        .expect("starts lock")
        .push((host_slot, accepted, path.clone()));
    let hit = {
        let mut hits = counters.hits.lock().expect("hits lock");
        let count = hits.entry(path.clone()).or_insert(0);
        *count += 1;
        *count
    };
    counters.requests.fetch_add(1, Ordering::SeqCst);
    let now = counters.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    raise_peak(&counters.peak, now);
    let host_now = counters.per_host[host_slot].fetch_add(1, Ordering::SeqCst) + 1;
    raise_peak(&counters.per_host_peak[host_slot], host_now);

    respond(&mut stream, &path, hit).await;

    counters.per_host[host_slot].fetch_sub(1, Ordering::SeqCst);
    counters.in_flight.fetch_sub(1, Ordering::SeqCst);
}

/// `hit` counts requests to this exact path so far, including this one.
async fn respond(stream: &mut TcpStream, path: &str, hit: usize) {
    let ok = |content_type: &str, body: &[u8]| {
        let mut out = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        out.extend_from_slice(body);
        out
    };
    let redirect = |status: &str, location: Option<&str>| {
        let location = location.map_or(String::new(), |l| format!("Location: {l}\r\n"));
        format!(
            "HTTP/1.1 {status} Redirect\r\n{location}Content-Length: 0\r\nConnection: close\r\n\r\n"
        )
        .into_bytes()
    };
    let reply = if let Some(rest) = path.strip_prefix("/hop/") {
        // "/hop/<n>/<label>": n > 0 redirects (relative) to "/hop/<n-1>/<label>";
        // n = 0 answers "<label>".
        let (n, label) = rest.split_once('/').unwrap_or((rest, ""));
        match n.parse::<usize>().unwrap_or(0) {
            0 => ok("text/plain; charset=utf-8", label.as_bytes()),
            n => redirect("302", Some(&format!("/hop/{}/{label}", n - 1))),
        }
    } else if let Some(rest) = path.strip_prefix("/redirect-to/") {
        // "/redirect-to/<status>?<location>" answers <status> with that Location;
        // an empty query sends no Location at all.
        let (status, location) = rest.split_once('?').unwrap_or((rest, ""));
        redirect(status, Some(location).filter(|l| !l.is_empty()))
    } else if let Some(rest) = path.strip_prefix("/delay/") {
        // "/delay/<ms>/<label>" answers "<label>" after <ms> milliseconds.
        let (ms, label) = rest.split_once('/').unwrap_or((rest, ""));
        tokio::time::sleep(Duration::from_millis(ms.parse().unwrap_or(0))).await;
        ok("text/plain; charset=utf-8", label.as_bytes())
    } else if let Some(rest) = path.strip_prefix("/slowdown/") {
        // "/slowdown/<status>/<label>": first hit asks for a 1 s pause, then "<label>".
        let (status, label) = rest.split_once('/').unwrap_or((rest, ""));
        if hit == 1 {
            format!(
                "HTTP/1.1 {status} Slow Down\r\nRetry-After: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .into_bytes()
        } else {
            ok("text/plain; charset=utf-8", label.as_bytes())
        }
    } else {
        match path {
            "/latin1" => ok("text/plain; charset=ISO-8859-1", b"caf\xe9"),
            "/nocharset" => ok("text/plain", "café".as_bytes()),
            "/status500" => b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
            "/always-429" => b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
            "/long-429" => b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 120\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
            "/redirect" => b"HTTP/1.1 302 Found\r\nLocation: /latin1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
            // Raw UTF-8 bytes in Location, as some servers send them.
            "/utf8-redirect" => b"HTTP/1.1 302 Found\r\nLocation: /hop/0/caf\xC3\xA9\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
            "/loop" => redirect("302", Some("/loop")),
            "/loop-a" => redirect("302", Some("/loop-b")),
            "/loop-b" => redirect("302", Some("/loop-a")),
            "/declared-big" => format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                MAX_BODY_BYTES + 1
            )
            .into_bytes(),
            "/undeclared-big" => {
                // No Content-Length: the body ends when the connection closes.
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n").await;
                let chunk = vec![b'a'; 64 * 1024];
                for _ in 0..(MAX_BODY_BYTES / chunk.len() + 2) {
                    if stream.write_all(&chunk).await.is_err() {
                        return;
                    }
                }
                return;
            }
            "/stall" => {
                let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc").await;
                tokio::time::sleep(Duration::from_secs(60)).await;
                return;
            }
            _ => b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
        }
    };
    let _ = stream.write_all(&reply).await;
    let _ = stream.shutdown().await;
}

#[tokio::test]
async fn results_follow_input_order_and_failures_stay_isolated() {
    let (port, _) = start_server().await;
    let base = format!("http://127.0.0.1:{port}");
    let urls = vec![
        format!("{base}/delay/300/slow"),
        format!("{base}/status500"),
        "not a url".to_owned(),
        format!("{base}/delay/0/fast"),
        "ftp://127.0.0.1/file".to_owned(),
        format!("{base}/redirect"),
        "http://127.0.0.1:1/".to_owned(),
        format!("{base}/latin1"),
    ];

    let results = download_texts(&urls, 4).await;

    let expected = [
        Some("slow"),
        None,
        None,
        Some("fast"),
        None,
        Some("café"),
        None,
        Some("café"),
    ];
    let actual: Vec<Option<&str>> = results.iter().map(Option::as_deref).collect();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn global_and_per_host_limits_hold() {
    let (port, counters) = start_server().await;
    // Two host names for one server give two per-host groups.
    let urls: Vec<String> = (0..12)
        .map(|i| {
            let host = if i % 2 == 0 { "127.0.0.1" } else { "localhost" };
            format!("http://{host}:{port}/delay/150/{i}")
        })
        .collect();

    let results = download_texts_with_limits(
        &urls,
        Limits {
            max_in_flight: 3,
            per_host: 2,
            // This test measures overlap, which pacing would reduce.
            min_interval: Duration::ZERO,
        },
    )
    .await;

    let expected: Vec<Option<String>> = (0..12).map(|i| Some(i.to_string())).collect();
    assert_eq!(results, expected);
    assert!(
        counters.peak.load(Ordering::SeqCst) <= 3,
        "global limit exceeded"
    );
    assert_eq!(
        counters.peak.load(Ordering::SeqCst),
        3,
        "requests should overlap up to the limit"
    );
    for slot in &counters.per_host_peak {
        assert!(slot.load(Ordering::SeqCst) <= 2, "per-host limit exceeded");
    }
}

#[tokio::test]
async fn oversized_bodies_are_refused_not_truncated() {
    let (port, _) = start_server().await;
    let base = format!("http://127.0.0.1:{port}");
    let urls = [
        format!("{base}/declared-big"),
        format!("{base}/undeclared-big"),
        format!("{base}/nocharset"),
    ];

    let results = download_texts(&urls, 3).await;

    assert_eq!(results, vec![None, None, Some("café".to_owned())]);
}

#[tokio::test]
async fn a_stalled_page_does_not_delay_the_others() {
    let (port, _) = start_server().await;
    let base = format!("http://127.0.0.1:{port}");
    let urls = [
        format!("{base}/stall"),
        format!("{base}/delay/0/a"),
        format!("{base}/delay/0/b"),
    ];
    let started = Instant::now();

    let results = download_texts(&urls, 3).await;

    assert_eq!(
        results,
        vec![None, Some("a".to_owned()), Some("b".to_owned())]
    );
    // The stalled read ends at the 5 s read timeout, well before the 60 s stall.
    assert!(started.elapsed() < Duration::from_secs(15));
}

#[tokio::test]
async fn zero_limits_send_nothing() {
    let (port, counters) = start_server().await;
    let urls = [format!("http://127.0.0.1:{port}/delay/0/x")];

    let results = download_texts_with_limits(
        &urls,
        Limits {
            max_in_flight: 0,
            per_host: 1,
            min_interval: Duration::ZERO,
        },
    )
    .await;
    let also = download_texts_with_limits(
        &urls,
        Limits {
            max_in_flight: 1,
            per_host: 0,
            min_interval: Duration::ZERO,
        },
    )
    .await;

    assert_eq!(results, vec![None]);
    assert_eq!(also, vec![None]);
    assert_eq!(counters.requests.load(Ordering::SeqCst), 0);
}

fn assert_spaced(starts: &[(Instant, String)], interval: Duration) {
    for pair in starts.windows(2) {
        let gap = pair[1].0.duration_since(pair[0].0);
        assert!(
            gap + TOLERANCE >= interval,
            "{} started {gap:?} after {}",
            pair[1].1,
            pair[0].1
        );
    }
}

#[tokio::test]
async fn request_starts_to_one_host_are_spaced() {
    // One host: consecutive starts are at least the interval apart even though
    // pages answer at once and two may run together.
    let (port, counters) = start_server().await;
    let interval = Duration::from_millis(200);
    let urls: Vec<String> = (0..5)
        .map(|i| format!("http://{HOST_A}:{port}/delay/0/{i}"))
        .collect();

    let results = download_texts_with_limits(
        &urls,
        Limits {
            max_in_flight: 4,
            per_host: 2,
            min_interval: interval,
        },
    )
    .await;

    let expected: Vec<Option<String>> = (0..5).map(|i| Some(i.to_string())).collect();
    assert_eq!(results, expected);
    let starts = counters.starts_for(0);
    assert_eq!(starts.len(), 5);
    assert_spaced(&starts, interval);

    // Two hosts, one global slot: while one host waits for its next start, the
    // other uses the slot, so a paced host holds no global slot.
    let (port, counters) = start_server().await;
    let interval = Duration::from_millis(300);
    let urls: Vec<String> = (0..6)
        .map(|i| {
            let host = if i % 2 == 0 { HOST_A } else { HOST_B };
            format!("http://{host}:{port}/delay/0/{i}")
        })
        .collect();

    let results = download_texts_with_limits(
        &urls,
        Limits {
            max_in_flight: 1,
            per_host: 1,
            min_interval: interval,
        },
    )
    .await;

    let expected: Vec<Option<String>> = (0..6).map(|i| Some(i.to_string())).collect();
    assert_eq!(results, expected);
    let a = counters.starts_for(0);
    let b = counters.starts_for(1);
    assert_eq!((a.len(), b.len()), (3, 3));
    assert_spaced(&a, interval);
    assert_spaced(&b, interval);
    for pair in a.windows(2) {
        assert!(
            b.iter().any(|(at, _)| *at > pair[0].0 && *at < pair[1].0),
            "no {HOST_B} request started between {} and {}",
            pair[0].1,
            pair[1].1
        );
    }
}

#[tokio::test]
async fn a_slow_down_response_pauses_only_that_host() {
    for status in [429, 503] {
        let (port, counters) = start_server().await;
        let mut urls = vec![
            format!("http://{HOST_A}:{port}/slowdown/{status}/a"),
            format!("http://{HOST_A}:{port}/delay/0/a2"),
        ];
        urls.extend((0..4).map(|i| format!("http://{HOST_B}:{port}/delay/0/b{i}")));

        let results = download_texts_with_limits(
            &urls,
            Limits {
                max_in_flight: 4,
                per_host: 1,
                min_interval: Duration::from_millis(100),
            },
        )
        .await;

        let expected = ["a", "a2", "b0", "b1", "b2", "b3"].map(|s| Some(s.to_owned()));
        assert_eq!(results, expected, "status {status}");
        let a = counters.starts_for(0);
        let paths: Vec<&str> = a.iter().map(|(_, path)| path.as_str()).collect();
        let slowdown = format!("/slowdown/{status}/a");
        assert_eq!(
            paths,
            [slowdown.as_str(), slowdown.as_str(), "/delay/0/a2"],
            "the paused page is retried first"
        );
        // Retry-After: 1 is counted from the answer, so the pause ends at least
        // one second after the first request started.
        let pause_ends = a[0].0 + Duration::from_secs(1);
        for (at, path) in &a[1..] {
            assert!(
                *at + TOLERANCE >= pause_ends,
                "{path} started during the pause"
            );
        }
        for (at, path) in counters.starts_for(1) {
            assert!(at < pause_ends, "{path} on the other host was delayed");
        }
    }
}

#[tokio::test]
async fn a_page_is_retried_only_once() {
    let (port, counters) = start_server().await;
    let urls = [format!("http://{HOST_A}:{port}/always-429")];
    let limits = Limits {
        min_interval: Duration::ZERO,
        ..Limits::default()
    };

    let results = download_texts_with_limits(&urls, limits).await;

    assert_eq!(results, vec![None]);
    assert_eq!(counters.requests.load(Ordering::SeqCst), 2);

    // A pause longer than the cap is honoured without a retry.
    let (port, counters) = start_server().await;
    let urls = [format!("http://{HOST_A}:{port}/long-429")];
    let started = Instant::now();

    let results = download_texts_with_limits(&urls, limits).await;

    assert_eq!(results, vec![None]);
    assert_eq!(counters.requests.load(Ordering::SeqCst), 1);
    assert!(started.elapsed() < Duration::from_secs(10));
}

fn no_pacing(max_in_flight: usize, per_host: usize) -> Limits {
    Limits {
        max_in_flight,
        per_host,
        min_interval: Duration::ZERO,
    }
}

#[tokio::test]
async fn same_host_redirects_are_followed_and_paced() {
    let (port, counters) = start_server().await;
    let base = format!("http://{HOST_A}:{port}");
    let urls = [
        format!("{base}/redirect"),
        format!("{base}/redirect-to/301?/delay/0/s301"),
        format!("{base}/redirect-to/303?/delay/0/s303"),
        format!("{base}/redirect-to/307?/delay/0/s307"),
        format!("{base}/redirect-to/308?/delay/0/s308"),
        format!("{base}/hop/5/five"),
        format!("{base}/hop/6/six"),
        format!("{base}/delay/0/plain"),
    ];
    let interval = Duration::from_millis(100);

    let results = download_texts_with_limits(
        &urls,
        Limits {
            max_in_flight: 4,
            per_host: 2,
            min_interval: interval,
        },
    )
    .await;

    let expected = [
        Some("café"),
        Some("s301"),
        Some("s303"),
        Some("s307"),
        Some("s308"),
        Some("five"),
        None,
        Some("plain"),
    ];
    let actual: Vec<Option<&str>> = results.iter().map(Option::as_deref).collect();
    assert_eq!(actual, expected);
    let hits = counters.hits.lock().expect("hits lock").clone();
    assert_eq!(hits.get("/hop/0/five"), Some(&1), "five hops are followed");
    assert_eq!(
        hits.get("/hop/1/six"),
        Some(&1),
        "the fifth hop is requested"
    );
    assert_eq!(hits.get("/hop/0/six"), None, "a sixth hop is not followed");
    let starts = counters.starts_for(0);
    assert_eq!(starts.len(), 2 + 4 * 2 + 6 + 6 + 1);
    assert_spaced(&starts, interval);
}

#[tokio::test]
async fn cross_host_redirects_respect_the_target_host_limit() {
    let (port, counters) = start_server().await;
    let mut urls: Vec<String> = (0..6)
        .map(|i| {
            format!("http://{HOST_A}:{port}/redirect-to/302?http://{HOST_B}:{port}/delay/150/{i}")
        })
        .collect();
    urls.extend((6..8).map(|i| format!("http://{HOST_B}:{port}/delay/150/{i}")));

    let results = download_texts_with_limits(&urls, no_pacing(6, 1)).await;

    let expected: Vec<Option<String>> = (0..8).map(|i| Some(i.to_string())).collect();
    assert_eq!(results, expected);
    assert_eq!(counters.starts_for(1).len(), 8);
    assert_eq!(
        counters.per_host_peak[1].load(Ordering::SeqCst),
        1,
        "redirected requests exceeded the target host's limit"
    );

    // Redirected requests are paced on the target host too.
    let (port, counters) = start_server().await;
    let interval = Duration::from_millis(200);
    let mut urls: Vec<String> = (0..3)
        .map(|i| {
            format!("http://{HOST_A}:{port}/redirect-to/302?http://{HOST_B}:{port}/delay/0/{i}")
        })
        .collect();
    urls.push(format!("http://{HOST_B}:{port}/delay/0/3"));

    let results = download_texts_with_limits(
        &urls,
        Limits {
            max_in_flight: 4,
            per_host: 2,
            min_interval: interval,
        },
    )
    .await;

    let expected: Vec<Option<String>> = (0..4).map(|i| Some(i.to_string())).collect();
    assert_eq!(results, expected);
    let b = counters.starts_for(1);
    assert_eq!(b.len(), 4);
    assert_spaced(&b, interval);
}

#[tokio::test]
async fn raw_utf8_redirect_locations_are_followed() {
    let (port, counters) = start_server().await;
    let urls = [format!("http://{HOST_A}:{port}/utf8-redirect")];

    let results = download_texts_with_limits(&urls, no_pacing(4, 2)).await;

    assert_eq!(results, vec![Some("caf%C3%A9".to_owned())]);
    let hits = counters.hits.lock().expect("hits lock").clone();
    assert_eq!(hits.get("/hop/0/caf%C3%A9"), Some(&1));
}

#[tokio::test]
async fn redirect_loops_stop() {
    let (port, counters) = start_server().await;
    let urls = [
        format!("http://{HOST_A}:{port}/loop"),
        format!("http://{HOST_A}:{port}/loop-a"),
    ];

    let results = download_texts_with_limits(&urls, no_pacing(4, 2)).await;

    assert_eq!(results, vec![None, None]);
    let hits = counters.hits.lock().expect("hits lock").clone();
    for path in ["/loop", "/loop-a", "/loop-b"] {
        assert_eq!(hits.get(path), Some(&1), "{path}");
    }
}

#[tokio::test]
async fn disallowed_redirect_targets_give_none() {
    let (port, counters) = start_server().await;
    let base = format!("http://{HOST_A}:{port}");
    let urls = [
        format!("{base}/redirect-to/302?ftp://{HOST_A}/file"),
        format!("{base}/redirect-to/302?http://user@{HOST_A}:{port}/latin1"),
        format!("{base}/redirect-to/302?http://{HOST_A}:0/"),
        format!("{base}/redirect-to/302"),
        format!("{base}/delay/0/ok"),
    ];

    let results = download_texts_with_limits(&urls, no_pacing(4, 2)).await;

    assert_eq!(results, vec![None, None, None, None, Some("ok".to_owned())]);
    let hits = counters.hits.lock().expect("hits lock").clone();
    assert_eq!(hits.get("/latin1"), None);
    assert_eq!(counters.requests.load(Ordering::SeqCst), 5);
}
