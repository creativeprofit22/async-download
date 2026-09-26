//! Behavioural tests against a small local HTTP server; no public network needed.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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
}

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
            tokio::spawn(handle(stream, Arc::clone(&counters)));
        }
    });
}

async fn handle(mut stream: TcpStream, counters: Arc<Counters>) {
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

    counters.requests.fetch_add(1, Ordering::SeqCst);
    let now = counters.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    raise_peak(&counters.peak, now);
    let host_now = counters.per_host[host_slot].fetch_add(1, Ordering::SeqCst) + 1;
    raise_peak(&counters.per_host_peak[host_slot], host_now);

    respond(&mut stream, &path).await;

    counters.per_host[host_slot].fetch_sub(1, Ordering::SeqCst);
    counters.in_flight.fetch_sub(1, Ordering::SeqCst);
}

async fn respond(stream: &mut TcpStream, path: &str) {
    let ok = |content_type: &str, body: &[u8]| {
        let mut out = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        out.extend_from_slice(body);
        out
    };
    let reply = if let Some(rest) = path.strip_prefix("/delay/") {
        // "/delay/<ms>/<label>" answers "<label>" after <ms> milliseconds.
        let (ms, label) = rest.split_once('/').unwrap_or((rest, ""));
        tokio::time::sleep(Duration::from_millis(ms.parse().unwrap_or(0))).await;
        ok("text/plain; charset=utf-8", label.as_bytes())
    } else {
        match path {
            "/latin1" => ok("text/plain; charset=ISO-8859-1", b"caf\xe9"),
            "/nocharset" => ok("text/plain", "café".as_bytes()),
            "/status500" => b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
            "/redirect" => b"HTTP/1.1 302 Found\r\nLocation: /latin1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
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
        None,
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
        },
    )
    .await;
    let also = download_texts_with_limits(
        &urls,
        Limits {
            max_in_flight: 1,
            per_host: 0,
        },
    )
    .await;

    assert_eq!(results, vec![None]);
    assert_eq!(also, vec![None]);
    assert_eq!(counters.requests.load(Ordering::SeqCst), 0);
}
