//! The per-page debug events that `examples/bench_driver.rs` reads: message
//! text plus `index` and `status` fields on the library's target. Renaming any
//! of them must fail here, not only in a benchmark run.
//!
//! This file holds a single test so it runs in its own process: the subscriber
//! is installed globally, exactly as the driver does, so events emitted on
//! tokio worker threads are seen and no other test can race it.

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use public_page_download::{Limits, download_texts_with_limits};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::field::{Field, Visit};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

/// One library event, reduced to the fields the benchmark driver reads.
#[derive(Debug, Default, PartialEq)]
struct PageEvent {
    message: String,
    index: Option<u64>,
    status: Option<u64>,
}

impl Visit for PageEvent {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "index" => self.index = Some(value),
            "status" => self.status = Some(value),
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            value.clone_into(&mut self.message);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

/// Records every event on the library's target, in emission order.
struct Capture(Arc<Mutex<Vec<PageEvent>>>);

impl<S: Subscriber> Layer<S> for Capture {
    fn enabled(&self, metadata: &Metadata<'_>, _: Context<'_, S>) -> bool {
        metadata.target().starts_with("public_page_download")
    }

    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = PageEvent::default();
        event.record(&mut fields);
        self.0.lock().expect("events lock").push(fields);
    }
}

/// Loopback server: `/ok` answers 200 "ok", anything else 404.
async fn start_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let port = listener.local_addr().expect("local address").port();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut buffer).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => request.extend_from_slice(&buffer[..n]),
                    }
                }
                let reply: &[u8] = if request.starts_with(b"GET /ok ") {
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
                } else {
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                };
                let _ = stream.write_all(reply).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    port
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_page_events_carry_index_and_status() {
    let events = Arc::new(Mutex::new(Vec::new()));
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(Capture(Arc::clone(&events))),
    )
    .expect("no other subscriber in this test process");
    let port = start_server().await;
    let base = format!("http://127.0.0.1:{port}");
    let urls = [format!("{base}/ok"), format!("{base}/missing")];
    let limits = Limits {
        max_in_flight: 2,
        per_host: 2,
        min_interval: std::time::Duration::ZERO,
    };

    let results = download_texts_with_limits(&urls, limits).await;

    assert_eq!(results, [Some("ok".to_owned()), None]);
    let events = events.lock().expect("events lock");
    let final_for = |index: u64| {
        events
            .iter()
            .rfind(|event| {
                event.index == Some(index)
                    && ["page collected", "page not collected"].contains(&event.message.as_str())
            })
            .unwrap_or_else(|| panic!("no final event for input {index}: {events:?}"))
    };
    let expected = [(0, "page collected", 200), (1, "page not collected", 404)];
    for (index, message, status) in expected {
        assert_eq!(
            final_for(index),
            &PageEvent {
                message: message.to_owned(),
                index: Some(index),
                status: Some(status),
            }
        );
    }
}
