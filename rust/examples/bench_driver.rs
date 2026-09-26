//! Benchmark driver: runs one batch and prints one JSON line per input URL.
//!
//! Usage: `bench_driver --concurrency N --per-host N --min-interval-ms N --urls FILE`
//!
//! `FILE` holds one URL per line (blank lines skipped). Timing is taken inside
//! the process: the clock starts immediately before the batch call, and each
//! page's `ms` is the time until that page's final debug event. Output lines are
//! `{"index","ms","status","bytes","collected","blocked"}`, then `{"wall_ms"}`.
//! Only numbers, booleans and `null` are printed. The Python driver
//! (`bench/py_driver.py`) prints the same protocol.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::process::ExitCode;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use public_page_download::{Limits, download_texts_with_limits};
use tracing::field::{Field, Visit};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

/// Target of the library's per-page events.
const TARGET: &str = "public_page_download";
/// Messages after which a page's slot no longer changes.
const FINAL_MESSAGES: [&str; 4] = [
    "page collected",
    "page not collected",
    "redirect not followed",
    "input rejected",
];
/// Case-insensitive markers of a challenge or block page. Kept identical to
/// `CHALLENGE_MARKERS` in `bench/settings.py` (checked by `test_bench.py`).
const CHALLENGE_MARKERS: [&str; 4] = ["just a moment", "cf-chl", "captcha", "access denied"];
/// Only bodies shorter than this are checked for challenge markers.
const CHALLENGE_MAX_BYTES: usize = 50_000;
/// Only the start of the body is searched.
const CHALLENGE_PREFIX_BYTES: usize = 4096;

/// Final event seen for one input: when it happened and the HTTP status, if any.
type Finals = Arc<Mutex<BTreeMap<u64, (Instant, Option<u64>)>>>;

/// Records the final per-page event of each input, keyed by input index.
struct Capture {
    finals: Finals,
}

#[derive(Default)]
struct Fields {
    message: String,
    index: Option<u64>,
    status: Option<u64>,
}

impl Visit for Fields {
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

impl<S: Subscriber> Layer<S> for Capture {
    fn enabled(&self, metadata: &Metadata<'_>, _: Context<'_, S>) -> bool {
        metadata.target().starts_with(TARGET)
    }

    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let at = Instant::now();
        let mut fields = Fields::default();
        event.record(&mut fields);
        let Some(index) = fields.index else { return };
        if FINAL_MESSAGES.contains(&fields.message.as_str()) {
            // The last final event wins (a redirect chain ends on its last hop).
            self.finals
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(index, (at, fields.status));
        }
    }
}

struct Args {
    limits: Limits,
    urls: Vec<String>,
}

fn parse_args() -> Result<Args, String> {
    let mut concurrency = None;
    let mut per_host = None;
    let mut interval = None;
    let mut path = None;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().ok_or(format!("{flag} needs a value"))?;
        let number = || {
            value
                .parse::<u64>()
                .map_err(|_| format!("{flag}: not a number"))
        };
        match flag.as_str() {
            "--concurrency" => concurrency = Some(number()?),
            "--per-host" => per_host = Some(number()?),
            "--min-interval-ms" => interval = Some(number()?),
            "--urls" => path = Some(value),
            _ => return Err(format!("unknown argument {flag}")),
        }
    }
    let size = |value: Option<u64>, name: &str| {
        value
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(format!("--{name} is required"))
    };
    let path = path.ok_or("--urls is required")?;
    let text = std::fs::read_to_string(&path).map_err(|error| format!("{path}: {error}"))?;
    Ok(Args {
        limits: Limits {
            max_in_flight: size(concurrency, "concurrency")?,
            per_host: size(per_host, "per-host")?,
            min_interval: Duration::from_millis(interval.ok_or("--min-interval-ms is required")?),
        },
        urls: text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect(),
    })
}

fn is_blocked(text: &str) -> bool {
    if text.len() >= CHALLENGE_MAX_BYTES {
        return false;
    }
    let prefix = &text.as_bytes()[..text.len().min(CHALLENGE_PREFIX_BYTES)];
    let lower = prefix.to_ascii_lowercase();
    CHALLENGE_MARKERS.iter().any(|marker| {
        lower
            .windows(marker.len())
            .any(|window| window == marker.as_bytes())
    })
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("bench_driver: {message}");
            return ExitCode::from(2);
        }
    };
    let finals = Finals::default();
    let capture = Capture {
        finals: Arc::clone(&finals),
    };
    // Global, not thread-local: page events are emitted on tokio worker threads.
    if tracing::subscriber::set_global_default(tracing_subscriber::registry().with(capture))
        .is_err()
    {
        eprintln!("bench_driver: a tracing subscriber is already installed");
        return ExitCode::FAILURE;
    }
    let started = Instant::now();
    let results = download_texts_with_limits(&args.urls, args.limits).await;
    let wall = started.elapsed();
    let finals = finals.lock().unwrap_or_else(PoisonError::into_inner);
    for (index, text) in results.iter().enumerate() {
        let (at, status) = u64::try_from(index)
            .ok()
            .and_then(|key| finals.get(&key).copied())
            .map_or((wall, None), |(at, status)| {
                (at.saturating_duration_since(started), status)
            });
        let status = status.map_or_else(|| "null".to_owned(), |code| code.to_string());
        println!(
            "{{\"index\":{index},\"ms\":{:.3},\"status\":{status},\"bytes\":{},\"collected\":{},\"blocked\":{}}}",
            millis(at),
            text.as_ref().map_or(0, String::len),
            text.is_some(),
            text.as_deref().is_some_and(is_blocked),
        );
    }
    println!("{{\"wall_ms\":{:.3}}}", millis(wall));
    ExitCode::SUCCESS
}
