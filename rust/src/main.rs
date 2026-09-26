//! Demonstration run against a few public pages. Prints sizes and timing, never page contents.
//!
//! Usage: public-page-download [--concurrency N] [--per-host N] [--min-interval-ms N]
//! [--verbose] [URL ...]

use std::process::ExitCode;
use std::time::{Duration, Instant};

use tracing_subscriber::filter::Targets;
use tracing_subscriber::prelude::*;

use public_page_download::{Limits, download_texts_with_limits};

// Pages chosen because they answer directly; redirects count as failures by design.
const DEMO_URLS: [&str; 8] = [
    "https://example.com/",
    "https://www.python.org/",
    "https://www.iana.org/domains/reserved",
    "https://rust-lang.org/",
    "https://en.wikipedia.org/wiki/Web_crawler",
    "https://en.wikipedia.org/wiki/Public_domain",
    "https://www.gnu.org/",
    "https://www.w3.org/",
];

struct Options {
    limits: Limits,
    verbose: bool,
    urls: Vec<String>,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Options, String> {
    let mut limits = Limits::default();
    let mut verbose = false;
    let mut urls = Vec::new();
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--concurrency" => limits.max_in_flight = parse_count(args.next(), &arg)?,
            "--per-host" => limits.per_host = parse_count(args.next(), &arg)?,
            "--min-interval-ms" => {
                limits.min_interval = args
                    .next()
                    .and_then(|v| v.parse::<u64>().ok())
                    .map(Duration::from_millis)
                    .ok_or_else(|| format!("{arg} needs a whole number of milliseconds"))?;
            }
            "--verbose" => verbose = true,
            _ => urls.push(arg),
        }
    }
    if urls.is_empty() {
        urls = DEMO_URLS.iter().map(|url| (*url).to_owned()).collect();
    }
    Ok(Options {
        limits,
        verbose,
        urls,
    })
}

fn parse_count(value: Option<String>, flag: &str) -> Result<usize, String> {
    value
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .ok_or_else(|| format!("{flag} needs a positive whole number"))
}

#[tokio::main]
async fn main() -> ExitCode {
    let options = match parse_args(std::env::args().skip(1)) {
        Ok(options) => options,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    if options.verbose {
        // Per-page outcome and elapsed time from this crate only, on stderr.
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_target(false)
                    .with_writer(std::io::stderr),
            )
            .with(Targets::new().with_target("public_page_download", tracing::Level::DEBUG))
            .init();
    }

    let started = Instant::now();
    let results = download_texts_with_limits(&options.urls, options.limits).await;
    let elapsed = started.elapsed().as_secs_f64();

    for (url, text) in options.urls.iter().zip(&results) {
        match text {
            Some(text) => println!("{url}: {} characters", text.chars().count()),
            None => println!("{url}: None"),
        }
    }
    let succeeded = results.iter().filter(|text| text.is_some()).count();
    let attempted = f64::from(u32::try_from(results.len()).unwrap_or(u32::MAX));
    println!(
        "{succeeded}/{} succeeded in {elapsed:.3}s; {:.2} URLs/s (limits: {} in flight, {} per host, {} ms between starts per host)",
        results.len(),
        attempted / elapsed.max(f64::EPSILON),
        options.limits.max_in_flight,
        options.limits.per_host.min(options.limits.max_in_flight),
        options.limits.min_interval.as_millis(),
    );
    ExitCode::SUCCESS
}
