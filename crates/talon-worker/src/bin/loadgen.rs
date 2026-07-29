//! Concurrent load generator for the worker data plane (#291).
//!
//! Answers the question the Divan microbenchmark structurally cannot: how do the
//! two data planes compare when many connections are in flight at once?
//!
//! `dataplane_benches` drives one client serially, so it measures a
//! single-request latency floor. io_uring's advantage is amortising syscalls
//! across concurrent operations, and with one request in flight there is nothing
//! to amortise — the two runtimes measure the same there, which says nothing
//! about the regime a cache fleet actually runs in.
//!
//! This binary opens N connections, drives closed-loop range requests on each
//! for a fixed duration, and reports the **latency distribution** plus
//! server-side CPU and RSS. It is a manual tool for a known machine, not a CI
//! gate: shared runners are too noisy for absolute-time comparisons.
//!
//! # Usage
//!
//! ```sh
//! talon-loadgen --addr 127.0.0.1:7001 --conns 1,64,256,1024
//! ```
//!
//! Percentiles, not means: the difference between the runtimes lives in the
//! tail, and a mean hides exactly the effect being measured.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use talon_core::{Backend, ObjectId};
use talon_transport::data::{encode_request, RangeRequest};
use talon_transport::frame::{FrameHeader, HEADER_LEN};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Parser)]
#[command(
    name = "talon-loadgen",
    about = "Concurrent load generator for the Talon data plane"
)]
struct Args {
    /// Worker data-plane address to drive.
    #[arg(long, default_value = "127.0.0.1:7001")]
    addr: String,
    /// Connection counts to sweep, comma-separated.
    #[arg(long, default_value = "1,64,256,1024", value_delimiter = ',')]
    conns: Vec<usize>,
    /// Bytes requested per range read.
    #[arg(long, default_value_t = 65536)]
    range: u64,
    /// Measured seconds per connection count.
    #[arg(long, default_value_t = 10)]
    seconds: u64,
    /// Warmup seconds excluded from the recorded samples.
    ///
    /// The first requests of a run pay connection setup and a cold block cache;
    /// including them would report the miss path rather than the serve path.
    #[arg(long, default_value_t = 3)]
    warmup: u64,
    /// Object key to read.
    #[arg(long, default_value = "bench")]
    object: String,
    /// Object-store backend serving the target object.
    #[arg(long, default_value = "az")]
    backend: Backend,
    /// Container or bucket the object lives in.
    #[arg(long, default_value = "c")]
    container: String,
    /// Worker PID to sample CPU and RSS from. Skipped when unset.
    #[arg(long)]
    server_pid: Option<u32>,
    /// Emit JSON Lines instead of a table, for scripted comparison.
    #[arg(long)]
    json: bool,
}

/// One connection count's result.
struct Run {
    conns: usize,
    rps: f64,
    p50: f64,
    p90: f64,
    p99: f64,
    p999: f64,
    max: f64,
    samples: usize,
    cpu_percent: Option<f64>,
    rss_kb: Option<u64>,
}

/// Cumulative user+system jiffies for a process.
///
/// Fields are counted from the last `") "` because `/proc/<pid>/stat` puts the
/// executable name in parentheses and it may itself contain spaces.
fn proc_cpu_jiffies(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after = stat.rsplit_once(") ")?.1;
    let fields: Vec<&str> = after.split_whitespace().collect();
    // utime/stime are overall fields 14/15, i.e. indices 11/12 after "state".
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

fn proc_rss_kb(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
}

/// Drive one connection closed-loop until `stop`, returning post-warmup
/// latencies in nanoseconds.
async fn drive_one(
    addr: String,
    object: ObjectId,
    range: u64,
    warmup: Duration,
    stop: Arc<AtomicBool>,
    errors: Arc<AtomicU64>,
    offset_seed: u64,
) -> Vec<u64> {
    let mut sock = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(_) => {
            errors.fetch_add(1, Ordering::Relaxed);
            return Vec::new();
        }
    };
    sock.set_nodelay(true).ok();

    let mut latencies: Vec<u64> = Vec::with_capacity(1 << 14);
    let mut body = vec![0u8; range as usize];
    let mut header_buf = [0u8; HEADER_LEN];
    // Stagger offsets so connections do not all hammer one block region.
    let mut offset = (offset_seed * range) % (16 << 20);
    let started = Instant::now();
    let mut recording = false;

    while !stop.load(Ordering::Relaxed) {
        if !recording && started.elapsed() >= warmup {
            recording = true;
            latencies.clear();
        }
        let request = RangeRequest {
            object: object.clone(),
            offset,
            len: range,
        };
        let Ok(encoded) = encode_request(0, &request) else {
            break;
        };

        let t0 = Instant::now();
        if sock.write_all(&encoded).await.is_err() {
            errors.fetch_add(1, Ordering::Relaxed);
            break;
        }
        if sock.read_exact(&mut header_buf).await.is_err() {
            errors.fetch_add(1, Ordering::Relaxed);
            break;
        }
        let Ok(header) = FrameHeader::decode(&header_buf) else {
            errors.fetch_add(1, Ordering::Relaxed);
            break;
        };
        let len = header.length as usize;
        if len > body.len() {
            body.resize(len, 0);
        }
        if len > 0 && sock.read_exact(&mut body[..len]).await.is_err() {
            errors.fetch_add(1, Ordering::Relaxed);
            break;
        }
        // An ERROR-flagged response is a served request but not a served read;
        // count it so a misconfigured run cannot look like a fast one.
        if header.flags.contains(talon_transport::Flags::ERROR) {
            if errors.fetch_add(1, Ordering::Relaxed) == 0 {
                // Surface the first error verbatim: a misconfigured run is far
                // more likely than a server bug, and the message says which.
                eprintln!(
                    "first error response: {}",
                    String::from_utf8_lossy(&body[..len])
                );
            }
        } else if recording {
            latencies.push(t0.elapsed().as_nanos() as u64);
        }
        offset = (offset + range) % (16 << 20);
    }
    latencies
}

async fn run_one(args: &Args, conns: usize) -> Run {
    let object = ObjectId::new(args.backend, &args.container, &args.object);
    let stop = Arc::new(AtomicBool::new(false));
    let errors = Arc::new(AtomicU64::new(0));
    let warmup = Duration::from_secs(args.warmup);

    let mut tasks = Vec::with_capacity(conns);
    for i in 0..conns {
        tasks.push(tokio::spawn(drive_one(
            args.addr.clone(),
            object.clone(),
            args.range,
            warmup,
            Arc::clone(&stop),
            Arc::clone(&errors),
            i as u64,
        )));
    }

    // Sample server CPU across the measured window only, so warmup does not
    // inflate it.
    tokio::time::sleep(warmup).await;
    let cpu_before = args.server_pid.and_then(proc_cpu_jiffies);
    let measure_start = Instant::now();
    tokio::time::sleep(Duration::from_secs(args.seconds)).await;
    let measured = measure_start.elapsed();
    let cpu_after = args.server_pid.and_then(proc_cpu_jiffies);
    let rss_kb = args.server_pid.and_then(proc_rss_kb);
    stop.store(true, Ordering::Relaxed);

    let mut all: Vec<u64> = Vec::new();
    for t in tasks {
        if let Ok(v) = t.await {
            all.extend(v);
        }
    }
    all.sort_unstable();

    let pct = |q: f64| -> f64 {
        if all.is_empty() {
            return 0.0;
        }
        all[(((all.len() - 1) as f64) * q) as usize] as f64 / 1000.0
    };
    // USER_HZ, standard on Linux.
    const TICKS_PER_SEC: f64 = 100.0;
    let cpu_percent = match (cpu_before, cpu_after) {
        (Some(a), Some(b)) => {
            Some((b.saturating_sub(a) as f64 / TICKS_PER_SEC) / measured.as_secs_f64() * 100.0)
        }
        _ => None,
    };

    let errs = errors.load(Ordering::Relaxed);
    if errs > 0 {
        eprintln!("warning: {errs} errored requests at {conns} connections");
    }

    Run {
        conns,
        rps: all.len() as f64 / measured.as_secs_f64(),
        p50: pct(0.50),
        p90: pct(0.90),
        p99: pct(0.99),
        p999: pct(0.999),
        max: all.last().copied().unwrap_or(0) as f64 / 1000.0,
        samples: all.len(),
        cpu_percent,
        rss_kb,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if !args.json {
        println!(
            "target {} | {} B ranges | {}s measured after {}s warmup\n",
            args.addr, args.range, args.seconds, args.warmup
        );
        println!(
            "{:>6} {:>12} {:>9} {:>9} {:>9} {:>9} {:>10} {:>7} {:>8}",
            "conns", "rps", "p50 us", "p90 us", "p99 us", "p999 us", "max us", "cpu %", "rss MB"
        );
        println!("{}", "-".repeat(90));
    }

    for &conns in &args.conns {
        let run = run_one(&args, conns).await;
        if args.json {
            println!(
                r#"{{"conns":{},"rps":{:.0},"p50_us":{:.1},"p90_us":{:.1},"p99_us":{:.1},"p999_us":{:.1},"max_us":{:.1},"samples":{},"cpu_percent":{},"rss_kb":{}}}"#,
                run.conns,
                run.rps,
                run.p50,
                run.p90,
                run.p99,
                run.p999,
                run.max,
                run.samples,
                run.cpu_percent
                    .map(|c| format!("{c:.1}"))
                    .unwrap_or_else(|| "null".into()),
                run.rss_kb
                    .map(|r| r.to_string())
                    .unwrap_or_else(|| "null".into()),
            );
        } else {
            println!(
                "{:>6} {:>12.0} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>10.1} {:>7} {:>8}",
                run.conns,
                run.rps,
                run.p50,
                run.p90,
                run.p99,
                run.p999,
                run.max,
                run.cpu_percent
                    .map(|c| format!("{c:.0}"))
                    .unwrap_or_else(|| "-".into()),
                run.rss_kb
                    .map(|r| format!("{:.1}", r as f64 / 1024.0))
                    .unwrap_or_else(|| "-".into()),
            );
        }
        if run.samples == 0 {
            anyhow::bail!("no samples recorded at {conns} connections; is the worker serving?");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_azure_backend() {
        let args = Args::try_parse_from(["talon-loadgen"]).unwrap();

        assert_eq!(args.backend, Backend::Azure);
    }

    #[test]
    fn accepts_explicit_s3_backend() {
        let args = Args::try_parse_from(["talon-loadgen", "--backend", "s3"]).unwrap();

        assert_eq!(args.backend, Backend::S3);
    }
}
