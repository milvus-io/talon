//! Manual, remote small-object admission/read probe. Uses the production codec.
use clap::Parser;
use serde_json::json;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use talon_core::{Backend, BlockId, ObjectId, Version};
use talon_transport::{
    data::{
        encode_cached_block_put_header, encode_cached_request, encode_versioned_request,
        CachedBlockPutRequest, CachedRangeRequest, RangeRequest, VersionedRangeRequest,
    },
    frame::{Flags, FrameHeader},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Barrier,
};

#[derive(Parser, Clone)]
struct Args {
    #[arg(long)]
    addr: String,
    /// fill: cache admission; fetch: origin read once; verify: cached read once; read: timed cached reads.
    #[arg(long)]
    mode: String,
    #[arg(long, default_value_t = 4096)]
    size: u64,
    /// JSON array of [size, count], expanded and deterministically shuffled.
    #[arg(long)]
    histogram: Option<std::path::PathBuf>,
    #[arg(long, default_value_t = 1 << 20)]
    block_size: u32,
    /// Sample every Nth object from the same shuffled histogram for verification.
    #[arg(long, default_value_t = 1)]
    sample_step: u64,
    #[arg(long)]
    limit: Option<u64>,
    #[arg(long, default_value_t = 262144)]
    count: u64,
    #[arg(long, default_value_t = 0)]
    first: u64,
    #[arg(long, default_value_t = 128)]
    concurrency: usize,
    #[arg(long, default_value_t = 30)]
    seconds: u64,
    #[arg(long, default_value_t = 5)]
    warmup: u64,
    #[arg(long, default_value_t = 20261029)]
    seed: u64,
    #[arg(long, default_value = "objects")]
    prefix: String,
    #[arg(long, default_value = "probe")]
    name: String,
}
fn object(args: &Args, i: u64) -> ObjectId {
    ObjectId::new(
        Backend::Azure,
        "bench",
        format!("{}/{}/{:012}", args.prefix, args.size, i),
    )
}
fn packet(args: &Args, i: u64) -> Vec<u8> {
    let object = object(args, i);
    let version = Version::new("small-bench-v1");
    match args.mode.as_str() {
        "fill" => encode_cached_block_put_header(
            1,
            &CachedBlockPutRequest {
                block: BlockId::new(object, 0, args.block_size, version),
                object_len: args.size,
                body_len: args.size,
            },
        )
        .unwrap(),
        "fetch" => encode_versioned_request(
            1,
            &VersionedRangeRequest {
                request: RangeRequest {
                    object,
                    offset: 0,
                    len: args.size,
                },
                version,
                object_len: args.size,
            },
        )
        .unwrap(),
        _ => encode_cached_request(
            1,
            &CachedRangeRequest {
                object,
                version,
                offset: 0,
                len: args.size,
            },
        )
        .unwrap(),
    }
}
async fn exchange(
    socket: &mut TcpStream,
    args: &Args,
    i: u64,
    body: &mut [u8],
) -> Result<(), String> {
    let mut p = packet(args, i);
    if args.mode == "fill" {
        body.fill((i % 251) as u8);
        p.extend_from_slice(body);
    }
    socket.write_all(&p).await.map_err(|e| e.to_string())?;
    let mut raw = [0; 16];
    socket
        .read_exact(&mut raw)
        .await
        .map_err(|e| e.to_string())?;
    let h = FrameHeader::decode(&raw).map_err(|e| e.to_string())?;
    if h.request_id != 1 || h.length > 1 << 20 {
        return Err("invalid reply header".into());
    }
    if h.flags.contains(Flags::ERROR) {
        let mut error = vec![0; h.length as usize];
        socket
            .read_exact(&mut error)
            .await
            .map_err(|e| e.to_string())?;
        return Err(format!("worker error: {}", String::from_utf8_lossy(&error)));
    }
    let expected = if args.mode == "fill" {
        0
    } else {
        args.size as usize
    };
    if h.length as usize != expected {
        return Err(format!("reply size {} expected {}", h.length, expected));
    }
    if expected > 0 {
        socket.read_exact(body).await.map_err(|e| e.to_string())?;
        if body.iter().any(|b| *b != (i % 251) as u8) {
            return Err(format!("payload mismatch at object {i}"));
        }
    }
    Ok(())
}
#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let mut args = Args::parse();
    assert!(args.sample_step > 0);
    let mut sizes = Vec::new();
    if let Some(path) = &args.histogram {
        let histogram: Vec<(u64, usize)> =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        for (size, count) in histogram {
            assert!(size > 0 && size <= 1 << 20);
            sizes.extend(std::iter::repeat(size).take(count));
        }
        let mut rng = args.seed;
        for i in (1..sizes.len()).rev() {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            sizes.swap(i, (rng % (i as u64 + 1)) as usize);
        }
        assert!(args.first < sizes.len() as u64);
        args.count = (sizes.len() as u64 - args.first).div_ceil(args.sample_step);
        if let Some(limit) = args.limit {
            args.count = args.count.min(limit);
        }
    }
    let sizes = Arc::new(sizes);
    let completed = Arc::new(AtomicU64::new(0));
    assert!(matches!(
        args.mode.as_str(),
        "fill" | "fetch" | "verify" | "read"
    ));
    assert!(args.count > 0 && args.size > 0 && args.size <= 1 << 20 && args.concurrency > 0);
    let barrier = Arc::new(Barrier::new(args.concurrency + 1));
    let start = Arc::new(OnceLock::new());
    let next = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for n in 0..args.concurrency {
        let mut a = args.clone();
        let sizes = sizes.clone();
        let completed = completed.clone();
        let barrier = barrier.clone();
        let start = start.clone();
        let next = next.clone();
        tasks.push(tokio::spawn(async move {
            let socket = TcpStream::connect(&a.addr).await;
            barrier.wait().await;
            let mut socket = match socket {
                Ok(s) => s,
                Err(e) => return (0u64, 0u64, Vec::new(), vec![e.to_string()]),
            };
            socket.set_nodelay(true).unwrap();
            let start: Instant = *start.get().unwrap();
            let mut rng = a.seed.wrapping_add(n as u64 + 1);
            let mut body = vec![0; a.size as usize];
            let mut good = 0;
            let mut warm = 0;
            let mut lat = Vec::new();
            let mut errors = Vec::new();
            loop {
                let i = if a.mode == "read" {
                    if start.elapsed().as_secs_f64() >= (a.seconds + a.warmup) as f64 {
                        break;
                    }
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    rng % a.count
                } else {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= a.count {
                        break;
                    }
                    i
                };
                let object_index = a.first + i * a.sample_step;
                if !sizes.is_empty() {
                    a.size = sizes[object_index as usize];
                    body.resize(a.size as usize, 0);
                }
                let t = Instant::now();
                match tokio::time::timeout(
                    Duration::from_secs(30),
                    exchange(&mut socket, &a, object_index, &mut body),
                )
                .await
                {
                    Ok(Ok(())) => {
                        let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                        if done % 100_000 == 0 {
                            println!(
                                "{}",
                                json!({"progress":done,"elapsed":start.elapsed().as_secs_f64()})
                            );
                        }
                        let elapsed = start.elapsed().as_secs_f64();
                        if a.mode != "read"
                            || (elapsed >= a.warmup as f64
                                && elapsed < (a.warmup + a.seconds) as f64)
                        {
                            good += 1;
                            lat.push(t.elapsed().as_nanos() as u64);
                        } else {
                            warm += 1;
                        }
                    }
                    Ok(Err(e)) => {
                        errors.push(e);
                        break;
                    }
                    Err(_) => {
                        errors.push("request timeout".into());
                        break;
                    }
                }
            }
            (good, warm, lat, errors)
        }));
    }
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    start.set(Instant::now()).unwrap();
    barrier.wait().await;
    let mut good = 0;
    let mut warm = 0;
    let mut lat = Vec::new();
    let mut errors = Vec::new();
    let mut served = 0;
    for t in tasks {
        let (g, w, mut l, e) = t.await.unwrap();
        good += g;
        warm += w;
        served += usize::from(g > 0);
        lat.append(&mut l);
        errors.extend(e);
    }
    let elapsed = start.get().unwrap().elapsed().as_secs_f64();
    lat.sort_unstable();
    let pct = |q: f64| {
        if lat.is_empty() {
            0.0
        } else {
            lat[((lat.len() - 1) as f64 * q) as usize] as f64 / 1000.0
        }
    };
    let seconds = if args.mode == "read" {
        args.seconds as f64
    } else {
        elapsed
    };
    println!(
        "{}",
        json!({"name":args.name,"mode":args.mode,"size":args.size,"count":args.count,"concurrency":args.concurrency,"served":served,"started":wall,"elapsed":elapsed,"measured_seconds":seconds,"successes":good,"warmup_successes":warm,"qps":good as f64/seconds,"p50_us":pct(0.5),"p95_us":pct(0.95),"p99_us":pct(0.99),"p999_us":pct(0.999),"errors":errors.len(),"error_examples":errors.iter().take(4).collect::<Vec<_>>()})
    );
    if !errors.is_empty() || (args.mode != "read" && good != args.count) {
        std::process::exit(1);
    }
}
