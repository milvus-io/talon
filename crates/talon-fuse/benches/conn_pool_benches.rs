//! Real-socket connection-pool benchmark quantifying #181.
//!
//! Measures the payoff of reusing worker connections instead of dialing a fresh
//! TCP connection per fetch. Both benches issue the same number of sequential
//! range fetches against a real looping mock worker over loopback:
//!
//! - `fetch_fresh_per_call`: a brand-new `WorkerClient` (hence a fresh pool and
//!   a fresh `TcpStream::connect`) for every fetch — the pre-#181 behavior.
//! - `fetch_pooled`: one `WorkerClient` whose shared pool reuses an established
//!   connection across fetches — the handshake is paid once.
//!
//! The delta is the TCP-handshake cost the pool removes on the read hot path.
//! Runs without `/dev/fuse`.

use std::sync::Arc;

use talon_core::{Backend, ObjectId};
use talon_fuse::{ConnectionPool, WorkerClient};
use talon_transport::data::{decode_request, response_header_ok, RangeRequest};
use talon_transport::frame::{FrameHeader, HEADER_LEN};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn main() {
    divan::main();
}

/// Number of sequential fetches per benchmark iteration.
const FETCHES: usize = 20;
/// Small payload so the handshake, not the transfer, dominates.
const LEN: u64 = 256;

fn obj() -> ObjectId {
    ObjectId::new(Backend::Azure, "c", "obj")
}

/// A mock worker that loops, serving many sequential requests on the SAME
/// connection (like the real worker's `handle_conn`).
async fn spawn_worker() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            tokio::spawn(async move {
                loop {
                    let mut hdr = [0u8; HEADER_LEN];
                    if sock.read_exact(&mut hdr).await.is_err() {
                        return;
                    }
                    let header = FrameHeader::decode(&hdr).unwrap();
                    let mut body = vec![0u8; header.length as usize];
                    if sock.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    let mut full = hdr.to_vec();
                    full.extend_from_slice(&body);
                    let (_h, req): (_, RangeRequest) = decode_request(&full).unwrap();
                    let payload = vec![7u8; req.len as usize];
                    let mut out = response_header_ok(0, payload.len() as u32).to_vec();
                    out.extend_from_slice(&payload);
                    if sock.write_all(&out).await.is_err() {
                        return;
                    }
                    let _ = sock.flush().await;
                }
            });
        }
    });
    addr
}

/// Pre-#181 behavior: a fresh WorkerClient (fresh pool → fresh dial) per fetch.
#[divan::bench]
fn fetch_fresh_per_call(bencher: divan::Bencher) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let addr = rt.block_on(spawn_worker());
    let o = obj();
    bencher.bench(|| {
        rt.block_on(async {
            for _ in 0..FETCHES {
                // A new client each call has its own empty pool, so it always
                // dials a fresh connection — the old per-request-connect cost.
                let client = WorkerClient::new(addr.clone());
                let bytes = client.fetch_range(&o, 0, LEN).await.unwrap();
                assert_eq!(bytes.len(), LEN as usize);
            }
        });
    });
}

/// Post-#181: one shared client whose pool reuses the connection across fetches.
#[divan::bench]
fn fetch_pooled(bencher: divan::Bencher) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let addr = rt.block_on(spawn_worker());
    let o = obj();
    let client = WorkerClient::with_pool(addr.clone(), Arc::new(ConnectionPool::new()));
    bencher.bench(|| {
        rt.block_on(async {
            for _ in 0..FETCHES {
                let bytes = client.fetch_range(&o, 0, LEN).await.unwrap();
                assert_eq!(bytes.len(), LEN as usize);
            }
        });
    });
}
