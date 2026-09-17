//! Isolated probe: real WorkerClient TCP RPCs and request-scope microbenchmarks.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    hint::black_box,
    time::Instant,
};
use talon_core::{Backend, ObjectId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
thread_local! { static COUNT: Cell<bool> = const { Cell::new(false) }; static ALLOCS: Cell<u64> = const { Cell::new(0) }; static BYTES: Cell<u64> = const { Cell::new(0) }; }
struct Allocator;
fn allocated(n: usize) {
    if COUNT.try_with(Cell::get).unwrap_or(false) {
        ALLOCS.with(|c| c.set(c.get() + 1));
        BYTES.with(|c| c.set(c.get() + n as u64));
    }
}
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        allocated(l.size());
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        allocated(l.size());
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        allocated(n);
        unsafe { System.realloc(p, l, n) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
#[cfg(feature = "recording")]
#[derive(Debug)]
struct Discard;
#[cfg(feature = "recording")]
impl opentelemetry_sdk::trace::SpanExporter for Discard {
    fn export(
        &mut self,
        _: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send>,
    > {
        Box::pin(async { Ok(()) })
    }
}
#[inline(never)]
fn work(n: u64) -> u64 {
    black_box(n).wrapping_mul(0x517cc1b727220a95).rotate_left(7)
}
async fn measured(
    kind: &str,
    client: &talon_cache_client::WorkerClient,
    object: &ObjectId,
    bytes: u64,
) {
    #[cfg(feature = "instrumentation")]
    let op = talon_telemetry::Operation::new(
        "talon.read",
        "internal",
        talon_telemetry::TraceParent::Inherit,
    );
    let future = async {
        if kind == "rpc" {
            black_box(client.fetch_range(object, 0, bytes).await.unwrap());
        } else {
            for n in 0..2 {
                #[cfg(feature = "instrumentation")]
                let child = talon_telemetry::Operation::new(
                    "talon.rpc",
                    "client",
                    talon_telemetry::TraceParent::Inherit,
                );
                #[cfg(feature = "instrumentation")]
                child.in_scope(|| {
                    black_box(work(n));
                });
                #[cfg(not(feature = "instrumentation"))]
                {
                    black_box(work(n));
                }
                #[cfg(feature = "instrumentation")]
                child.outcome("success");
            }
        }
    };
    #[cfg(feature = "instrumentation")]
    {
        op.scope(future).await;
        op.outcome("success");
    }
    #[cfg(not(feature = "instrumentation"))]
    future.await;
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let kind = &args[1];
    let mode = &args[2];
    let count: usize = args[3].parse().unwrap();
    let bytes: u64 = args[4].parse().unwrap();
    #[cfg(feature = "instrumentation")]
    talon_telemetry::configure(talon_telemetry::Config {
        mode: match mode.as_str() {
            "off" => talon_telemetry::Mode::Off,
            "propagate" => talon_telemetry::Mode::Propagate,
            _ => talon_telemetry::Mode::Standard,
        },
        root_sample_ratio: match mode.as_str() {
            "unsampled" => 0.0,
            "one-percent" => 0.01,
            _ => 1.0,
        },
        v2_endpoints: if mode == "off" {
            vec![]
        } else {
            vec!["*".into()]
        },
        ..Default::default()
    })
    .unwrap();
    #[cfg(feature = "recording")]
    let (_provider, _dispatch_guard, dispatch) = {
        use opentelemetry::trace::TracerProvider;
        use tracing_subscriber::layer::SubscriberExt;
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(Discard)
            .with_max_attributes_per_span(48)
            .build();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry()
                .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("probe"))),
        );
        let guard = tracing::dispatcher::set_default(&dispatch);
        (provider, guard, dispatch)
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
  let listener=std::net::TcpListener::bind("127.0.0.1:0").unwrap(); let addr=listener.local_addr().unwrap(); listener.set_nonblocking(true).unwrap();
  let server=std::thread::spawn(move || {
   tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async move {
    let listener=tokio::net::TcpListener::from_std(listener).unwrap();
    let (mut socket,_)=listener.accept().await.unwrap(); socket.set_nodelay(true).unwrap();
    let mut header=[0;16]; let mut payload=vec![0;8192]; let mut response=vec![0u8;16+bytes as usize];
    while socket.read_exact(&mut header).await.is_ok() {
     let length=u32::from_be_bytes(header[12..16].try_into().unwrap()) as usize;
     socket.read_exact(&mut payload[..length]).await.unwrap();
     response[..16].copy_from_slice(&header); response[12..16].copy_from_slice(&(bytes as u32).to_be_bytes());
     socket.write_all(&response).await.unwrap();
    }
   })
  });
  let client=talon_cache_client::WorkerClient::new(addr.to_string()); let object=ObjectId::new(Backend::S3,"bucket","object");
  // Ensure the stub has a connection even for the no-network scope case.
  client.fetch_range(&object,0,bytes).await.unwrap();
  #[cfg(feature="instrumentation")]
  let parent=talon_telemetry::TraceContext::from_w3c("00-11111111111111111111111111111111-2222222222222222-00",None).unwrap();
  #[cfg(feature="instrumentation")]
  let propagate=talon_telemetry::Operation::new("caller","internal", if mode=="propagate" {talon_telemetry::TraceParent::Explicit(&parent)} else {talon_telemetry::TraceParent::Root});
  let run=async {
   for _ in 0..1000 {measured(kind,&client,&object,bytes).await;}
   let mut latencies=Vec::with_capacity(count); let started=Instant::now();
   for _ in 0..count {let t=Instant::now(); measured(kind,&client,&object,bytes).await; latencies.push(t.elapsed().as_nanos() as u64);}
   let elapsed=started.elapsed().as_nanos(); latencies.sort_unstable();
   ALLOCS.with(|c| c.set(0)); BYTES.with(|c| c.set(0)); COUNT.with(|c|c.set(true));
   for _ in 0..1000 {measured(kind,&client,&object,bytes).await;}
   COUNT.with(|c|c.set(false));
   println!("{{\"kind\":\"{}\",\"mode\":\"{}\",\"bytes\":{},\"iterations\":{},\"ns_per_op\":{},\"p50_ns\":{},\"p99_ns\":{},\"allocs_per_op\":{},\"allocated_bytes_per_op\":{}}}",kind,mode,bytes,count,elapsed as f64/count as f64,latencies[count/2],latencies[count*99/100],ALLOCS.with(Cell::get) as f64/1000.0,BYTES.with(Cell::get) as f64/1000.0);
  };
  // Only propagation uses an enclosing request. Sampling must happen per read.
  #[cfg(feature="instrumentation")]
  if mode=="propagate" {propagate.scope(run).await;} else {run.await;}
  #[cfg(not(feature="instrumentation"))]
  run.await;
  drop(client); server.join().unwrap();
 });
    #[cfg(feature = "recording")]
    drop(dispatch);
}
