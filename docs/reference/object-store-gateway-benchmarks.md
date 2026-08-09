# Object-store gateway benchmarks

The shared gateway has a deterministic loopback harness:

```sh
just gateway-bench
```

It emits JSON Lines so runs can be archived without scraping a presentation
table. `TALON_GATEWAY_BENCH_REQUESTS` controls phase samples and
`TALON_GATEWAY_BENCH_OBJECT_BYTES` controls the slow-stream object size.

## What is separated

The harness drives the real HTTP parser, limits, middleware, request IDs,
metrics observation, Axum dispatch, and streaming body runtime. The current
loopback-only mode has a no-op incoming authentication boundary; #446 will add
the authentication phase to this same measurement.

Controlled delays model boundaries outside the shared runtime:

| Phase | Injected delay | Interpretation |
|---|---:|---|
| `empty` | 0 | HTTP parse, middleware, no-op auth boundary, adapter dispatch, response. |
| `cache` | 100 us | Cache-client call plus runtime; sub-millisecond Tokio timers may round upward. |
| `worker` | 1 ms | Cache client and modeled worker round trip. |
| `origin` | 5 ms | Modeled direct-origin metadata/body latency. |

These are regression measurements, not claims about a real worker or cloud
service. Provider compatibility is measured separately by the Azurite and
LocalStack SDK CI jobs.

## Recorded run

Run on the Agent Console devbox on 2026-08-09, release build, one warm HTTP/1.1
connection, 100 warmups and 1,000 measured serial requests per phase:

| Phase | p50 | p99 | p50 above injected delay |
|---|---:|---:|---:|
| `empty` | 117 us | 171 us | 117 us |
| `cache` | 1.216 ms | 2.273 ms | 1.116 ms |
| `worker` | 2.223 ms | 2.329 ms | 1.223 ms |
| `origin` | 6.235 ms | 6.342 ms | 1.235 ms |

The 100 us modeled cache delay rounds to a scheduler tick on this host. The
useful comparison is the stable `empty` floor and the roughly 1.2 ms timer plus
runtime cost around delayed phases, not the absolute cloud latency.

## Slow client and large object

The same run streamed 64 MiB in 64 KiB frames while the client paused 1 ms
after every received frame:

| Metric | Result |
|---|---:|
| Elapsed | 2.478 s |
| Throughput | 25.82 MiB/s |
| Baseline RSS | 6,406,144 bytes |
| Peak RSS | 7,102,464 bytes |
| Peak RSS increase | 696,320 bytes |

The client intentionally determines throughput. The result of interest is that
a 64 MiB object increased process RSS by less than 0.7 MiB: the response stream
remained demand-driven instead of buffering the object. Repeat on the target
kernel and allocator before setting pod memory limits; RSS numbers are not
portable capacity guarantees.
