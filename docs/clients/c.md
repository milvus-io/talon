# C client

A C ABI client for C and C++ applications that need asynchronous ranged reads
without mounting FUSE.

The client binds Talon's Rust read path. It talks to the coordinator for
`stat`, membership, and placement refresh, then reads data directly from
workers.

## Build

```sh
cargo build --release -p talon-c
```

The public header is at `clients/c/include/talon.h`. The build produces shared
and static libraries from the `talon_c` crate.

To stage a standalone C SDK directory containing the header and libraries, run:

```sh
just package-c
```

This produces `target/talon-c-sdk/` with:

```text
include/talon.h
lib/libtalon_c.so
lib/libtalon_c.a
LICENSE
```

Pass a destination directory with `just package-c path/to/talon-c-sdk`. When
linking the shared library, add `lib/` to the platform's dynamic-library search
path (for example, `LD_LIBRARY_PATH` on Linux), or configure an rpath in the
application binary.

## Async read

```c
#include "talon.h"

static void on_read(talon_result *result, void *user_data) {
    if (talon_result_status(result) == TALON_STATUS_OK) {
        size_t n = talon_result_bytes_written(result);
        (void)n;
    } else {
        const char *error = talon_result_error(result);
        (void)error;
    }
    talon_result_free(result);
}

talon_client_options options;
talon_client_options_init(&options);

talon_client *client = NULL;
talon_client_new("coordinator-host:7000", &options, &client);

uint8_t *buffer = malloc(1 << 20);
uint64_t request_id = 0;
talon_read_async(
    client,
    "az://container/datasets/train.parquet",
    0,
    buffer,
    1 << 20,
    NULL,  /* version: resolve it */
    NULL,  /* object size: resolve it */
    on_read,
    NULL,
    &request_id);
```

The buffer passed to `talon_read_async` is owned by the caller and must remain
valid until the callback runs. While the operation is in flight, the SDK has
exclusive access to that byte range: do not read, write, free, or reuse
overlapping storage for another operation until the callback runs. A zero-length
read may pass `NULL` for the buffer. The callback receives a `talon_result`;
release it with `talon_result_free`.

Passing both the version and size from `talon_stat_async` skips another stat.
The supplied version is exact: workers serve matching cached bytes or
conditionally fetch that generation from the backend. If it is unavailable,
the callback reports an error instead of returning replacement bytes.

The client handle must also remain alive until callbacks for submitted
operations have run. Freeing it earlier cancels in-flight work.

## Callback dispatch

By default, callbacks run inline on the SDK's Tokio runtime thread that
completed the operation. Keep callbacks short and non-blocking so they do not
delay other I/O work. To run callbacks on an application thread pool, set
`options.callback_executor` to a `talon_callback_executor`.

The executor's `submit` function receives a task function and task context. It
may be called concurrently from internal SDK threads, so it must be thread-safe.
It must schedule that task and eventually call `run(task_ctx)` exactly once.

## Stat

```c
static void on_stat(talon_result *result, void *user_data) {
    if (talon_result_status(result) == TALON_STATUS_OK) {
        uint64_t size = talon_result_object_size(result);
        const char *version = talon_result_version(result);
        (void)size;
        (void)version;
    }
    talon_result_free(result);
}

talon_stat_async(client, "az://container/datasets/train.parquet",
                 on_stat, NULL, &request_id);
```

## Prewarm

```c
static void on_load(talon_result *result, void *user_data) {
    (void)user_data;
    if (talon_result_status(result) == TALON_STATUS_OK) {
        for (size_t i = 0; i < talon_result_load_count(result); ++i) {
            const talon_load_result *file = talon_result_load(result, i);
            /* file->size and file->blocks describe the completed input file. */
            (void)file;
        }
    }
    talon_result_free(result);
}

talon_load_async(client, "s3://bucket/file", "etag-1", 4096,
                 on_load, NULL, &request_id);
talon_load_request files[] = {
    {"s3://bucket/a", "etag-a", 4096},
    {"s3://bucket/b", "etag-b", 8192}
};
talon_batch_load_async(client, files, 2, on_load, NULL, &request_id);
```

Both calls require a source version and size; neither issues HEAD. Batch LOAD
uses protocol batches with up to 1024 block assignments per frame. The request
array and strings are copied before submission returns. A successful submission
delivers one callback through the configured callback executor; keep the client
and `user_data` alive until it runs. File results follow input order, including
empty files, and are borrowed until `talon_result_free`. On failure there are no
file results, but completed fills may remain cached. The `_with_options` variants
accept the same tracing options as reads. Worker origin retries apply to both.

## Not yet available

The C client does not yet expose `list`, writes, cancellation, or request priorities.
