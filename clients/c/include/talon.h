#ifndef TALON_H
#define TALON_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct talon_client talon_client;
typedef struct talon_result talon_result;

typedef enum talon_status {
    TALON_STATUS_OK = 0,
    TALON_STATUS_INVALID_ARGUMENT = 1,
    TALON_STATUS_RUNTIME_ERROR = 2,
    TALON_STATUS_SUBMIT_ERROR = 3,
    TALON_STATUS_OPERATION_ERROR = 4
} talon_status;

typedef enum talon_operation {
    TALON_OPERATION_READ = 1,
    TALON_OPERATION_STAT = 2
} talon_operation;

typedef void (*talon_callback)(talon_result *result, void *user_data);

typedef void (*talon_task_fn)(void *task_ctx);
typedef void (*talon_executor_submit_fn)(
    void *executor_ctx,
    talon_task_fn run,
    void *task_ctx);

typedef struct talon_callback_executor {
    void *executor_ctx;
    /*
     * May be called concurrently from internal SDK threads. It must be
     * thread-safe and must eventually call run(task_ctx) exactly once.
     */
    talon_executor_submit_fn submit;
} talon_callback_executor;

typedef struct talon_client_options {
    uint32_t block_size;
    const talon_callback_executor *callback_executor;
    /* Maximum idle connections per peer in each pool. Zero uses the default 8.
     * This does not limit active connections. */
    uint32_t max_idle_per_addr;
} talon_client_options;

void talon_client_options_init(talon_client_options *options);

int talon_client_new(
    const char *coordinator_addr,
    const talon_client_options *options,
    talon_client **out);

void talon_client_free(talon_client *client);

/*
 * The client must remain alive until callbacks for all submitted operations have
 * run. Freeing the client earlier cancels in-flight work.
 *
 * Without a callback executor, callbacks run inline on the SDK operation
 * thread. Callbacks must not block that thread;
 * provide a callback executor to schedule blocking or expensive work elsewhere.
 *
 * If dst_len is greater than zero, dst must be non-NULL. The byte range
 * [dst, dst + dst_len) is exclusively owned by the SDK until the callback runs:
 * callers must not read, write, free, or reuse overlapping storage for another
 * operation during that interval. The storage need not be initialized: received
 * bytes go directly into dst. Completion (including error) retires all kernel
 * access; errors may leave partially modified contents.
 * A zero-length read may pass NULL for dst.
 *
 * version and object_size are each optional and independently nullable; NULL
 * means the caller does not have that value. The read skips the StatObject round
 * trip only when BOTH are non-NULL, using the caller-supplied size and exact
 * source version. A worker may serve that generation from cache or conditionally
 * fetch it from the backend, but never substitutes a newer generation. If either
 * is NULL the SDK resolves both with a StatObject first and any lone value
 * supplied is ignored, since a read cannot skip the stat without both.
 *
 * When used, *object_size is the object's total byte length and bounds the read
 * at EOF (a POSIX short read): a value smaller than offset + dst_len yields a
 * short read, 0 denotes a genuinely empty object (an unambiguous zero-byte
 * read), and a value larger than the object surfaces as a read error rather than
 * fabricated bytes. A caller with no valid size passes NULL.
 *
 * Blocks spanned by the read are fetched concurrently.
 */
#define TALON_REQUEST_OPTIONS_VERSION_1 1u
/* Explicit telemetry lifecycle; uses TALON_TELEMETRY_* environment variables.
 * Defaults off. Shutdown after all operations, outside SDK callbacks. */
int talon_telemetry_init(void);
void talon_telemetry_shutdown(void);
#define TALON_TRACE_PARENT_NONE 0u
#define TALON_TRACE_PARENT_EXPLICIT 1u

typedef struct talon_request_options {
    uint32_t struct_size;
    uint32_t version;
    uint32_t flags;
    uint32_t parent_mode;
    const char *traceparent;
    size_t traceparent_len;
    const char *tracestate;
    size_t tracestate_len;
} talon_request_options;

/* Initialize only the v1 fields; size must be at least sizeof(options).
 * Submission copies the carrier before returning. NULL/NONE never inherits
 * ambient context. Invalid W3C data does not fail a valid business request.
 * Unknown version/flags/mode or a short struct returns INVALID_ARGUMENT
 * synchronously, without scheduling a callback. */
int talon_request_options_init(talon_request_options *options, size_t size);

int talon_read_async_with_options(
    talon_client *client,
    const char *uri,
    uint64_t offset,
    uint8_t *dst,
    size_t dst_len,
    const char *version,
    const uint64_t *object_size,
    const talon_request_options *options,
    talon_callback callback,
    void *user_data,
    uint64_t *request_id_out);

int talon_stat_async_with_options(
    talon_client *client,
    const char *uri,
    const talon_request_options *options,
    talon_callback callback,
    void *user_data,
    uint64_t *request_id_out);

int talon_read_async(
    talon_client *client,
    const char *uri,
    uint64_t offset,
    uint8_t *dst,
    size_t dst_len,
    const char *version,
    const uint64_t *object_size,
    talon_callback callback,
    void *user_data,
    uint64_t *request_id_out);

int talon_stat_async(
    talon_client *client,
    const char *uri,
    talon_callback callback,
    void *user_data,
    uint64_t *request_id_out);

int talon_result_status(const talon_result *result);
int talon_result_operation(const talon_result *result);
uint64_t talon_result_request_id(const talon_result *result);
size_t talon_result_bytes_written(const talon_result *result);
uint64_t talon_result_object_size(const talon_result *result);
const char *talon_result_version(const talon_result *result);
const char *talon_result_error(const talon_result *result);
void talon_result_free(talon_result *result);

const char *talon_last_error(void);

#ifdef __cplusplus
}
#endif

#endif
