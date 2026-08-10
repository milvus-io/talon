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
 * Without a callback executor, callbacks run inline on the SDK's Tokio runtime
 * thread that completed the operation. Callbacks must not block that thread;
 * provide a callback executor to schedule blocking or expensive work elsewhere.
 *
 * If dst_len is greater than zero, dst must be non-NULL. The byte range
 * [dst, dst + dst_len) is exclusively owned by the SDK until the callback runs:
 * callers must not read, write, free, or reuse overlapping storage for another
 * operation during that interval. A zero-length read may pass NULL for dst.
 */
int talon_read_async(
    talon_client *client,
    const char *uri,
    uint64_t offset,
    uint8_t *dst,
    size_t dst_len,
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
