#include "talon.h"

#include <stdatomic.h>
#include <string.h>

static atomic_int callback_done;
static atomic_int callback_ok;

static void capture_zero_length_read(talon_result *result, void *user_data) {
    int ok = user_data == NULL &&
             talon_result_status(result) == TALON_STATUS_OK &&
             talon_result_operation(result) == TALON_OPERATION_READ &&
             talon_result_request_id(result) == 1 &&
             talon_result_bytes_written(result) == 0 &&
             talon_result_object_size(result) == 0 &&
             talon_result_version(result) == NULL &&
             talon_result_error(result) == NULL;

    talon_result_free(result);
    atomic_store_explicit(&callback_ok, ok, memory_order_release);
    atomic_store_explicit(&callback_done, 1, memory_order_release);
}

/*
 * Called from a Rust unit test. Keeping this test in C verifies that the
 * published header, exported ABI, callback signature, and opaque handles work
 * together for a real C caller.
 */
int talon_c_api_smoke_test(void) {
    talon_client_options options;
    talon_client *client = NULL;
    uint64_t request_id = 0;
    const char *last_error;

    talon_client_options_init(&options);
    if (options.block_size != (256u << 20) || options.callback_executor != NULL) {
        return 1;
    }

    if (talon_client_new(NULL, &options, &client) != TALON_STATUS_INVALID_ARGUMENT) {
        return 2;
    }
    last_error = talon_last_error();
    if (last_error == NULL || strstr(last_error, "coordinator_addr is null") == NULL) {
        return 3;
    }

    if (talon_client_new("127.0.0.1:1", &options, &client) != TALON_STATUS_OK ||
        client == NULL) {
        return 4;
    }

    if (talon_read_async(client, "s3://bucket/object", 0, NULL, 1,
                         capture_zero_length_read, NULL, &request_id) !=
        TALON_STATUS_INVALID_ARGUMENT) {
        talon_client_free(client);
        return 5;
    }
    if (talon_stat_async(client, NULL, capture_zero_length_read, NULL, &request_id) !=
        TALON_STATUS_INVALID_ARGUMENT) {
        talon_client_free(client);
        return 6;
    }

    atomic_store_explicit(&callback_done, 0, memory_order_relaxed);
    atomic_store_explicit(&callback_ok, 0, memory_order_relaxed);
    if (talon_read_async(client, "s3://bucket/object", 0, NULL, 0,
                         capture_zero_length_read, NULL, &request_id) != TALON_STATUS_OK ||
        request_id != 1) {
        talon_client_free(client);
        return 7;
    }

    for (unsigned int i = 0; i < 100000000u; ++i) {
        if (atomic_load_explicit(&callback_done, memory_order_acquire)) {
            break;
        }
    }
    if (!atomic_load_explicit(&callback_done, memory_order_acquire) ||
        !atomic_load_explicit(&callback_ok, memory_order_acquire)) {
        talon_client_free(client);
        return 8;
    }

    if (talon_result_status(NULL) != TALON_STATUS_INVALID_ARGUMENT ||
        talon_result_operation(NULL) != 0 || talon_result_request_id(NULL) != 0 ||
        talon_result_bytes_written(NULL) != 0 || talon_result_object_size(NULL) != 0 ||
        talon_result_version(NULL) != NULL || talon_result_error(NULL) != NULL) {
        talon_client_free(client);
        return 9;
    }
    talon_result_free(NULL);
    talon_client_free(client);
    return 0;
}
