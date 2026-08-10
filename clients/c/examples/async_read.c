#include "talon.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct read_context {
    uint8_t *buffer;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int done;
    int status;
} read_context;

static void on_read(talon_result *result, void *user_data) {
    read_context *ctx = (read_context *)user_data;
    int status = 0;
    if (talon_result_status(result) == TALON_STATUS_OK) {
        printf("read %zu bytes\n", talon_result_bytes_written(result));
    } else {
        fprintf(stderr, "read failed: %s\n", talon_result_error(result));
        status = 1;
    }
    talon_result_free(result);

    pthread_mutex_lock(&ctx->mutex);
    ctx->status = status;
    ctx->done = 1;
    pthread_cond_signal(&ctx->cond);
    pthread_mutex_unlock(&ctx->mutex);
}

int main(void) {
    talon_client_options options;
    talon_client_options_init(&options);

    talon_client *client = NULL;
    if (talon_client_new("127.0.0.1:7000", &options, &client) != TALON_STATUS_OK) {
        fprintf(stderr, "client init failed: %s\n", talon_last_error());
        return 1;
    }

    read_context *ctx = (read_context *)calloc(1, sizeof(read_context));
    ctx->buffer = (uint8_t *)malloc(4096);
    pthread_mutex_init(&ctx->mutex, NULL);
    pthread_cond_init(&ctx->cond, NULL);

    uint64_t request_id = 0;
    int status = talon_read_async(
        client,
        "s3://bucket/path/object.bin",
        0,
        ctx->buffer,
        4096,
        on_read,
        ctx,
        &request_id);
    if (status != TALON_STATUS_OK) {
        fprintf(stderr, "submit failed: %s\n", talon_last_error());
        pthread_mutex_lock(&ctx->mutex);
        ctx->status = 1;
        pthread_mutex_unlock(&ctx->mutex);
    } else {
        pthread_mutex_lock(&ctx->mutex);
        while (!ctx->done) {
            pthread_cond_wait(&ctx->cond, &ctx->mutex);
        }
        pthread_mutex_unlock(&ctx->mutex);
    }

    talon_client_free(client);
    status = ctx->status;
    pthread_cond_destroy(&ctx->cond);
    pthread_mutex_destroy(&ctx->mutex);
    free(ctx->buffer);
    free(ctx);
    return status;
}
