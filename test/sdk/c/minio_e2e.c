/*
 * End-to-end test for the C SDK against a real distributed Talon cluster backed
 * by MinIO.
 *
 * This is the first C client test that drives a real cluster: it connects to
 * the coordinator exposed by test/stack/deploy.sh, resolves placement through
 * it, and reads bytes straight from a worker that serves a real MinIO origin
 * seeded with a deterministic (i % 251) ramp object. A byte-exact result proves
 * the full SDK -> worker -> MinIO chain through the public C ABI.
 *
 * Dependency-free by design (mirrors clients/c/tests/c_api_smoke.c): a plain
 * main(), hand-rolled pass/fail accounting, and a pthread condvar to wait on
 * the async callbacks. The SDK buffers passed to talon_read_async are owned by
 * the SDK until the callback runs, so each read uses its own buffer.
 *
 * Usage: minio_e2e <coordinator:port> <block_size>
 */

#include "talon.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct op_context {
    uint8_t *buffer;
    size_t buffer_len;
    size_t bytes_written;
    uint64_t object_size;
    char version[64];
    int status;
    int done;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
} op_context;

static void wait_done(op_context *ctx) {
    pthread_mutex_lock(&ctx->mutex);
    while (!ctx->done) {
        pthread_cond_wait(&ctx->cond, &ctx->mutex);
    }
    pthread_mutex_unlock(&ctx->mutex);
}

static void on_result(talon_result *result, void *user_data) {
    op_context *ctx = (op_context *)user_data;
    if (talon_result_status(result) == TALON_STATUS_OK) {
        if (talon_result_operation(result) == TALON_OPERATION_READ) {
            ctx->bytes_written = talon_result_bytes_written(result);
        } else {
            ctx->object_size = talon_result_object_size(result);
            const char *version = talon_result_version(result);
            if (version != NULL) {
                snprintf(ctx->version, sizeof(ctx->version), "%s", version);
            }
        }
    } else {
        ctx->status = 1;
        fprintf(stderr, "  op failed: %s\n", talon_result_error(result));
    }
    talon_result_free(result);

    pthread_mutex_lock(&ctx->mutex);
    ctx->done = 1;
    pthread_cond_signal(&ctx->cond);
    pthread_mutex_unlock(&ctx->mutex);
}

/* The deterministic bytes the MinIO seed object contains (i % 251). */
static void ramp(uint8_t *out, uint64_t start, size_t length) {
    for (size_t i = 0; i < length; i++) {
        out[i] = (uint8_t)((start + i) % 251);
    }
}

static int check_bytes(const uint8_t *got, size_t got_len, uint64_t start,
                       size_t expected_len, const char *name) {
    if (got_len != expected_len) {
        fprintf(stderr, "  FAIL %s: length %zu != %zu\n", name, got_len, expected_len);
        return 0;
    }
    uint8_t *want = (uint8_t *)malloc(expected_len);
    if (want == NULL) {
        fprintf(stderr, "  FAIL %s: out of memory\n", name);
        return 0;
    }
    ramp(want, start, expected_len);
    int same = memcmp(got, want, expected_len) == 0;
    if (!same) {
        fprintf(stderr, "  FAIL %s: bytes differ from ramp\n", name);
    }
    free(want);
    return same;
}

static int do_read(talon_client *client, const char *uri, uint64_t offset,
                   size_t len, op_context *ctx, const char *name) {
    memset(ctx, 0, sizeof(*ctx));
    ctx->buffer_len = len;
    /* Zero-length reads may pass NULL for dst (talon.h). */
    ctx->buffer = len > 0 ? (uint8_t *)malloc(len) : NULL;
    pthread_mutex_init(&ctx->mutex, NULL);
    pthread_cond_init(&ctx->cond, NULL);
    if (len > 0 && ctx->buffer == NULL) {
        fprintf(stderr, "  FAIL %s: out of memory\n", name);
        return 0;
    }

    uint64_t request_id = 0;
    int rc = talon_read_async(client, uri, offset, ctx->buffer, len, on_result, ctx,
                              &request_id);
    if (rc != TALON_STATUS_OK) {
        fprintf(stderr, "  FAIL %s: submit failed: %s\n", name, talon_last_error());
        pthread_cond_destroy(&ctx->cond);
        pthread_mutex_destroy(&ctx->mutex);
        free(ctx->buffer);
        return 0;
    }
    wait_done(ctx);

    int ok = ctx->status == 0;
    if (ok && len > 0) {
        ok = check_bytes(ctx->buffer, ctx->bytes_written, offset, len, name);
    } else if (ok && len == 0) {
        ok = ctx->bytes_written == 0;
        if (!ok) {
            fprintf(stderr, "  FAIL %s: zero-length read wrote %zu bytes\n", name,
                    ctx->bytes_written);
        }
    }
    pthread_cond_destroy(&ctx->cond);
    pthread_mutex_destroy(&ctx->mutex);
    free(ctx->buffer);
    return ok;
}

static int do_stat(talon_client *client, const char *uri, op_context *ctx) {
    memset(ctx, 0, sizeof(*ctx));
    pthread_mutex_init(&ctx->mutex, NULL);
    pthread_cond_init(&ctx->cond, NULL);

    uint64_t request_id = 0;
    int rc = talon_stat_async(client, uri, on_result, ctx, &request_id);
    if (rc != TALON_STATUS_OK) {
        fprintf(stderr, "  FAIL stat: submit failed: %s\n", talon_last_error());
        pthread_mutex_destroy(&ctx->mutex);
        pthread_cond_destroy(&ctx->cond);
        return 0;
    }
    wait_done(ctx);
    int ok = ctx->status == 0 && ctx->object_size > 0 && ctx->version[0] != '\0';
    if (!ok) {
        fprintf(stderr, "  FAIL stat: size=%llu version='%s'\n",
                (unsigned long long)ctx->object_size, ctx->version);
    }
    pthread_cond_destroy(&ctx->cond);
    pthread_mutex_destroy(&ctx->mutex);
    return ok;
}

static int do_bad_uri(talon_client *client, op_context *ctx) {
    memset(ctx, 0, sizeof(*ctx));
    pthread_mutex_init(&ctx->mutex, NULL);
    pthread_cond_init(&ctx->cond, NULL);
    uint8_t byte;
    uint64_t request_id = 0;
    int rc = talon_read_async(client, "ftp://bucket/key", 0, &byte, 1, on_result, ctx,
                              &request_id);
    int ok = rc != TALON_STATUS_OK;
    if (!ok) {
        fprintf(stderr, "  FAIL bad-uri: submit unexpectedly succeeded\n");
    }
    pthread_mutex_destroy(&ctx->mutex);
    pthread_cond_destroy(&ctx->cond);
    return ok;
}

int main(int argc, char **argv) {
    const char *coordinator = argc > 1 ? argv[1] : "127.0.0.1:17000";
    uint64_t block_size = argc > 2 ? strtoull(argv[2], NULL, 10) : (8u << 20);
    const char *bucket = argc > 3 ? argv[3]
            : (getenv("TALON_E2E_BUCKET") != NULL ? getenv("TALON_E2E_BUCKET") : "talon-e2e");
    const char *key = argc > 4 ? argv[4]
            : (getenv("TALON_E2E_KEY") != NULL ? getenv("TALON_E2E_KEY") : "bench");
    char uri[512];
    snprintf(uri, sizeof(uri), "s3://%s/%s", bucket, key);

    talon_client_options options;
    talon_client_options_init(&options);

    talon_client *client = NULL;
    if (talon_client_new(coordinator, &options, &client) != TALON_STATUS_OK) {
        fprintf(stderr, "client init failed: %s\n", talon_last_error());
        return 1;
    }

    op_context ctx;
    int passed = 0;
    int failed = 0;
    int ok;

    ok = do_stat(client, uri, &ctx);
    printf("  %s stat returns size and version\n", ok ? "ok" : "FAIL");
    ok ? passed++ : failed++;

    ok = do_read(client, uri, 0, 4096, &ctx, "reads exact bytes at offset 0");
    printf("  %s reads exact bytes at offset 0\n", ok ? "ok" : "FAIL");
    ok ? passed++ : failed++;

    ok = do_read(client, uri, 1000, 8192, &ctx, "reads exact bytes at offset 1000");
    printf("  %s reads exact bytes at offset 1000\n", ok ? "ok" : "FAIL");
    ok ? passed++ : failed++;

    ok = do_read(client, uri, 0, (size_t)(block_size + (4u << 20)), &ctx,
                 "reassembles a range spanning block boundaries");
    printf("  %s reassembles a range spanning block boundaries\n", ok ? "ok" : "FAIL");
    ok ? passed++ : failed++;

    ok = do_read(client, uri, block_size - 2048, 4096, &ctx,
                 "reads across exactly one block edge");
    printf("  %s reads across exactly one block edge\n", ok ? "ok" : "FAIL");
    ok ? passed++ : failed++;

    ok = do_read(client, uri, 0, 0, &ctx, "zero-length read returns empty");
    printf("  %s zero-length read returns empty\n", ok ? "ok" : "FAIL");
    ok ? passed++ : failed++;

    ok = do_bad_uri(client, &ctx);
    printf("  %s malformed URI is rejected before any I/O\n", ok ? "ok" : "FAIL");
    ok ? passed++ : failed++;

    talon_client_free(client);

    printf("\nminio e2e: %d passed, %d failed\n", passed, failed);
    return failed == 0 ? 0 : 1;
}
