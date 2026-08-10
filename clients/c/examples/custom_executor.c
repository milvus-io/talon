#include "talon.h"

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct scheduled_task {
    talon_task_fn run;
    void *task_ctx;
} scheduled_task;

typedef struct stat_context {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int done;
    int status;
} stat_context;

static void *task_main(void *arg) {
    scheduled_task *task = (scheduled_task *)arg;
    task->run(task->task_ctx);
    free(task);
    return NULL;
}

static void submit_to_application_executor(
    void *executor_ctx,
    talon_task_fn run,
    void *task_ctx) {
    (void)executor_ctx;
    scheduled_task *task = (scheduled_task *)malloc(sizeof(scheduled_task));
    task->run = run;
    task->task_ctx = task_ctx;

    pthread_t thread;
    pthread_create(&thread, NULL, task_main, task);
    pthread_detach(thread);
}

static void on_stat(talon_result *result, void *user_data) {
    stat_context *ctx = (stat_context *)user_data;
    int status = 0;
    if (talon_result_status(result) == TALON_STATUS_OK) {
        printf("size=%llu version=%s\n",
               (unsigned long long)talon_result_object_size(result),
               talon_result_version(result));
    } else {
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
    talon_callback_executor executor = {
        .executor_ctx = NULL,
        .submit = submit_to_application_executor,
    };

    talon_client_options options;
    talon_client_options_init(&options);
    options.callback_executor = &executor;

    talon_client *client = NULL;
    if (talon_client_new("127.0.0.1:7000", &options, &client) != TALON_STATUS_OK) {
        fprintf(stderr, "client init failed: %s\n", talon_last_error());
        return 1;
    }

    uint64_t request_id = 0;
    stat_context ctx = {0};
    pthread_mutex_init(&ctx.mutex, NULL);
    pthread_cond_init(&ctx.cond, NULL);
    int status = talon_stat_async(
        client,
        "s3://bucket/path/object.bin",
        on_stat,
        &ctx,
        &request_id);
    if (status != TALON_STATUS_OK) {
        fprintf(stderr, "submit failed: %s\n", talon_last_error());
        ctx.status = 1;
    } else {
        pthread_mutex_lock(&ctx.mutex);
        while (!ctx.done) {
            pthread_cond_wait(&ctx.cond, &ctx.mutex);
        }
        pthread_mutex_unlock(&ctx.mutex);
    }

    talon_client_free(client);
    status = ctx.status;
    pthread_cond_destroy(&ctx.cond);
    pthread_mutex_destroy(&ctx.mutex);
    return status;
}
