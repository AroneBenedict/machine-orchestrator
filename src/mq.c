/* mq.c - Thread-safe bounded message queue (see mq.h). */
#include "mq.h"

#include <errno.h>
#include <stdlib.h>
#include <time.h>

uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

int mq_init(mq_t *q, size_t cap)
{
    q->buf = malloc(cap * sizeof(msg_t));
    if (!q->buf) return -1;
    q->cap = cap;
    q->head = q->count = 0;
    q->closed = 0;
    pthread_condattr_t ca;
    pthread_condattr_init(&ca);
    pthread_condattr_setclock(&ca, CLOCK_MONOTONIC); /* immune to wall-clock jumps */
    pthread_mutex_init(&q->mu, NULL);
    pthread_cond_init(&q->not_empty, &ca);
    pthread_cond_init(&q->not_full, &ca);
    pthread_condattr_destroy(&ca);
    return 0;
}

void mq_destroy(mq_t *q)
{
    pthread_mutex_destroy(&q->mu);
    pthread_cond_destroy(&q->not_empty);
    pthread_cond_destroy(&q->not_full);
    free(q->buf);
}

int mq_push(mq_t *q, const msg_t *m)
{
    pthread_mutex_lock(&q->mu);
    while (q->count == q->cap && !q->closed) pthread_cond_wait(&q->not_full, &q->mu);
    if (q->closed) {
        pthread_mutex_unlock(&q->mu);
        return MQ_CLOSED;
    }
    q->buf[(q->head + q->count) % q->cap] = *m;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mu);
    return MQ_OK;
}

int mq_pop(mq_t *q, msg_t *out, int64_t timeout_ns)
{
    struct timespec dl;
    if (timeout_ns >= 0) {
        uint64_t t = now_ns() + (uint64_t)timeout_ns;
        dl.tv_sec = (time_t)(t / 1000000000ull);
        dl.tv_nsec = (long)(t % 1000000000ull);
    }
    pthread_mutex_lock(&q->mu);
    while (q->count == 0 && !q->closed) {
        if (timeout_ns < 0) {
            pthread_cond_wait(&q->not_empty, &q->mu);
        } else if (pthread_cond_timedwait(&q->not_empty, &q->mu, &dl) == ETIMEDOUT) {
            if (q->count == 0) {
                pthread_mutex_unlock(&q->mu);
                return MQ_TIMEOUT;
            }
        }
    }
    if (q->count == 0) { /* closed and drained */
        pthread_mutex_unlock(&q->mu);
        return MQ_CLOSED;
    }
    *out = q->buf[q->head];
    q->head = (q->head + 1) % q->cap;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mu);
    return MQ_OK;
}

void mq_close(mq_t *q)
{
    pthread_mutex_lock(&q->mu);
    q->closed = 1;
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->mu);
}
