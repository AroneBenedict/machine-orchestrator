/* mq.h - Thread-safe bounded message queue (mutex + condition variables). */
#ifndef MQ_H
#define MQ_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
    int type;          /* message kind (command / reply)            */
    int step;          /* recipe step this message belongs to       */
    uint32_t cmd_id;   /* unique per dispatch, detects stale replies */
    int status;        /* 0 = ok, -1 = device error                 */
    int duration_us;   /* commanded action time                     */
    uint64_t t_post_ns;/* timestamp when the message was posted     */
} msg_t;

typedef struct {
    msg_t *buf;
    size_t cap, head, count;
    int closed;
    pthread_mutex_t mu;
    pthread_cond_t not_empty, not_full;
} mq_t;

enum { MQ_OK = 0, MQ_TIMEOUT = 1, MQ_CLOSED = -1 };

uint64_t now_ns(void);
int mq_init(mq_t *q, size_t cap);
void mq_destroy(mq_t *q);
/* Blocks while full. Returns MQ_CLOSED if the queue was closed. */
int mq_push(mq_t *q, const msg_t *m);
/* timeout_ns < 0 waits forever. Uses CLOCK_MONOTONIC. */
int mq_pop(mq_t *q, msg_t *out, int64_t timeout_ns);
void mq_close(mq_t *q);

#endif
