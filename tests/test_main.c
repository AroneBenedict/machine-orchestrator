/* test_main.c - Unit and stress tests for the queue and the orchestrator. */
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "machine.h"
#include "mq.h"

static int failures, checks;
#define CHECK(c)                                                                  \
    do {                                                                          \
        checks++;                                                                 \
        if (!(c)) {                                                               \
            failures++;                                                           \
            fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);        \
        }                                                                         \
    } while (0)
#define RUN(t)                                                                    \
    do {                                                                          \
        int b = failures;                                                         \
        t();                                                                      \
        printf("%s %s\n", failures == b ? "[ OK ]" : "[FAIL]", #t);               \
    } while (0)

/* ---------------------------------------------------------------- queue */
#define PRODUCERS 4
#define CONSUMERS 4
#define PER_PRODUCER 20000

static mq_t q;
static unsigned char seen[PRODUCERS * PER_PRODUCER];
static pthread_mutex_t seen_mu = PTHREAD_MUTEX_INITIALIZER;
static int duplicates;

static void *producer(void *a)
{
    int id = (int)(intptr_t)a;
    for (int i = 0; i < PER_PRODUCER; i++) {
        msg_t m = {.step = id * PER_PRODUCER + i};
        mq_push(&q, &m);
    }
    return NULL;
}

static void *consumer(void *a)
{
    (void)a;
    msg_t m;
    while (mq_pop(&q, &m, -1) == MQ_OK) {
        pthread_mutex_lock(&seen_mu);
        if (seen[m.step]++) duplicates++;
        pthread_mutex_unlock(&seen_mu);
    }
    return NULL;
}

static void test_queue_mpmc_no_loss_no_duplicates(void)
{
    mq_init(&q, 16); /* small capacity forces producers to block */
    pthread_t p[PRODUCERS], c[CONSUMERS];
    for (int i = 0; i < CONSUMERS; i++) pthread_create(&c[i], NULL, consumer, NULL);
    for (int i = 0; i < PRODUCERS; i++) pthread_create(&p[i], NULL, producer, (void *)(intptr_t)i);
    for (int i = 0; i < PRODUCERS; i++) pthread_join(p[i], NULL);
    mq_close(&q); /* consumers drain what is left, then exit */
    for (int i = 0; i < CONSUMERS; i++) pthread_join(c[i], NULL);
    int missing = 0;
    for (int i = 0; i < PRODUCERS * PER_PRODUCER; i++) missing += !seen[i];
    CHECK(missing == 0);
    CHECK(duplicates == 0);
    mq_destroy(&q);
}

static void test_queue_timeout_and_close(void)
{
    mq_t t;
    mq_init(&t, 4);
    msg_t m;
    uint64_t a = now_ns();
    CHECK(mq_pop(&t, &m, 20 * 1000000) == MQ_TIMEOUT);
    uint64_t waited = (now_ns() - a) / 1000000;
    CHECK(waited >= 19 && waited < 200);
    msg_t x = {.step = 7};
    CHECK(mq_push(&t, &x) == MQ_OK);
    mq_close(&t);
    CHECK(mq_push(&t, &x) == MQ_CLOSED);
    CHECK(mq_pop(&t, &m, 0) == MQ_OK && m.step == 7); /* drains after close */
    CHECK(mq_pop(&t, &m, 0) == MQ_CLOSED);
    mq_destroy(&t);
}

/* --------------------------------------------------------- orchestrator */
static step_t steps[128];

static machine_cfg_t base_cfg(int ndev)
{
    return (machine_cfg_t){.ndevices = ndev, .p_hang = 0, .p_error = 0, .jitter = 0.2,
                           .broken_device = -1, .max_retries = 3, .seed = 1};
}

static void test_recipe_runs_concurrently_and_correctly(void)
{
    int ndev, n = build_slide_recipe(steps, 128, 8, &ndev);
    machine_cfg_t cfg = base_cfg(ndev);
    run_result_t r;
    CHECK(machine_run(&cfg, steps, n, &r, NULL) == 0);
    CHECK(r.done == n);
    CHECK(r.order_violations == 0);
    CHECK(r.zone_violations == 0);
    CHECK(r.timeouts == 0);
    double speedup = (double)r.serial_us / (double)r.makespan_us;
    printf("       %d steps, makespan %llu us vs %llu us serial (%.2fx), handling p99 %.0f us\n",
           n, (unsigned long long)r.makespan_us, (unsigned long long)r.serial_us, speedup,
           r.lat_p99_us);
    CHECK(speedup > 1.5);
}

static void test_hangs_and_errors_are_retried(void)
{
    int ndev, n = build_slide_recipe(steps, 128, 8, &ndev);
    machine_cfg_t cfg = base_cfg(ndev);
    cfg.p_hang = 0.08;
    cfg.p_error = 0.08;
    cfg.max_retries = 6;
    cfg.seed = 42;
    run_result_t r;
    machine_run(&cfg, steps, n, &r, NULL);
    CHECK(r.timeouts > 0);
    CHECK(r.device_errors > 0);
    CHECK(r.retries > 0);
    CHECK(r.done == n); /* 6 retries make permanent failure vanishingly rare */
    CHECK(r.order_violations == 0);
    CHECK(r.zone_violations == 0);
}

static void test_failure_is_isolated(void)
{
    int ndev, n = build_slide_recipe(steps, 128, 6, &ndev);
    machine_cfg_t cfg = base_cfg(ndev);
    cfg.broken_device = 5; /* pump_b: serves the odd-numbered samples only */
    cfg.max_retries = 1;
    run_result_t r;
    step_state_t st[128];
    machine_run(&cfg, steps, n, &r, st);
    for (int k = 0; k < 6; k++) {
        int base = k * 6;
        if (k % 2 == 0) {
            for (int s = 0; s < 6; s++) CHECK(st[base + s] == ST_DONE); /* unaffected */
        } else {
            CHECK(st[base + 1] == ST_DONE);    /* before the failure */
            CHECK(st[base + 2] == ST_FAILED);  /* dispense on broken pump */
            for (int s = 3; s < 6; s++) CHECK(st[base + s] == ST_SKIPPED);
        }
    }
    CHECK(r.failed == 3 && r.skipped == 9 && r.done == 24);
}

/* Many randomised runs: the test finishing at all shows there is no deadlock;
 * the device logs show ordering and zone exclusion always held. */
static void test_stress_random_faults(void)
{
    int ndev, n = build_slide_recipe(steps, 128, 6, &ndev);
    int bad = 0, runs = 40;
    for (int i = 0; i < runs; i++) {
        machine_cfg_t cfg = base_cfg(ndev);
        cfg.p_hang = 0.05;
        cfg.p_error = 0.05;
        cfg.seed = 1000u + (uint32_t)i;
        cfg.max_retries = 2;
        run_result_t r;
        machine_run(&cfg, steps, n, &r, NULL);
        if (r.order_violations || r.zone_violations || r.done + r.failed + r.skipped != n) bad++;
    }
    CHECK(bad == 0);
}

int main(void)
{
    setvbuf(stdout, NULL, _IONBF, 0);
    RUN(test_queue_mpmc_no_loss_no_duplicates);
    RUN(test_queue_timeout_and_close);
    RUN(test_recipe_runs_concurrently_and_correctly);
    RUN(test_hangs_and_errors_are_retried);
    RUN(test_failure_is_isolated);
    RUN(test_stress_random_faults);
    printf("\n%d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
