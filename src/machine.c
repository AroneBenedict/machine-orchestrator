/* machine.c - Concurrent multi-device orchestrator (see machine.h). */
#include "machine.h"

#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "mq.h"

enum { MSG_CMD = 1, MSG_REPLY = 2 };

typedef struct {
    int id;
    const machine_cfg_t *cfg;
    mq_t cmdq;
    mq_t *events;
    uint32_t rng;
    /* Ground-truth execution log written by the device thread. Each step is
     * only ever executed by its own device thread, so no lock is needed here;
     * the orchestrator reads it after pthread_join. */
    uint64_t *start_ns, *end_ns;
} device_t;

static uint32_t xs32(uint32_t *s)
{
    uint32_t x = *s;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    return *s = x;
}
static double unit(uint32_t *s) { return (xs32(s) >> 8) / 16777216.0; }

static void sleep_us(int us)
{
    struct timespec ts = {us / 1000000, (long)(us % 1000000) * 1000};
    while (nanosleep(&ts, &ts) != 0) {
    }
}

static void *device_main(void *arg)
{
    device_t *d = arg;
    msg_t m;
    while (mq_pop(&d->cmdq, &m, -1) == MQ_OK) {
        double j = 1.0 + d->cfg->jitter * (2.0 * unit(&d->rng) - 1.0);
        int t = (int)(m.duration_us * j);
        double r = unit(&d->rng);
        uint64_t start = now_ns();
        sleep_us(t); /* the physical action */
        if (r < d->cfg->p_hang) continue; /* lost reply: watchdog must catch it */
        msg_t rep = m;
        rep.type = MSG_REPLY;
        rep.status = (d->id == d->cfg->broken_device || r < d->cfg->p_hang + d->cfg->p_error) ? -1 : 0;
        if (rep.status == 0) {
            d->start_ns[m.step] = start;
            d->end_ns[m.step] = now_ns();
        }
        rep.t_post_ns = now_ns();
        if (mq_push(d->events, &rep) != MQ_OK) break;
    }
    return NULL;
}

static int cmp_double(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

int machine_run(const machine_cfg_t *cfg, const step_t *steps, int nsteps,
                run_result_t *out, step_state_t *states_out)
{
    memset(out, 0, sizeof(*out));
    if (cfg->ndevices > MAX_DEVICES) return -1;
    step_state_t *st = calloc((size_t)nsteps, sizeof(*st));
    uint32_t *cur_cmd = calloc((size_t)nsteps, sizeof(uint32_t));
    int *attempts = calloc((size_t)nsteps, sizeof(int));
    uint64_t *deadline = calloc((size_t)nsteps, sizeof(uint64_t));
    uint64_t *start_ns = calloc((size_t)nsteps, sizeof(uint64_t));
    uint64_t *end_ns = calloc((size_t)nsteps, sizeof(uint64_t));
    double *lat = calloc((size_t)nsteps * (size_t)(cfg->max_retries + 1) + 1, sizeof(double));
    if (!st || !cur_cmd || !attempts || !deadline || !start_ns || !end_ns || !lat) return -1;

    mq_t events;
    mq_init(&events, 256);
    device_t dev[MAX_DEVICES];
    pthread_t th[MAX_DEVICES];
    for (int i = 0; i < cfg->ndevices; i++) {
        dev[i] = (device_t){.id = i, .cfg = cfg, .events = &events,
                            .rng = cfg->seed * 2654435761u + (uint32_t)i * 97u + 1u,
                            .start_ns = start_ns, .end_ns = end_ns};
        mq_init(&dev[i].cmdq, 64);
        pthread_create(&th[i], NULL, device_main, &dev[i]);
    }

    uint32_t zones_busy = 0, next_cmd = 1;
    int dev_busy[MAX_DEVICES] = {0}; /* one command in flight per device */
    int finished = 0, nlat = 0;
    uint64_t t0 = now_ns();
    for (int i = 0; i < nsteps; i++) out->serial_us += (uint64_t)steps[i].duration_us;

    while (finished < nsteps) {
        /* 1. Skip steps whose dependencies failed; dispatch ready steps. */
        int progress = 1;
        while (progress) {
            progress = 0;
            for (int i = 0; i < nsteps; i++) {
                if (st[i] != ST_PENDING) continue;
                int ready = 1, doomed = 0;
                for (int k = 0; k < steps[i].ndeps; k++) {
                    step_state_t ds = st[steps[i].deps[k]];
                    if (ds == ST_FAILED || ds == ST_SKIPPED) doomed = 1;
                    else if (ds != ST_DONE) ready = 0;
                }
                if (doomed) {
                    st[i] = ST_SKIPPED;
                    out->skipped++;
                    finished++;
                    progress = 1;
                    continue;
                }
                /* All-or-nothing zone reservation: never hold one zone while
                 * waiting for another, so circular waits cannot form. */
                if (!ready || (steps[i].zones & zones_busy) || dev_busy[steps[i].device]) continue;
                zones_busy |= steps[i].zones;
                dev_busy[steps[i].device] = 1;
                st[i] = ST_RUNNING;
                attempts[i] = 1;
                cur_cmd[i] = next_cmd++;
                deadline[i] = now_ns() + (uint64_t)steps[i].timeout_us * 1000u;
                msg_t c = {.type = MSG_CMD, .step = i, .cmd_id = cur_cmd[i],
                           .duration_us = steps[i].duration_us, .t_post_ns = now_ns()};
                mq_push(&dev[steps[i].device].cmdq, &c);
            }
        }
        if (finished >= nsteps) break;

        /* 2. Wait for the next reply or the nearest watchdog deadline. */
        uint64_t nearest = UINT64_MAX;
        for (int i = 0; i < nsteps; i++)
            if (st[i] == ST_RUNNING && deadline[i] < nearest) nearest = deadline[i];
        uint64_t now = now_ns();
        int64_t wait = nearest == UINT64_MAX ? -1 : (nearest > now ? (int64_t)(nearest - now) : 0);
        msg_t m;
        int rc = mq_pop(&events, &m, wait);
        now = now_ns();

        int retry_step = -1, fail_step = -1;
        if (rc == MQ_OK) {
            int i = m.step;
            if (st[i] != ST_RUNNING || m.cmd_id != cur_cmd[i]) {
                out->stale_replies++; /* late reply to a timed-out attempt */
                continue;
            }
            lat[nlat++] = (double)(now - m.t_post_ns) / 1000.0;
            if (m.status == 0) {
                st[i] = ST_DONE;
                out->done++;
                finished++;
                zones_busy &= ~steps[i].zones;
                dev_busy[steps[i].device] = 0;
            } else {
                out->device_errors++;
                if (attempts[i] <= cfg->max_retries) retry_step = i;
                else fail_step = i;
            }
        }
        /* 3. Watchdog: any running step past its deadline is retried or failed. */
        for (int i = 0; i < nsteps; i++) {
            if (st[i] != ST_RUNNING || deadline[i] > now || i == retry_step || i == fail_step) continue;
            out->timeouts++;
            if (attempts[i] <= cfg->max_retries) {
                out->retries++;
                attempts[i]++;
                cur_cmd[i] = next_cmd++; /* old reply becomes stale */
                deadline[i] = now + (uint64_t)steps[i].timeout_us * 1000u * (uint64_t)attempts[i];
                msg_t c = {.type = MSG_CMD, .step = i, .cmd_id = cur_cmd[i],
                           .duration_us = steps[i].duration_us, .t_post_ns = now};
                mq_push(&dev[steps[i].device].cmdq, &c);
            } else {
                st[i] = ST_FAILED;
                out->failed++;
                finished++;
                zones_busy &= ~steps[i].zones;
                dev_busy[steps[i].device] = 0;
            }
        }
        if (retry_step >= 0) {
            int i = retry_step;
            out->retries++;
            attempts[i]++;
            cur_cmd[i] = next_cmd++;
            deadline[i] = now + (uint64_t)steps[i].timeout_us * 1000u * (uint64_t)attempts[i];
            msg_t c = {.type = MSG_CMD, .step = i, .cmd_id = cur_cmd[i],
                       .duration_us = steps[i].duration_us, .t_post_ns = now};
            mq_push(&dev[steps[i].device].cmdq, &c);
        }
        if (fail_step >= 0) {
            st[fail_step] = ST_FAILED;
            out->failed++;
            finished++;
            zones_busy &= ~steps[fail_step].zones;
            dev_busy[steps[fail_step].device] = 0;
        }
    }
    out->makespan_us = (now_ns() - t0) / 1000u;

    for (int i = 0; i < cfg->ndevices; i++) mq_close(&dev[i].cmdq);
    mq_close(&events); /* unblocks any device posting a late reply */
    for (int i = 0; i < cfg->ndevices; i++) {
        pthread_join(th[i], NULL);
        mq_destroy(&dev[i].cmdq);
    }
    mq_destroy(&events);

    /* Independent verification from the devices' own execution log. */
    for (int i = 0; i < nsteps; i++) {
        if (st[i] != ST_DONE) continue;
        for (int k = 0; k < steps[i].ndeps; k++)
            if (start_ns[i] < end_ns[steps[i].deps[k]]) out->order_violations++;
        for (int j = i + 1; j < nsteps; j++)
            if (st[j] == ST_DONE && (steps[i].zones & steps[j].zones) &&
                start_ns[i] < end_ns[j] && start_ns[j] < end_ns[i])
                out->zone_violations++;
    }
    if (nlat) {
        qsort(lat, (size_t)nlat, sizeof(double), cmp_double);
        out->lat_p50_us = lat[nlat / 2];
        out->lat_p99_us = lat[(int)(nlat * 0.99) < nlat ? (int)(nlat * 0.99) : nlat - 1];
        out->lat_max_us = lat[nlat - 1];
    }
    if (states_out) memcpy(states_out, st, (size_t)nsteps * sizeof(*st));
    free(st);
    free(cur_cmd);
    free(attempts);
    free(deadline);
    free(start_ns);
    free(end_ns);
    free(lat);
    return 0;
}

/* ---- Demo recipe: slide processing in a robotic pathology machine ---- */

enum { ARM_L, ARM_R, ACT_X, ACT_Z, PUMP_A, PUMP_B, HEATER, CAMERA, NDEV };
static const char *const names[NDEV] = {"arm_left", "arm_right", "actuator_x", "actuator_z",
                                         "pump_a",   "pump_b",    "heater",     "camera"};
enum { ZONE_STAGE = 1, ZONE_OPTICS = 2, ZONE_FLUIDICS = 4 };

const char *device_name(int device) { return device >= 0 && device < NDEV ? names[device] : "?"; }

int build_slide_recipe(step_t *s, int max_steps, int nsamples, int *ndevices)
{
    int n = 0;
    *ndevices = NDEV;
    for (int k = 0; k < nsamples; k++) {
        if (n + 6 > max_steps) return -1;
        int arm = k % 2 ? ARM_R : ARM_L;
        int pump = k % 2 ? PUMP_B : PUMP_A;
        int pick = n, pos = n + 1, disp = n + 2, heat = n + 3, img = n + 4, place = n + 5;
        s[pick] = (step_t){"pick slide", arm, 3000, {0}, 0, ZONE_STAGE, 15000};
        s[pos] = (step_t){"position", k % 2 ? ACT_Z : ACT_X, 2000, {pick}, 1, 0, 10000};
        s[disp] = (step_t){"dispense reagent", pump, 2500, {pos}, 1, ZONE_FLUIDICS, 12000};
        s[heat] = (step_t){"heat", HEATER, 2000, {disp}, 1, 0, 10000};
        s[img] = (step_t){"image", CAMERA, 1500, {heat}, 1, ZONE_OPTICS, 8000};
        s[place] = (step_t){"place slide", arm, 3000, {img}, 1, ZONE_STAGE, 15000};
        n += 6;
    }
    return n;
}
