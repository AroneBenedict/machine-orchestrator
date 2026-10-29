/*
 * machine.h - Concurrent orchestrator for a simulated multi-device machine.
 *
 * Each hardware device (robot arm, actuator, pump, heater, camera) runs in its
 * own thread with its own command queue and executes one command at a time,
 * like a real device controller. The orchestrator thread runs a recipe: a
 * dependency graph (DAG) of steps. It dispatches every ready step concurrently,
 * reserves shared physical zones all-or-nothing (so no deadlock is possible),
 * enforces per-step timeouts with a watchdog, retries failed or hung commands,
 * ignores stale replies, and isolates failures: dependants of a failed step are
 * skipped while independent work carries on.
 */
#ifndef MACHINE_H
#define MACHINE_H

#include <stdint.h>

#define MAX_DEPS 4
#define MAX_DEVICES 16

typedef struct {
    const char *name;
    int device;            /* index of the device that executes the step */
    int duration_us;       /* nominal action time                       */
    int deps[MAX_DEPS];    /* steps that must finish first              */
    int ndeps;
    uint32_t zones;        /* bitmask of shared physical zones needed   */
    int timeout_us;        /* watchdog deadline per attempt             */
} step_t;

typedef struct {
    int ndevices;
    double p_hang;         /* chance a command never gets a reply       */
    double p_error;        /* chance a command reports an error         */
    double jitter;         /* +/- fraction applied to action time       */
    int broken_device;     /* device that always errors, -1 = none      */
    int max_retries;
    uint32_t seed;
} machine_cfg_t;

typedef enum { ST_PENDING = 0, ST_RUNNING, ST_DONE, ST_FAILED, ST_SKIPPED } step_state_t;

typedef struct {
    int done, failed, skipped;
    int retries, timeouts, device_errors, stale_replies;
    uint64_t makespan_us;       /* wall time for the whole recipe        */
    uint64_t serial_us;         /* sum of nominal step times             */
    double lat_p50_us, lat_p99_us, lat_max_us; /* reply -> handled latency */
    int order_violations;       /* step started before a dependency ended */
    int zone_violations;        /* two steps overlapped in one zone       */
} run_result_t;

/* Runs the recipe to completion. states may be NULL. Returns 0 on success. */
int machine_run(const machine_cfg_t *cfg, const step_t *steps, int nsteps,
                run_result_t *out, step_state_t *states);

/* Builds the demo slide-processing recipe for nsamples samples. */
int build_slide_recipe(step_t *steps, int max_steps, int nsamples, int *ndevices);
const char *device_name(int device);

#endif
