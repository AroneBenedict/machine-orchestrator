/* orchsim.c - Run the demo recipe and print JSON metrics.
 *   orchsim [--samples 8] [--hang 0.05] [--error 0.05] [--retries 3] [--seed 1] */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "machine.h"

static const char *arg(int argc, char **argv, const char *k, const char *d)
{
    for (int i = 1; i + 1 < argc; i++)
        if (!strcmp(argv[i], k)) return argv[i + 1];
    return d;
}

int main(int argc, char **argv)
{
    static step_t steps[600];
    int ndev, n = build_slide_recipe(steps, 600, atoi(arg(argc, argv, "--samples", "8")), &ndev);
    if (n < 0) return 2;
    machine_cfg_t cfg = {.ndevices = ndev,
                         .p_hang = atof(arg(argc, argv, "--hang", "0")),
                         .p_error = atof(arg(argc, argv, "--error", "0")),
                         .jitter = 0.2,
                         .broken_device = atoi(arg(argc, argv, "--broken", "-1")),
                         .max_retries = atoi(arg(argc, argv, "--retries", "3")),
                         .seed = (unsigned)atoi(arg(argc, argv, "--seed", "1"))};
    run_result_t r;
    machine_run(&cfg, steps, n, &r, NULL);
    printf("{\"steps\":%d,\"done\":%d,\"failed\":%d,\"skipped\":%d,\"retries\":%d,"
           "\"timeouts\":%d,\"device_errors\":%d,\"stale_replies\":%d,\"makespan_us\":%llu,"
           "\"serial_us\":%llu,\"speedup\":%.2f,\"lat_p50_us\":%.1f,\"lat_p99_us\":%.1f,"
           "\"lat_max_us\":%.1f,\"order_violations\":%d,\"zone_violations\":%d}\n",
           n, r.done, r.failed, r.skipped, r.retries, r.timeouts, r.device_errors,
           r.stale_replies, (unsigned long long)r.makespan_us, (unsigned long long)r.serial_us,
           (double)r.serial_us / (double)r.makespan_us, r.lat_p50_us, r.lat_p99_us,
           r.lat_max_us, r.order_violations, r.zone_violations);
    return r.order_violations || r.zone_violations;
}
