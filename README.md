# Concurrent Machine Orchestrator

This is a multithreaded C11/pthreads controller for a simulated robotic lab machine. It shows the core problems of software that coordinates real hardware: concurrency, timing, hung or failing devices, and making sure one failure doesn't take down the whole machine.

## The simulated machine
The machine has 8 devices: 2 robotic arms, 2 linear actuators, 2 pumps, a heater and a camera. Each device runs in **its own thread** behind a thread-safe command queue and executes one command at a time, like a real device controller. The simulation adds action-time jitter and, optionally, injects **hangs** (no reply) and **device errors**.

The demo recipe processes microscope slides. For each sample the steps are: pick slide → position → dispense reagent → heat → image → place slide. With 8 samples that comes to 48 steps in a dependency graph.

## How the orchestrator works
- **Concurrent dispatch:** every step whose dependencies are done and whose device is idle is sent at once.
- **Deadlock avoidance:** shared physical zones (stage, optics, fluidics) are reserved *all-or-nothing*. A step never holds one zone while waiting for another, so circular waits can't form.
- **Watchdog:** each command has a deadline. A hung device is detected, and the command is retried with a longer deadline.
- **Stale-reply detection:** every dispatch gets a unique command ID, so a late reply to a timed-out attempt is ignored.
- **Fault isolation:** when a step fails after its retries, only the steps that depend on it are skipped. Independent work keeps running.
- **Independent verification:** device threads log when each action actually ran. After every run, the logs are checked to confirm that no step started before its dependencies finished and that no two steps overlapped in a shared zone.
- **Latency metrics:** the time from a device posting its reply to the orchestrator handling it is recorded as p50 / p99 / max.

## Results
| Scenario | Result |
|---|---|
| Fault-free, 30 runs | 2.1× faster than sequential execution; event handling p50 49 µs, p99 408 µs |
| 5% hangs + 5% errors, 30 runs | recovered from 103 timeouts and 64 device errors with 167 retries; **1,440 / 1,440 steps completed** |
| Pump B permanently broken | its 4 samples stop cleanly (4 failed, 12 skipped); the other 32 steps complete |
| All 60 runs | **0** ordering violations, **0** zone violations |

## Build and test
```bash
make test   # 6 tests: queue MPMC stress (80,000 messages), timeouts, concurrency, retries, isolation, 40-run fault stress
make tsan   # same tests under ThreadSanitizer (data races, lock-order problems)
make asan   # AddressSanitizer + UBSan
./build/orchsim --samples 8 --hang 0.05 --error 0.05 --retries 4 --seed 7   # JSON metrics
```
CI builds with GCC and Clang and runs all three test targets.

## Layout
```
include/mq.h  src/mq.c          bounded blocking queue (mutex + condvars, monotonic timed wait)
include/machine.h src/machine.c device threads, orchestrator, demo recipe
src/orchsim.c                   CLI that prints run metrics as JSON
tests/test_main.c               unit and stress tests
```
