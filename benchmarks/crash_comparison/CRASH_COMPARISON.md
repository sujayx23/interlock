# Crash recovery: interlock vs. Celery+Redis, observed not assumed

**Both systems recover automatically. Celery does not "do nothing."** The
real difference, directly observed across 3 runs each (identical results
every time on both sides — this is not noise): **interlock re-executes
exactly the one step that was actually interrupted, and nothing else.
Celery, correctly configured for redelivery, recovers too — but re-executes
an already-completed step needlessly, and the interrupted step plus the
next step each fire twice, almost simultaneously.** For a task with a
real side effect (charge a card, send an email), that's a live duplicate-
execution bug, not just a slower recovery.

## Setup

Same 3-step chain as the throughput benchmark (`fetch -> transform ->
write`), except `transform` now sleeps 4s so it can be caught mid-execution
— the slow step moved to position 2 of 3 specifically so both systems are
killed "during step 2 of 3," the same logical point.

**Celery was not left on defaults.** Bare defaults (`task_acks_late=False`)
ack a task the instant a worker *receives* it, before running it — a
worker dying mid-task means the message is already gone from the broker
with no redelivery ever attempted. That would make this comparison
one-sided and not a real test of Celery's actual recovery story. Configured
instead (`benchmarks/crash_comparison/tasks_celery_crash.py`):

- `task_acks_late=True` — ack after the task finishes, not before.
- `task_reject_on_worker_lost=True` — best practice alongside acks_late.
  **Does not get to help in this specific test**: it depends on the worker
  pool detecting a child process died, which needs a separate parent/child
  process relationship. This benchmark runs under `-P threads` (Celery's
  default `prefork` pool is broken under Python 3.14 in this environment —
  see `benchmarks/RESULTS.md`), where SIGKILLing the worker kills the one
  process outright with no surviving parent to notice. Included anyway
  because a real prefork deployment would want it configured, and it's more
  honest to say plainly it can't fire here than to quietly drop it.
- `broker_transport_options={"visibility_timeout": 8}` — an unacked
  message becomes redeliverable after 8s with no ack. Chosen short enough
  to demonstrate in reasonable time; Kombu's default is 3600s (1 hour),
  which would be correct but useless to actually run.

Both sides: real worker subprocess, real `SIGKILL` (not a graceful
shutdown) sent once the interrupted step is observably executing, a fresh
worker started afterward, timing and per-step execution counts recorded via
a shared append-only log every task invocation writes to on entry —
counted after the fact, not inferred from either framework's documented
behavior.

## What was observed (3 runs each, identical every time)

| step      | interlock: times executed | celery: times executed |
|-----------|---------------------------|--------------------------|
| fetch     | **1**                     | **2**                    |
| transform | **2** (killed + redone)   | **3** (killed + 2x redone, ~5ms apart) |
| write     | **1**                     | **2** (2x, ~2-12ms apart) |

Raw log excerpt, one of the celery runs (pid 25639 = original/killed
worker, pid 25653 = fresh worker started after the kill):

```
{"task": "fetch",     "ts": ...492.125, "pid": 25639}   <- original, succeeds
{"task": "transform", "ts": ...492.130, "pid": 25639}   <- original, KILLED mid-sleep
                                          [8s+ wait for visibility_timeout]
{"task": "fetch",     "ts": ...504.036, "pid": 25653}   <- re-executed, even though it already succeeded
{"task": "transform", "ts": ...504.038, "pid": 25653}   <- redelivery #1
{"task": "transform", "ts": ...504.042, "pid": 25653}   <- redelivery #2, 4.5ms later
                                          [transform's 4s sleep]
{"task": "write",     "ts": ...508.055, "pid": 25653}   <- redelivery #1
{"task": "write",     "ts": ...508.057, "pid": 25653}   <- redelivery #2, 1.8ms later
```

The ~2-12ms gaps between duplicate pairs rule out sequential retries (those
would be seconds apart, gated by the timeout); this is two near-simultaneous
deliveries of the same logical task, consistent with `-P threads`'
concurrency=4 thread pool picking up a redelivered message on two threads
at once, or two independent redelivery paths both firing (visibility_timeout
and something in the reconnect path — **not independently isolated to a
single root cause**; stated as an open question, not a confirmed mechanism,
since Celery/Kombu's internals here weren't traced further than what the
exec log directly shows).

interlock, by contrast, same 3 runs: `fetch: 1, transform: 2, write: 1`
every time — the killed step (transform) re-executes exactly once, driven
by the fenced claim mechanism `test_claim_race.py` already proves prevents
double-claims even under real concurrent workers. `fetch`'s output (already
committed via a fenced `complete()` write before the kill) is never touched
again; `write` only ever runs once, after transform's real completion.

## Timing (not a matched comparison — see caveat)

| system   | time to kill | wait for recovery window | transform re-run | total  |
|----------|--------------|---------------------------|-------------------|--------|
| interlock | ~0.05-0.07s | ~2s (lease_ttl)           | 4s                | **~6.1s** |
| celery    | ~0.07s      | ~10s (8s visibility_timeout + 2s buffer) | 4s    | **~16.0s** |

**Caveat, stated plainly:** this is not "interlock recovers faster than
Celery" as a general property. `lease_ttl` (2s) and `visibility_timeout`
(8s) are both operator-configured values chosen somewhat independently here
— either could be tuned lower. The 8s `visibility_timeout` was picked to be
"short enough to demonstrate, not benchmark-tuned to be unrealistically
fast"; a production deployment might reasonably run it lower or higher
depending on expected task duration. What the timing numbers here actually
show is that both systems' recovery latency is dominated by *whatever value
the operator configured*, not by an inherent speed difference between the
two systems.

## What this actually means

- **Both recover automatically. Neither needs manual data-recovery
  intervention** (both demonstrations auto-start a second worker process,
  which is the normal operational expectation either way — an ops restart,
  not a manual replay of lost work).
- **The real gap is duplicate-execution risk, not "can it recover."**
  interlock's fencing guarantees each step's real side effect happens
  exactly once. Celery's redelivery, correctly configured, still let an
  already-succeeded step (fetch) and the next not-yet-attempted step
  (write) both fire twice. For this benchmark's pure-arithmetic tasks the
  final answer was still correct every time (idempotent by construction:
  re-running `fetch` or `write` produces the same value). A task with a
  real side effect — charge a card, send an email, append a row — would
  not be so lucky: this demonstration would have fired that side effect
  2-3 times per crash.
- **This is the actual differentiator over Celery**, and it's more precise
  than "Celery can't recover": Celery *can* recover, but the granularity
  and exactness of that recovery is materially weaker than what interlock's
  epoch-fencing proves — not asserted from the unit tests alone, but shown
  directly here, side by side, under the same kill.

## Reproducing

```bash
redis-server --port 6379
python3 benchmarks/crash_comparison/run_interlock_crash.py
python3 benchmarks/crash_comparison/run_celery_crash.py
```
