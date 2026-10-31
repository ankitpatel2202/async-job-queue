# Delivery semantics: where at-least-once holds, and where it doesn't

**Summary:** every accepted job is *attempted* at least once and at most `maxRetries + 1` times. A job's
*recorded outcome* is written exactly once (by the owner of the current attempt). Handler *side effects* are
at-least-once, so handlers must be idempotent; the design gives them the tools (job id + fencing token)
and bounds and detects duplicates.

## 1. What is guaranteed

| Stage | Guarantee | Mechanism |
|---|---|---|
| Submission | **Durable once acknowledged.** A `202` is returned only after the `INSERT` commits. | Synchronous insert; no in-memory buffer between the API and the store. |
| Submission retries by the client | **Exactly one job per `Idempotency-Key`.** | `UNIQUE(idempotency_key)` + `INSERT … ON CONFLICT DO NOTHING`; the replay returns `200` with the original job. |
| Claim | **One live lease per job.** Two workers never hold the same attempt. | `FOR UPDATE SKIP LOCKED` inside one `UPDATE … RETURNING`. Proven by `QueueStoreIT.concurrentWorkersNeverClaimTheSameJob` (8 threads × 300 jobs). |
| Worker crash / OOM / kill -9 / network partition | **The job is retried.** | Lease expires at `timeout + grace`; the reaper converts it into a failed attempt (`LEASE_EXPIRED`) and schedules a retry, or dead-letters it if the budget is spent. |
| Graceful shutdown (deploy) | **In-flight work finishes or is retried.** | Stop claiming → wait `shutdownGrace` → interrupt; unfinished leases expire and are reaped. |
| Failure past max retries | **Never silently dropped.** | Same transaction: job → `DEAD_LETTERED` and row in `dead_letters`. Replayable via API. |
| Outcome recording | **At most one writer per attempt; stale writers are rejected.** | Every post-claim write is fenced: `WHERE status='RUNNING' AND attempts=:n AND lease_owner=:w`. |

## 2. Where at-least-once does NOT hold (by design or by limitation)

1. **Bounded, not unbounded, retries.** At-least-once applies to *attempts*. A job that keeps failing stops
   after `maxRetries + 1` attempts and moves to the dead-letter store. It is not lost, but it is not retried
   forever without an operator replay.
2. **Cancelled / purged jobs.** Operator intent wins: a cancelled pending job is never executed.
3. **Running jobs cannot be cancelled.** Cancellation is only for pending jobs. Cooperative cancellation of
   running jobs (a `cancel_requested` flag checked by handlers) is a documented next step.
4. **Recurring-job misfires collapse.** If every worker is down across several cron fire times, they are merged
   into one run when the scheduler recovers. Each individual fire is enqueued at most once (idempotency key
   `cron:<definition>:<fireTime>`), so a scheduler crash between insert and advance cannot double-enqueue.
5. **Durability is Postgres durability.** Loss of an un-replicated primary loses jobs. Production should use a
   managed cluster with a synchronous standby (DigitalOcean Managed Postgres HA) if zero-loss is required.
6. **The handler's own external side effects** (calling an API, sending an email) are at-least-once. See §3.

## 3. Where duplicate execution can happen, and how it is bounded and detected

Duplicates happen when a handler's side effect occurred but the outcome was never recorded:

| Scenario | What happens |
|---|---|
| Worker crashes after the side effect, before `COMMIT` | Lease expires → reaper → attempt *n+1* re-runs the handler. |
| Handler ignores interruption after its timeout | The worker records `TIMED_OUT` and a new attempt may start while the old thread is still running. |
| Worker is partitioned from Postgres for longer than its lease | The reaper retries elsewhere while the original may still be executing. |
| Commit succeeded but the worker crashed before knowing | Not a duplicate: the state is already terminal, nothing re-runs it. |

**Bounded:** `attempts` is incremented *at claim time*, inside the claim transaction. Every execution,
including ones whose worker died silently, consumes an attempt. Therefore the handler runs **at most
`maxRetries + 1` times per job**, regardless of crashes. (A replay from the dead-letter store grants a new,
explicit budget.)

**Detected:**

- `job_attempts` stores one row per attempt with `worker_id` and outcome. More than one row means the handler
  ran more than once; an outcome of `LEASE_EXPIRED` or `STALE` marks the suspicious ones.
- The fence rejects late results; each rejection increments `jobqueue_jobs_stale_completions_total` and writes
  a `STALE` attempt outcome. Alert on this metric being non-zero.
- `jobqueue_jobs_lease_expired_total` counts crash recoveries.

**Made harmless (handler contract):** `JobContext` carries `jobId` (stable across attempts → idempotency key
for downstream APIs, e.g. `Idempotency-Key: <jobId>` to a payment provider, or `INSERT … ON CONFLICT (job_id)`)
and `attempt` (monotonic, never reused, even across replays → fencing token for compare-and-set writes such as
`UPDATE … WHERE last_attempt < :attempt`).

## 4. Why not exactly-once?

Exactly-once execution of arbitrary side effects is impossible without the side effect participating in the
same transaction as the queue. When the handler's only effect is a write to this same Postgres database, it
*can* be made exactly-once by performing that write in the completion transaction (outbox-style). That is a
natural extension of `JobLifecycle.recordSuccess`, but out of scope here.
