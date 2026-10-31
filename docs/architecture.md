# Architecture

## Components and data flow

![Architecture](architecture.svg)

<details>
<summary>Mermaid source (rendered natively by GitHub)</summary>

```mermaid
flowchart LR
    client([Client])

    subgraph apiTier["API tier · JOBQUEUE_ROLE=api · scales on request rate"]
        api["REST API<br/>validate → INSERT → 202 + job id"]
    end

    subgraph pg["Postgres 16 — queue AND result store"]
        jobs[("jobs<br/>status · attempts · lease · result")]
        attempts[("job_attempts<br/>one row per execution")]
        dlq[("dead_letters")]
        ctrl[("queue_control<br/>paused flag")]
        cron[("recurring_jobs")]
    end

    subgraph workerTier["Worker tier · JOBQUEUE_ROLE=worker · scales on queue depth / age"]
        dispatcher["Dispatcher<br/>claims ≤ free slots"]
        pool["Worker pool<br/>virtual threads, N slots"]
        handler["Pluggable JobHandler<br/>per-attempt timeout"]
        reaper["Reaper<br/>recovers expired leases"]
        sched["Cron scheduler<br/>leaderless"]
    end

    client -- "POST /jobs (Idempotency-Key)" --> api
    client -- "GET /jobs/:id · /queue/stats" --> api
    api -- "INSERT status=QUEUED<br/>(commit = durability point)" --> jobs
    api -- "pause / resume" --> ctrl
    api -- "cancel / purge pending" --> jobs

    jobs -- "claim: FOR UPDATE SKIP LOCKED<br/>attempts+1, lease = timeout+grace" --> dispatcher
    dispatcher --> pool --> handler
    pool -- "record attempt" --> attempts

    handler -- "success (fenced)" --> ok["SUCCEEDED + result"]
    handler -- "error / timeout,<br/>retries left (fenced)" --> retry["FAILED<br/>run_at = now + back-off"]
    retry -- "eligible again" --> jobs
    handler -- "retries exhausted /<br/>non-retryable (fenced)" --> dlq

    reaper -- "lease expired =<br/>failed attempt" --> retry
    reaper -. "or, if exhausted" .-> dlq
    sched -- "1 job per fire,<br/>idempotency key" --> jobs
    cron --> sched
    dlq -- "POST /dead-letters/:id/replay" --> jobs
```
</details>

## Job lifecycle

```mermaid
stateDiagram-v2
    [*] --> QUEUED: POST /jobs (run_at = now or later)
    QUEUED --> RUNNING: claimed (attempts += 1, lease granted)
    FAILED --> RUNNING: claimed after back-off
    RUNNING --> SUCCEEDED: handler returned
    RUNNING --> FAILED: error / timeout / lease expired, retries left
    RUNNING --> DEAD_LETTERED: retries exhausted or NonRetryableJobException
    QUEUED --> CANCELLED: cancel / purge
    FAILED --> CANCELLED: cancel / purge
    DEAD_LETTERED --> QUEUED: replay
    SUCCEEDED --> [*]
    CANCELLED --> [*]
```

`FAILED` is intentionally **not terminal**: it means "the last attempt failed and a retry is scheduled", so
callers can see a job is struggling before it gives up. The terminal failure state is `DEAD_LETTERED`.

## One attempt, end to end

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as API
    participant DB as Postgres
    participant W as Worker
    participant H as Handler

    C->>A: POST /api/v1/jobs {type, payload, priority, maxRetries, timeoutMs}
    A->>DB: INSERT ... ON CONFLICT (idempotency_key) DO NOTHING
    A-->>C: 202 Accepted {id, status: QUEUED}
    W->>DB: UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING *
    Note over W,DB: status=RUNNING, attempts=n, lease_owner=W,<br/>lease_expires_at = now + timeout + grace
    W->>H: handle(ctx) on a virtual thread
    alt returns within timeout
        W->>DB: UPDATE SET SUCCEEDED WHERE attempts=n AND lease_owner=W
    else throws (transient) and n ≤ maxRetries
        W->>DB: UPDATE SET FAILED, run_at = now + backoff(n) WHERE attempts=n AND lease_owner=W
    else timeout
        W->>H: interrupt
        W->>DB: same as failure, outcome TIMED_OUT
    else exhausted / permanent
        W->>DB: UPDATE SET DEAD_LETTERED + INSERT INTO dead_letters (same tx)
    end
    C->>A: GET /api/v1/jobs/{id}
    A-->>C: {status, attempts, lastError, result}
```

## Key design decisions

| Decision | Why | Trade-off accepted |
|---|---|---|
| **Postgres as the queue** (`SELECT … FOR UPDATE SKIP LOCKED`) | One durable system for queue + status + results; transactional state changes (job + dead letter + attempt row commit together); no dual-write between a broker and a status DB. | Throughput ceiling of one primary (thousands of jobs/s, not millions). See README → *Handling high load* for the path past that. |
| **Lease per attempt = timeout + grace**, no heartbeats | An attempt can never legitimately run longer than its timeout, so the lease is known at claim time. Removes a whole heartbeat subsystem. | A crashed worker's job waits up to `timeout + grace` before recovery. |
| **`attempts` incremented at claim, used as a fencing token** | Bounds total executions to `maxRetries + 1`, including executions a crashed worker never reported. Every later write is `WHERE attempts = n AND lease_owner = w`, so a zombie cannot overwrite a newer attempt. | A crash consumes a retry. |
| **Database clock for all scheduling** (`now()` in SQL) | API nodes, workers and the reaper may have skewed clocks; lease expiry and `run_at` are compared on one clock. | `runAt` from the client is converted to a delay on the API node (skew there only shifts the start time). |
| **Claim only as many jobs as free slots** | A worker never holds leases on jobs it is not running (no claimed-but-idle head-of-line blocking; a crash strands at most `concurrency` jobs). | One claim query per slot-batch; mitigated by `batchSize`. |
| **One artifact, role selected by `JOBQUEUE_ROLE`** | API and workers scale independently but share code, schema and handler registry. | Handler classes are on the API classpath too (used only to validate job types). |
| **Leaderless reaper and cron scheduler** | Every worker runs them; `SKIP LOCKED` ensures each expired lease / cron fire is processed once. No ZooKeeper, no leader election. | Some redundant polling across nodes (cheap index scans). |
| **Polling, not LISTEN/NOTIFY** | Simple and robust across connection poolers; latency bounded by `pollInterval` only when idle (busy workers loop immediately). | Up to `pollInterval` (500 ms default) pick-up latency on an idle queue. |
