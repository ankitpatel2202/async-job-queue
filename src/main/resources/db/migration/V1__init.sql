-- Jobs: the queue AND the result store. One row per job, mutated through its lifecycle.
CREATE TABLE jobs (
    id               uuid        PRIMARY KEY,
    type             text        NOT NULL,
    payload          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    priority         int         NOT NULL DEFAULT 0,
    status           text        NOT NULL,
    attempts         int         NOT NULL DEFAULT 0,   -- also the fencing token of the current attempt
    max_retries      int         NOT NULL,
    timeout_ms       bigint      NOT NULL,
    run_at           timestamptz NOT NULL DEFAULT now(), -- not eligible before this (delay / back-off)
    idempotency_key  text        UNIQUE,
    lease_owner      text,
    lease_expires_at timestamptz,
    last_error       text,
    result           jsonb,
    recurring_job_id uuid,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    started_at       timestamptz,
    finished_at      timestamptz,
    CONSTRAINT jobs_status_chk CHECK (status IN
        ('QUEUED', 'RUNNING', 'FAILED', 'SUCCEEDED', 'DEAD_LETTERED', 'CANCELLED')),
    CONSTRAINT jobs_retries_chk CHECK (max_retries >= 0),
    CONSTRAINT jobs_timeout_chk CHECK (timeout_ms > 0)
);

-- Hot path: the claim query. Partial index keeps it small: only pending rows.
CREATE INDEX jobs_ready_idx  ON jobs (priority DESC, run_at, created_at) WHERE status IN ('QUEUED', 'FAILED');
-- Reaper: find RUNNING jobs whose lease expired.
CREATE INDEX jobs_lease_idx  ON jobs (lease_expires_at) WHERE status = 'RUNNING';
CREATE INDEX jobs_status_idx ON jobs (status, created_at);

-- Attempt history: one row per execution attempt. Makes duplicate execution visible.
CREATE TABLE job_attempts (
    job_id      uuid        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt     int         NOT NULL,
    worker_id   text        NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    outcome     text,       -- SUCCEEDED | FAILED | TIMED_OUT | LEASE_EXPIRED | STALE
    error       text,
    PRIMARY KEY (job_id, attempt)
);

-- Dead-letter store: jobs that exhausted retries or failed permanently. Never silently dropped.
CREATE TABLE dead_letters (
    job_id           uuid        PRIMARY KEY REFERENCES jobs (id) ON DELETE CASCADE,
    type             text        NOT NULL,
    payload          jsonb       NOT NULL,
    attempts         int         NOT NULL,
    last_error       text,
    reason           text        NOT NULL,   -- RETRIES_EXHAUSTED | NON_RETRYABLE
    dead_lettered_at timestamptz NOT NULL DEFAULT now(),
    replayed_at      timestamptz
);
CREATE INDEX dead_letters_time_idx ON dead_letters (dead_lettered_at DESC);

-- Cluster-wide operational switch: workers stop claiming while paused.
CREATE TABLE queue_control (
    id         int         PRIMARY KEY CHECK (id = 1),
    paused     boolean     NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO queue_control (id, paused) VALUES (1, false);

-- Recurring (cron) job definitions. A scheduler materialises each fire into a normal job.
CREATE TABLE recurring_jobs (
    id               uuid        PRIMARY KEY,
    name             text        NOT NULL UNIQUE,
    cron             text        NOT NULL,
    zone             text        NOT NULL DEFAULT 'UTC',
    type             text        NOT NULL,
    payload          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    priority         int         NOT NULL DEFAULT 0,
    max_retries      int         NOT NULL,
    timeout_ms       bigint      NOT NULL,
    enabled          boolean     NOT NULL DEFAULT true,
    next_run_at      timestamptz NOT NULL,
    last_enqueued_at timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX recurring_due_idx ON recurring_jobs (next_run_at) WHERE enabled;
