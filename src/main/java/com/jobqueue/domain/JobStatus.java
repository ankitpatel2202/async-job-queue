package com.jobqueue.domain;

/**
 * Job lifecycle.
 *
 * <pre>
 * QUEUED ──claim──▶ RUNNING ──ok──────────────▶ SUCCEEDED
 *    ▲                 │
 *    │                 ├─error / timeout / lease expired, retries left ──▶ FAILED (waiting back-off) ──claim──▶ RUNNING
 *    │                 └─retries exhausted or non-retryable ───────────▶ DEAD_LETTERED ──replay──▶ QUEUED
 *    └── QUEUED / FAILED ──cancel / purge──▶ CANCELLED
 * </pre>
 *
 * FAILED is deliberately not terminal: it means "the last attempt failed and a retry is scheduled".
 * The terminal failure state is DEAD_LETTERED.
 */
public enum JobStatus {
    QUEUED,
    RUNNING,
    FAILED,
    SUCCEEDED,
    DEAD_LETTERED,
    CANCELLED;

    public boolean isPending() {
        return this == QUEUED || this == FAILED;
    }

    public boolean isTerminal() {
        return this == SUCCEEDED || this == DEAD_LETTERED || this == CANCELLED;
    }
}
