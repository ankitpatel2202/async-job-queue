package com.jobqueue.domain;

import java.util.Map;

/**
 * @param depth               jobs eligible to run right now (QUEUED/FAILED with run_at &lt;= now)
 * @param delayed             pending jobs not yet eligible (scheduled for later or waiting back-off)
 * @param running             jobs currently leased by a worker
 * @param oldestReadyAgeSeconds how long the oldest eligible job has waited; the best "are we keeping up" signal
 * @param byStatus            count of every job by status
 * @param paused              whether dispatch is paused cluster-wide
 */
public record QueueStats(
        long depth,
        long delayed,
        long running,
        double oldestReadyAgeSeconds,
        Map<String, Long> byStatus,
        boolean paused) {
}
