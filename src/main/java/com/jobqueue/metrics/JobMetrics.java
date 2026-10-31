package com.jobqueue.metrics;

import java.time.Duration;
import java.time.Instant;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.springframework.stereotype.Component;

/**
 * Job-level metrics. Exposed at /actuator/prometheus. Useful queries:
 * <pre>
 *   p95 latency     histogram_quantile(0.95, sum by (le) (rate(jobqueue_job_end_to_end_seconds_bucket[5m])))
 *   dead-letter rate sum(rate(jobqueue_jobs_dead_lettered_total[5m]))
 *   DLQ ratio        sum(rate(jobqueue_jobs_dead_lettered_total[5m])) / sum(rate(jobqueue_jobs_finished_total[5m]))
 * </pre>
 * p50/p95 are also published client-side so they are visible in /actuator/metrics without Prometheus.
 */
@Component
public class JobMetrics {

    private final MeterRegistry registry;

    public JobMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /** Time a job spent eligible-but-unclaimed (run_at → claimed). Grows when workers can't keep up. */
    public void queueWait(String type, Instant runAt, Instant startedAt) {
        if (runAt != null && startedAt != null && !startedAt.isBefore(runAt)) {
            timer("jobqueue.job.queue_wait", "Time from eligible to claimed", type, null)
                    .record(Duration.between(runAt, startedAt));
        }
    }

    /** Handler execution time for one attempt, tagged by outcome. */
    public void execution(String type, String outcome, long nanos) {
        timer("jobqueue.job.execution", "Handler execution time per attempt", type, outcome)
                .record(Duration.ofNanos(nanos));
    }

    /** Submission → terminal state, including all retries and back-off. */
    public void endToEnd(String type, String outcome, Instant createdAt, Instant finishedAt) {
        if (createdAt != null && finishedAt != null) {
            timer("jobqueue.job.end_to_end", "Submission to terminal state", type, outcome)
                    .record(Duration.between(createdAt, finishedAt));
        }
    }

    public void finished(String type, String outcome) {
        Counter.builder("jobqueue.jobs.finished")
                .description("Jobs reaching a terminal state")
                .tag("type", type).tag("outcome", outcome)
                .register(registry).increment();
    }

    public void retried(String type, String reason) {
        Counter.builder("jobqueue.jobs.retried")
                .description("Failed attempts scheduled for retry")
                .tag("type", type).tag("reason", reason)
                .register(registry).increment();
    }

    public void deadLettered(String type, String reason) {
        Counter.builder("jobqueue.jobs.dead_lettered")
                .description("Jobs moved to the dead-letter store")
                .tag("type", type).tag("reason", reason)
                .register(registry).increment();
    }

    /** A worker finished an attempt whose lease it had already lost. Its outcome was discarded (fenced). */
    public void staleCompletion(String type) {
        Counter.builder("jobqueue.jobs.stale_completions")
                .description("Attempt outcomes rejected by the fencing check (duplicate execution detected)")
                .tag("type", type)
                .register(registry).increment();
    }

    public void leaseExpired(String type) {
        Counter.builder("jobqueue.jobs.lease_expired")
                .description("Attempts recovered by the reaper after the worker's lease expired")
                .tag("type", type)
                .register(registry).increment();
    }

    private Timer timer(String name, String description, String type, String outcome) {
        Timer.Builder b = Timer.builder(name)
                .description(description)
                .tag("type", type)
                .publishPercentiles(0.5, 0.95)
                .publishPercentileHistogram();
        if (outcome != null) {
            b.tag("outcome", outcome);
        }
        return b.register(registry);
    }
}
