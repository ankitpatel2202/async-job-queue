package com.jobqueue.worker;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.jobqueue.config.JobQueueProperties;
import com.jobqueue.config.Roles;
import com.jobqueue.domain.Job;
import com.jobqueue.metrics.JobMetrics;
import com.jobqueue.service.JobLifecycle;
import com.jobqueue.service.JobLifecycle.AttemptOutcome;
import com.jobqueue.store.JobRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Pulls jobs from the queue and executes them.
 *
 * <ul>
 *   <li>One dispatcher thread claims at most as many jobs as there are free slots, so a worker never holds a
 *       lease on a job it is not executing (no head-of-line blocking of claimed-but-idle jobs).</li>
 *   <li>Each attempt runs on a virtual thread; the handler runs on its own virtual thread so the attempt
 *       timeout can be enforced by interrupting it.</li>
 *   <li>The reaper recovers leases of crashed workers. It runs on every worker node; SKIP LOCKED makes that safe.</li>
 *   <li>Graceful shutdown: stop claiming, let in-flight attempts finish for {@code shutdownGrace}, then
 *       interrupt the rest. Their leases expire and the reaper retries them elsewhere.</li>
 * </ul>
 */
@Component
@ConditionalOnExpression(Roles.WORKER_ENABLED)
public class WorkerPool implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);
    private static final int REAP_BATCH = 100;
    private static final long PAUSE_CHECK_INTERVAL_MS = 1000;

    private final JobLifecycle lifecycle;
    private final JobRepository jobs;
    private final HandlerRegistry handlers;
    private final JobMetrics metrics;
    private final JobQueueProperties.Worker config;
    private final String workerId;

    private final Semaphore slots;
    private final AtomicInteger busy = new AtomicInteger();
    private final ExecutorService attempts = Executors.newVirtualThreadPerTaskExecutor();
    private final ExecutorService handlerThreads = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService maintenance = Executors.newSingleThreadScheduledExecutor(
            r -> Thread.ofPlatform().name("job-reaper").daemon().unstarted(r));

    private volatile boolean running;
    private volatile Thread dispatcher;
    private volatile boolean pausedCache;
    private long lastPauseCheck;

    public WorkerPool(JobLifecycle lifecycle, JobRepository jobs, HandlerRegistry handlers, JobMetrics metrics,
                      JobQueueProperties props, MeterRegistry registry) {
        this.lifecycle = lifecycle;
        this.jobs = jobs;
        this.handlers = handlers;
        this.metrics = metrics;
        this.config = props.worker();
        this.workerId = config.resolvedId();
        this.slots = new Semaphore(config.concurrency());

        Gauge.builder("jobqueue.workers.busy", busy, AtomicInteger::get)
                .description("Attempts currently executing in this process").register(registry);
        Gauge.builder("jobqueue.workers.capacity", config, c -> c.concurrency())
                .description("Configured worker concurrency in this process").register(registry);
        Gauge.builder("jobqueue.workers.utilization", this, p -> (double) p.busy.get() / p.config.concurrency())
                .description("busy / capacity, 0..1").register(registry);
    }

    public String workerId() {
        return workerId;
    }

    public int busy() {
        return busy.get();
    }

    // ------------------------------------------------------------------ lifecycle

    @Override
    public void start() {
        running = true;
        dispatcher = Thread.ofPlatform().name("job-dispatcher").start(this::dispatchLoop);
        long reapMs = config.reaperInterval().toMillis();
        maintenance.scheduleWithFixedDelay(this::reap, reapMs, reapMs, TimeUnit.MILLISECONDS);
        log.info("Worker {} started: concurrency={}, batchSize={}, poll={}, handlers={}",
                workerId, config.concurrency(), config.batchSize(), config.pollInterval(), handlers.types());
    }

    @Override
    public void stop() {
        running = false;
        maintenance.shutdownNow();
        Thread d = dispatcher;
        if (d != null) {
            try {
                d.join(config.pollInterval().toMillis() + 5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        attempts.shutdown();
        try {
            if (!attempts.awaitTermination(config.shutdownGrace().toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("Worker {}: {} attempt(s) still running after shutdown grace; abandoning leases",
                        workerId, busy.get());
                attempts.shutdownNow();
            }
        } catch (InterruptedException e) {
            attempts.shutdownNow();
            Thread.currentThread().interrupt();
        }
        handlerThreads.shutdownNow();
        log.info("Worker {} stopped", workerId);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    // ------------------------------------------------------------------ dispatch

    private void dispatchLoop() {
        long pollMs = config.pollInterval().toMillis();
        while (running) {
            try {
                if (paused()) {
                    Thread.sleep(pollMs);
                    continue;
                }
                int free = slots.availablePermits();
                if (free == 0) {
                    // Wait for a slot to free up rather than spinning.
                    if (slots.tryAcquire(pollMs, TimeUnit.MILLISECONDS)) {
                        slots.release();
                    }
                    continue;
                }
                List<Job> batch = lifecycle.claim(workerId, Math.min(free, config.batchSize()));
                if (batch.isEmpty()) {
                    Thread.sleep(pollMs);
                    continue;
                }
                for (Job job : batch) {
                    slots.acquireUninterruptibly(); // never blocks: only this thread acquires, and free >= batch
                    attempts.execute(() -> {
                        try {
                            runAttempt(job);
                        } finally {
                            slots.release();
                        }
                    });
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // Typically the database is unavailable. Back off; claimed-but-undispatched jobs (none, since
                // claim and dispatch are in the same iteration) would be recovered by lease expiry anyway.
                log.error("Worker {} dispatch error: {}", workerId, e.toString());
                sleepQuietly(pollMs);
            }
        }
    }

    private boolean paused() {
        long now = System.currentTimeMillis();
        if (now - lastPauseCheck >= PAUSE_CHECK_INTERVAL_MS) {
            pausedCache = jobs.isPaused();
            lastPauseCheck = now;
        }
        return pausedCache;
    }

    // ------------------------------------------------------------------ execution

    void runAttempt(Job job) {
        MDC.put("jobId", job.id().toString());
        MDC.put("jobType", job.type());
        MDC.put("attempt", Integer.toString(job.attempts()));
        busy.incrementAndGet();
        metrics.queueWait(job.type(), job.runAt(), job.startedAt());
        long start = System.nanoTime();
        Future<JsonNode> future = null;
        try {
            Optional<JobHandler> handler = handlers.find(job.type());
            if (handler.isEmpty()) {
                lifecycle.recordFailure(job, workerId, "no handler registered for type '" + job.type() + "'",
                        true, AttemptOutcome.FAILED);
                return;
            }
            JobContext ctx = new JobContext(job.id(), job.type(), job.payload(), job.attempts(),
                    job.maxRetries() + 1);
            future = handlerThreads.submit(() -> handler.get().handle(ctx));

            JsonNode result;
            try {
                result = future.get(job.timeoutMs(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                future.cancel(true); // interrupt the handler; a non-cooperative handler may keep running (fenced)
                metrics.execution(job.type(), "timed_out", System.nanoTime() - start);
                lifecycle.recordFailure(job, workerId,
                        "attempt " + job.attempts() + " timed out after " + job.timeoutMs() + " ms",
                        false, AttemptOutcome.TIMED_OUT);
                return;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                boolean permanent = cause instanceof NonRetryableJobException;
                metrics.execution(job.type(), "failed", System.nanoTime() - start);
                lifecycle.recordFailure(job, workerId, describe(cause), permanent, AttemptOutcome.FAILED);
                return;
            }
            metrics.execution(job.type(), "succeeded", System.nanoTime() - start);
            lifecycle.recordSuccess(job, workerId, result == null ? NullNode.getInstance() : result);
        } catch (InterruptedException e) {
            // Shutdown past the grace period. Leave the job RUNNING: its lease expires and the reaper retries it.
            if (future != null) {
                future.cancel(true);
            }
            Thread.currentThread().interrupt();
            log.warn("Attempt interrupted by shutdown; lease will expire and the job will be retried");
        } catch (RuntimeException e) {
            // Could not record the outcome (e.g. database down). Same recovery path: lease expiry.
            log.error("Failed to record outcome; relying on lease expiry: {}", e.toString());
        } finally {
            busy.decrementAndGet();
            MDC.clear();
        }
    }

    private void reap() {
        try {
            int n;
            do {
                n = lifecycle.reapExpiredLeases(REAP_BATCH);
                if (n > 0) {
                    log.warn("Reaper recovered {} expired lease(s)", n);
                }
            } while (n == REAP_BATCH && running);
        } catch (Exception e) {
            log.error("Reaper error: {}", e.toString());
        }
    }

    private static String describe(Throwable t) {
        String msg = t.getMessage();
        return t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
