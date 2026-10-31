package com.jobqueue.scheduling;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.jobqueue.config.JobQueueProperties;
import com.jobqueue.config.Roles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/** Runs the recurring-job tick on every worker node; row locks (SKIP LOCKED) make it leaderless and safe. */
@Component
@ConditionalOnExpression(Roles.WORKER_ENABLED)
public class RecurringJobScheduler implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RecurringJobScheduler.class);
    private static final int BATCH = 100;

    private final RecurringJobService service;
    private final long intervalMs;
    private ScheduledExecutorService executor;
    private volatile boolean running;

    public RecurringJobScheduler(RecurringJobService service, JobQueueProperties props) {
        this.service = service;
        this.intervalMs = props.scheduler().recurringPollInterval().toMillis();
    }

    @Override
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(
                r -> Thread.ofPlatform().name("recurring-scheduler").daemon().unstarted(r));
        executor.scheduleWithFixedDelay(this::tick, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        running = true;
    }

    private void tick() {
        try {
            while (service.tick(BATCH) == BATCH) {
                // drain a large backlog in batches
            }
        } catch (Exception e) {
            log.error("Recurring scheduler tick failed: {}", e.toString());
        }
    }

    @Override
    public void stop() {
        running = false;
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
