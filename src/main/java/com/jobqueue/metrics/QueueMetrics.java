package com.jobqueue.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.jobqueue.config.JobQueueProperties;
import com.jobqueue.domain.QueueStats;
import com.jobqueue.store.JobRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Queue-level gauges, refreshed from the database on an interval (never on scrape, so a Prometheus scrape
 * cannot load the database). Gauges:
 * <ul>
 *   <li>{@code jobqueue.queue.depth{state=ready|delayed|running}}</li>
 *   <li>{@code jobqueue.queue.oldest_ready_age_seconds} – the autoscaling signal</li>
 *   <li>{@code jobqueue.dead_letters.pending} – dead letters not yet replayed</li>
 * </ul>
 */
@Component
public class QueueMetrics implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(QueueMetrics.class);

    private final JobRepository jobs;
    private final long refreshMs;
    private final Map<String, AtomicLong> depth = new ConcurrentHashMap<>();
    private final AtomicLong oldestAgeMillis = new AtomicLong();
    private final AtomicLong deadLetters = new AtomicLong();
    private ScheduledExecutorService executor;
    private volatile boolean running;

    public QueueMetrics(JobRepository jobs, JobQueueProperties props, MeterRegistry registry) {
        this.jobs = jobs;
        this.refreshMs = props.scheduler().statsRefreshInterval().toMillis();
        for (String state : new String[] {"ready", "delayed", "running"}) {
            AtomicLong holder = depth.computeIfAbsent(state, s -> new AtomicLong());
            Gauge.builder("jobqueue.queue.depth", holder, AtomicLong::get)
                    .description("Jobs by queue state").tag("state", state).register(registry);
        }
        Gauge.builder("jobqueue.queue.oldest_ready_age_seconds", oldestAgeMillis, a -> a.get() / 1000.0)
                .description("Age of the oldest job eligible to run").register(registry);
        Gauge.builder("jobqueue.dead_letters.pending", deadLetters, AtomicLong::get)
                .description("Dead-lettered jobs not yet replayed").register(registry);
    }

    void refresh() {
        try {
            QueueStats s = jobs.stats();
            depth.get("ready").set(s.depth());
            depth.get("delayed").set(s.delayed());
            depth.get("running").set(s.running());
            oldestAgeMillis.set((long) (s.oldestReadyAgeSeconds() * 1000));
            deadLetters.set(jobs.deadLetterCount());
        } catch (Exception e) {
            log.warn("Queue metrics refresh failed: {}", e.toString());
        }
    }

    @Override
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(
                r -> Thread.ofPlatform().name("queue-metrics").daemon().unstarted(r));
        executor.scheduleWithFixedDelay(this::refresh, 0, refreshMs, TimeUnit.MILLISECONDS);
        running = true;
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
