package com.jobqueue.config;

import java.time.Duration;
import java.util.UUID;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * All tunables live here so the same artifact can run as an API node, a worker node, or both.
 */
@ConfigurationProperties(prefix = "jobqueue")
public record JobQueueProperties(
        @DefaultValue("all") Role role,
        @DefaultValue Worker worker,
        @DefaultValue Retry retry,
        @DefaultValue Defaults defaults,
        @DefaultValue Scheduler scheduler,
        @DefaultValue("262144") int maxPayloadBytes) {

    public enum Role {
        ALL, API, WORKER;

        public boolean runsWorkers() {
            return this != API;
        }
    }

    /**
     * @param id            stable worker-instance id; defaults to a random id per process
     * @param concurrency   max jobs executing at once in this process
     * @param batchSize     max jobs claimed per poll
     * @param pollInterval  sleep when the queue is empty (or paused)
     * @param leaseGrace    lease = attempt timeout + grace; covers commit latency and GC pauses
     * @param reaperInterval how often expired leases are recovered
     * @param shutdownGrace how long in-flight jobs may finish on shutdown before leases are abandoned
     */
    public record Worker(
            @DefaultValue("") String id,
            @DefaultValue("8") int concurrency,
            @DefaultValue("8") int batchSize,
            @DefaultValue("500ms") Duration pollInterval,
            @DefaultValue("5s") Duration leaseGrace,
            @DefaultValue("5s") Duration reaperInterval,
            @DefaultValue("30s") Duration shutdownGrace) {

        public String resolvedId() {
            if (id != null && !id.isBlank()) {
                return id;
            }
            String host = System.getenv().getOrDefault("HOSTNAME", "worker");
            return host + "-" + UUID.randomUUID().toString().substring(0, 8);
        }
    }

    /** Exponential back-off: delay(n) = min(maxDelay, baseDelay * multiplier^(n-1)), optionally jittered. */
    public record Retry(
            @DefaultValue("1s") Duration baseDelay,
            @DefaultValue("5m") Duration maxDelay,
            @DefaultValue("2.0") double multiplier,
            @DefaultValue("true") boolean jitter) {
    }

    /** Values applied when a submission omits them. */
    public record Defaults(
            @DefaultValue("0") int priority,
            @DefaultValue("3") int maxRetries,
            @DefaultValue("30s") Duration timeout) {
    }

    public record Scheduler(
            @DefaultValue("1s") Duration recurringPollInterval,
            @DefaultValue("5s") Duration statsRefreshInterval) {
    }
}
