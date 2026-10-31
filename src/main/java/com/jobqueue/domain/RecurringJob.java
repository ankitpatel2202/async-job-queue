package com.jobqueue.domain;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

public record RecurringJob(
        UUID id,
        String name,
        String cron,
        String zone,
        String type,
        JsonNode payload,
        int priority,
        int maxRetries,
        long timeoutMs,
        boolean enabled,
        Instant nextRunAt,
        Instant lastEnqueuedAt,
        Instant createdAt) {
}
