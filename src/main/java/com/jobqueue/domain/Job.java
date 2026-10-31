package com.jobqueue.domain;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

public record Job(
        UUID id,
        String type,
        JsonNode payload,
        int priority,
        JobStatus status,
        int attempts,
        int maxRetries,
        long timeoutMs,
        Instant runAt,
        String idempotencyKey,
        String leaseOwner,
        Instant leaseExpiresAt,
        String lastError,
        JsonNode result,
        UUID recurringJobId,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Instant finishedAt) {
}
