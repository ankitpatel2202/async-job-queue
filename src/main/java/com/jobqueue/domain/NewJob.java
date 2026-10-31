package com.jobqueue.domain;

import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

/** A validated, fully-defaulted job ready to be inserted. */
public record NewJob(
        String type,
        JsonNode payload,
        int priority,
        int maxRetries,
        long timeoutMs,
        long delayMs,
        String idempotencyKey,
        UUID recurringJobId) {
}
