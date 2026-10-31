package com.jobqueue.worker;

import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * What a handler sees for one attempt.
 *
 * @param attempt 1-based and monotonic for the lifetime of the job; usable as a fencing token
 */
public record JobContext(UUID jobId, String type, JsonNode payload, int attempt, int maxAttempts) {

    public boolean isLastAttempt() {
        return attempt >= maxAttempts;
    }
}
