package com.jobqueue.api;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.jobqueue.domain.Job;
import com.jobqueue.domain.JobStatus;

/** Public view of a job. Lease internals are exposed only as the current worker, for debugging. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record JobResponse(
        UUID id,
        String type,
        JobStatus status,
        int priority,
        int attempts,
        int maxRetries,
        long timeoutMs,
        Instant runAt,
        String lastError,
        JsonNode result,
        JsonNode payload,
        String idempotencyKey,
        String worker,
        UUID recurringJobId,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt) {

    public static JobResponse from(Job j) {
        return new JobResponse(j.id(), j.type(), j.status(), j.priority(), j.attempts(), j.maxRetries(),
                j.timeoutMs(), j.runAt(), j.lastError(), j.result(), j.payload(), j.idempotencyKey(),
                j.leaseOwner(), j.recurringJobId(), j.createdAt(), j.startedAt(), j.finishedAt());
    }
}
