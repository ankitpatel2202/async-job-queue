package com.jobqueue.domain;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

public record DeadLetter(
        UUID jobId,
        String type,
        JsonNode payload,
        int attempts,
        String lastError,
        String reason,
        Instant deadLetteredAt,
        Instant replayedAt) {
}
