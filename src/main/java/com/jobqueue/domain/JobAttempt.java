package com.jobqueue.domain;

import java.time.Instant;

public record JobAttempt(
        int attempt,
        String workerId,
        Instant startedAt,
        Instant finishedAt,
        String outcome,
        String error) {
}
