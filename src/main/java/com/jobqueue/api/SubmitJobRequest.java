package com.jobqueue.api;

import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Job submission. Only {@code type} is required.
 *
 * @param priority   0 (lowest) – 9 (highest); higher priorities are always claimed first
 * @param maxRetries retries after the first attempt (total attempts = maxRetries + 1)
 * @param timeoutMs  per-attempt timeout
 * @param runAt      run no earlier than this instant (mutually exclusive with delayMs)
 * @param delayMs    run no earlier than now + delayMs
 */
public record SubmitJobRequest(
        @NotBlank @Size(max = 100) String type,
        JsonNode payload,
        @Min(0) @Max(9) Integer priority,
        @Min(0) @Max(25) Integer maxRetries,
        @Min(100) @Max(3_600_000) Long timeoutMs,
        Instant runAt,
        @Min(0) @Max(31_536_000_000L) Long delayMs) {
}
