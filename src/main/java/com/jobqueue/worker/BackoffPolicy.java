package com.jobqueue.worker;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

import com.jobqueue.config.JobQueueProperties;

/**
 * Exponential back-off with "equal jitter": the delay after attempt n is drawn from [d/2, d] where
 * d = min(maxDelay, base * multiplier^(n-1)). Jitter spreads retries of jobs that failed together
 * (e.g. a downstream outage) so they do not hammer the dependency in lock-step when it recovers.
 */
public class BackoffPolicy {

    private final long baseMs;
    private final long maxMs;
    private final double multiplier;
    private final boolean jitter;
    private final DoubleSupplier random;

    public BackoffPolicy(JobQueueProperties.Retry retry) {
        this(retry.baseDelay().toMillis(), retry.maxDelay().toMillis(), retry.multiplier(), retry.jitter(),
                () -> ThreadLocalRandom.current().nextDouble());
    }

    public BackoffPolicy(long baseMs, long maxMs, double multiplier, boolean jitter, DoubleSupplier random) {
        if (baseMs < 0 || maxMs < baseMs || multiplier < 1.0) {
            throw new IllegalArgumentException("invalid back-off configuration");
        }
        this.baseMs = baseMs;
        this.maxMs = maxMs;
        this.multiplier = multiplier;
        this.jitter = jitter;
        this.random = random;
    }

    /** @param failedAttempt the 1-based attempt number that just failed */
    public long delayMs(int failedAttempt) {
        int exponent = Math.max(0, failedAttempt - 1);
        double raw = baseMs * Math.pow(multiplier, exponent); // may overflow to Infinity; min() handles it
        long capped = (long) Math.min(maxMs, raw);
        if (!jitter || capped == 0) {
            return capped;
        }
        long half = capped / 2;
        return half + (long) (random.getAsDouble() * (capped - half));
    }

    /** Pure decision: retry while the job still has attempts left and the failure is transient. */
    public static boolean shouldRetry(int attemptsSoFar, int maxRetries, boolean nonRetryable) {
        return !nonRetryable && attemptsSoFar <= maxRetries;
    }
}
