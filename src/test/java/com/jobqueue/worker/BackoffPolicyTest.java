package com.jobqueue.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class BackoffPolicyTest {

    @Test
    void growsExponentiallyWithoutJitter() {
        BackoffPolicy p = new BackoffPolicy(100, 10_000, 2.0, false, () -> 0.5);
        assertThat(p.delayMs(1)).isEqualTo(100);
        assertThat(p.delayMs(2)).isEqualTo(200);
        assertThat(p.delayMs(3)).isEqualTo(400);
        assertThat(p.delayMs(4)).isEqualTo(800);
    }

    @Test
    void isCappedAtMaxDelay() {
        BackoffPolicy p = new BackoffPolicy(100, 1_000, 2.0, false, () -> 0.5);
        assertThat(p.delayMs(10)).isEqualTo(1_000);
        assertThat(p.delayMs(10_000)).isEqualTo(1_000); // pow() overflow to Infinity is still capped
    }

    @Test
    void jitterStaysWithinHalfToFullDelay() {
        BackoffPolicy low = new BackoffPolicy(1_000, 60_000, 2.0, true, () -> 0.0);
        BackoffPolicy high = new BackoffPolicy(1_000, 60_000, 2.0, true, () -> 0.999_999);
        assertThat(low.delayMs(3)).isEqualTo(2_000);       // d = 4000 → lower bound d/2
        assertThat(high.delayMs(3)).isBetween(3_999L, 4_000L); // upper bound d
    }

    @Test
    void retryDecisionHonoursBudgetAndPermanence() {
        // maxRetries = 2 → 3 attempts total
        assertThat(BackoffPolicy.shouldRetry(1, 2, false)).isTrue();
        assertThat(BackoffPolicy.shouldRetry(2, 2, false)).isTrue();
        assertThat(BackoffPolicy.shouldRetry(3, 2, false)).isFalse();
        assertThat(BackoffPolicy.shouldRetry(1, 2, true)).isFalse();
        assertThat(BackoffPolicy.shouldRetry(1, 0, false)).isFalse();
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> new BackoffPolicy(100, 50, 2.0, false, () -> 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffPolicy(100, 500, 0.5, false, () -> 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
