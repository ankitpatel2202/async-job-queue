package com.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.TextNode;
import com.jobqueue.domain.Job;
import com.jobqueue.domain.JobAttempt;
import com.jobqueue.domain.JobStatus;
import com.jobqueue.domain.NewJob;
import com.jobqueue.service.JobLifecycle;
import com.jobqueue.service.JobLifecycle.AttemptOutcome;
import com.jobqueue.store.JobRepository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * Queue semantics at the storage layer, with no workers running (role=api) so every claim is deterministic.
 * These are the properties the whole design rests on: ordering, exclusivity, fencing and lease recovery.
 */
@SpringBootTest(properties = {
        "jobqueue.role=api",
        "jobqueue.retry.base-delay=10ms",
        "jobqueue.retry.max-delay=50ms",
        "jobqueue.retry.jitter=false",
        "jobqueue.worker.lease-grace=0ms"
})
class QueueStoreIT extends PostgresSupport {

    @Autowired
    JobRepository jobs;

    @Autowired
    JobLifecycle lifecycle;

    private UUID insert(int priority, long delayMs, long timeoutMs, int maxRetries) {
        UUID id = UUID.randomUUID();
        jobs.insert(id, new NewJob("echo", JsonNodeFactory.instance.objectNode(), priority, maxRetries, timeoutMs,
                delayMs, null, null)).orElseThrow();
        return id;
    }

    @Test
    void claimsHighestPriorityFirstThenFifo() {
        UUID low = insert(0, 0, 1000, 0);
        UUID highOld = insert(9, 0, 1000, 0);
        UUID mid = insert(5, 0, 1000, 0);
        UUID highNew = insert(9, 0, 1000, 0);

        List<UUID> order = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            order.add(lifecycle.claim("w", 1).get(0).id());
        }
        assertThat(order).containsExactly(highOld, highNew, mid, low);
    }

    @Test
    void delayedJobsAreNotClaimedEarly() {
        insert(9, 60_000, 1000, 0);
        UUID now = insert(0, 0, 1000, 0);

        List<Job> claimed = lifecycle.claim("w", 10);
        assertThat(claimed).extracting(Job::id).containsExactly(now);
        assertThat(jobs.stats().delayed()).isEqualTo(1);
    }

    @Test
    void claimSetsLeaseIncrementsAttemptAndRecordsHistory() {
        UUID id = insert(0, 0, 1000, 0);
        Job j = lifecycle.claim("worker-a", 1).get(0);

        assertThat(j.status()).isEqualTo(JobStatus.RUNNING);
        assertThat(j.attempts()).isEqualTo(1);
        assertThat(j.leaseOwner()).isEqualTo("worker-a");
        assertThat(j.leaseExpiresAt()).isAfter(j.startedAt());
        assertThat(jobs.attempts(id)).extracting(JobAttempt::workerId).containsExactly("worker-a");
    }

    @Test
    void concurrentWorkersNeverClaimTheSameJob() throws Exception {
        int total = 300;
        for (int i = 0; i < total; i++) {
            insert(i % 10, 0, 1000, 0);
        }
        List<UUID> claimed = Collections.synchronizedList(new ArrayList<>());
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<?>> futures = new ArrayList<>();
        for (int w = 0; w < 8; w++) {
            String worker = "w" + w;
            futures.add(pool.submit(() -> {
                List<Job> batch;
                do {
                    batch = lifecycle.claim(worker, 7);
                    batch.forEach(j -> claimed.add(j.id()));
                } while (!batch.isEmpty());
            }));
        }
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();

        assertThat(claimed).hasSize(total);
        assertThat(new HashSet<>(claimed)).hasSize(total);
    }

    @Test
    void transientFailureSchedulesRetryWithBackoffThenDeadLettersWhenExhausted() {
        UUID id = insert(0, 0, 1000, 1); // 2 attempts total

        Job first = lifecycle.claim("w", 1).get(0);
        assertThat(lifecycle.recordFailure(first, "w", "boom-1", false, AttemptOutcome.FAILED)).isTrue();
        Job afterFirst = jobs.findById(id).orElseThrow();
        assertThat(afterFirst.status()).isEqualTo(JobStatus.FAILED);
        assertThat(afterFirst.lastError()).isEqualTo("boom-1");
        assertThat(afterFirst.runAt()).isAfter(first.startedAt()); // back-off pushed run_at forward

        Job second = await().atMost(Duration.ofSeconds(2))
                .until(() -> lifecycle.claim("w", 1), l -> !l.isEmpty()).get(0);
        assertThat(second.attempts()).isEqualTo(2);
        lifecycle.recordFailure(second, "w", "boom-2", false, AttemptOutcome.FAILED);

        Job dead = jobs.findById(id).orElseThrow();
        assertThat(dead.status()).isEqualTo(JobStatus.DEAD_LETTERED);
        assertThat(jobs.deadLetters(10)).singleElement()
                .satisfies(d -> {
                    assertThat(d.jobId()).isEqualTo(id);
                    assertThat(d.reason()).isEqualTo("RETRIES_EXHAUSTED");
                    assertThat(d.attempts()).isEqualTo(2);
                    assertThat(d.lastError()).isEqualTo("boom-2");
                });
    }

    @Test
    void nonRetryableFailureDeadLettersImmediately() {
        UUID id = insert(0, 0, 1000, 5);
        Job j = lifecycle.claim("w", 1).get(0);
        lifecycle.recordFailure(j, "w", "bad input", true, AttemptOutcome.FAILED);

        assertThat(jobs.findById(id).orElseThrow().status()).isEqualTo(JobStatus.DEAD_LETTERED);
        assertThat(jobs.deadLetters(10)).singleElement().extracting("reason").isEqualTo("NON_RETRYABLE");
    }

    /**
     * The crash-recovery path and the duplicate-execution guard in one scenario:
     * worker A claims, goes silent; its lease expires; the reaper fails the attempt; worker B runs attempt 2;
     * then A wakes up and tries to report success for attempt 1. The fence rejects it.
     */
    @Test
    void expiredLeaseIsReapedAndTheZombieWorkersResultIsFenced() {
        UUID id = insert(0, 0, 100, 3); // 100 ms timeout, grace 0 → lease expires after 100 ms
        Job attemptA = lifecycle.claim("worker-a", 1).get(0);

        await().atMost(Duration.ofSeconds(2)).until(() -> lifecycle.reapExpiredLeases(10) == 1);
        Job reaped = jobs.findById(id).orElseThrow();
        assertThat(reaped.status()).isEqualTo(JobStatus.FAILED);
        assertThat(reaped.lastError()).contains("lease expired");

        Job attemptB = await().atMost(Duration.ofSeconds(2))
                .until(() -> lifecycle.claim("worker-b", 1), l -> !l.isEmpty()).get(0);
        assertThat(attemptB.attempts()).isEqualTo(2);

        // Zombie A reports late: rejected, nothing changes.
        assertThat(lifecycle.recordSuccess(attemptA, "worker-a", new TextNode("stale"))).isFalse();
        assertThat(jobs.findById(id).orElseThrow().status()).isEqualTo(JobStatus.RUNNING);

        // The legitimate owner of attempt 2 succeeds.
        assertThat(lifecycle.recordSuccess(attemptB, "worker-b", new TextNode("fresh"))).isTrue();
        Job done = jobs.findById(id).orElseThrow();
        assertThat(done.status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(done.result().asText()).isEqualTo("fresh");
        assertThat(jobs.attempts(id)).extracting(JobAttempt::outcome).containsExactly("LEASE_EXPIRED", "SUCCEEDED");
    }

    @Test
    void reaperIgnoresLiveLeases() {
        insert(0, 0, 60_000, 0);
        lifecycle.claim("w", 1);
        assertThat(lifecycle.reapExpiredLeases(10)).isZero();
    }

    @Test
    void idempotencyKeyPreventsDuplicates() {
        NewJob job = new NewJob("echo", JsonNodeFactory.instance.objectNode(), 0, 0, 1000, 0, "order-42", null);
        assertThat(jobs.insert(UUID.randomUUID(), job)).isPresent();
        assertThat(jobs.insert(UUID.randomUUID(), job)).isEmpty();
        assertThat(jobs.findByIdempotencyKey("order-42")).isPresent();
    }

    @Test
    void cancelAndPurgeOnlyTouchPendingJobs() {
        UUID running = insert(9, 0, 60_000, 0);
        lifecycle.claim("w", 1); // claims the priority-9 job
        UUID a = insert(0, 0, 1000, 0);
        insert(0, 0, 1000, 0);
        insert(0, 60_000, 1000, 0); // delayed jobs are pending too

        assertThat(jobs.cancel(running)).isFalse();
        assertThat(jobs.cancel(a)).isTrue();
        assertThat(jobs.purgePending(null)).isEqualTo(2);
        assertThat(jobs.findById(running).orElseThrow().status()).isEqualTo(JobStatus.RUNNING);
        assertThat(jobs.stats().depth()).isZero();
        assertThat(jobs.stats().byStatus().get("CANCELLED")).isEqualTo(3);
    }

    @Test
    void replayKeepsAttemptNumbersMonotonic() {
        UUID id = insert(0, 0, 1000, 0);
        Job j = lifecycle.claim("w", 1).get(0);
        lifecycle.recordFailure(j, "w", "nope", false, AttemptOutcome.FAILED);
        assertThat(jobs.replay(id)).isTrue();

        Job replayed = jobs.findById(id).orElseThrow();
        assertThat(replayed.status()).isEqualTo(JobStatus.QUEUED);
        assertThat(replayed.attempts()).isEqualTo(1);
        assertThat(replayed.maxRetries()).isEqualTo(1); // one fresh attempt, numbered 2
        assertThat(lifecycle.claim("w", 1).get(0).attempts()).isEqualTo(2);
        assertThat(jobs.replay(id)).isFalse(); // only DEAD_LETTERED jobs can be replayed
    }
}
