package com.jobqueue.service;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.jobqueue.config.JobQueueProperties;
import com.jobqueue.domain.Job;
import com.jobqueue.metrics.JobMetrics;
import com.jobqueue.store.JobRepository;
import com.jobqueue.store.JsonCodec;
import com.jobqueue.worker.BackoffPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Every state transition a worker or the reaper performs. Each public method is one transaction, and every
 * post-claim transition is fenced on (attempt, lease owner), so outcomes of stale attempts are discarded.
 */
@Service
public class JobLifecycle {

    private static final Logger log = LoggerFactory.getLogger(JobLifecycle.class);
    private static final int MAX_ERROR_LENGTH = 4000;

    public enum AttemptOutcome { FAILED, TIMED_OUT, LEASE_EXPIRED }

    private final JobRepository jobs;
    private final JsonCodec json;
    private final JobMetrics metrics;
    private final BackoffPolicy backoff;
    private final long leaseGraceMs;

    public JobLifecycle(JobRepository jobs, JsonCodec json, JobMetrics metrics, JobQueueProperties props) {
        this.jobs = jobs;
        this.json = json;
        this.metrics = metrics;
        this.backoff = new BackoffPolicy(props.retry());
        this.leaseGraceMs = props.worker().leaseGrace().toMillis();
    }

    /** Claim + record attempt rows in one transaction. */
    @Transactional
    public List<Job> claim(String workerId, int limit) {
        List<Job> claimed = jobs.claim(workerId, limit, leaseGraceMs);
        for (Job j : claimed) {
            jobs.insertAttempt(j.id(), j.attempts(), workerId);
        }
        return claimed;
    }

    /** @return false if the attempt was stale (lease lost) and its result was discarded */
    @Transactional
    public boolean recordSuccess(Job job, String workerId, JsonNode result) {
        Optional<Job> done = jobs.markSucceeded(job.id(), job.attempts(), workerId, json.write(result));
        if (done.isEmpty()) {
            jobs.finishAttempt(job.id(), job.attempts(), "STALE", "succeeded after lease was lost; result discarded");
            metrics.staleCompletion(job.type());
            log.warn("Discarded stale success for job {} attempt {} (lease lost)", job.id(), job.attempts());
            return false;
        }
        jobs.finishAttempt(job.id(), job.attempts(), "SUCCEEDED", null);
        metrics.finished(job.type(), "succeeded");
        metrics.endToEnd(job.type(), "succeeded", done.get().createdAt(), done.get().finishedAt());
        return true;
    }

    /**
     * Retry with back-off, or dead-letter when retries are exhausted or the failure is permanent.
     *
     * @param owner the lease owner of the attempt being failed (the worker, or the expired owner for the reaper)
     * @return false if the attempt was stale and nothing was changed
     */
    @Transactional
    public boolean recordFailure(Job job, String owner, String error, boolean nonRetryable, AttemptOutcome outcome) {
        String err = truncate(error);
        if (BackoffPolicy.shouldRetry(job.attempts(), job.maxRetries(), nonRetryable)) {
            long delay = backoff.delayMs(job.attempts());
            if (!jobs.markRetry(job.id(), job.attempts(), owner, err, delay)) {
                return stale(job, err);
            }
            jobs.finishAttempt(job.id(), job.attempts(), outcome.name(), err);
            metrics.retried(job.type(), outcome.name().toLowerCase());
            log.info("Job {} attempt {}/{} {}; retrying in {} ms: {}",
                    job.id(), job.attempts(), job.maxRetries() + 1, outcome, delay, err);
            return true;
        }

        Optional<Job> dead = jobs.markDeadLettered(job.id(), job.attempts(), owner, err);
        if (dead.isEmpty()) {
            return stale(job, err);
        }
        String reason = nonRetryable ? "NON_RETRYABLE" : "RETRIES_EXHAUSTED";
        jobs.insertDeadLetter(dead.get(), reason);
        jobs.finishAttempt(job.id(), job.attempts(), outcome.name(), err);
        metrics.deadLettered(job.type(), reason.toLowerCase());
        metrics.finished(job.type(), "dead_lettered");
        metrics.endToEnd(job.type(), "dead_lettered", dead.get().createdAt(), dead.get().finishedAt());
        log.warn("Job {} dead-lettered after {} attempt(s) ({}): {}", job.id(), job.attempts(), reason, err);
        return true;
    }

    /**
     * Recovers RUNNING jobs whose lease expired: the worker crashed, was partitioned from the database, or
     * overran its timeout without reporting. Each is treated as a failed attempt (counts against retries).
     */
    @Transactional
    public int reapExpiredLeases(int limit) {
        List<Job> expired = jobs.lockExpiredLeases(limit);
        for (Job j : expired) {
            metrics.leaseExpired(j.type());
            recordFailure(j, j.leaseOwner(),
                    "lease expired: worker " + j.leaseOwner() + " did not report within timeout + grace",
                    false, AttemptOutcome.LEASE_EXPIRED);
        }
        return expired.size();
    }

    private boolean stale(Job job, String err) {
        jobs.finishAttempt(job.id(), job.attempts(), "STALE", err);
        metrics.staleCompletion(job.type());
        log.warn("Discarded stale failure for job {} attempt {} (lease lost)", job.id(), job.attempts());
        return false;
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= MAX_ERROR_LENGTH ? s : s.substring(0, MAX_ERROR_LENGTH);
    }
}
