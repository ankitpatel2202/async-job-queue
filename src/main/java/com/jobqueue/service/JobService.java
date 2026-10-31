package com.jobqueue.service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jobqueue.api.SubmitJobRequest;
import com.jobqueue.config.JobQueueProperties;
import com.jobqueue.domain.ConflictException;
import com.jobqueue.domain.DeadLetter;
import com.jobqueue.domain.Job;
import com.jobqueue.domain.JobAttempt;
import com.jobqueue.domain.JobStatus;
import com.jobqueue.domain.NewJob;
import com.jobqueue.domain.NotFoundException;
import com.jobqueue.domain.QueueStats;
import com.jobqueue.store.JobRepository;
import com.jobqueue.store.JsonCodec;
import com.jobqueue.worker.HandlerRegistry;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobService {

    public record SubmitResult(Job job, boolean created) {
    }

    private final JobRepository jobs;
    private final HandlerRegistry handlers;
    private final JsonCodec json;
    private final JobQueueProperties props;

    public JobService(JobRepository jobs, HandlerRegistry handlers, JsonCodec json, JobQueueProperties props) {
        this.jobs = jobs;
        this.handlers = handlers;
        this.json = json;
        this.props = props;
    }

    /**
     * Persists the job and returns immediately. The INSERT commit is the durability point: once the caller
     * has a job id, the job will be attempted (unless cancelled). Execution is fully decoupled from this call.
     */
    public SubmitResult submit(SubmitJobRequest req, String idempotencyKey) {
        NewJob job = toNewJob(req, idempotencyKey, null);
        UUID id = UUID.randomUUID();
        return jobs.insert(id, job)
                .map(j -> new SubmitResult(j, true))
                .orElseGet(() -> new SubmitResult(
                        // Idempotent replay: same key returns the original job instead of creating a duplicate.
                        jobs.findByIdempotencyKey(idempotencyKey).orElseThrow(() ->
                                new IllegalStateException("idempotency conflict but job not found")),
                        false));
    }

    public NewJob toNewJob(SubmitJobRequest req, String idempotencyKey, UUID recurringJobId) {
        if (!handlers.supports(req.type())) {
            throw new IllegalArgumentException("unknown job type '" + req.type() + "'; supported: " + handlers.types());
        }
        if (req.runAt() != null && req.delayMs() != null) {
            throw new IllegalArgumentException("specify at most one of runAt and delayMs");
        }
        if (idempotencyKey != null && (idempotencyKey.isBlank() || idempotencyKey.length() > 200)) {
            throw new IllegalArgumentException("Idempotency-Key must be 1-200 characters");
        }
        JsonNode payload = req.payload() == null ? JsonNodeFactory.instance.objectNode() : req.payload();
        int size = json.write(payload).getBytes(StandardCharsets.UTF_8).length;
        if (size > props.maxPayloadBytes()) {
            throw new IllegalArgumentException("payload is " + size + " bytes; limit is " + props.maxPayloadBytes());
        }
        JobQueueProperties.Defaults d = props.defaults();
        long delayMs = 0;
        if (req.delayMs() != null) {
            delayMs = req.delayMs();
        } else if (req.runAt() != null) {
            // Converted to a delay so run_at is computed on the database clock (no API/DB clock mixing
            // on the claim path); skew between this node and the DB only shifts the start by that skew.
            delayMs = Math.max(0, Duration.between(Instant.now(), req.runAt()).toMillis());
        }
        return new NewJob(
                req.type(),
                payload,
                req.priority() != null ? req.priority() : d.priority(),
                req.maxRetries() != null ? req.maxRetries() : d.maxRetries(),
                req.timeoutMs() != null ? req.timeoutMs() : d.timeout().toMillis(),
                delayMs,
                idempotencyKey,
                recurringJobId);
    }

    public Job get(UUID id) {
        return jobs.findById(id).orElseThrow(() -> new NotFoundException("job " + id + " not found"));
    }

    public List<Job> list(JobStatus status, String type, int limit) {
        return jobs.list(status, type, clamp(limit));
    }

    public List<JobAttempt> attempts(UUID id) {
        get(id);
        return jobs.attempts(id);
    }

    /** Cancels a pending job. Running jobs cannot be cancelled (see README: cooperative cancellation). */
    @Transactional
    public Job cancel(UUID id) {
        if (jobs.cancel(id)) {
            return get(id);
        }
        Job job = get(id); // 404 if missing
        throw new ConflictException("job " + id + " is " + job.status() + "; only QUEUED or FAILED jobs can be cancelled");
    }

    public QueueStats stats() {
        return jobs.stats();
    }

    public void pause() {
        jobs.setPaused(true);
    }

    public void resume() {
        jobs.setPaused(false);
    }

    public int purge(String type) {
        return jobs.purgePending(type);
    }

    public List<DeadLetter> deadLetters(int limit) {
        return jobs.deadLetters(clamp(limit));
    }

    @Transactional
    public Job replay(UUID id) {
        if (jobs.replay(id)) {
            return get(id);
        }
        Job job = get(id);
        throw new ConflictException("job " + id + " is " + job.status() + "; only DEAD_LETTERED jobs can be replayed");
    }

    private static int clamp(int limit) {
        return Math.max(1, Math.min(limit, 500));
    }
}
