package com.jobqueue.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.jobqueue.domain.DeadLetter;
import com.jobqueue.domain.Job;
import com.jobqueue.domain.JobAttempt;
import com.jobqueue.domain.JobStatus;
import com.jobqueue.domain.NewJob;
import com.jobqueue.domain.QueueStats;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * All queue SQL in one place. Postgres is both the queue and the result store:
 * <ul>
 *   <li>claims use {@code FOR UPDATE SKIP LOCKED} so N workers never block on or double-claim a row;</li>
 *   <li>every state change after a claim is <b>fenced</b> on (status = RUNNING, attempts = n, lease_owner = w),
 *       so a worker whose lease expired cannot overwrite the outcome of a newer attempt;</li>
 *   <li>all time comparisons use the database clock ({@code now()}), never a worker's clock.</li>
 * </ul>
 */
@Repository
public class JobRepository {

    private static final String PENDING = "('QUEUED','FAILED')";

    private final JdbcClient jdbc;
    private final JsonCodec json;
    private final RowMapper<Job> jobMapper;

    public JobRepository(JdbcClient jdbc, JsonCodec json) {
        this.jdbc = jdbc;
        this.json = json;
        this.jobMapper = this::mapJob;
    }

    // ---------------------------------------------------------------- submission

    /** Inserts a job. Returns empty if the idempotency key already exists. */
    public Optional<Job> insert(UUID id, NewJob job) {
        return jdbc.sql("""
                        INSERT INTO jobs (id, type, payload, priority, status, max_retries, timeout_ms,
                                          run_at, idempotency_key, recurring_job_id)
                        VALUES (:id, :type, CAST(:payload AS jsonb), :priority, 'QUEUED', :maxRetries, :timeoutMs,
                                now() + :delayMs * interval '1 millisecond', :key, :recurringId)
                        ON CONFLICT (idempotency_key) DO NOTHING
                        RETURNING *
                        """)
                .param("id", id)
                .param("type", job.type())
                .param("payload", json.write(job.payload()))
                .param("priority", job.priority())
                .param("maxRetries", job.maxRetries())
                .param("timeoutMs", job.timeoutMs())
                .param("delayMs", job.delayMs())
                .param("key", job.idempotencyKey())
                .param("recurringId", job.recurringJobId())
                .query(jobMapper)
                .optional();
    }

    public Optional<Job> findById(UUID id) {
        return jdbc.sql("SELECT * FROM jobs WHERE id = :id").param("id", id).query(jobMapper).optional();
    }

    public Optional<Job> findByIdempotencyKey(String key) {
        return jdbc.sql("SELECT * FROM jobs WHERE idempotency_key = :key").param("key", key).query(jobMapper).optional();
    }

    public List<Job> list(JobStatus status, String type, int limit) {
        return jdbc.sql("""
                        SELECT * FROM jobs
                        WHERE (CAST(:status AS text) IS NULL OR status = :status)
                          AND (CAST(:type AS text) IS NULL OR type = :type)
                        ORDER BY created_at DESC
                        LIMIT :limit
                        """)
                .param("status", status == null ? null : status.name())
                .param("type", type)
                .param("limit", limit)
                .query(jobMapper)
                .list();
    }

    // ---------------------------------------------------------------- worker path

    /**
     * Atomically claims up to {@code limit} eligible jobs, highest priority first, and leases them to
     * {@code workerId} for (timeout + grace). Incrementing {@code attempts} here (not on completion) is what
     * bounds duplicate execution: every execution, including ones a crashed worker never reported, consumes
     * an attempt.
     */
    public List<Job> claim(String workerId, int limit, long leaseGraceMs) {
        return jdbc.sql("""
                        WITH next AS (
                            SELECT id FROM jobs
                            WHERE status IN %s AND run_at <= now()
                              AND NOT (SELECT paused FROM queue_control WHERE id = 1)
                            ORDER BY priority DESC, run_at, created_at
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE jobs j
                        SET status = 'RUNNING',
                            attempts = j.attempts + 1,
                            lease_owner = :worker,
                            lease_expires_at = now() + (j.timeout_ms + :grace) * interval '1 millisecond',
                            started_at = now(),
                            updated_at = now()
                        FROM next
                        WHERE j.id = next.id
                        RETURNING j.*
                        """.formatted(PENDING))
                .param("limit", limit)
                .param("worker", workerId)
                .param("grace", leaseGraceMs)
                .query(jobMapper)
                .list();
    }

    public void insertAttempt(UUID jobId, int attempt, String workerId) {
        jdbc.sql("""
                        INSERT INTO job_attempts (job_id, attempt, worker_id) VALUES (:id, :attempt, :worker)
                        ON CONFLICT (job_id, attempt) DO NOTHING
                        """)
                .param("id", jobId).param("attempt", attempt).param("worker", workerId)
                .update();
    }

    public void finishAttempt(UUID jobId, int attempt, String outcome, String error) {
        jdbc.sql("""
                        UPDATE job_attempts SET finished_at = now(), outcome = :outcome, error = :error
                        WHERE job_id = :id AND attempt = :attempt AND finished_at IS NULL
                        """)
                .param("id", jobId).param("attempt", attempt).param("outcome", outcome).param("error", error)
                .update();
    }

    /** Fenced success transition. Empty means the lease was lost: another attempt owns the job now. */
    public Optional<Job> markSucceeded(UUID id, int attempt, String owner, String resultJson) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'SUCCEEDED', result = CAST(:result AS jsonb), last_error = NULL,
                                        lease_owner = NULL, lease_expires_at = NULL,
                                        finished_at = now(), updated_at = now()
                        WHERE id = :id AND status = 'RUNNING' AND attempts = :attempt AND lease_owner = :owner
                        RETURNING *
                        """)
                .param("id", id).param("attempt", attempt).param("owner", owner).param("result", resultJson)
                .query(jobMapper)
                .optional();
    }

    /** Fenced transition to FAILED with the next attempt scheduled after {@code delayMs}. */
    public boolean markRetry(UUID id, int attempt, String owner, String error, long delayMs) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'FAILED', last_error = :error,
                                        run_at = now() + :delay * interval '1 millisecond',
                                        lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
                        WHERE id = :id AND status = 'RUNNING' AND attempts = :attempt AND lease_owner = :owner
                        """)
                .param("id", id).param("attempt", attempt).param("owner", owner)
                .param("error", error).param("delay", delayMs)
                .update() == 1;
    }

    /** Fenced transition to DEAD_LETTERED. Caller must also call {@link #insertDeadLetter} in the same tx. */
    public Optional<Job> markDeadLettered(UUID id, int attempt, String owner, String error) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'DEAD_LETTERED', last_error = :error,
                                        lease_owner = NULL, lease_expires_at = NULL,
                                        finished_at = now(), updated_at = now()
                        WHERE id = :id AND status = 'RUNNING' AND attempts = :attempt AND lease_owner = :owner
                        RETURNING *
                        """)
                .param("id", id).param("attempt", attempt).param("owner", owner).param("error", error)
                .query(jobMapper)
                .optional();
    }

    public void insertDeadLetter(Job job, String reason) {
        jdbc.sql("""
                        INSERT INTO dead_letters (job_id, type, payload, attempts, last_error, reason)
                        VALUES (:id, :type, CAST(:payload AS jsonb), :attempts, :error, :reason)
                        ON CONFLICT (job_id) DO UPDATE
                           SET attempts = EXCLUDED.attempts, last_error = EXCLUDED.last_error,
                               reason = EXCLUDED.reason, dead_lettered_at = now(), replayed_at = NULL
                        """)
                .param("id", job.id()).param("type", job.type()).param("payload", json.write(job.payload()))
                .param("attempts", job.attempts()).param("error", job.lastError()).param("reason", reason)
                .update();
    }

    /** Locks RUNNING jobs whose lease has expired (crashed / partitioned / overrunning worker). */
    public List<Job> lockExpiredLeases(int limit) {
        return jdbc.sql("""
                        SELECT * FROM jobs
                        WHERE status = 'RUNNING' AND lease_expires_at < now()
                        ORDER BY lease_expires_at
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """)
                .param("limit", limit)
                .query(jobMapper)
                .list();
    }

    // ---------------------------------------------------------------- operational control

    /** Cancels a single pending job. Returns true if it was pending. */
    public boolean cancel(UUID id) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'CANCELLED', finished_at = now(), updated_at = now(),
                                        last_error = 'cancelled by operator'
                        WHERE id = :id AND status IN %s
                        """.formatted(PENDING))
                .param("id", id)
                .update() == 1;
    }

    /** Cancels every pending job (optionally of one type). Running jobs are left to finish. */
    public int purgePending(String type) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'CANCELLED', finished_at = now(), updated_at = now(),
                                        last_error = 'purged by operator'
                        WHERE status IN %s AND (CAST(:type AS text) IS NULL OR type = :type)
                        """.formatted(PENDING))
                .param("type", type)
                .update();
    }

    public boolean isPaused() {
        return jdbc.sql("SELECT paused FROM queue_control WHERE id = 1").query(Boolean.class).single();
    }

    public void setPaused(boolean paused) {
        jdbc.sql("UPDATE queue_control SET paused = :p, updated_at = now() WHERE id = 1").param("p", paused).update();
    }

    public QueueStats stats() {
        Map<String, Long> byStatus = new LinkedHashMap<>();
        for (JobStatus s : JobStatus.values()) {
            byStatus.put(s.name(), 0L);
        }
        jdbc.sql("SELECT status, count(*) AS n FROM jobs GROUP BY status")
                .query(rs -> {
                    byStatus.put(rs.getString("status"), rs.getLong("n"));
                });

        return jdbc.sql("""
                        SELECT count(*) FILTER (WHERE run_at <= now()) AS ready,
                               count(*) FILTER (WHERE run_at >  now()) AS delayed,
                               COALESCE(EXTRACT(EPOCH FROM now() - min(run_at) FILTER (WHERE run_at <= now())), 0)
                                   AS oldest
                        FROM jobs WHERE status IN %s
                        """.formatted(PENDING))
                .query((rs, n) -> new QueueStats(
                        rs.getLong("ready"),
                        rs.getLong("delayed"),
                        byStatus.get(JobStatus.RUNNING.name()),
                        rs.getDouble("oldest"),
                        byStatus,
                        isPaused()))
                .single();
    }

    // ---------------------------------------------------------------- history & dead letters

    public List<JobAttempt> attempts(UUID jobId) {
        return jdbc.sql("SELECT * FROM job_attempts WHERE job_id = :id ORDER BY attempt")
                .param("id", jobId)
                .query((rs, n) -> new JobAttempt(
                        rs.getInt("attempt"),
                        rs.getString("worker_id"),
                        instant(rs, "started_at"),
                        instant(rs, "finished_at"),
                        rs.getString("outcome"),
                        rs.getString("error")))
                .list();
    }

    public List<DeadLetter> deadLetters(int limit) {
        return jdbc.sql("SELECT * FROM dead_letters ORDER BY dead_lettered_at DESC LIMIT :limit")
                .param("limit", limit)
                .query((rs, n) -> new DeadLetter(
                        rs.getObject("job_id", UUID.class),
                        rs.getString("type"),
                        json.read(rs.getString("payload")),
                        rs.getInt("attempts"),
                        rs.getString("last_error"),
                        rs.getString("reason"),
                        instant(rs, "dead_lettered_at"),
                        instant(rs, "replayed_at")))
                .list();
    }

    public long deadLetterCount() {
        return jdbc.sql("SELECT count(*) FROM dead_letters WHERE replayed_at IS NULL").query(Long.class).single();
    }

    /**
     * Moves a dead-lettered job back to QUEUED with a fresh retry budget. Attempt numbers are never reset:
     * they are fencing tokens, and reusing one could let a zombie from the job's previous life match the
     * fence. Instead the budget is extended: max_retries grows by the original (max_retries + 1) attempts.
     */
    public boolean replay(UUID jobId) {
        int updated = jdbc.sql("""
                        UPDATE jobs SET status = 'QUEUED', max_retries = attempts + max_retries,
                                        run_at = now(), last_error = NULL, result = NULL,
                                        finished_at = NULL, updated_at = now()
                        WHERE id = :id AND status = 'DEAD_LETTERED'
                        """)
                .param("id", jobId)
                .update();
        if (updated == 1) {
            jdbc.sql("UPDATE dead_letters SET replayed_at = now() WHERE job_id = :id").param("id", jobId).update();
        }
        return updated == 1;
    }

    // ---------------------------------------------------------------- mapping

    private Job mapJob(ResultSet rs, int rowNum) throws SQLException {
        return new Job(
                rs.getObject("id", UUID.class),
                rs.getString("type"),
                json.read(rs.getString("payload")),
                rs.getInt("priority"),
                JobStatus.valueOf(rs.getString("status")),
                rs.getInt("attempts"),
                rs.getInt("max_retries"),
                rs.getLong("timeout_ms"),
                instant(rs, "run_at"),
                rs.getString("idempotency_key"),
                rs.getString("lease_owner"),
                instant(rs, "lease_expires_at"),
                rs.getString("last_error"),
                json.read(rs.getString("result")),
                rs.getObject("recurring_job_id", UUID.class),
                instant(rs, "created_at"),
                instant(rs, "updated_at"),
                instant(rs, "started_at"),
                instant(rs, "finished_at"));
    }

    static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime t = rs.getObject(column, OffsetDateTime.class);
        return t == null ? null : t.toInstant();
    }
}
