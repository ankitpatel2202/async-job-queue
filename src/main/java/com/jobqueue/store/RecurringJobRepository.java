package com.jobqueue.store;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import com.jobqueue.domain.RecurringJob;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class RecurringJobRepository {

    /** A due recurring job plus the database time it was read at. */
    public record Due(RecurringJob job, Instant dbNow) {
    }

    private final JdbcClient jdbc;
    private final JsonCodec json;

    public RecurringJobRepository(JdbcClient jdbc, JsonCodec json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    public Optional<RecurringJob> insert(RecurringJob r) {
        return jdbc.sql("""
                        INSERT INTO recurring_jobs (id, name, cron, zone, type, payload, priority, max_retries,
                                                    timeout_ms, enabled, next_run_at)
                        VALUES (:id, :name, :cron, :zone, :type, CAST(:payload AS jsonb), :priority, :maxRetries,
                                :timeoutMs, true, :next)
                        ON CONFLICT (name) DO NOTHING
                        RETURNING *
                        """)
                .param("id", r.id()).param("name", r.name()).param("cron", r.cron()).param("zone", r.zone())
                .param("type", r.type()).param("payload", json.write(r.payload()))
                .param("priority", r.priority()).param("maxRetries", r.maxRetries())
                .param("timeoutMs", r.timeoutMs())
                .param("next", OffsetDateTime.ofInstant(r.nextRunAt(), ZoneOffset.UTC))
                .query(this::map)
                .optional();
    }

    public List<RecurringJob> list() {
        return jdbc.sql("SELECT * FROM recurring_jobs ORDER BY name").query(this::map).list();
    }

    public boolean delete(UUID id) {
        return jdbc.sql("DELETE FROM recurring_jobs WHERE id = :id").param("id", id).update() == 1;
    }

    /**
     * Locks due definitions. SKIP LOCKED lets every worker node run the scheduler loop with no leader
     * election: each due definition is materialised by exactly one node per tick.
     */
    public List<Due> lockDue(int limit) {
        return jdbc.sql("""
                        SELECT *, now() AS db_now FROM recurring_jobs
                        WHERE enabled AND next_run_at <= now()
                        ORDER BY next_run_at
                        LIMIT :limit
                        FOR UPDATE SKIP LOCKED
                        """)
                .param("limit", limit)
                .query((rs, n) -> new Due(map(rs, n), JobRepository.instant(rs, "db_now")))
                .list();
    }

    public void advance(UUID id, Instant nextRunAt) {
        jdbc.sql("UPDATE recurring_jobs SET next_run_at = :next, last_enqueued_at = now() WHERE id = :id")
                .param("id", id)
                .param("next", OffsetDateTime.ofInstant(nextRunAt, ZoneOffset.UTC))
                .update();
    }

    private RecurringJob map(ResultSet rs, int rowNum) throws SQLException {
        return new RecurringJob(
                rs.getObject("id", UUID.class),
                rs.getString("name"),
                rs.getString("cron"),
                rs.getString("zone"),
                rs.getString("type"),
                json.read(rs.getString("payload")),
                rs.getInt("priority"),
                rs.getInt("max_retries"),
                rs.getLong("timeout_ms"),
                rs.getBoolean("enabled"),
                JobRepository.instant(rs, "next_run_at"),
                JobRepository.instant(rs, "last_enqueued_at"),
                JobRepository.instant(rs, "created_at"));
    }
}
