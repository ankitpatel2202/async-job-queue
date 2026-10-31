package com.jobqueue.scheduling;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.jobqueue.api.SubmitJobRequest;
import com.jobqueue.domain.ConflictException;
import com.jobqueue.domain.NewJob;
import com.jobqueue.domain.NotFoundException;
import com.jobqueue.domain.RecurringJob;
import com.jobqueue.service.JobService;
import com.jobqueue.store.JobRepository;
import com.jobqueue.store.RecurringJobRepository;
import com.jobqueue.store.RecurringJobRepository.Due;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class RecurringJobService {

    private static final Logger log = LoggerFactory.getLogger(RecurringJobService.class);

    public record CreateRecurringJob(String name, String cron, String zone, SubmitJobRequest job) {
    }

    private final RecurringJobRepository recurring;
    private final JobRepository jobs;
    private final JobService jobService;

    public RecurringJobService(RecurringJobRepository recurring, JobRepository jobs, JobService jobService) {
        this.recurring = recurring;
        this.jobs = jobs;
        this.jobService = jobService;
    }

    public RecurringJob create(CreateRecurringJob req) {
        if (req.name() == null || req.name().isBlank()) {
            throw new IllegalArgumentException("name is required");
        }
        if (req.job() == null) {
            throw new IllegalArgumentException("job template is required");
        }
        CronExpression cron = CronSupport.parse(req.cron());
        ZoneId zone = CronSupport.zone(req.zone());
        NewJob template = jobService.toNewJob(req.job(), null, null); // validates type, payload, limits
        JsonNode payload = template.payload() == null ? JsonNodeFactory.instance.objectNode() : template.payload();
        RecurringJob r = new RecurringJob(UUID.randomUUID(), req.name(), req.cron(), zone.getId(), template.type(),
                payload, template.priority(), template.maxRetries(), template.timeoutMs(), true,
                CronSupport.next(cron, zone, Instant.now()), null, null);
        return recurring.insert(r).orElseThrow(() ->
                new ConflictException("recurring job '" + req.name() + "' already exists"));
    }

    public List<RecurringJob> list() {
        return recurring.list();
    }

    public void delete(UUID id) {
        if (!recurring.delete(id)) {
            throw new NotFoundException("recurring job " + id + " not found");
        }
    }

    /**
     * One scheduler tick. Materialises each due definition into an ordinary job, idempotently keyed by
     * (definition, fire time) so a crash between insert and advance cannot double-enqueue a fire.
     * Misfire policy: if the scheduler was down across several fire times, the backlog collapses into a
     * single run and the schedule resumes from now.
     *
     * @return number of jobs enqueued
     */
    @Transactional
    public int tick(int limit) {
        int enqueued = 0;
        for (Due due : recurring.lockDue(limit)) {
            RecurringJob r = due.job();
            String key = "cron:" + r.id() + ":" + r.nextRunAt().toEpochMilli();
            NewJob job = new NewJob(r.type(), r.payload(), r.priority(), r.maxRetries(), r.timeoutMs(), 0, key, r.id());
            if (jobs.insert(UUID.randomUUID(), job).isPresent()) {
                enqueued++;
            }
            Instant base = due.dbNow().isAfter(r.nextRunAt()) ? due.dbNow() : r.nextRunAt();
            Instant next = CronSupport.next(CronSupport.parse(r.cron()), CronSupport.zone(r.zone()), base);
            recurring.advance(r.id(), next);
            log.info("Recurring job '{}' fired for {}; next at {}", r.name(), r.nextRunAt(), next);
        }
        return enqueued;
    }
}
