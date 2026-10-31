package com.jobqueue.api;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.jobqueue.config.Roles;
import com.jobqueue.domain.DeadLetter;
import com.jobqueue.domain.QueueStats;
import com.jobqueue.service.JobService;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Operational control. In production these would sit behind admin authN/Z (e.g. an API gateway route
 * restricted to an operator role); kept open here to keep the exercise focused.
 */
@RestController
@RequestMapping("/api/v1")
@ConditionalOnExpression(Roles.API_ENABLED)
public class QueueController {

    private final JobService service;

    public QueueController(JobService service) {
        this.service = service;
    }

    @GetMapping("/queue/stats")
    public QueueStats stats() {
        return service.stats();
    }

    /** Drain: stop dispatching new jobs cluster-wide. In-flight attempts finish; submissions are still accepted. */
    @PostMapping("/queue/pause")
    public QueueStats pause() {
        service.pause();
        return service.stats();
    }

    @PostMapping("/queue/resume")
    public QueueStats resume() {
        service.resume();
        return service.stats();
    }

    /** Cancel every pending (QUEUED/FAILED) job, optionally only of one type. Running jobs are untouched. */
    @PostMapping("/queue/purge")
    public Map<String, Integer> purge(@RequestParam(required = false) String type) {
        return Map.of("cancelled", service.purge(type));
    }

    @GetMapping("/dead-letters")
    public List<DeadLetter> deadLetters(@RequestParam(defaultValue = "50") int limit) {
        return service.deadLetters(limit);
    }

    @PostMapping("/dead-letters/{jobId}/replay")
    public JobResponse replay(@PathVariable UUID jobId) {
        return JobResponse.from(service.replay(jobId));
    }
}
