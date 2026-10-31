package com.jobqueue.api;

import java.net.URI;
import java.util.List;
import java.util.UUID;

import com.jobqueue.config.Roles;
import com.jobqueue.domain.JobAttempt;
import com.jobqueue.domain.JobStatus;
import com.jobqueue.service.JobService;
import com.jobqueue.service.JobService.SubmitResult;

import jakarta.validation.Valid;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/jobs")
@ConditionalOnExpression(Roles.API_ENABLED)
public class JobController {

    private final JobService service;

    public JobController(JobService service) {
        this.service = service;
    }

    /**
     * 202 Accepted with the new job; 200 OK with the existing job when the Idempotency-Key was seen before.
     */
    @PostMapping
    public ResponseEntity<JobResponse> submit(@Valid @RequestBody SubmitJobRequest request,
                                              @RequestHeader(value = "Idempotency-Key", required = false) String key) {
        SubmitResult r = service.submit(request, key);
        return ResponseEntity.status(r.created() ? HttpStatus.ACCEPTED : HttpStatus.OK)
                .location(URI.create("/api/v1/jobs/" + r.job().id()))
                .body(JobResponse.from(r.job()));
    }

    @GetMapping("/{id}")
    public JobResponse get(@PathVariable UUID id) {
        return JobResponse.from(service.get(id));
    }

    @GetMapping
    public List<JobResponse> list(@RequestParam(required = false) JobStatus status,
                                  @RequestParam(required = false) String type,
                                  @RequestParam(defaultValue = "50") int limit) {
        return service.list(status, type, limit).stream().map(JobResponse::from).toList();
    }

    @GetMapping("/{id}/attempts")
    public List<JobAttempt> attempts(@PathVariable UUID id) {
        return service.attempts(id);
    }

    @PostMapping("/{id}/cancel")
    public JobResponse cancel(@PathVariable UUID id) {
        return JobResponse.from(service.cancel(id));
    }
}
