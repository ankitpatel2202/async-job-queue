package com.jobqueue.api;

import java.util.List;
import java.util.UUID;

import com.jobqueue.config.Roles;
import com.jobqueue.domain.RecurringJob;
import com.jobqueue.scheduling.RecurringJobService;
import com.jobqueue.scheduling.RecurringJobService.CreateRecurringJob;

import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/recurring-jobs")
@ConditionalOnExpression(Roles.API_ENABLED)
public class RecurringJobController {

    private final RecurringJobService service;

    public RecurringJobController(RecurringJobService service) {
        this.service = service;
    }

    /** Body: {"name":"nightly-report","cron":"0 2 * * *","zone":"Asia/Kolkata","job":{"type":"echo",...}} */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public RecurringJob create(@RequestBody CreateRecurringJob request) {
        return service.create(request);
    }

    @GetMapping
    public List<RecurringJob> list() {
        return service.list();
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable UUID id) {
        service.delete(id);
    }
}
