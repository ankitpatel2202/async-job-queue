package com.jobqueue.worker;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import org.springframework.stereotype.Component;

/** Maps job types to handlers. Fails fast at startup on duplicate registrations. */
@Component
public class HandlerRegistry {

    private final Map<String, JobHandler> handlers = new TreeMap<>();

    public HandlerRegistry(List<JobHandler> beans) {
        for (JobHandler h : beans) {
            JobHandler previous = handlers.putIfAbsent(h.type(), h);
            if (previous != null) {
                throw new IllegalStateException("Duplicate handlers for job type '" + h.type() + "': "
                        + previous.getClass().getName() + " and " + h.getClass().getName());
            }
        }
    }

    public Optional<JobHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }

    public boolean supports(String type) {
        return handlers.containsKey(type);
    }

    public Set<String> types() {
        return handlers.keySet();
    }
}
