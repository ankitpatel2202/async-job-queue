package com.jobqueue.config;

/**
 * SpEL conditions used to split the single artifact into independently scalable tiers.
 * <ul>
 *   <li>{@code JOBQUEUE_ROLE=api}    – REST API only; no workers, no reaper, no cron scheduler.</li>
 *   <li>{@code JOBQUEUE_ROLE=worker} – workers, reaper and cron scheduler; only actuator over HTTP.</li>
 *   <li>{@code JOBQUEUE_ROLE=all}    – both (local dev, tests).</li>
 * </ul>
 */
public final class Roles {

    public static final String WORKER_ENABLED = "'${jobqueue.role:all}'.toLowerCase() != 'api'";
    public static final String API_ENABLED = "'${jobqueue.role:all}'.toLowerCase() != 'worker'";

    private Roles() {
    }
}
