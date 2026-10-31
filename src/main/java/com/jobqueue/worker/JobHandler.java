package com.jobqueue.worker;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The pluggable unit of work. Register a handler by declaring it as a Spring bean.
 *
 * <p>Contract:
 * <ul>
 *   <li>Return a JSON result on success (stored as the job's result payload).</li>
 *   <li>Throw {@link NonRetryableJobException} for permanent failures (bad input): the job is dead-lettered
 *       immediately. Any other exception is treated as transient and retried with back-off.</li>
 *   <li>Respond to interruption: when the attempt times out the worker interrupts the handler thread.</li>
 *   <li>Be idempotent. Delivery is at-least-once; use {@link JobContext#jobId()} as an idempotency key
 *       and {@link JobContext#attempt()} as a fencing token for external side effects.</li>
 * </ul>
 */
public interface JobHandler {

    /** The job type this handler serves, e.g. {@code "send-email"}. */
    String type();

    JsonNode handle(JobContext context) throws Exception;
}
