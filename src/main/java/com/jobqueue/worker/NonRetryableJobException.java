package com.jobqueue.worker;

/** Thrown by a handler for a permanent failure; the job skips remaining retries and is dead-lettered. */
public class NonRetryableJobException extends RuntimeException {

    public NonRetryableJobException(String message) {
        super(message);
    }

    public NonRetryableJobException(String message, Throwable cause) {
        super(message, cause);
    }
}
