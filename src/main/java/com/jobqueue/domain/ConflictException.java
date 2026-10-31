package com.jobqueue.domain;

public class ConflictException extends RuntimeException {
    public ConflictException(String message) {
        super(message);
    }
}
