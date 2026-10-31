package com.jobqueue;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Real Postgres for integration tests. The queue's correctness depends on Postgres semantics
 * (SKIP LOCKED, now(), partial indexes), so there is deliberately no H2 / in-memory substitute.
 *
 * <p>Default: a Testcontainers Postgres 16 (shared by all test classes). To use an existing database
 * instead (e.g. no Docker available), set JOBQUEUE_TEST_JDBC_URL, JOBQUEUE_TEST_DB_USER, JOBQUEUE_TEST_DB_PASSWORD.
 */
public abstract class PostgresSupport {

    private static final String EXTERNAL_URL = System.getenv("JOBQUEUE_TEST_JDBC_URL");
    private static final PostgreSQLContainer<?> POSTGRES = EXTERNAL_URL == null ? start() : null;

    private static PostgreSQLContainer<?> start() {
        PostgreSQLContainer<?> c = new PostgreSQLContainer<>("postgres:16-alpine");
        c.start(); // stopped by Testcontainers' Ryuk when the JVM exits
        return c;
    }

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        if (POSTGRES != null) {
            registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
            registry.add("spring.datasource.username", POSTGRES::getUsername);
            registry.add("spring.datasource.password", POSTGRES::getPassword);
        } else {
            registry.add("spring.datasource.url", () -> EXTERNAL_URL);
            registry.add("spring.datasource.username", () -> System.getenv().getOrDefault("JOBQUEUE_TEST_DB_USER", "postgres"));
            registry.add("spring.datasource.password", () -> System.getenv().getOrDefault("JOBQUEUE_TEST_DB_PASSWORD", "postgres"));
        }
    }

    @Autowired
    protected JdbcClient jdbc;

    @BeforeEach
    void resetDatabase() {
        jdbc.sql("TRUNCATE jobs, job_attempts, dead_letters, recurring_jobs CASCADE").update();
        jdbc.sql("UPDATE queue_control SET paused = false").update();
    }
}
