package com.jobqueue.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jobqueue.api.SubmitJobRequest;
import com.jobqueue.config.JobQueueProperties;
import com.jobqueue.domain.NewJob;
import com.jobqueue.store.JobRepository;
import com.jobqueue.store.JsonCodec;
import com.jobqueue.worker.HandlerRegistry;
import com.jobqueue.worker.JobContext;
import com.jobqueue.worker.JobHandler;

import org.junit.jupiter.api.Test;

class JobServiceTest {

    private final ObjectMapper mapper = new ObjectMapper();

    private final JobQueueProperties props = new JobQueueProperties(
            JobQueueProperties.Role.ALL,
            new JobQueueProperties.Worker("w", 4, 4, Duration.ofMillis(100), Duration.ofSeconds(1),
                    Duration.ofSeconds(1), Duration.ofSeconds(1)),
            new JobQueueProperties.Retry(Duration.ofSeconds(1), Duration.ofMinutes(1), 2.0, true),
            new JobQueueProperties.Defaults(1, 3, Duration.ofSeconds(30)),
            new JobQueueProperties.Scheduler(Duration.ofSeconds(1), Duration.ofSeconds(5)),
            64);

    private final JobHandler echo = new JobHandler() {
        @Override
        public String type() {
            return "echo";
        }

        @Override
        public com.fasterxml.jackson.databind.JsonNode handle(JobContext context) {
            return context.payload();
        }
    };

    private final JobService service = new JobService(mock(JobRepository.class),
            new HandlerRegistry(List.of(echo)), new JsonCodec(mapper), props);

    @Test
    void appliesDefaultsWhenFieldsAreOmitted() {
        NewJob job = service.toNewJob(new SubmitJobRequest("echo", null, null, null, null, null, null), null, null);
        assertThat(job.priority()).isEqualTo(1);
        assertThat(job.maxRetries()).isEqualTo(3);
        assertThat(job.timeoutMs()).isEqualTo(30_000);
        assertThat(job.delayMs()).isZero();
        assertThat(job.payload().isObject()).isTrue();
    }

    @Test
    void convertsRunAtIntoADelay() {
        Instant in10s = Instant.now().plusSeconds(10);
        NewJob job = service.toNewJob(new SubmitJobRequest("echo", null, 5, 0, 1000L, in10s, null), null, null);
        assertThat(job.delayMs()).isBetween(8_000L, 10_000L);
        assertThat(job.priority()).isEqualTo(5);
        assertThat(job.maxRetries()).isZero();
    }

    @Test
    void pastRunAtMeansNow() {
        NewJob job = service.toNewJob(new SubmitJobRequest("echo", null, null, null, null,
                Instant.now().minusSeconds(60), null), null, null);
        assertThat(job.delayMs()).isZero();
    }

    @Test
    void rejectsUnknownTypes() {
        assertThatThrownBy(() -> service.toNewJob(
                new SubmitJobRequest("nope", null, null, null, null, null, null), null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown job type");
    }

    @Test
    void rejectsBothRunAtAndDelay() {
        assertThatThrownBy(() -> service.toNewJob(
                new SubmitJobRequest("echo", null, null, null, null, Instant.now(), 5L), null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsOversizedPayloads() {
        ObjectNode big = mapper.createObjectNode().put("data", "x".repeat(100));
        assertThatThrownBy(() -> service.toNewJob(
                new SubmitJobRequest("echo", big, null, null, null, null, null), null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("limit");
    }

    @Test
    void rejectsBlankIdempotencyKey() {
        assertThatThrownBy(() -> service.toNewJob(
                new SubmitJobRequest("echo", null, null, null, null, null, null), "  ", null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
