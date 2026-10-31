package com.jobqueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;

/**
 * End-to-end: HTTP API + real worker pool + real Postgres. Timings are shrunk so the whole lifecycle
 * (retries, back-off, timeouts, dead-lettering, cron) runs in seconds.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "jobqueue.role=all",
        "jobqueue.worker.concurrency=4",
        "jobqueue.worker.poll-interval=50ms",
        "jobqueue.worker.lease-grace=500ms",
        "jobqueue.worker.reaper-interval=200ms",
        "jobqueue.worker.shutdown-grace=1s",
        "jobqueue.retry.base-delay=50ms",
        "jobqueue.retry.max-delay=200ms",
        "jobqueue.retry.jitter=false",
        "jobqueue.scheduler.recurring-poll-interval=100ms",
        "jobqueue.scheduler.stats-refresh-interval=100ms"
})
@AutoConfigureObservability
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS) // stop workers before other test classes run
class JobApiIT extends PostgresSupport {

    private static final Duration WAIT = Duration.ofSeconds(15);

    @Autowired
    TestRestTemplate http;

    // ------------------------------------------------------------------ helpers

    private ResponseEntity<JsonNode> submit(Map<String, Object> body) {
        return submit(body, null);
    }

    private ResponseEntity<JsonNode> submit(Map<String, Object> body, String idempotencyKey) {
        HttpHeaders headers = new HttpHeaders();
        if (idempotencyKey != null) {
            headers.add("Idempotency-Key", idempotencyKey);
        }
        return http.exchange("/api/v1/jobs", HttpMethod.POST, new HttpEntity<>(body, headers), JsonNode.class);
    }

    private String submitOk(Map<String, Object> body) {
        ResponseEntity<JsonNode> r = submit(body);
        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        return r.getBody().get("id").asText();
    }

    private JsonNode fetch(String id) {
        return http.getForObject("/api/v1/jobs/" + id, JsonNode.class);
    }

    private JsonNode awaitStatus(String id, String status) {
        return await().atMost(WAIT).pollInterval(Duration.ofMillis(50))
                .until(() -> fetch(id), j -> status.equals(j.path("status").asText()));
    }

    private JsonNode post(String path) {
        return http.postForObject(path, null, JsonNode.class);
    }

    private static Map<String, Object> job(String type, Object... kv) {
        Map<String, Object> m = new HashMap<>();
        m.put("type", type);
        Map<String, Object> payload = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            String k = (String) kv[i];
            if (k.startsWith("$")) {
                m.put(k.substring(1), kv[i + 1]); // "$maxRetries" → top-level field
            } else {
                payload.put(k, kv[i + 1]);
            }
        }
        m.put("payload", payload);
        return m;
    }

    // ------------------------------------------------------------------ submission & execution

    @Test
    void submissionReturnsImmediatelyAndExecutesAsynchronously() {
        ResponseEntity<JsonNode> r = submit(job("sleep", "ms", 700));

        assertThat(r.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(r.getBody().get("status").asText()).isEqualTo("QUEUED");
        String id = r.getBody().get("id").asText();
        assertThat(r.getHeaders().getLocation()).hasToString("/api/v1/jobs/" + id);

        awaitStatus(id, "RUNNING");
        JsonNode done = awaitStatus(id, "SUCCEEDED");
        assertThat(done.get("result").get("sleptMs").asInt()).isEqualTo(700);
    }

    @Test
    void successfulJobExposesResultAndAttemptCount() {
        String id = submitOk(job("echo", "hello", "world"));
        JsonNode done = awaitStatus(id, "SUCCEEDED");

        assertThat(done.get("attempts").asInt()).isEqualTo(1);
        assertThat(done.get("result").get("echo").get("hello").asText()).isEqualTo("world");
        assertThat(done.has("lastError")).isFalse();
        assertThat(done.get("finishedAt")).isNotNull();
    }

    @Test
    void transientFailuresAreRetriedUntilSuccess() {
        String id = submitOk(job("flaky", "failTimes", 2, "$maxRetries", 3));
        JsonNode done = awaitStatus(id, "SUCCEEDED");

        assertThat(done.get("attempts").asInt()).isEqualTo(3);
        assertThat(done.get("result").get("succeededOnAttempt").asInt()).isEqualTo(3);
        JsonNode attempts = http.getForObject("/api/v1/jobs/" + id + "/attempts", JsonNode.class);
        assertThat(attempts).hasSize(3);
        assertThat(attempts.get(0).get("outcome").asText()).isEqualTo("FAILED");
        assertThat(attempts.get(1).get("outcome").asText()).isEqualTo("FAILED");
        assertThat(attempts.get(2).get("outcome").asText()).isEqualTo("SUCCEEDED");
    }

    @Test
    void exhaustedRetriesMoveTheJobToTheDeadLetterStore() {
        String id = submitOk(job("fail", "message", "downstream 503", "$maxRetries", 2));
        JsonNode dead = awaitStatus(id, "DEAD_LETTERED");

        assertThat(dead.get("attempts").asInt()).isEqualTo(3);
        assertThat(dead.get("lastError").asText()).contains("downstream 503");
        JsonNode dlq = http.getForObject("/api/v1/dead-letters", JsonNode.class);
        assertThat(dlq).hasSize(1);
        assertThat(dlq.get(0).get("jobId").asText()).isEqualTo(id);
        assertThat(dlq.get(0).get("reason").asText()).isEqualTo("RETRIES_EXHAUSTED");
    }

    @Test
    void permanentFailuresSkipRetries() {
        String id = submitOk(job("fail", "retryable", false, "$maxRetries", 5));
        JsonNode dead = awaitStatus(id, "DEAD_LETTERED");

        assertThat(dead.get("attempts").asInt()).isEqualTo(1);
        assertThat(http.getForObject("/api/v1/dead-letters", JsonNode.class).get(0).get("reason").asText())
                .isEqualTo("NON_RETRYABLE");
    }

    @Test
    void attemptsThatExceedTheirTimeoutAreRetriedThenDeadLettered() {
        String id = submitOk(job("sleep", "ms", 5_000, "$timeoutMs", 200, "$maxRetries", 1));
        JsonNode dead = awaitStatus(id, "DEAD_LETTERED");

        assertThat(dead.get("attempts").asInt()).isEqualTo(2);
        assertThat(dead.get("lastError").asText()).contains("timed out after 200 ms");
        JsonNode attempts = http.getForObject("/api/v1/jobs/" + id + "/attempts", JsonNode.class);
        assertThat(attempts).allSatisfy(a -> assertThat(a.get("outcome").asText()).isEqualTo("TIMED_OUT"));
    }

    @Test
    void deadLetteredJobsCanBeReplayed() {
        String id = submitOk(job("fail", "retryable", false, "$maxRetries", 0));
        awaitStatus(id, "DEAD_LETTERED");

        JsonNode replayed = post("/api/v1/dead-letters/" + id + "/replay");
        assertThat(replayed.get("status").asText()).isIn("QUEUED", "RUNNING", "DEAD_LETTERED");
        JsonNode again = await().atMost(WAIT).until(() -> fetch(id),
                j -> j.get("attempts").asInt() == 2 && "DEAD_LETTERED".equals(j.get("status").asText()));
        assertThat(again.get("attempts").asInt()).isEqualTo(2);

        ResponseEntity<JsonNode> notDead = http.postForEntity(
                "/api/v1/dead-letters/" + submitOk(job("echo")) + "/replay", null, JsonNode.class);
        assertThat(notDead.getStatusCode()).isIn(HttpStatus.CONFLICT); // echo job is not dead-lettered
    }

    // ------------------------------------------------------------------ scheduling

    @Test
    void delayedJobsWaitUntilTheirTime() {
        String id = submitOk(job("echo", "$delayMs", 1_500));

        JsonNode stats = await().atMost(WAIT).until(
                () -> http.getForObject("/api/v1/queue/stats", JsonNode.class),
                s -> s.get("delayed").asLong() == 1);
        assertThat(stats.get("depth").asLong()).isZero();
        assertThat(fetch(id).get("status").asText()).isEqualTo("QUEUED");

        JsonNode done = awaitStatus(id, "SUCCEEDED");
        Instant created = Instant.parse(done.get("createdAt").asText());
        Instant started = Instant.parse(done.get("startedAt").asText());
        assertThat(Duration.between(created, started)).isGreaterThanOrEqualTo(Duration.ofMillis(1_400));
    }

    @Test
    void runAtSchedulesForAnAbsoluteTime() {
        Instant runAt = Instant.now().plusSeconds(1);
        String id = submitOk(job("echo", "$runAt", runAt.toString()));
        JsonNode done = awaitStatus(id, "SUCCEEDED");
        assertThat(Instant.parse(done.get("startedAt").asText())).isAfterOrEqualTo(runAt.minusMillis(200));
    }

    @Test
    void recurringJobsEnqueueOnEveryFireAndStopWhenDeleted() {
        Map<String, Object> body = Map.of(
                "name", "heartbeat",
                "cron", "* * * * * *", // every second
                "job", Map.of("type", "echo", "payload", Map.of("ping", true)));
        ResponseEntity<JsonNode> created = http.postForEntity("/api/v1/recurring-jobs", body, JsonNode.class);
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String recurringId = created.getBody().get("id").asText();

        assertThat(http.postForEntity("/api/v1/recurring-jobs", body, JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);

        await().atMost(WAIT).until(() -> {
            JsonNode list = http.getForObject("/api/v1/jobs?status=SUCCEEDED&type=echo", JsonNode.class);
            long fromCron = 0;
            for (JsonNode j : list) {
                if (recurringId.equals(j.path("recurringJobId").asText())) {
                    fromCron++;
                }
            }
            return fromCron >= 2;
        });

        http.delete("/api/v1/recurring-jobs/" + recurringId);
        assertThat(http.getForObject("/api/v1/recurring-jobs", JsonNode.class)).isEmpty();
    }

    @Test
    void invalidCronIsRejected() {
        Map<String, Object> body = Map.of("name", "bad", "cron", "whenever", "job", Map.of("type", "echo"));
        assertThat(http.postForEntity("/api/v1/recurring-jobs", body, JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
    }

    // ------------------------------------------------------------------ operational control

    @Test
    void pauseDrainsDispatchAndResumeRestartsIt() {
        assertThat(post("/api/v1/queue/pause").get("paused").asBoolean()).isTrue();
        String id = submitOk(job("echo"));

        // Workers re-read the pause flag at most once per second; give them well over that.
        await().during(Duration.ofMillis(1_500)).atMost(Duration.ofSeconds(3))
                .until(() -> "QUEUED".equals(fetch(id).get("status").asText()));
        assertThat(http.getForObject("/api/v1/queue/stats", JsonNode.class).get("depth").asLong()).isEqualTo(1);

        assertThat(post("/api/v1/queue/resume").get("paused").asBoolean()).isFalse();
        awaitStatus(id, "SUCCEEDED");
    }

    @Test
    void pendingJobsCanBeCancelledAndNeverRun() {
        post("/api/v1/queue/pause");
        String id = submitOk(job("echo"));

        JsonNode cancelled = post("/api/v1/jobs/" + id + "/cancel");
        assertThat(cancelled.get("status").asText()).isEqualTo("CANCELLED");

        post("/api/v1/queue/resume");
        await().during(Duration.ofMillis(1_500)).atMost(Duration.ofSeconds(3))
                .until(() -> "CANCELLED".equals(fetch(id).get("status").asText()));
        assertThat(fetch(id).get("attempts").asInt()).isZero();
    }

    @Test
    void runningAndUnknownJobsCannotBeCancelled() {
        String id = submitOk(job("sleep", "ms", 1_500));
        awaitStatus(id, "RUNNING");
        assertThat(http.postForEntity("/api/v1/jobs/" + id + "/cancel", null, JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);
        assertThat(http.postForEntity("/api/v1/jobs/00000000-0000-0000-0000-000000000000/cancel", null,
                JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        awaitStatus(id, "SUCCEEDED");
    }

    @Test
    void purgeCancelsAllPendingJobsOptionallyByType() {
        post("/api/v1/queue/pause");
        for (int i = 0; i < 5; i++) {
            submitOk(job("echo"));
        }
        submitOk(job("sleep", "ms", 10));
        submitOk(job("sleep", "ms", 10));

        JsonNode stats = http.getForObject("/api/v1/queue/stats", JsonNode.class);
        assertThat(stats.get("depth").asLong()).isEqualTo(7);

        assertThat(post("/api/v1/queue/purge?type=echo").get("cancelled").asInt()).isEqualTo(5);
        assertThat(post("/api/v1/queue/purge").get("cancelled").asInt()).isEqualTo(2);
        stats = http.getForObject("/api/v1/queue/stats", JsonNode.class);
        assertThat(stats.get("depth").asLong()).isZero();
        assertThat(stats.get("byStatus").get("CANCELLED").asLong()).isEqualTo(7);
    }

    // ------------------------------------------------------------------ API contract

    @Test
    void idempotencyKeyReturnsTheOriginalJob() {
        ResponseEntity<JsonNode> first = submit(job("echo", "n", 1), "order-123");
        ResponseEntity<JsonNode> second = submit(job("echo", "n", 1), "order-123");

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().get("id")).isEqualTo(first.getBody().get("id"));
        assertThat(http.getForObject("/api/v1/jobs?type=echo", JsonNode.class)).hasSize(1);
    }

    @Test
    void invalidSubmissionsAreRejectedWithProblemDetails() {
        List<Map<String, Object>> bad = List.of(
                Map.of("payload", Map.of()),                                  // missing type
                Map.of("type", "does-not-exist"),                             // unknown handler
                Map.of("type", "echo", "maxRetries", 99),                     // out of range
                Map.of("type", "echo", "timeoutMs", 5),                       // out of range
                Map.of("type", "echo", "priority", 10),                       // out of range
                Map.of("type", "echo", "delayMs", 10, "runAt", Instant.now().toString())); // both
        for (Map<String, Object> body : bad) {
            ResponseEntity<JsonNode> r = submit(body);
            assertThat(r.getStatusCode()).as("body %s", body).isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(r.getHeaders().getContentType()).hasToString("application/problem+json");
        }
        assertThat(http.getForEntity("/api/v1/jobs/00000000-0000-0000-0000-000000000000", JsonNode.class)
                .getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    // ------------------------------------------------------------------ observability

    @Test
    void metricsAreExposedForPrometheus() {
        awaitStatus(submitOk(job("echo")), "SUCCEEDED");
        awaitStatus(submitOk(job("fail", "retryable", false)), "DEAD_LETTERED");

        String prom = await().atMost(WAIT).until(
                () -> http.getForObject("/actuator/prometheus", String.class),
                body -> body != null && body.contains("jobqueue_jobs_dead_lettered_total"));
        assertThat(prom).contains(
                "jobqueue_queue_depth",
                "jobqueue_queue_oldest_ready_age_seconds",
                "jobqueue_workers_utilization",
                "jobqueue_job_end_to_end_seconds",
                "jobqueue_job_queue_wait_seconds",
                "jobqueue_jobs_finished_total");
    }
}
