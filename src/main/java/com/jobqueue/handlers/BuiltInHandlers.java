package com.jobqueue.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jobqueue.worker.JobContext;
import com.jobqueue.worker.JobHandler;
import com.jobqueue.worker.NonRetryableJobException;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Demo handlers that exercise every lifecycle path. Real handlers are just more {@link JobHandler} beans.
 *
 * <ul>
 *   <li>{@code echo}  – returns its payload.</li>
 *   <li>{@code sleep} – sleeps {@code payload.ms} milliseconds (use to demo timeouts and load).</li>
 *   <li>{@code flaky} – fails transiently until attempt &gt; {@code payload.failTimes}, then succeeds.</li>
 *   <li>{@code fail}  – always fails; {@code payload.retryable=false} makes it a permanent failure.</li>
 * </ul>
 */
@Configuration
public class BuiltInHandlers {

    @Bean
    JobHandler echoHandler(ObjectMapper mapper) {
        return handler("echo", ctx -> {
            ObjectNode out = mapper.createObjectNode();
            out.set("echo", ctx.payload());
            out.put("attempt", ctx.attempt());
            return out;
        });
    }

    @Bean
    JobHandler sleepHandler(ObjectMapper mapper) {
        return handler("sleep", ctx -> {
            long ms = ctx.payload().path("ms").asLong(1000);
            Thread.sleep(ms); // interruptible: honours the attempt timeout
            return mapper.createObjectNode().put("sleptMs", ms);
        });
    }

    @Bean
    JobHandler flakyHandler(ObjectMapper mapper) {
        return handler("flaky", ctx -> {
            int failTimes = ctx.payload().path("failTimes").asInt(1);
            if (ctx.attempt() <= failTimes) {
                throw new IllegalStateException("transient failure on attempt " + ctx.attempt());
            }
            return mapper.createObjectNode().put("succeededOnAttempt", ctx.attempt());
        });
    }

    @Bean
    JobHandler failHandler() {
        return handler("fail", ctx -> {
            String message = ctx.payload().path("message").asText("intentional failure");
            if (!ctx.payload().path("retryable").asBoolean(true)) {
                throw new NonRetryableJobException(message);
            }
            throw new IllegalStateException(message);
        });
    }

    @FunctionalInterface
    interface Body {
        JsonNode run(JobContext ctx) throws Exception;
    }

    private static JobHandler handler(String type, Body body) {
        return new JobHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public JsonNode handle(JobContext context) throws Exception {
                return body.run(context);
            }
        };
    }
}
