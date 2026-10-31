package com.jobqueue.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;

class HandlerRegistryTest {

    record Fixed(String type) implements JobHandler {
        @Override
        public JsonNode handle(JobContext context) {
            return null;
        }
    }

    @Test
    void resolvesHandlersByType() {
        HandlerRegistry r = new HandlerRegistry(List.of(new Fixed("a"), new Fixed("b")));
        assertThat(r.supports("a")).isTrue();
        assertThat(r.find("b")).isPresent();
        assertThat(r.find("c")).isEmpty();
        assertThat(r.types()).containsExactly("a", "b");
    }

    @Test
    void failsFastOnDuplicateTypes() {
        assertThatThrownBy(() -> new HandlerRegistry(List.of(new Fixed("a"), new Fixed("a"))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Duplicate");
    }

    @Test
    void contextKnowsItsLastAttempt() {
        assertThat(new JobContext(null, "a", null, 3, 3).isLastAttempt()).isTrue();
        assertThat(new JobContext(null, "a", null, 2, 3).isLastAttempt()).isFalse();
    }
}
