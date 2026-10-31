package com.jobqueue.store;

import java.io.UncheckedIOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;

import org.springframework.stereotype.Component;

/** Converts between JsonNode and the text form bound to jsonb columns. */
@Component
public class JsonCodec {

    private final ObjectMapper mapper;

    public JsonCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public String write(JsonNode node) {
        if (node == null) {
            return null;
        }
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    public JsonNode read(String json) {
        if (json == null) {
            return null;
        }
        try {
            JsonNode node = mapper.readTree(json);
            return node == null ? NullNode.getInstance() : node;
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
