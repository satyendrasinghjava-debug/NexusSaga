/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Deterministic JSON rendering (recursively sorted keys, compact) and SHA-256 helpers. PostgreSQL {@code jsonb}
 * re-orders keys and whitespace, so hashes must be computed over a canonical form, never over raw text.
 */
public final class CanonicalJson {

    public static final String GENESIS_HASH = "0".repeat(64);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CanonicalJson() {
    }

    public static String canonicalize(JsonNode node) {
        try {
            return MAPPER.writeValueAsString(sort(node));
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Cannot canonicalise JSON", e);
        }
    }

    public static String canonicalize(String json) {
        try {
            return canonicalize(MAPPER.readTree(json));
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("Cannot parse JSON for canonicalisation", e);
        }
    }

    public static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static JsonNode sort(JsonNode node) {
        if (node == null) {
            return JsonNodeFactory.instance.nullNode();
        }
        if (node.isObject()) {
            List<String> names = new ArrayList<>();
            Iterator<String> it = node.fieldNames();
            it.forEachRemaining(names::add);
            names.sort(String::compareTo);
            ObjectNode sorted = JsonNodeFactory.instance.objectNode();
            for (String name : names) {
                sorted.set(name, sort(node.get(name)));
            }
            return sorted;
        }
        if (node.isArray()) {
            ArrayNode array = JsonNodeFactory.instance.arrayNode();
            node.forEach(child -> array.add(sort(child)));
            return array;
        }
        return node;
    }

    /** Convenience for building canonical maps in tests and receipts. */
    public static ObjectNode objectOf(Map<String, String> values) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        values.forEach(node::put);
        return node;
    }
}
