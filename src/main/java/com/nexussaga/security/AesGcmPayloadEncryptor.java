/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.security;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;

/**
 * AES-256-GCM field-level encryption for PII inside saga payloads.
 * <p>
 * Ciphertext format: {@code enc:v1:<base64(iv || ciphertext || tag)>} for text values and
 * {@code enc:v1j:<...>} for non-text JSON values (numbers, objects), which are restored as JSON on decryption.
 * Field paths use dot notation ({@code customer.cardNumber}). A fresh 96-bit IV is used for every value.
 */
public class AesGcmPayloadEncryptor {

    public static final String MASK = "****";
    static final String TEXT_PREFIX = "enc:v1:";
    static final String JSON_PREFIX = "enc:v1j:";

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int IV_LENGTH = 12;
    private static final int TAG_BITS = 128;

    private final SecretKey key;
    private final SecureRandom random = new SecureRandom();
    private final ObjectMapper objectMapper;

    public AesGcmPayloadEncryptor(String base64Key, ObjectMapper objectMapper) {
        if (base64Key == null || base64Key.isBlank()) {
            throw new IllegalArgumentException("nexussaga.security.aes-key must be configured");
        }
        byte[] raw = Base64.getDecoder().decode(base64Key.trim());
        if (raw.length != 32) {
            throw new IllegalArgumentException("nexussaga.security.aes-key must decode to exactly 32 bytes (AES-256), got "
                    + raw.length);
        }
        this.key = new SecretKeySpec(raw, "AES");
        this.objectMapper = objectMapper;
    }

    /** Encrypts a UTF-8 string. */
    public String encrypt(String plaintext) {
        return TEXT_PREFIX + seal(plaintext);
    }

    /** Decrypts a value produced by {@link #encrypt}. */
    public String decrypt(String ciphertext) {
        if (ciphertext.startsWith(JSON_PREFIX)) {
            return open(ciphertext.substring(JSON_PREFIX.length()));
        }
        if (ciphertext.startsWith(TEXT_PREFIX)) {
            return open(ciphertext.substring(TEXT_PREFIX.length()));
        }
        throw new IllegalArgumentException("Value is not NexusSaga ciphertext");
    }

    public static boolean isEncrypted(String value) {
        return value != null && (value.startsWith(TEXT_PREFIX) || value.startsWith(JSON_PREFIX));
    }

    /** Returns a deep copy of {@code payload} with every listed field encrypted (already encrypted values are kept). */
    public JsonNode encryptFields(JsonNode payload, List<String> fieldPaths) {
        JsonNode copy = payload.deepCopy();
        for (String path : fieldPaths) {
            transform(copy, path, value -> {
                if (value.isTextual()) {
                    return isEncrypted(value.textValue()) ? value : TextNode.valueOf(encrypt(value.textValue()));
                }
                return TextNode.valueOf(JSON_PREFIX + seal(value.toString()));
            });
        }
        return copy;
    }

    /** Returns a deep copy of {@code payload} with every listed field decrypted back to its original JSON value. */
    public JsonNode decryptFields(JsonNode payload, List<String> fieldPaths) {
        JsonNode copy = payload.deepCopy();
        for (String path : fieldPaths) {
            transform(copy, path, value -> {
                if (!value.isTextual() || !isEncrypted(value.textValue())) {
                    return value;
                }
                String text = value.textValue();
                if (text.startsWith(JSON_PREFIX)) {
                    try {
                        return objectMapper.readTree(decrypt(text));
                    } catch (JsonProcessingException e) {
                        throw new IllegalStateException("Decrypted JSON value is malformed", e);
                    }
                }
                return TextNode.valueOf(decrypt(text));
            });
        }
        return copy;
    }

    /** Returns a deep copy of {@code payload} with every listed field replaced by {@value #MASK}. Safe for logs. */
    public static JsonNode maskFields(JsonNode payload, List<String> fieldPaths) {
        JsonNode copy = payload.deepCopy();
        for (String path : fieldPaths) {
            transform(copy, path, value -> TextNode.valueOf(MASK));
        }
        return copy;
    }

    private String seal(String plaintext) {
        try {
            byte[] iv = new byte[IV_LENGTH];
            random.nextBytes(iv);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            byte[] sealed = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(ByteBuffer.allocate(iv.length + sealed.length)
                    .put(iv).put(sealed).array());
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-GCM encryption failed", e);
        }
    }

    private String open(String encoded) {
        try {
            byte[] all = Base64.getDecoder().decode(encoded);
            if (all.length <= IV_LENGTH) {
                throw new IllegalArgumentException("Ciphertext too short");
            }
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, all, 0, IV_LENGTH));
            byte[] plain = cipher.doFinal(all, IV_LENGTH, all.length - IV_LENGTH);
            return new String(plain, StandardCharsets.UTF_8);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-GCM decryption failed: ciphertext was tampered with or key is wrong", e);
        }
    }

    /** Applies {@code fn} to the node at {@code dottedPath} if it exists and is not null. */
    private static void transform(JsonNode root, String dottedPath, java.util.function.UnaryOperator<JsonNode> fn) {
        String[] segments = dottedPath.split("\\.");
        JsonNode parent = root;
        for (int i = 0; i < segments.length - 1; i++) {
            if (parent == null || !parent.isObject()) {
                return;
            }
            parent = parent.get(segments[i]);
        }
        if (parent instanceof ObjectNode objectNode) {
            String leaf = segments[segments.length - 1];
            JsonNode value = objectNode.get(leaf);
            if (value != null && !value.isNull()) {
                objectNode.set(leaf, fn.apply(value));
            }
        }
    }
}
