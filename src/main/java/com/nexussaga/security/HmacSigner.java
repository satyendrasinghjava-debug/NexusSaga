/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.security;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.HexFormat;

/**
 * HMAC-SHA256 helpers with constant-time comparison.
 */
public final class HmacSigner {

    private static final String ALGORITHM = "HmacSHA256";

    private HmacSigner() {
    }

    /** Returns the lowercase hex HMAC-SHA256 of {@code message} under {@code secret}. */
    public static String sign(String secret, byte[] message) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM));
            return HexFormat.of().formatHex(mac.doFinal(message));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HMAC-SHA256 unavailable", e);
        }
    }

    public static String sign(String secret, String message) {
        return sign(secret, message.getBytes(StandardCharsets.UTF_8));
    }

    /** Constant-time comparison of two hex signatures (case-insensitive). */
    public static boolean matches(String expectedHex, String providedHex) {
        if (expectedHex == null || providedHex == null) {
            return false;
        }
        return MessageDigest.isEqual(expectedHex.toLowerCase().getBytes(StandardCharsets.US_ASCII),
                providedHex.trim().toLowerCase().getBytes(StandardCharsets.US_ASCII));
    }

    /** Builds the string-to-sign: {@code <timestamp>.<body>} when a timestamp is used, otherwise the raw body. */
    public static byte[] stringToSign(String timestamp, byte[] body) {
        if (timestamp == null) {
            return body;
        }
        byte[] prefix = (timestamp + ".").getBytes(StandardCharsets.UTF_8);
        byte[] all = new byte[prefix.length + body.length];
        System.arraycopy(prefix, 0, all, 0, prefix.length);
        System.arraycopy(body, 0, all, prefix.length, body.length);
        return all;
    }
}
