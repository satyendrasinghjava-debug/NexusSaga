/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.provider;

/**
 * Maps arbitrary lock keys onto a bounded set of lock stripes, so providers never create one
 * distributed lock object per saga (which would leak memory on long-running clusters).
 */
public final class LockStripes {

    private LockStripes() {
    }

    /** Returns the stripe index of {@code key} in the range {@code [0, stripes)}. */
    public static int stripeOf(String key, int stripes) {
        if (stripes <= 0) {
            throw new IllegalArgumentException("stripes must be positive");
        }
        return Math.floorMod(key.hashCode(), stripes);
    }

    /** Returns a stable lock name for {@code key}. */
    public static String lockName(String key, int stripes) {
        return "nexussaga-lock-" + stripeOf(key, stripes);
    }
}
