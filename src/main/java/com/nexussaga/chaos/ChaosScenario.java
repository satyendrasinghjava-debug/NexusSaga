/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.chaos;

/**
 * Behaviour the mock participants apply to a demo saga (carried in the payload field {@code chaosMode}).
 */
public enum ChaosScenario {
    /** Every step succeeds. */
    HAPPY_PATH("All 3 steps complete -> COMPLETED"),
    /** Step 3 (ship-order) fails on every attempt -> compensate step 2, then step 1 -> FAILED. */
    STEP3_CRASH("Step 3 fails after retries -> reverse rollback of steps 2 and 1 -> FAILED"),
    /** Step 2 (charge-payment) never replies -> the zombie reaper times it out and compensates steps 2 and 1. */
    ZOMBIE_HANG("Step 2 never answers -> zombie reaper TIMED_OUT -> rollback -> FAILED");

    private final String expectedOutcome;

    ChaosScenario(String expectedOutcome) {
        this.expectedOutcome = expectedOutcome;
    }

    public String expectedOutcome() {
        return expectedOutcome;
    }
}
