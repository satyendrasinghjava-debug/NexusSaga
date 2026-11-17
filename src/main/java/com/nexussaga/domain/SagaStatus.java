/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.domain;

/**
 * Lifecycle states of a saga instance.
 */
public enum SagaStatus {
    STARTED,
    IN_PROGRESS,
    COMPLETED,
    COMPENSATING,
    /** Transitional marker recorded when the zombie reaper detects a hung step; compensation follows. */
    TIMED_OUT,
    FAILED,
    COMPENSATION_FAILED_DLQ;

    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == COMPENSATION_FAILED_DLQ;
    }

    public boolean isActive() {
        return this == STARTED || this == IN_PROGRESS || this == COMPENSATING || this == TIMED_OUT;
    }
}
