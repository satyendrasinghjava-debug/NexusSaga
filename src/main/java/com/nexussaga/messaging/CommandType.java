/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.messaging;

/**
 * Direction of a step command or reply.
 */
public enum CommandType {
    /** Execute the step's business action. */
    FORWARD,
    /** Undo the step's business action. */
    COMPENSATE
}
