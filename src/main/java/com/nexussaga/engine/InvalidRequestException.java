/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.engine;

/**
 * Thrown when input is semantically invalid (HTTP 400).
 */
public class InvalidRequestException extends RuntimeException {

    public InvalidRequestException(String message) {
        super(message);
    }
}
