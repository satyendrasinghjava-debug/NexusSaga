/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.domain;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;

/**
 * Read model of a saga exposed by the API. {@code payload} is always the masked view (PII shown as {@code ****}).
 */
public record SagaContext(
        String sagaId,
        String correlationId,
        String tenantId,
        String workflowName,
        int definitionVersion,
        int currentStepIndex,
        String currentStepName,
        List<String> steps,
        SagaStatus status,
        int retryCount,
        JsonNode payload,
        String failureReason,
        Instant startedAt,
        Instant updatedAt,
        Instant completedAt,
        long version) {
}
