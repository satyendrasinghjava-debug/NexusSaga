/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.api;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Size;

/**
 * Body of {@code POST /api/v1/sagas/{workflowName}/start}.
 *
 * @param correlationId optional business key; repeated starts with the same key return the same saga
 * @param payload       arbitrary JSON object passed to every step
 */
public record StartSagaRequest(@Size(max = 128) String correlationId, JsonNode payload) {
}
