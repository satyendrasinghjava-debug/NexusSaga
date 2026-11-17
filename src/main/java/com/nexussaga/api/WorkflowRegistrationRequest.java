/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.api;

import com.nexussaga.domain.SagaStepDefinition;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * Body of {@code POST /api/v1/workflows}.
 */
public record WorkflowRegistrationRequest(
        @NotBlank @Size(max = 128) String workflowName,
        @Size(max = 512) String description,
        @NotEmpty @Size(max = 100) List<@Valid SagaStepDefinition> steps) {
}
