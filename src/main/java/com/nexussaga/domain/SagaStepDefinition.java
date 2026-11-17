/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.domain;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

import java.util.List;

/**
 * One step of a workflow.
 *
 * @param stepName          unique name inside the workflow
 * @param forwardTopic      Kafka topic receiving the forward command
 * @param compensationTopic Kafka topic receiving the compensating command
 * @param maxRetries        additional attempts after the first failure (0 = no retry)
 * @param timeoutSeconds    maximum time the step may stay unanswered before the zombie reaper intervenes
 * @param encryptedFields   JSON paths (dot notation, e.g. {@code customer.cardNumber}) to encrypt at rest
 */
public record SagaStepDefinition(
        @NotBlank @Pattern(regexp = "[A-Za-z0-9._-]{1,128}") String stepName,
        @NotBlank @Pattern(regexp = "[A-Za-z0-9._-]{1,249}") String forwardTopic,
        @NotBlank @Pattern(regexp = "[A-Za-z0-9._-]{1,249}") String compensationTopic,
        @Min(0) @Max(20) int maxRetries,
        @Min(1) @Max(86_400) int timeoutSeconds,
        List<String> encryptedFields) {

    public SagaStepDefinition {
        encryptedFields = encryptedFields == null ? List.of() : List.copyOf(encryptedFields);
    }
}
