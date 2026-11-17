/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.domain;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Immutable, versioned workflow definition.
 *
 * @param workflowName name used to start sagas
 * @param version      monotonically increasing registration version
 * @param description  free text
 * @param steps        ordered steps; index 0 runs first, compensation runs in reverse
 */
public record SagaDefinition(String workflowName, int version, String description, List<SagaStepDefinition> steps) {

    public SagaDefinition {
        steps = List.copyOf(steps);
    }

    public SagaStepDefinition step(int index) {
        return steps.get(index);
    }

    public int stepCount() {
        return steps.size();
    }

    public boolean isLastStep(int index) {
        return index == steps.size() - 1;
    }

    /** Union of every step's encrypted field paths, in declaration order. */
    public List<String> allEncryptedFields() {
        Set<String> fields = new LinkedHashSet<>();
        steps.forEach(step -> fields.addAll(step.encryptedFields()));
        return List.copyOf(fields);
    }

    public List<String> stepNames() {
        return steps.stream().map(SagaStepDefinition::stepName).toList();
    }
}
