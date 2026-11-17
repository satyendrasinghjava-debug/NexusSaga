/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.realtime;

import com.nexussaga.domain.SagaStatus;

import java.time.Instant;
import java.util.List;

/**
 * Event pushed to dashboard subscribers over STOMP. Never contains payload data.
 *
 * @param sagaId       saga identifier
 * @param tenantId     owning tenant
 * @param workflowName workflow name
 * @param eventType    audit event type, e.g. STEP_COMPLETED, COMPENSATION_DISPATCHED
 * @param status       saga status after the event
 * @param stepIndex    step the event refers to
 * @param stepName     name of that step
 * @param steps        all step names of the workflow, in order
 * @param message      short human readable description
 * @param timestamp    event time
 */
public record SagaLiveEvent(String sagaId, String tenantId, String workflowName, String eventType, SagaStatus status,
                            int stepIndex, String stepName, List<String> steps, String message, Instant timestamp) {
}
