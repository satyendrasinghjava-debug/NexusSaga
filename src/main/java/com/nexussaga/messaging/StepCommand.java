/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.messaging;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * Command published to a step's forward or compensation topic. The Kafka record key is {@code sagaId}.
 *
 * @param messageId    unique id of this command
 * @param sagaId       saga identifier (also the Kafka partition key)
 * @param workflowName workflow the saga belongs to
 * @param stepName     step to execute or compensate
 * @param stepIndex    zero based index of the step
 * @param type         FORWARD or COMPENSATE
 * @param attempt      1-based attempt number; participants must echo it in the reply
 * @param payload      saga context; fields listed in {@code encryptedFields} are AES-256-GCM ciphertext
 * @param replyTopic   topic the participant must reply to
 * @param issuedAt     creation time
 */
public record StepCommand(String messageId, String sagaId, String workflowName, String stepName, int stepIndex,
                          CommandType type, int attempt, JsonNode payload, String replyTopic, Instant issuedAt) {
}
