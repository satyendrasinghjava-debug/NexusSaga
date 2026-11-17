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

/**
 * Reply sent by a participant to {@code nexussaga.step.replies}.
 *
 * @param messageId unique id of this reply message
 * @param commandId {@code messageId} of the command being answered (mandatory)
 * @param sagaId    saga identifier
 * @param stepName  step that was executed
 * @param stepIndex index echoed from the command
 * @param type      FORWARD or COMPENSATE, echoed from the command
 * @param outcome   SUCCESS or FAILURE
 * @param attempt   attempt echoed from the command
 * @param output    optional result data, merged into the saga context under {@code stepOutputs.<stepName>}
 * @param error     failure description when {@code outcome} is FAILURE
 */
public record StepReply(String messageId, String commandId, String sagaId, String stepName, int stepIndex,
                        CommandType type, ReplyOutcome outcome, int attempt, JsonNode output, String error) {

    /**
     * Key that is identical for every redelivery of a reply to the same command. The outcome is deliberately
     * excluded so a contradictory second reply to one command is also treated as a duplicate.
     */
    public String idempotencyKey() {
        return "reply:" + sagaId + ":" + commandId;
    }

    /** Builds the reply a participant sends for {@code command}. */
    public static StepReply answer(StepCommand command, String messageId, ReplyOutcome outcome, JsonNode output,
                                   String error) {
        return new StepReply(messageId, command.messageId(), command.sagaId(), command.stepName(),
                command.stepIndex(), command.type(), outcome, command.attempt(), output, error);
    }
}
