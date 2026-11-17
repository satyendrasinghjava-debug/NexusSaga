/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.domain.OutboxEvent;
import com.nexussaga.domain.SagaDefinition;
import com.nexussaga.domain.SagaInstance;
import com.nexussaga.domain.SagaStepDefinition;
import com.nexussaga.messaging.CommandType;
import com.nexussaga.messaging.StepCommand;
import com.nexussaga.repository.OutboxEventRepository;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Turns a saga position into a {@link StepCommand} and stores it in the transactional outbox.
 * Must run inside the transaction that changes the saga row, so the command and the state change
 * commit (or roll back) together.
 */
@Component
public class SagaCommandDispatcher {

    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final NexusSagaProperties properties;

    public SagaCommandDispatcher(OutboxEventRepository outboxRepository, ObjectMapper objectMapper,
                                 NexusSagaProperties properties) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    /**
     * Enqueues the command for the instance's current step and records its id as the only reply still expected.
     *
     * @return the enqueued command
     */
    public StepCommand dispatch(SagaInstance instance, SagaDefinition definition, CommandType type, Instant now) {
        SagaStepDefinition step = definition.step(instance.getCurrentStepIndex());
        String topic = type == CommandType.FORWARD ? step.forwardTopic() : step.compensationTopic();
        StepCommand command;
        try {
            command = new StepCommand(UUID.randomUUID().toString(), instance.getSagaId(), instance.getWorkflowName(),
                    step.stepName(), instance.getCurrentStepIndex(), type, instance.getRetryCount() + 1,
                    objectMapper.readTree(instance.getPayload()), properties.kafka().replyTopic(), now);
            outboxRepository.save(new OutboxEvent(instance.getSagaId(), topic, instance.getSagaId(),
                    type == CommandType.FORWARD ? "STEP_FORWARD_COMMAND" : "STEP_COMPENSATE_COMMAND",
                    objectMapper.writeValueAsString(command), now));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialise command for saga " + instance.getSagaId(), e);
        }
        instance.setLastCommandId(command.messageId());
        return command;
    }
}
