/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexussaga.engine.SagaOrchestrator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes the unified reply topic. Replies are keyed by {@code sagaId}, so all replies of one saga arrive
 * in order on a single partition.
 */
@Component
public class StepReplyListener {

    private static final Logger log = LoggerFactory.getLogger(StepReplyListener.class);

    private final SagaOrchestrator orchestrator;
    private final ObjectMapper objectMapper;

    public StepReplyListener(SagaOrchestrator orchestrator, ObjectMapper objectMapper) {
        this.orchestrator = orchestrator;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(id = "nexussaga-reply-listener", topics = "${nexussaga.kafka.reply-topic:nexussaga.step.replies}",
            groupId = "${spring.kafka.consumer.group-id:nexussaga-orchestrator}")
    public void onReply(ConsumerRecord<String, String> record) throws JsonProcessingException {
        StepReply reply = objectMapper.readValue(record.value(), StepReply.class);
        log.debug("Reply {} for saga {} step {} {} {} (partition {}, offset {})", reply.messageId(), reply.sagaId(),
                reply.stepIndex(), reply.type(), reply.outcome(), record.partition(), record.offset());
        orchestrator.handleReply(reply);
    }
}
