/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.chaos;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.messaging.CommandType;
import com.nexussaga.messaging.ReplyOutcome;
import com.nexussaga.messaging.StepCommand;
import com.nexussaga.messaging.StepReply;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * In-process mock of the three demo microservices (inventory, payment, shipping), so the demo works without
 * external applications. It consumes the demo forward/compensation topics like a real participant and replies
 * to {@code nexussaga.step.replies} after a random latency. With probability
 * {@code nexussaga.demo.duplicate-reply-rate} it sends the same reply twice, reproducing Kafka's at-least-once
 * redelivery so the in-memory duplicate shield can be observed.
 */
@Component
@ConditionalOnProperty(name = "nexussaga.demo.enabled", havingValue = "true")
public class ChaosStepWorker {

    private static final Logger log = LoggerFactory.getLogger(ChaosStepWorker.class);
    private static final int CRASH_STEP_INDEX = 2;
    private static final int HANG_STEP_INDEX = 1;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final NexusSagaProperties.Demo demo;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4,
            Thread.ofVirtual().name("chaos-worker-", 0).factory());

    public ChaosStepWorker(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper,
                           NexusSagaProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.demo = properties.demo();
    }

    @KafkaListener(id = "nexussaga-chaos-worker", groupId = "nexussaga-chaos-worker",
            topics = {"demo.inventory.reserve", "demo.inventory.release", "demo.payment.charge",
                    "demo.payment.refund", "demo.shipping.create", "demo.shipping.cancel"})
    public void onCommand(ConsumerRecord<String, String> record) throws JsonProcessingException {
        StepCommand command = objectMapper.readValue(record.value(), StepCommand.class);
        StepReply reply = decide(command);
        if (reply == null) {
            log.info("[chaos] {} for saga {} is hanging on purpose (zombie scenario)", command.stepName(),
                    command.sagaId());
            return;
        }
        long latency = ThreadLocalRandom.current().nextLong(demo.minLatencyMs(), demo.maxLatencyMs() + 1);
        scheduler.schedule(() -> send(command.replyTopic(), reply), latency, TimeUnit.MILLISECONDS);
        if (ThreadLocalRandom.current().nextDouble() < demo.duplicateReplyRate()) {
            scheduler.schedule(() -> send(command.replyTopic(), reply), latency + ThreadLocalRandom.current().nextLong(50, 400),
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Returns the reply a participant would send, or {@code null} to simulate a hung service.
     */
    StepReply decide(StepCommand command) {
        ChaosScenario scenario = scenarioOf(command);
        String replyId = UUID.randomUUID().toString();
        if (command.type() == CommandType.COMPENSATE) {
            ObjectNode output = objectMapper.createObjectNode();
            output.put("compensated", true);
            output.put("by", serviceOf(command.stepIndex()));
            return StepReply.answer(command, replyId, ReplyOutcome.SUCCESS, output, null);
        }
        if (scenario == ChaosScenario.ZOMBIE_HANG && command.stepIndex() == HANG_STEP_INDEX) {
            return null;
        }
        if (scenario == ChaosScenario.STEP3_CRASH && command.stepIndex() == CRASH_STEP_INDEX) {
            return StepReply.answer(command, replyId, ReplyOutcome.FAILURE, null,
                    "Carrier API returned HTTP 503 (simulated crash, attempt " + command.attempt() + ")");
        }
        ObjectNode output = objectMapper.createObjectNode();
        switch (command.stepIndex()) {
            case 0 -> output.put("reservationId", "RSV-" + shortId());
            case 1 -> output.put("paymentId", "PAY-" + shortId()).put("authCode", shortId());
            default -> output.put("trackingNumber", "TRK-" + shortId());
        }
        return StepReply.answer(command, replyId, ReplyOutcome.SUCCESS, output, null);
    }

    private void send(String replyTopic, StepReply reply) {
        try {
            kafkaTemplate.send(replyTopic, reply.sagaId(), objectMapper.writeValueAsString(reply));
        } catch (JsonProcessingException | RuntimeException e) {
            log.warn("[chaos] reply for saga {} could not be sent: {}", reply.sagaId(), e.getMessage());
        }
    }

    private static ChaosScenario scenarioOf(StepCommand command) {
        String mode = command.payload() == null ? null : command.payload().path("chaosMode").asText(null);
        if (mode == null) {
            return ChaosScenario.HAPPY_PATH;
        }
        try {
            return ChaosScenario.valueOf(mode);
        } catch (IllegalArgumentException e) {
            return ChaosScenario.HAPPY_PATH;
        }
    }

    private static String serviceOf(int stepIndex) {
        return switch (stepIndex) {
            case 0 -> "inventory-service";
            case 1 -> "payment-service";
            default -> "shipping-service";
        };
    }

    private static String shortId() {
        return UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    @PreDestroy
    void shutdown() {
        scheduler.shutdownNow();
    }
}
