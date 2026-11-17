/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.chaos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexussaga.config.NexusSagaProperties;
import com.nexussaga.messaging.CommandType;
import com.nexussaga.messaging.ReplyOutcome;
import com.nexussaga.messaging.StepCommand;
import com.nexussaga.messaging.StepReply;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChaosStepWorkerTest {

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private ChaosStepWorker worker = worker(0.0);

    @AfterEach
    void tearDown() {
        worker.shutdown();
    }

    @Test
    void happyPathSucceedsWithStepSpecificOutput() {
        for (int i = 0; i < 3; i++) {
            StepReply reply = worker.decide(command("HAPPY_PATH", i, CommandType.FORWARD));
            assertThat(reply.outcome()).isEqualTo(ReplyOutcome.SUCCESS);
            assertThat(reply.commandId()).isEqualTo("cmd-" + i);
        }
        assertThat(worker.decide(command("HAPPY_PATH", 0, CommandType.FORWARD)).output().has("reservationId")).isTrue();
        assertThat(worker.decide(command("HAPPY_PATH", 1, CommandType.FORWARD)).output().has("paymentId")).isTrue();
        assertThat(worker.decide(command("HAPPY_PATH", 2, CommandType.FORWARD)).output().has("trackingNumber")).isTrue();
    }

    @Test
    void step3CrashFailsOnlyTheThirdForwardStep() {
        assertThat(worker.decide(command("STEP3_CRASH", 1, CommandType.FORWARD)).outcome())
                .isEqualTo(ReplyOutcome.SUCCESS);
        StepReply crash = worker.decide(command("STEP3_CRASH", 2, CommandType.FORWARD));
        assertThat(crash.outcome()).isEqualTo(ReplyOutcome.FAILURE);
        assertThat(crash.error()).contains("503");
    }

    @Test
    void zombieHangNeverAnswersStep2AndCompensationsAlwaysSucceed() {
        assertThat(worker.decide(command("ZOMBIE_HANG", 1, CommandType.FORWARD))).isNull();
        for (int i = 0; i < 3; i++) {
            StepReply reply = worker.decide(command("ZOMBIE_HANG", i, CommandType.COMPENSATE));
            assertThat(reply.outcome()).isEqualTo(ReplyOutcome.SUCCESS);
            assertThat(reply.output().path("compensated").asBoolean()).isTrue();
        }
    }

    @Test
    void unknownOrMissingModeDefaultsToHappyPath() {
        assertThat(worker.decide(command("NONSENSE", 2, CommandType.FORWARD)).outcome())
                .isEqualTo(ReplyOutcome.SUCCESS);
        StepCommand noPayload = new StepCommand("c", "s", "wf", "st", 2, CommandType.FORWARD, 1, null, "r",
                Instant.EPOCH);
        assertThat(worker.decide(noPayload).outcome()).isEqualTo(ReplyOutcome.SUCCESS);
        StepCommand noMode = new StepCommand("c", "s", "wf", "st", 2, CommandType.FORWARD, 1,
                mapper.createObjectNode(), "r", Instant.EPOCH);
        assertThat(worker.decide(noMode).outcome()).isEqualTo(ReplyOutcome.SUCCESS);
    }

    @Test
    void repliesToCommandReplyTopicKeyedBySagaIdAndCanDuplicate() throws Exception {
        worker.shutdown();
        worker = worker(1.0);
        worker.onCommand(record(command("HAPPY_PATH", 0, CommandType.FORWARD)));
        verify(kafka, timeout(3000).times(2)).send(eq("replies"), eq("saga-1"), anyString());
    }

    @Test
    void hangingCommandSendsNothing() throws Exception {
        worker.onCommand(record(command("ZOMBIE_HANG", 1, CommandType.FORWARD)));
        verify(kafka, after(300).never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void sendFailuresAreSwallowed() throws Exception {
        when(kafka.send(anyString(), anyString(), anyString())).thenThrow(new IllegalStateException("down"));
        worker.onCommand(record(command("HAPPY_PATH", 0, CommandType.FORWARD)));
        verify(kafka, timeout(3000).times(1)).send(anyString(), anyString(), anyString());
    }

    @Test
    void scenarioDescriptionsAndDemoTopics() {
        assertThat(ChaosScenario.STEP3_CRASH.expectedOutcome()).contains("rollback");
        assertThat(DemoWorkflow.topics()).hasSize(6).contains("demo.payment.refund");
    }

    private ChaosStepWorker worker(double duplicateRate) {
        NexusSagaProperties properties = new NexusSagaProperties(null, null, null, null, null, null, null,
                new NexusSagaProperties.Demo(true, "chaos-demo", duplicateRate, 1, 5));
        return new ChaosStepWorker(kafka, mapper, properties);
    }

    private StepCommand command(String mode, int index, CommandType type) {
        ObjectNode payload = mapper.createObjectNode().put("chaosMode", mode);
        return new StepCommand("cmd-" + index, "saga-1", DemoWorkflow.NAME, "step" + index, index, type, 1, payload,
                "replies", Instant.EPOCH);
    }

    private ConsumerRecord<String, String> record(StepCommand command) throws Exception {
        return new ConsumerRecord<>("demo.inventory.reserve", 0, 0, command.sagaId(),
                mapper.writeValueAsString(command));
    }
}
