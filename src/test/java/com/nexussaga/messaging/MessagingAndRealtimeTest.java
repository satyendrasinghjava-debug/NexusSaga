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
import com.nexussaga.domain.SagaStatus;
import com.nexussaga.engine.SagaOrchestrator;
import com.nexussaga.realtime.SagaEventPublisher;
import com.nexussaga.realtime.SagaLiveEvent;
import com.nexussaga.support.TestFixtures;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class MessagingAndRealtimeTest {

    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void listenerParsesReplyAndDelegates() throws Exception {
        SagaOrchestrator orchestrator = mock(SagaOrchestrator.class);
        StepReplyListener listener = new StepReplyListener(orchestrator, mapper);
        StepReply reply = new StepReply("m", "c", "s", "st", 0, CommandType.FORWARD, ReplyOutcome.SUCCESS, 1,
                mapper.createObjectNode().put("k", "v"), "none");

        listener.onReply(new ConsumerRecord<>("nexussaga.step.replies", 0, 0, "s", mapper.writeValueAsString(reply)));

        verify(orchestrator).handleReply(reply);
        assertThat(reply.idempotencyKey()).isEqualTo("reply:s:c");
    }

    @Test
    void listenerRejectsMalformedJsonForDeadLettering() {
        StepReplyListener listener = new StepReplyListener(mock(SagaOrchestrator.class), mapper);
        assertThatThrownBy(() -> listener.onReply(new ConsumerRecord<>("t", 0, 0, "s", "{not json")))
                .isInstanceOf(JsonProcessingException.class);
    }

    @Test
    void onlyDemoTenantEventsAreBroadcastPublicly() {
        SimpMessagingTemplate template = mock(SimpMessagingTemplate.class);
        SagaEventPublisher publisher = new SagaEventPublisher(template, TestFixtures.properties());

        publisher.publishAll(List.of(event("chaos-demo"), event("private-tenant")));

        verify(template).convertAndSend(eq(SagaEventPublisher.PUBLIC_TOPIC), any(SagaLiveEvent.class));
        verify(template, never()).convertAndSend(anyString(), eq(event("private-tenant")));
    }

    @Test
    void publishingFailuresNeverPropagate() {
        SimpMessagingTemplate template = mock(SimpMessagingTemplate.class);
        doThrow(new IllegalStateException("closed")).when(template).convertAndSend(anyString(), any(Object.class));
        SagaEventPublisher publisher = new SagaEventPublisher(template, TestFixtures.properties());
        publisher.publish(event("chaos-demo"));
        publisher.publishMetrics("x");
    }

    private static SagaLiveEvent event(String tenant) {
        return new SagaLiveEvent("s", tenant, "wf", "STEP_COMPLETED", SagaStatus.IN_PROGRESS, 0, "a", List.of("a"),
                "m", Instant.EPOCH);
    }
}
