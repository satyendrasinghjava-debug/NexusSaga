/**
 * =============================================================================
 * © 2026 Satyendra Singh. All Rights Reserved.
 * =============================================================================
 * PROJECT: NexusSaga - Enterprise Generic Saga Orchestration Engine
 * AUTHOR: Satyendra Singh (Senior Java Backend Developer)
 * =============================================================================
 */
package com.nexussaga.engine;

import com.nexussaga.domain.OutboxEvent;
import com.nexussaga.repository.OutboxEventRepository;
import com.nexussaga.support.MutableClock;
import com.nexussaga.support.TestFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxRelaySchedulerTest {

    private final MutableClock clock = new MutableClock(Instant.parse("2026-06-01T00:00:00Z"));
    private OutboxEventRepository repository;
    private KafkaTemplate<String, String> kafka;
    private OutboxRelayScheduler relay;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        repository = mock(OutboxEventRepository.class);
        kafka = mock(KafkaTemplate.class);
        relay = new OutboxRelayScheduler(repository, kafka,
                new TransactionTemplate(mock(PlatformTransactionManager.class)), TestFixtures.properties(), clock);
    }

    @Test
    void publishesBatchKeyedBySagaIdAndMarksRows() {
        OutboxEvent a = event("saga-a", "t1");
        OutboxEvent b = event("saga-b", "t2");
        when(repository.lockPendingBatch(anyInt())).thenReturn(List.of(a, b));
        when(kafka.send(any(String.class), any(String.class), any(String.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        assertThat(relay.relayBatch()).isEqualTo(2);

        verify(kafka).send("t1", "saga-a", "{}");
        verify(kafka).send("t2", "saga-b", "{}");
        assertThat(a.getStatus()).isEqualTo(OutboxEvent.Status.PUBLISHED);
        assertThat(a.getPublishedAt()).isEqualTo(clock.instant());
        verify(repository).saveAll(List.of(a, b));
    }

    @Test
    void failedSendKeepsRowPendingUntilMaxAttempts() {
        OutboxEvent event = event("saga-a", "t1");
        when(repository.lockPendingBatch(anyInt())).thenReturn(List.of(event));
        when(kafka.send(any(String.class), any(String.class), any(String.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        assertThat(relay.relayBatch()).isZero();
        assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.PENDING);
        assertThat(event.getAttempts()).isEqualTo(1);
        assertThat(event.getLastError()).contains("broker down");

        for (int i = 1; i < 10; i++) {
            relay.relayBatch();
        }
        assertThat(event.getStatus()).isEqualTo(OutboxEvent.Status.FAILED);
    }

    @Test
    void interruptedWaitCountsAsFailedAttempt() {
        OutboxEvent event = event("saga-a", "t1");
        when(repository.lockPendingBatch(anyInt())).thenReturn(List.of(event));
        when(kafka.send(any(String.class), any(String.class), any(String.class))).thenReturn(new CompletableFuture<>());
        Thread.currentThread().interrupt();
        assertThat(relay.relayBatch()).isZero();
        assertThat(Thread.interrupted()).isTrue();
        assertThat(event.getLastError()).isEqualTo("interrupted");
    }

    @Test
    void emptyBatchDoesNothingAndScheduledLoopStopsOnPartialBatch() {
        when(repository.lockPendingBatch(anyInt())).thenReturn(Collections.emptyList());
        relay.relayScheduled();
        verify(repository, times(1)).lockPendingBatch(100);
        verify(repository, never()).saveAll(any());
    }

    @Test
    void scheduledLoopSurvivesInfrastructureErrors() {
        when(repository.lockPendingBatch(anyInt())).thenThrow(new IllegalStateException("db down"));
        relay.relayScheduled();
        verify(repository).lockPendingBatch(100);
    }

    @Test
    void purgesOldPublishedRows() {
        when(repository.deletePublishedBefore(any())).thenReturn(3, 0);
        relay.purgePublished();
        relay.purgePublished();
        verify(repository, times(2)).deletePublishedBefore(eq(clock.instant().minus(java.time.Duration.ofDays(7))));
    }

    @Test
    void outboxEventTruncatesLongErrors() {
        OutboxEvent event = event("s", "t");
        event.markAttemptFailed("x".repeat(2000), 10);
        assertThat(event.getLastError()).hasSize(1024);
        event.markAttemptFailed(null, 10);
        assertThat(event.getLastError()).isEqualTo("unknown");
        assertThat(event.getAggregateId()).isEqualTo("s");
        assertThat(event.getEventType()).isEqualTo("STEP_FORWARD_COMMAND");
        assertThat(event.getCreatedAt()).isNotNull();
        assertThat(event.getId()).isNull();
    }

    private OutboxEvent event(String sagaId, String topic) {
        return new OutboxEvent(sagaId, topic, sagaId, "STEP_FORWARD_COMMAND", "{}", clock.instant());
    }
}
